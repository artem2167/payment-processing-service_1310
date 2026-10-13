"""Verify recovery of the local async-payments Compose project without deleting data."""

import argparse
import asyncio
import subprocess
from pathlib import Path
from uuid import uuid4

import httpx

from app.config import Settings
from scripts.smoke import wait_for_delivery

WORKSPACE = Path(__file__).resolve().parent.parent
COMPOSE_FILE = WORKSPACE / "compose.yaml"
PROJECT = "async-payments"


def run_compose(*arguments: str) -> None:
    command = [
        "docker",
        "compose",
        "--project-name",
        PROJECT,
        "--file",
        str(COMPOSE_FILE),
        *arguments,
    ]
    result = subprocess.run(
        command, cwd=WORKSPACE, capture_output=True, text=True, timeout=150, check=False
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Compose {' '.join(arguments)} failed: {detail}")


async def compose(*arguments: str) -> None:
    task = asyncio.create_task(asyncio.to_thread(run_compose, *arguments))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        # Finish an in-flight stop before finally starts the affected service.
        await task
        raise


async def wait_for_api(client: httpx.AsyncClient, max_wait_seconds: float = 60) -> None:
    async with asyncio.timeout(max_wait_seconds):
        while True:
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    return
                if response.status_code == 401:
                    response.raise_for_status()
            except httpx.RequestError:
                pass
            await asyncio.sleep(0.5)


async def create_payment(
    client: httpx.AsyncClient, webhook_target: str, phase: str, *, require_pending: bool = True
) -> str:
    key = f"resilience-{phase}-{uuid4()}"
    payload = {
        "amount": "100.00",
        "currency": "RUB",
        "description": f"Recovery check: {phase}",
        "metadata": {"resilience_phase": phase},
        "webhook_url": f"{webhook_target.rstrip('/')}/webhook",
    }
    response = await client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": key})
    assert response.status_code == 202, response.text
    accepted = response.json()
    assert accepted["status"] == "pending", accepted
    payment_id = accepted["payment_id"]
    repeated = await client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": key})
    assert repeated.status_code == 202, repeated.text
    assert repeated.json()["payment_id"] == payment_id
    response = await client.get(f"/api/v1/payments/{payment_id}")
    response.raise_for_status()
    payment = response.json()
    if require_pending:
        assert payment["status"] == "pending", payment
        assert payment["processed_at"] is None
        assert payment["delivery"]["delivered_at"] is None
    print(f"{phase}: API 202, stable idempotent payment_id={payment_id}", flush=True)
    return payment_id


async def check(args: argparse.Namespace) -> None:
    settings = Settings()
    stopped: set[str] = set()
    snapshots: list[dict] = []
    async with httpx.AsyncClient(
        base_url=args.api, headers={"X-API-Key": settings.api_key}, timeout=10
    ) as client:
        await wait_for_api(client)
        try:
            print("Stopping RabbitMQ in the async-payments project", flush=True)
            stopped.add("rabbitmq")
            await compose("stop", "rabbitmq")
            payment_id = await create_payment(client, args.webhook_target, "broker-offline")
            await compose("up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
            stopped.discard("rabbitmq")
            payment = await wait_for_delivery(client, payment_id, False, args.timeout)
            snapshots.append(payment)
            print(f"Broker recovery: delivered payment_id={payment_id}", flush=True)

            print("Stopping consumer in the async-payments project", flush=True)
            stopped.add("consumer")
            await compose("stop", "consumer")
            payment_id = await create_payment(client, args.webhook_target, "consumer-offline")
            await compose("start", "consumer")
            stopped.discard("consumer")
            payment = await wait_for_delivery(client, payment_id, False, args.timeout)
            snapshots.append(payment)
            print(f"Consumer recovery: delivered payment_id={payment_id}", flush=True)

            print(
                "Restarting PostgreSQL; API and consumer will recover without restart", flush=True
            )
            stopped.add("postgres")
            await compose("restart", "postgres")
            await compose("up", "-d", "--wait", "--wait-timeout", "120", "postgres")
            stopped.discard("postgres")
            await wait_for_api(client)
            payment_id = await create_payment(
                client, args.webhook_target, "postgres-recovery", require_pending=False
            )
            payment = await wait_for_delivery(client, payment_id, False, args.timeout)
            snapshots.append(payment)
            print(
                f"PostgreSQL recovery: new payment delivered without restarting API/consumer, "
                f"payment_id={payment_id}",
                flush=True,
            )

            print("Separately restarting API and consumer to check persisted results", flush=True)
            stopped.update({"api", "consumer"})
            await compose("restart", "api", "consumer")
            await wait_for_api(client)
            stopped.difference_update({"api", "consumer"})
            for before in snapshots:
                response = await client.get(f"/api/v1/payments/{before['payment_id']}")
                response.raise_for_status()
                after = response.json()
                assert after["payment_id"] == before["payment_id"]
                assert after["status"] == before["status"]
                assert after["processed_at"] == before["processed_at"]
                assert after["delivery"] == before["delivery"]
                assert after["delivery"]["delivered_at"] is not None
            print("Restart recovery: IDs, results and delivery timestamps persisted", flush=True)
        finally:
            # Order matters if an interrupted restart also affected PostgreSQL.
            restoration_errors = []
            for service in ("postgres", "rabbitmq", "api", "consumer"):
                if service in stopped:
                    try:
                        await compose("start", service)
                        print(f"Restored service: {service}", flush=True)
                    except Exception as error:
                        restoration_errors.append((service, error))
            if restoration_errors:
                services = ", ".join(service for service, _ in restoration_errors)
                raise RuntimeError(f"Could not restore Compose services: {services}") from (
                    restoration_errors[0][1]
                )
    print("Compose resilience checks passed", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--webhook-target", default="http://webhook-receiver:8080")
    parser.add_argument("--timeout", type=float, default=120)
    asyncio.run(check(parser.parse_args()))


if __name__ == "__main__":
    main()
