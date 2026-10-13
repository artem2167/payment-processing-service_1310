"""Check a running Compose demonstration using only newly created payments."""

import argparse
import asyncio
from datetime import datetime
from uuid import uuid4

import httpx

from app.config import Settings


async def wait_for_delivery(
    client: httpx.AsyncClient, payment_id: str, exhausted: bool, max_wait_seconds: float
) -> dict:
    expected = "exhausted_at" if exhausted else "delivered_at"
    try:
        async with asyncio.timeout(max_wait_seconds):
            while True:
                response = await client.get(f"/api/v1/payments/{payment_id}")
                response.raise_for_status()
                payment = response.json()
                if payment["delivery"][expected] is not None:
                    return payment
                await asyncio.sleep(0.5)
    except TimeoutError as error:
        raise TimeoutError(
            f"Payment {payment_id} did not reach {expected} in {max_wait_seconds}s"
        ) from error


async def check(args: argparse.Namespace) -> None:
    settings = Settings()
    run_id = str(uuid4())
    headers = {"X-API-Key": settings.api_key}
    async with (
        httpx.AsyncClient(base_url=args.api, headers=headers, timeout=15) as client,
        httpx.AsyncClient(base_url=args.receiver, timeout=15) as receiver,
    ):
        unauthorized = await client.get("/health", headers={"X-API-Key": ""})
        assert unauthorized.status_code == 401, unauthorized.status_code
        (await client.get("/health")).raise_for_status()
        (await receiver.get("/health")).raise_for_status()
        payload = {
            "amount": "1500.00",
            "currency": "RUB",
            "description": "Smoke check",
            "metadata": {"smoke_run_id": run_id},
            "webhook_url": f"{args.webhook_target}/webhook",
        }
        key = f"smoke-{run_id}-success"
        created = await client.post(
            "/api/v1/payments", json=payload, headers={"Idempotency-Key": key}
        )
        assert created.status_code == 202, created.text
        success_id = created.json()["payment_id"]
        duplicates = await asyncio.gather(
            *(
                client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": key})
                for _ in range(20)
            )
        )
        assert all(response.status_code == 202 for response in duplicates)
        assert all(response.json()["payment_id"] == success_id for response in duplicates)
        conflict = await client.post(
            "/api/v1/payments",
            json={**payload, "amount": "1501.00"},
            headers={"Idempotency-Key": key},
        )
        assert conflict.status_code == 409, conflict.text
        print("API auth, 20 concurrent duplicates and idempotency conflict: OK", flush=True)
        scenarios = [(success_id, "/webhook", False, 1)]
        for route, exhausted in [("/flaky", False), ("/fail", True)]:
            response = await client.post(
                "/api/v1/payments",
                json={**payload, "webhook_url": f"{args.webhook_target}{route}"},
                headers={"Idempotency-Key": f"smoke-{run_id}{route}"},
            )
            assert response.status_code == 202, response.text
            scenarios.append((response.json()["payment_id"], route, exhausted, 3))
        final_payments = await asyncio.gather(
            *(
                wait_for_delivery(client, payment_id, exhausted, args.timeout)
                for payment_id, _, exhausted, _ in scenarios
            )
        )
        recorded = await receiver.get("/events")
        recorded.raise_for_status()
        events = recorded.json()["events"]
        for (payment_id, route, exhausted, count), payment in zip(
            scenarios, final_payments, strict=True
        ):
            received = [
                event
                for event in events
                if event["payload"].get("payment_id") == payment_id and event["path"] == route
            ]
            assert len(received) == count, (payment_id, len(received), count)
            assert len({event["event_id"] for event in received}) == 1
            assert payment["status"] in {"succeeded", "failed"}
            assert payment["processed_at"] is not None
            assert all(event["payload"]["status"] == payment["status"] for event in received)
            if count == 3:
                timestamps = [datetime.fromisoformat(event["received_at"]) for event in received]
                for index, minimum in enumerate(
                    [settings.retry_base_delay, settings.retry_base_delay * 2]
                ):
                    elapsed = (timestamps[index + 1] - timestamps[index]).total_seconds()
                    assert elapsed >= minimum, (route, elapsed, minimum)
            outcome = "exhausted" if exhausted else "delivered"
            print(f"{route}: {outcome}, {count} HTTP requests, payment_id={payment_id}", flush=True)
        print("Compose demonstration checks passed", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--receiver", default="http://localhost:8080")
    parser.add_argument("--webhook-target", default="http://webhook-receiver:8080")
    parser.add_argument("--timeout", type=float, default=120)
    asyncio.run(check(parser.parse_args()))


if __name__ == "__main__":
    main()
