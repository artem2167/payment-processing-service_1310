import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError

from app import worker
from app.events import WorkMessage
from app.messaging import outbox, topology
from app.messaging.consumer import PaymentProcessor
from app.messaging.outbox import OutboxRelay, RabbitPublisher
from app.models import Outbox, Payment
from app.services.webhooks import WebhookSender
from app.worker import create_broker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def rabbit(settings, monkeypatch):
    url = os.environ.get("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL to an explicitly designated RabbitMQ test broker")
    token = uuid4().hex
    # Unique topology avoids deleting or draining application queues on a shared broker.
    exchange_spec = RabbitExchange(
        f"payments.events.test.{token}", type=ExchangeType.DIRECT, durable=True
    )
    dlx_spec = RabbitExchange(f"payments.dlx.test.{token}", type=ExchangeType.DIRECT, durable=True)
    queue_spec = RabbitQueue(
        f"payments.new.test.{token}", durable=True, arguments={"x-single-active-consumer": True}
    )
    dlq_spec = RabbitQueue(f"payments.dlq.test.{token}", durable=True)
    monkeypatch.setattr(topology, "PAYMENTS_EXCHANGE", exchange_spec)
    monkeypatch.setattr(topology, "DLX_EXCHANGE", dlx_spec)
    monkeypatch.setattr(topology, "PAYMENTS_QUEUE", queue_spec)
    monkeypatch.setattr(topology, "DLQ_QUEUE", dlq_spec)
    monkeypatch.setattr(outbox, "PAYMENTS_EXCHANGE", exchange_spec)
    monkeypatch.setattr(outbox, "DLX_EXCHANGE", dlx_spec)
    monkeypatch.setattr(worker, "PAYMENTS_EXCHANGE", exchange_spec)
    monkeypatch.setattr(worker, "DLX_EXCHANGE", dlx_spec)
    monkeypatch.setattr(worker, "PAYMENTS_QUEUE", queue_spec)
    broker_settings = settings.model_copy(update={"rabbitmq_url": url})
    broker = create_broker(broker_settings)
    owned_queues = []
    owned_exchanges = []
    await broker.connect()
    try:
        await topology.declare_topology(broker)
        queue = await broker.declare_queue(queue_spec)
        owned_queues.append(queue)
        dlq = await broker.declare_queue(dlq_spec)
        owned_queues.append(dlq)
        exchange = await broker.declare_exchange(exchange_spec)
        owned_exchanges.append(exchange)
        dlx = await broker.declare_exchange(dlx_spec)
        owned_exchanges.append(dlx)
        yield broker, queue, dlq, exchange
    finally:
        for owned_queue in owned_queues:
            await owned_queue.delete(if_unused=False, if_empty=False)
        for owned_exchange in owned_exchanges:
            await owned_exchange.delete(if_unused=False)
        await broker.stop()


async def create_event(client, database, payment_body):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "broker"}
    )
    assert response.status_code == 202
    async with database.sessions() as session:
        return await session.scalar(select(Outbox))


async def test_real_confirmed_publication_and_consumer_result(
    client, database, settings, payment_body, rabbit
):
    broker, queue, _, _ = rabbit
    event = await create_event(client, database, payment_body)
    relay = OutboxRelay(database, settings, RabbitPublisher(broker, timeout=3))
    assert await relay.publish_once() == 1
    incoming = await queue.get(timeout=3)
    assert incoming.message_id == str(event.id)
    assert incoming.delivery_mode == 2
    work = WorkMessage.model_validate_json(incoming.body)
    requests = []

    def receive(request):
        requests.append(json.loads(request.content))
        return httpx.Response(204)

    gateway = AsyncMock(return_value="succeeded")
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        processor = PaymentProcessor(database, settings, gateway, WebhookSender(http, timeout=1))
        await processor.process(work)
        await incoming.ack()
        await processor.process(work)
    async with database.sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        stored = await session.get(Outbox, event.id)
        assert stored.published_at is not None
        assert payment.status == "succeeded"
        assert payment.webhook_delivered_at is not None
        assert requests[0]["event_id"] == str(payment.webhook_event_id)
    assert len(requests) == 1
    gateway.assert_awaited_once()
    assert await queue.get(fail=False, timeout=1) is None


async def test_real_missing_binding_is_not_marked_published_and_recovers(
    client, database, settings, payment_body, rabbit
):
    broker, queue, _, exchange = rabbit
    event = await create_event(client, database, payment_body)
    await queue.unbind(exchange, routing_key="payments.new")
    relay = OutboxRelay(database, settings, RabbitPublisher(broker, timeout=3))
    assert await relay.publish_once() == 0
    async with database.sessions() as session:
        pending = await session.get(Outbox, event.id)
        assert pending.published_at is None
        assert pending.last_error
        assert pending.publish_attempts == 1
    await queue.bind(exchange, routing_key="payments.new")
    async with database.sessions() as session, session.begin():
        await session.execute(
            update(Outbox)
            .where(Outbox.id == event.id)
            .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    assert await relay.publish_once() == 1
    incoming = await queue.get(timeout=3)
    assert incoming.message_id == str(event.id)
    await incoming.ack()


async def test_three_real_deliveries_publish_one_dead_letter(
    client, database, settings, payment_body, rabbit
):
    broker, queue, dlq, _ = rabbit
    event = await create_event(client, database, payment_body)
    relay = OutboxRelay(database, settings, RabbitPublisher(broker, timeout=3))
    gateway = AsyncMock(return_value="succeeded")
    calls = []

    def receive(request):
        calls.append(json.loads(request.content))
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        for attempt in (1, 2, 3):
            async with database.sessions() as session, session.begin():
                # Scheduling delays are checked separately; make each saved event due here.
                await session.execute(
                    update(Outbox)
                    .where(Outbox.payment_id == event.payment_id, Outbox.published_at.is_(None))
                    .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
                )
            assert await relay.publish_once() == 1
            incoming = await queue.get(timeout=3)
            work = WorkMessage.model_validate_json(incoming.body)
            assert work.attempt_no == attempt
            await PaymentProcessor(database, settings, gateway, WebhookSender(http, 1)).process(
                work
            )
            await incoming.ack()
    assert await relay.publish_once() == 1
    dead = await dlq.get(timeout=3)
    dead_payload = json.loads(dead.body)
    assert dead_payload["payment_id"] == str(event.payment_id)
    assert dead_payload["attempt_no"] == 3
    await dead.ack()
    assert len(calls) == 3
    assert len({call["event_id"] for call in calls}) == 1
    gateway.assert_awaited_once()
    async with database.sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "succeeded"
        assert payment.exhausted_at is not None
        assert payment.webhook_delivered_at is None
    assert await relay.publish_once() == 0
    assert await queue.get(fail=False, timeout=1) is None
    assert await dlq.get(fail=False, timeout=1) is None


async def test_real_worker_requeues_unacked_database_failure_then_recovers(
    client, database, settings, payment_body, rabbit, monkeypatch
):
    from types import SimpleNamespace

    _, queue, _, _ = rabbit
    event = await create_event(client, database, payment_body)
    broker_settings = settings.model_copy(
        update={
            "rabbitmq_url": os.environ["TEST_RABBITMQ_URL"],
            "outbox_poll_interval": 0.01,
        }
    )
    original_processor = worker.PaymentProcessor
    failing_processor = SimpleNamespace(
        process=AsyncMock(
            side_effect=OperationalError("SELECT payments", {}, RuntimeError("database offline"))
        )
    )
    monkeypatch.setattr(worker, "PaymentProcessor", lambda *args: failing_processor)
    requests = []

    def receive(request):
        requests.append(json.loads(request.content))
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        # Recovery closes the real consumer channel, allowing RabbitMQ to redeliver.
        await asyncio.wait_for(
            worker.run_connected(broker_settings, database, http, asyncio.Event()), 5
        )
        failing_processor.process.assert_awaited_once()
        redelivery = await queue.get(timeout=3)
        assert redelivery.redelivered is True
        assert redelivery.message_id == str(event.id)
        assert WorkMessage.model_validate_json(redelivery.body).attempt_no == 1
        await redelivery.nack(requeue=True)
        async with database.sessions() as session:
            payment = await session.get(Payment, event.payment_id)
            assert payment.status == "pending"
            assert payment.current_attempt == 1
        assert requests == []

        monkeypatch.setattr(worker, "PaymentProcessor", original_processor)
        gateway = AsyncMock(return_value="succeeded")
        monkeypatch.setattr(worker, "EmulatedGateway", lambda settings: gateway)
        stop = asyncio.Event()
        running = asyncio.create_task(worker.run_connected(broker_settings, database, http, stop))
        try:
            async with asyncio.timeout(5):
                while True:
                    async with database.sessions() as session:
                        payment = await session.get(Payment, event.payment_id)
                        if payment.webhook_delivered_at is not None:
                            break
                    if running.done():
                        await running
                        pytest.fail("Worker stopped before completing the requeued payment")
                    await asyncio.sleep(0.01)
        finally:
            stop.set()
            await asyncio.wait_for(running, 5)
    assert payment.status == "succeeded"
    assert len(requests) == 1
    gateway.assert_awaited_once()
    assert await queue.get(fail=False, timeout=1) is None
