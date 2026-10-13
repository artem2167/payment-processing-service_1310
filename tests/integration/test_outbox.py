import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select, update
from sqlalchemy.orm import Session

from app.messaging.outbox import OutboxRelay
from app.models import Outbox, Payment

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def create_outbox(client, database, payment_body):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "outbox"}
    )
    assert response.status_code == 202
    async with database.sessions() as session:
        return await session.scalar(select(Outbox))


async def read_outbox(database, identity):
    async with database.sessions() as session:
        return await session.get(Outbox, identity)


async def test_failed_confirm_or_unroutable_message_stays_in_outbox_then_recovers(
    client, database, settings, payment_body
):
    row = await create_outbox(client, database, payment_body)
    publisher = AsyncMock(side_effect=RuntimeError("unroutable message"))
    relay = OutboxRelay(database, settings, publisher)
    assert await relay.publish_once() == 0
    failed = await read_outbox(database, row.id)
    assert failed.published_at is None
    assert failed.publish_attempts == 1
    assert failed.last_error == "RuntimeError"
    assert failed.available_at > row.available_at

    async with database.sessions() as session, session.begin():
        await session.execute(
            update(Outbox)
            .where(Outbox.id == row.id)
            .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    publisher.side_effect = None
    assert await relay.publish_once() == 1
    recovered = await read_outbox(database, row.id)
    assert recovered.published_at is not None
    assert recovered.publish_attempts == 2
    assert recovered.last_error is None
    assert await relay.publish_once() == 0
    assert publisher.await_count == 2
    async with database.sessions() as session:
        payment = await session.get(Payment, row.payment_id)
        assert payment.current_attempt == 1  # Broker reconnects do not spend logical attempts.


async def test_published_timestamp_is_written_only_after_publisher_confirmation(
    client, database, settings, payment_body
):
    row = await create_outbox(client, database, payment_body)
    publishing = asyncio.Event()
    confirmation = asyncio.Event()

    async def publisher(event):
        publishing.set()
        await confirmation.wait()

    relay = OutboxRelay(database, settings, publisher)
    task = asyncio.create_task(relay.publish_once())
    try:
        await asyncio.wait_for(publishing.wait(), timeout=3)
        before_confirm = await read_outbox(database, row.id)
        assert before_confirm.published_at is None
    finally:
        confirmation.set()
        await task
    assert (await read_outbox(database, row.id)).published_at is not None


async def test_parallel_relays_do_not_publish_the_same_locked_row(
    client, database, settings, payment_body
):
    await create_outbox(client, database, payment_body)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def publisher(row):
        calls.append(row.id)
        started.set()
        await release.wait()

    first = asyncio.create_task(OutboxRelay(database, settings, publisher).publish_once())
    try:
        await asyncio.wait_for(started.wait(), 3)
        assert await OutboxRelay(database, settings, publisher).publish_once() == 0
    finally:
        release.set()
        assert await first == 1
    assert len(calls) == 1


async def test_future_retry_event_is_not_published_early(client, database, settings, payment_body):
    row = await create_outbox(client, database, payment_body)
    async with database.sessions() as session, session.begin():
        await session.execute(
            update(Outbox)
            .where(Outbox.id == row.id)
            .values(available_at=datetime.now(UTC) + timedelta(minutes=1))
        )
    publisher = AsyncMock()
    assert await OutboxRelay(database, settings, publisher).publish_once() == 0
    publisher.assert_not_awaited()


async def test_failure_after_confirm_before_commit_keeps_event_for_redelivery(
    client, database, settings, payment_body
):
    row = await create_outbox(client, database, payment_body)
    confirmed_ids = []

    async def confirm(row):
        confirmed_ids.append(row.id)

    publisher = AsyncMock(side_effect=confirm)
    relay = OutboxRelay(database, settings, publisher)

    def interrupt_commit(session):
        if any(
            isinstance(item, Outbox) and item.published_at is not None
            for item in session.identity_map.values()
        ):
            raise RuntimeError("crash after publisher confirm")

    event.listen(Session, "before_commit", interrupt_commit)
    try:
        with pytest.raises(RuntimeError, match="after publisher confirm"):
            await relay.publish_once()
    finally:
        event.remove(Session, "before_commit", interrupt_commit)
    assert (await read_outbox(database, row.id)).published_at is None
    assert await relay.publish_once() == 1
    assert publisher.await_count == 2
    assert confirmed_ids == [row.id, row.id]
    assert (await read_outbox(database, row.id)).published_at is not None
