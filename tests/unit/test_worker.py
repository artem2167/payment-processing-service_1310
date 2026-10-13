import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from faststream import AckPolicy
from sqlalchemy.exc import OperationalError

from app import worker
from app.events import WorkMessage


class ControlledBroker:
    def __init__(self, message, stop, *, stop_after_message, confirmation=True):
        self.message = message
        self.stop_event = stop
        self.stop_after_message = stop_after_message
        self.publish = AsyncMock(return_value=confirmation)
        self.connect = AsyncMock()
        self.stop = AsyncMock()
        self.subscriptions = []

    def subscriber(self, *args, **kwargs):
        self.subscriptions.append((args, kwargs))

        def register(handler):
            self.handler = handler
            return handler

        return register

    async def start(self):
        await self.handler(self.message)
        if self.stop_after_message:
            self.stop_event.set()


async def idle_relay():
    await asyncio.Event().wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "database", "invalid_schema", "quarantine_failure"])
async def test_worker_acks_only_committed_or_quarantined_work_and_recovers_unacked(
    settings, monkeypatch, failure
):
    event = WorkMessage(schema_version=1, event_id=uuid4(), payment_id=uuid4(), attempt_no=1)
    body = (
        b"broken JSON"
        if failure in {"invalid_schema", "quarantine_failure"}
        else event.model_dump_json().encode()
    )
    journal = []

    async def ack():
        journal.append("ack")

    message = SimpleNamespace(
        body=body,
        raw_message=SimpleNamespace(message_id=str(event.event_id)),
        ack=AsyncMock(side_effect=ack),
    )
    stop = asyncio.Event()
    broker = ControlledBroker(
        message,
        stop,
        stop_after_message=failure in {None, "invalid_schema"},
        confirmation=None if failure == "quarantine_failure" else True,
    )
    processor = SimpleNamespace(process=AsyncMock())

    async def process(event):
        if failure == "database":
            raise OperationalError("SELECT payments", {}, RuntimeError("offline"))
        journal.append("committed")

    processor.process.side_effect = process
    processor_factory = Mock(return_value=processor)
    relay = SimpleNamespace(run=idle_relay)
    monkeypatch.setattr(worker, "create_broker", lambda settings: broker)
    monkeypatch.setattr(worker, "declare_topology", AsyncMock())
    monkeypatch.setattr(worker, "PaymentProcessor", processor_factory)
    monkeypatch.setattr(worker, "OutboxRelay", lambda *args: relay)

    await asyncio.wait_for(worker.run_connected(settings, object(), object(), stop), 1)
    assert len(broker.subscriptions) == 1
    assert broker.subscriptions[0][1]["ack_policy"] == AckPolicy.MANUAL
    broker.stop.assert_awaited_once()
    if failure is None:
        assert journal == ["committed", "ack"]
        processor.process.assert_awaited_once_with(event)
        broker.publish.assert_not_awaited()
    elif failure == "database":
        assert journal == []
        message.ack.assert_not_awaited()
        broker.publish.assert_not_awaited()
        assert not stop.is_set()  # Recovery ended the session independently of shutdown.
    elif failure == "invalid_schema":
        processor.process.assert_not_awaited()
        broker.publish.assert_awaited_once()
        message.ack.assert_awaited_once()
    else:
        processor.process.assert_not_awaited()
        message.ack.assert_not_awaited()
        assert not stop.is_set()
