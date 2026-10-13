import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.messaging.outbox import PublicationNotConfirmed
from app.worker import quarantine


@pytest.mark.asyncio
async def test_invalid_message_is_acknowledged_only_after_confirmed_durable_quarantine():
    body = b"invalid" * 1000
    message = SimpleNamespace(
        body=body, raw_message=SimpleNamespace(message_id="broken-42"), ack=AsyncMock()
    )
    broker = AsyncMock()

    async def confirm(*args, **kwargs):
        message.ack.assert_not_awaited()
        return True

    broker.publish.side_effect = confirm
    await quarantine(broker, message, "invalid_message_schema")
    message.ack.assert_awaited_once()
    args, kwargs = broker.publish.await_args
    payload = args[0]
    assert payload["original_message_id"] == "broken-42"
    assert payload["body_truncated"] is True
    assert base64.b64decode(payload["original_body_base64"]) == body[:4096]
    assert kwargs["routing_key"] == "payments.dlq"
    assert kwargs["persist"] is kwargs["mandatory"] is True


@pytest.mark.asyncio
async def test_quarantine_failure_keeps_original_message_unacknowledged():
    message = SimpleNamespace(
        body=b"broken", raw_message=SimpleNamespace(message_id=None), ack=AsyncMock()
    )
    broker = AsyncMock()
    broker.publish.return_value = None
    with pytest.raises(PublicationNotConfirmed):
        await quarantine(broker, message, "invalid_message_schema")
    message.ack.assert_not_awaited()
