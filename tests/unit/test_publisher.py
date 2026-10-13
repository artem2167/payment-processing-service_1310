from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pamqp.commands import Basic

from app.messaging.outbox import PublicationNotConfirmed, RabbitPublisher
from app.messaging.topology import DLX_EXCHANGE, PAYMENTS_EXCHANGE
from app.models import Outbox


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", [None, False, Basic.Nack(delivery_tag=1)])
async def test_missing_or_negative_confirmation_is_not_success(confirmation):
    broker = AsyncMock()
    broker.publish.return_value = confirmation
    row = Outbox(id=uuid4(), routing_key="payments.new", event_type="payment.process", payload={})
    with pytest.raises(PublicationNotConfirmed):
        await RabbitPublisher(broker, timeout=1)(row)


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", [True, Basic.Ack(delivery_tag=1)])
@pytest.mark.parametrize(
    "routing_key,exchange", [("payments.new", PAYMENTS_EXCHANGE), ("payments.dlq", DLX_EXCHANGE)]
)
async def test_confirmed_publication_is_persistent_mandatory_and_identified(
    confirmation, routing_key, exchange
):
    broker = AsyncMock()
    broker.publish.return_value = confirmation
    row = Outbox(
        id=uuid4(),
        routing_key=routing_key,
        event_type="payment.process",
        payload={"schema_version": 1},
    )
    await RabbitPublisher(broker, timeout=2)(row)
    broker.publish.assert_awaited_once_with(
        row.payload,
        exchange=exchange,
        routing_key=routing_key,
        message_id=str(row.id),
        message_type=row.event_type,
        persist=True,
        mandatory=True,
        timeout=2,
    )
