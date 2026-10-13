import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta

from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.db import Database
from app.messaging.topology import DLX_EXCHANGE, PAYMENTS_EXCHANGE
from app.models import Outbox, utc_now

logger = logging.getLogger(__name__)


class PublicationNotConfirmed(Exception):
    pass


class RabbitPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self.broker = broker
        self.timeout = timeout

    async def __call__(self, event: Outbox) -> None:
        exchange = DLX_EXCHANGE if event.routing_key == "payments.dlq" else PAYMENTS_EXCHANGE
        confirmation = await self.broker.publish(
            event.payload,
            exchange=exchange,
            routing_key=event.routing_key,
            message_id=str(event.id),
            message_type=event.event_type,
            persist=True,
            mandatory=True,
            timeout=self.timeout,
        )
        if confirmation is not True and not isinstance(confirmation, Basic.Ack):
            raise PublicationNotConfirmed("Broker did not confirm publication")


class OutboxRelay:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        publisher: Callable[[Outbox], Awaitable[None]],
    ) -> None:
        self.database = database
        self.settings = settings
        self.publisher = publisher

    async def publish_once(self) -> int:
        async with self.database.sessions() as session, session.begin():
            event = await session.scalar(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= utc_now())
                .order_by(Outbox.available_at, Outbox.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if event is None:
                return 0
            event.publish_attempts += 1
            try:
                async with asyncio.timeout(self.settings.publish_timeout):
                    await self.publisher(event)
            except Exception as error:
                # Keep the durable event; publication failures have no attempt limit.
                event.last_error = type(error).__name__
                event.available_at = utc_now() + timedelta(seconds=self.settings.recovery_delay)
                logger.warning(
                    "outbox_publish_failed message_id=%s reason=%s", event.id, event.last_error
                )
                return 0
            event.published_at = utc_now()
            event.last_error = None
            logger.info(
                "outbox_published message_id=%s routing_key=%s", event.id, event.routing_key
            )
            return 1

    async def run(self) -> None:
        while True:
            try:
                published = await self.publish_once()
            except SQLAlchemyError as error:
                logger.warning("outbox_database_unavailable reason=%s", type(error).__name__)
                await asyncio.sleep(self.settings.recovery_delay)
                continue
            if not published:
                await asyncio.sleep(self.settings.outbox_poll_interval)
