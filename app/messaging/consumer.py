import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta

from sqlalchemy import select

from app.config import Settings
from app.db import Database
from app.events import WorkMessage, make_dead_letter_event, make_work_event
from app.models import Outbox, Payment, utc_now
from app.services.webhooks import WebhookDeliveryError

logger = logging.getLogger(__name__)


class InvalidWorkMessage(Exception):
    pass


class PaymentProcessor:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        gateway: Callable[[], Awaitable[str]],
        webhook_sender: Callable[[Payment], Awaitable[None]],
    ) -> None:
        self.database = database
        self.settings = settings
        self.gateway = gateway
        self.webhook_sender = webhook_sender
        self._lock = asyncio.Lock()

    async def process(self, event: WorkMessage) -> None:
        async with self._lock:
            await self._process(event)

    async def _process(self, event: WorkMessage) -> None:
        async with self.database.sessions() as session:
            payment = await session.get(Payment, event.payment_id)
            if payment is None:
                raise InvalidWorkMessage("payment_not_found")
            if self._finished(payment, event):
                return
            if event.attempt_no != payment.current_attempt:
                raise InvalidWorkMessage("unexpected_attempt")
            source = await session.get(Outbox, event.event_id)
            if (
                source is None
                or source.payment_id != payment.id
                or source.attempt_no != event.attempt_no
                or source.event_type != "payment.process"
            ):
                raise InvalidWorkMessage("unknown_event")

        if payment.status == "pending":
            try:
                result = await self.gateway()
                if result not in {"succeeded", "failed"}:
                    raise ValueError("Invalid gateway result")
            except Exception as error:
                await self._record_failure(event, type(error).__name__, "gateway")
                return
            async with self.database.sessions() as session, session.begin():
                payment = await session.scalar(
                    select(Payment).where(Payment.id == event.payment_id).with_for_update()
                )
                if self._finished(payment, event):
                    return
                if payment.status == "pending":
                    payment.status = result
                    payment.processed_at = utc_now()
                logger.info("payment_processed payment_id=%s status=%s", payment.id, payment.status)

        try:
            await self.webhook_sender(payment)
        except Exception as error:
            reason = str(error) if isinstance(error, WebhookDeliveryError) else type(error).__name__
            await self._record_failure(event, reason, "webhook")
            return

        async with self.database.sessions() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if not self._finished(payment, event):
                payment.webhook_delivered_at = utc_now()
                payment.last_error = None
                logger.info(
                    "webhook_delivered payment_id=%s event_id=%s attempt=%s",
                    payment.id,
                    payment.webhook_event_id,
                    event.attempt_no,
                )

    @staticmethod
    def _finished(payment: Payment, event: WorkMessage) -> bool:
        return bool(
            payment.webhook_delivered_at
            or payment.exhausted_at
            or event.attempt_no < payment.current_attempt
        )

    async def _record_failure(self, event: WorkMessage, reason: str, stage: str) -> None:
        async with self.database.sessions() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if self._finished(payment, event):
                return
            payment.last_error = reason[:500]
            if event.attempt_no < 3:
                payment.current_attempt = event.attempt_no + 1
                delay = self.settings.retry_base_delay * 2 ** (event.attempt_no - 1)
                session.add(
                    make_work_event(
                        payment.id,
                        payment.current_attempt,
                        available_at=utc_now() + timedelta(seconds=delay),
                    )
                )
                logger.warning(
                    "payment_retry_scheduled payment_id=%s attempt=%s stage=%s reason=%s",
                    payment.id,
                    event.attempt_no,
                    stage,
                    reason,
                )
            else:
                payment.exhausted_at = utc_now()
                session.add(make_dead_letter_event(payment, event, reason, stage))
                logger.warning(
                    "payment_exhausted payment_id=%s attempt=%s stage=%s reason=%s",
                    payment.id,
                    event.attempt_no,
                    stage,
                    reason,
                )
