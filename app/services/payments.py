import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.events import make_work_event
from app.models import Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    """A previously used key describes a different payment request."""


def request_hash(data: PaymentCreate) -> str:
    canonical = json.dumps(
        data.model_dump(mode="json"),
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    fingerprint = request_hash(data)
    async with session.begin():
        # The unique constraint arbitrates concurrent requests before the lookup.
        result = await session.execute(
            insert(Payment)
            .values(
                id=uuid4(),
                amount=data.amount,
                currency=data.currency.value,
                description=data.description,
                metadata_json=data.metadata,
                status="pending",
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                webhook_url=str(data.webhook_url),
                webhook_event_id=uuid4(),
                current_attempt=1,
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
        payment = result.scalar_one_or_none()
        if payment is not None:
            session.add(make_work_event(payment.id, 1))
        else:
            payment = (
                await session.execute(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
            ).scalar_one()
            if payment.request_hash != fingerprint:
                raise IdempotencyConflict
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
