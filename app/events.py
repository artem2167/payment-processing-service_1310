from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from app.models import Outbox, Payment, utc_now


class WorkMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_id: UUID
    payment_id: UUID
    attempt_no: int = Field(ge=1, le=3, strict=True)


def make_work_event(
    payment_id: UUID, attempt_no: int, available_at: datetime | None = None
) -> Outbox:
    event_id = uuid4()
    payload = WorkMessage(event_id=event_id, payment_id=payment_id, attempt_no=attempt_no)
    return Outbox(
        id=event_id,
        payment_id=payment_id,
        event_type="payment.process",
        attempt_no=attempt_no,
        routing_key="payments.new",
        payload=payload.model_dump(mode="json"),
        available_at=available_at or utc_now(),
        publish_attempts=0,
    )


def make_dead_letter_event(payment: Payment, event: WorkMessage, error: str, stage: str) -> Outbox:
    event_id = uuid4()
    exhausted_at = payment.exhausted_at or utc_now()
    return Outbox(
        id=event_id,
        payment_id=payment.id,
        event_type="payment.dead_letter",
        attempt_no=event.attempt_no,
        routing_key="payments.dlq",
        payload={
            "schema_version": 1,
            "event_id": str(event_id),
            "event_type": "payment.dead_letter",
            "payment_id": str(payment.id),
            "original_event_id": str(event.event_id),
            "attempt_no": event.attempt_no,
            "status": payment.status,
            "stage": stage[:64],
            "error": error[:500],
            "exhausted_at": exhausted_at.isoformat(),
        },
        available_at=utc_now(),
        publish_attempts=0,
    )
