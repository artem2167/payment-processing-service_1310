from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.request import DecimalAmountRoute
from app.schemas import DeliveryInfo, PaymentAccepted, PaymentCreate, PaymentResponse
from app.services.payments import IdempotencyConflict, create_payment, get_payment

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], route_class=DecimalAmountRoute)


async def session_dependency(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.database.sessions() as session:
        yield session


Session = Annotated[AsyncSession, Depends(session_dependency)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def post_payment(
    data: PaymentCreate, session: Session, idempotency_key: IdempotencyKey
) -> PaymentAccepted:
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    try:
        payment = await create_payment(session, data, idempotency_key)
    except IdempotencyConflict as error:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key was used with a different request"
        ) from error
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}", response_model=PaymentResponse)
async def read_payment(payment_id: UUID, session: Session) -> PaymentResponse:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_json,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        delivery=DeliveryInfo(
            delivered_at=payment.webhook_delivered_at, exhausted_at=payment.exhausted_at
        ),
    )
