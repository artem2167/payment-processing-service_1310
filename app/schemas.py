import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    field_serializer,
    field_validator,
)


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


Money = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Money
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("amount", mode="before")
    @classmethod
    def reject_boolean_amount(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("amount must be a decimal number")
        return value

    @field_validator("description")
    @classmethod
    def validate_description_text(cls, value: str) -> str:
        try:
            value.encode("utf-8")
        except UnicodeError as error:
            raise ValueError("description must contain valid Unicode text") from error
        if "\x00" in value:
            raise ValueError("description must not contain a null character")
        return value

    @field_validator("metadata")
    @classmethod
    def limit_metadata_size(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        try:
            encoded = json.dumps(
                value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")
        except (ValueError, UnicodeError) as error:
            raise ValueError("metadata must contain valid JSON values") from error
        if len(encoded) > 16 * 1024:
            raise ValueError("metadata must not exceed 16 KiB")

        # PostgreSQL JSONB cannot store the Unicode null character.
        def validate_strings(item: JsonValue) -> None:
            if isinstance(item, str) and "\x00" in item:
                raise ValueError("metadata must not contain a null character")
            if isinstance(item, list):
                for child in item:
                    validate_strings(child)
            elif isinstance(item, dict):
                for key, child in item.items():
                    validate_strings(key)
                    validate_strings(child)

        validate_strings(value)
        return value

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class DeliveryInfo(BaseModel):
    delivered_at: datetime | None
    exhausted_at: datetime | None


class PaymentResponse(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    delivery: DeliveryInfo

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")


class WebhookPayload(BaseModel):
    event_id: UUID
    event_type: Literal["payment.completed"] = "payment.completed"
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")
