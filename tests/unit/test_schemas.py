from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.events import WorkMessage
from app.schemas import PaymentCreate


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "10000000000000000.00", "NaN", "Infinity"])
def test_amount_rejects_values_that_cannot_be_stored_without_loss(payment_body, amount):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**payment_body, "amount": amount})


@pytest.mark.parametrize(
    "field,value",
    [
        ("currency", "GBP"),
        ("webhook_url", "ftp://receiver.test/file"),
        ("webhook_url", "not a url"),
        ("metadata", []),
        ("description", "a" * 1001),
    ],
)
def test_input_contract_rejects_unsupported_values(payment_body, field, value):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**payment_body, field: value})


def test_decimal_boundary_is_exact_and_metadata_defaults_are_independent(payment_body):
    data = {key: value for key, value in payment_body.items() if key != "metadata"}
    first = PaymentCreate.model_validate({**data, "amount": "9999999999999999.99"})
    second = PaymentCreate.model_validate(data)
    assert first.amount == Decimal("9999999999999999.99")
    assert first.metadata == second.metadata == {}
    first.metadata["changed"] = True
    assert second.metadata == {}


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"attempt_no": 0},
        {"attempt_no": 4},
        {"payment_id": "broken"},
        {"event_id": "broken"},
    ],
)
def test_work_messages_validate_version_ids_and_attempt_budget(change):
    data = {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "payment_id": str(uuid4()),
        "attempt_no": 1,
    }
    with pytest.raises(ValidationError):
        WorkMessage.model_validate({**data, **change})
