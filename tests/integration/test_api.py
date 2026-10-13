import asyncio
import json
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, func, select

from app.models import Outbox, Payment

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def counts(database):
    async with database.sessions() as session:
        return (
            await session.scalar(select(func.count()).select_from(Payment)),
            await session.scalar(select(func.count()).select_from(Outbox)),
        )


def raw_payment_json(payment_body, amount_token):
    """Keep a JSON number's original digits out of Python's binary float conversion."""
    rest = json.dumps({key: value for key, value in payment_body.items() if key != "amount"})
    return f'{{"amount":{amount_token},{rest[1:]}'.encode()


@pytest.mark.parametrize("amount", ["9007199254740990.99", "9999999999999999.99"])
async def test_raw_json_number_amount_keeps_exact_cents_and_numeric_boundary(
    client, database, payment_body, amount
):
    response = await client.post(
        "/api/v1/payments",
        content=raw_payment_json(payment_body, amount),
        headers={"Idempotency-Key": "exact-number", "Content-Type": "application/json"},
    )
    assert response.status_code == 202
    identity = UUID(response.json()["payment_id"])
    detail = await client.get(f"/api/v1/payments/{identity}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == amount
    async with database.sessions() as session:
        payment = await session.get(Payment, identity)
        assert payment.amount == Decimal(amount)
    assert await counts(database) == (1, 1)


@pytest.mark.parametrize(
    "amount", ["1500.00000000000001", "9007199254740990.991", "10000000000000000.00"]
)
async def test_raw_json_number_cannot_round_away_excess_precision_or_overflow(
    client, database, payment_body, amount
):
    response = await client.post(
        "/api/v1/payments",
        content=raw_payment_json(payment_body, amount),
        headers={"Idempotency-Key": "invalid-number", "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert await counts(database) == (0, 0)


async def test_exact_amount_parsing_preserves_metadata_json_number_types(
    client, database, payment_body
):
    metadata = {
        "ratio": 1.25,
        "count": 2,
        "enabled": True,
        "optional": None,
        "nested": [0.125, {"rate": 2.75}],
    }
    response = await client.post(
        "/api/v1/payments",
        content=raw_payment_json({**payment_body, "metadata": metadata}, "9007199254740990.99"),
        headers={"Idempotency-Key": "metadata-numbers", "Content-Type": "application/json"},
    )
    assert response.status_code == 202
    identity = UUID(response.json()["payment_id"])
    detail = await client.get(f"/api/v1/payments/{identity}")
    returned = detail.json()["metadata"]
    assert returned == metadata
    assert type(returned["ratio"]) is float
    assert type(returned["count"]) is int
    assert type(returned["enabled"]) is bool
    async with database.sessions() as session:
        payment = await session.get(Payment, identity)
        assert payment.metadata_json == metadata
        assert type(payment.metadata_json["ratio"]) is float


async def test_raw_json_number_and_string_have_identical_exact_idempotency_hash(
    client, database, payment_body
):
    amount = "9007199254740990.99"
    first = await client.post(
        "/api/v1/payments",
        content=raw_payment_json(payment_body, amount),
        headers={"Idempotency-Key": "number-string", "Content-Type": "application/json"},
    )
    replay = await client.post(
        "/api/v1/payments",
        json={**payment_body, "amount": amount},
        headers={"Idempotency-Key": "number-string"},
    )
    assert first.status_code == replay.status_code == 202
    assert first.json() == replay.json()
    assert await counts(database) == (1, 1)


async def test_valid_surrogate_pair_and_nan_string_are_stored_as_normal_json(
    client, database, payment_body
):
    # A UTF-16 surrogate pair represents a valid emoji; "NaN" here is a JSON string.
    payload = (
        b'{"amount":"1.00","currency":"RUB","description":"Order \\ud83d\\ude00",'
        b'"metadata":{"value":"NaN","emoji":"\\ud83d\\ude00"},'
        b'"webhook_url":"http://receiver.test/events"}'
    )
    response = await client.post(
        "/api/v1/payments",
        content=payload,
        headers={"Idempotency-Key": "valid-unicode-string", "Content-Type": "application/json"},
    )
    assert response.status_code == 202
    identity = UUID(response.json()["payment_id"])
    detail = await client.get(f"/api/v1/payments/{identity}")
    assert detail.status_code == 200
    assert detail.json()["description"] == "Order \U0001f600"
    assert detail.json()["metadata"] == {"value": "NaN", "emoji": "\U0001f600"}
    async with database.sessions() as session:
        payment = await session.get(Payment, identity)
        assert payment.description == "Order \U0001f600"
        assert payment.metadata_json == {"value": "NaN", "emoji": "\U0001f600"}


async def test_create_read_and_idempotent_replay(client, database, payment_body):
    headers = {"Idempotency-Key": "order-42"}
    first = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert first.status_code == 202
    created = first.json()
    UUID(created["payment_id"])
    assert created["status"] == "pending"
    assert created["created_at"]

    second = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert second.status_code == 202
    assert second.json() == created
    detail = await client.get(f"/api/v1/payments/{created['payment_id']}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["amount"] == "1500.00"
    assert body["currency"] == payment_body["currency"]
    assert body["description"] == payment_body["description"]
    assert body["metadata"] == payment_body["metadata"]
    assert body["processed_at"] is None
    assert await counts(database) == (1, 1)


async def test_idempotency_hash_normalizes_decimal_and_default_metadata(
    client, database, payment_body
):
    body = {key: value for key, value in payment_body.items() if key != "metadata"}
    first = await client.post(
        "/api/v1/payments", json={**body, "amount": "1.0"}, headers={"Idempotency-Key": "normalize"}
    )
    replay = await client.post(
        "/api/v1/payments",
        json={**body, "amount": "1.00", "metadata": {}},
        headers={"Idempotency-Key": "normalize"},
    )
    assert first.status_code == replay.status_code == 202
    assert first.json()["payment_id"] == replay.json()["payment_id"]
    assert await counts(database) == (1, 1)


async def test_reusing_key_with_changed_payload_conflicts(client, database, payment_body):
    headers = {"Idempotency-Key": "conflict"}
    created = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    conflict = await client.post(
        "/api/v1/payments", json={**payment_body, "amount": "10.00"}, headers=headers
    )
    assert created.status_code == 202
    assert conflict.status_code == 409
    assert await counts(database) == (1, 1)
    detail = await client.get(f"/api/v1/payments/{created.json()['payment_id']}")
    assert detail.json()["amount"] == "1500.00"


@pytest.mark.parametrize("key", [None, "", " " * 5, "k" * 256])
async def test_invalid_idempotency_keys_never_write_rows(client, database, payment_body, key):
    headers = {} if key is None else {"Idempotency-Key": key}
    response = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert response.status_code in (400, 422)
    assert await counts(database) == (0, 0)


@pytest.mark.parametrize(
    "patch",
    [
        {"amount": "0"},
        {"amount": "-0.01"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://bad.test"},
    ],
)
async def test_invalid_payment_never_writes_rows(client, database, payment_body, patch):
    response = await client.post(
        "/api/v1/payments", json={**payment_body, **patch}, headers={"Idempotency-Key": "invalid"}
    )
    assert response.status_code == 422
    assert await counts(database) == (0, 0)


async def test_unknown_payment_returns_404(client):
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


async def test_concurrent_identical_requests_create_exactly_one_payment_and_event(
    client, database, payment_body
):
    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "same-key"}
            )
            for _ in range(20)
        ]
    )
    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert await counts(database) == (1, 1)


async def test_concurrent_different_requests_have_one_winner(client, database, payment_body):
    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments",
                json={**payment_body, "description": f"request-{number}"},
                headers={"Idempotency-Key": "racing-key"},
            )
            for number in range(12)
        ]
    )
    assert sum(response.status_code == 202 for response in responses) == 1
    assert sum(response.status_code == 409 for response in responses) == 11
    assert await counts(database) == (1, 1)


async def test_failed_outbox_insert_rolls_back_payment(client, database, payment_body):
    def fail_insert(mapper, connection, target):
        raise RuntimeError("Injected failure between payment and outbox insert")

    event.listen(Outbox, "before_insert", fail_insert)
    try:
        with pytest.raises(RuntimeError, match="Injected failure"):
            await client.post(
                "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "rollback"}
            )
    finally:
        event.remove(Outbox, "before_insert", fail_insert)
    assert await counts(database) == (0, 0)
