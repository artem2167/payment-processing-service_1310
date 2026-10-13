from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest

from app.main import create_app


class UnusedDatabase:
    def sessions(self):
        raise AssertionError("Rejected requests must not start a database operation")


class ValidationDatabase:
    @asynccontextmanager
    async def sessions(self):
        # FastAPI may resolve the session dependency before body validation ends.
        # A session is allowed, while executing domain logic would fail this test.
        yield object()


def raw_body(amount='"1.00"', metadata="{}", description='"ok"'):
    return (
        '{"amount":'
        + amount
        + ',"currency":"RUB","description":'
        + description
        + ',"metadata":'
        + metadata
        + ',"webhook_url":"http://receiver.test/events"}'
    ).encode()


async def reject_body(settings, payload):
    app = create_app(settings=settings, database=ValidationDatabase())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key},
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            content=payload,
            headers={"Content-Type": "application/json", "Idempotency-Key": "invalid-body"},
        )
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/json")
    errors = response.json()["detail"]
    assert errors
    assert all(set(error) == {"loc", "type", "msg"} for error in errors)
    assert all(isinstance(error["msg"], str) and error["msg"] for error in errors)
    return errors


@pytest.mark.asyncio
@pytest.mark.parametrize("api_key", [None, "wrong-key", ""])
async def test_unauthorized_requests_are_rejected_before_database_access(settings, api_key):
    app = create_app(settings=settings, database=UnusedDatabase())
    headers = {} if api_key is None else {"X-API-Key": api_key}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        responses = [
            await client.post("/api/v1/payments", headers=headers, json={}),
            await client.get(f"/api/v1/payments/{uuid4()}", headers=headers),
            await client.get("/health", headers=headers),
        ]
    assert all(response.status_code == 401 for response in responses)


@pytest.mark.asyncio
async def test_interactive_documentation_is_disabled(settings):
    app = create_app(settings=settings, database=UnusedDatabase())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key},
    ) as client:
        for path in ("/docs", "/redoc", "/openapi.json"):
            assert (await client.get(path)).status_code == 404


@pytest.mark.asyncio
async def test_malformed_json_returns_validation_error_before_database_access(settings):
    app = create_app(settings=settings, database=UnusedDatabase())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key},
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            content=b'{"amount": 1500.99,',
            headers={"Content-Type": "application/json", "Idempotency-Key": "bad-json"},
        )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("level", ["amount", "metadata_value", "metadata_array", "nested_metadata"])
async def test_nonstandard_json_numbers_are_rejected_without_reflecting_input(
    settings, token, level
):
    if level == "amount":
        payload = raw_body(amount=token)
    elif level == "metadata_value":
        payload = raw_body(metadata='{"value":' + token + "}")
    elif level == "metadata_array":
        payload = raw_body(metadata='{"values":[' + token + "]}")
    else:
        payload = raw_body(metadata='{"nested":{"value":' + token + "}}")
    errors = await reject_body(settings, payload)
    assert all(error["type"] == "json_invalid" for error in errors)
    assert all(error["loc"][0] == "body" for error in errors)


@pytest.mark.asyncio
@pytest.mark.parametrize("exponent", ["1e400", "-1e400"])
async def test_metadata_float_overflow_has_a_serializable_validation_response(settings, exponent):
    errors = await reject_body(settings, raw_body(metadata='{"value":' + exponent + "}"))
    assert errors[0]["loc"] == ["body", "metadata"]
    assert errors[0]["type"] == "value_error"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,field,error_type",
    [
        (raw_body(description='"\\ud800"'), "description", "string_unicode"),
        (raw_body(description='"\\udfff"'), "description", "string_unicode"),
        (raw_body(metadata='{"value":"\\ud800"}'), "metadata", "value_error"),
        (raw_body(metadata='{"\\ud800":"value"}'), "metadata", "value_error"),
    ],
)
async def test_lone_surrogates_have_a_serializable_validation_response(
    settings, payload, field, error_type
):
    errors = await reject_body(settings, payload)
    assert errors[0]["loc"] == ["body", field]
    assert errors[0]["type"] == error_type


@pytest.mark.asyncio
async def test_lone_surrogate_in_extra_field_location_cannot_break_validation_response(settings):
    payload = raw_body()[:-1] + b',"\\ud800":"private-value"}'
    errors = await reject_body(settings, payload)
    assert errors[0]["type"] in {"extra_forbidden", "string_unicode"}
    assert errors[0]["loc"][0] == "body"
    assert "private-value" not in str(errors)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,field,error_type",
    [
        (raw_body(amount="0"), "amount", "greater_than"),
        (raw_body(amount='{"private":"data"}'), "amount", "decimal_type"),
        (raw_body(metadata="[]"), "metadata", "dict_type"),
        (raw_body(description='""'), "description", "string_too_short"),
    ],
)
async def test_ordinary_validation_keeps_location_type_and_message(
    settings, payload, field, error_type
):
    errors = await reject_body(settings, payload)
    assert errors[0]["loc"] == ["body", field]
    assert errors[0]["type"] == error_type
