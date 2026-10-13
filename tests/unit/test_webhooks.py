import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.models import Payment
from app.services.webhooks import WebhookDeliveryError, WebhookSender


@pytest.fixture
def processed_payment():
    return Payment(
        id=uuid4(),
        amount=Decimal("1500.00"),
        currency="RUB",
        description="Order 42",
        metadata_json={"order_id": "42"},
        webhook_url="http://receiver.test/events",
        webhook_event_id=uuid4(),
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
        status="succeeded",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 201, 204, 299])
async def test_any_2xx_response_confirms_delivery_with_exact_payload(processed_payment, status):
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        sender = WebhookSender(client, timeout=1)
        await sender(processed_payment)
        await sender(processed_payment)

    assert len(requests) == 2
    assert requests[0].method == "POST"
    assert str(requests[0].url) == processed_payment.webhook_url
    assert requests[0].headers["Webhook-Event-Id"] == str(processed_payment.webhook_event_id)
    assert "X-API-Key" not in requests[0].headers
    first_payload = json.loads(requests[0].content)
    assert first_payload == json.loads(requests[1].content)
    assert first_payload["event_id"] == str(processed_payment.webhook_event_id)
    assert first_payload["payment_id"] == str(processed_payment.id)
    assert first_payload["event_type"] == "payment.completed"
    assert first_payload["status"] == "succeeded"
    assert first_payload["amount"] == "1500.00"
    assert first_payload["metadata"] == processed_payment.metadata_json
    assert first_payload["processed_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 400, 404, 429, 500, 503])
async def test_non_2xx_is_retryable_and_redirects_are_not_followed(processed_payment, status):
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(status, headers={"Location": "http://elsewhere.test/private"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(receive), follow_redirects=True
    ) as client:
        with pytest.raises(WebhookDeliveryError, match=f"http_status_{status}"):
            await WebhookSender(client, timeout=1)(processed_payment)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_total_timeout_bounds_even_a_transport_that_never_returns(processed_payment):
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def never_returns(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(never_returns)) as client:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(WebhookSender(client, timeout=0.02)(processed_payment), 1)
    assert entered.is_set()
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_uncommitted_payment_result_cannot_trigger_a_webhook(processed_payment):
    processed_payment.status = "pending"
    processed_payment.processed_at = None

    def unexpected_request(request):
        raise AssertionError("A pending payment has no webhook outcome")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected_request)) as client:
        with pytest.raises(ValueError, match="unprocessed"):
            await WebhookSender(client, timeout=1)(processed_payment)
