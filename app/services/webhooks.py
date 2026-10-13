import asyncio

import httpx

from app.models import Payment
from app.schemas import WebhookPayload


class WebhookDeliveryError(Exception):
    pass


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, timeout: float) -> None:
        self.client = client
        self.timeout = timeout

    async def __call__(self, payment: Payment) -> None:
        if payment.processed_at is None or payment.status == "pending":
            raise ValueError("Cannot notify an unprocessed payment")
        payload = WebhookPayload(
            event_id=payment.webhook_event_id,
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            metadata=payment.metadata_json,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
        # Bound the whole exchange, including a server sending bytes very slowly.
        async with asyncio.timeout(self.timeout):
            async with self.client.stream(
                "POST",
                payment.webhook_url,
                json=payload.model_dump(mode="json"),
                headers={"Webhook-Event-Id": str(payment.webhook_event_id)},
                timeout=self.timeout,
                follow_redirects=False,
            ) as response:
                if not 200 <= response.status_code < 300:
                    raise WebhookDeliveryError(f"http_status_{response.status_code}")
