import asyncio
import base64
import contextlib
import hashlib
import logging
import signal

import httpx
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pamqp.commands import Basic
from pydantic import ValidationError

from app.config import Settings, get_settings
from app.db import Database
from app.events import WorkMessage
from app.messaging.consumer import InvalidWorkMessage, PaymentProcessor
from app.messaging.outbox import OutboxRelay, PublicationNotConfirmed, RabbitPublisher
from app.messaging.topology import (
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    declare_topology,
)
from app.services.gateway import EmulatedGateway
from app.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=settings.webhook_timeout + settings.gateway_max_delay + 2,
        timeout=settings.publish_timeout,
        reconnect_interval=settings.recovery_delay,
        fail_fast=True,
        logger=logger,
    )


async def quarantine(
    broker: RabbitBroker, message: RabbitMessage, reason: str, publish_limit: float = 5
) -> None:
    body = message.body
    identifier = message.raw_message.message_id or hashlib.sha256(body).hexdigest()
    confirmation = await broker.publish(
        {
            "schema_version": 1,
            "event_type": "payment.invalid_message",
            "original_message_id": identifier,
            "error": reason,
            "original_body_base64": base64.b64encode(body[:4096]).decode("ascii"),
            "body_truncated": len(body) > 4096,
        },
        exchange=DLX_EXCHANGE,
        routing_key="payments.dlq",
        message_id=f"invalid-{identifier}",
        persist=True,
        mandatory=True,
        timeout=publish_limit,
    )
    if confirmation is not True and not isinstance(confirmation, Basic.Ack):
        raise PublicationNotConfirmed("Quarantine was not confirmed")
    await message.ack()


async def run_connected(
    settings: Settings,
    database: Database,
    client: httpx.AsyncClient,
    stop: asyncio.Event,
) -> None:
    broker = create_broker(settings)
    recovery = asyncio.Event()
    processor = PaymentProcessor(
        database,
        settings,
        EmulatedGateway(settings),
        WebhookSender(client, settings.webhook_timeout),
    )

    @broker.subscriber(
        PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL, no_reply=True
    )
    async def handle(message: RabbitMessage) -> None:
        try:
            try:
                event = WorkMessage.model_validate_json(message.body)
            except ValidationError:
                await quarantine(
                    broker, message, "invalid_message_schema", settings.publish_timeout
                )
                return
            try:
                await processor.process(event)
            except InvalidWorkMessage as error:
                await quarantine(broker, message, str(error), settings.publish_timeout)
                return
            await message.ack()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("consumer_recovery_required reason=%s", type(error).__name__)
            # Closing the channel requeues this unacked delivery; do not consume
            # its logical retry budget when the DB or connection is unavailable.
            recovery.set()

    tasks: list[asyncio.Task] = []
    try:
        await broker.connect()
        await declare_topology(broker)
        await broker.start()
        relay = OutboxRelay(database, settings, RabbitPublisher(broker, settings.publish_timeout))
        tasks = [
            asyncio.create_task(relay.run(), name="outbox-relay"),
            asyncio.create_task(recovery.wait(), name="consumer-recovery"),
            asyncio.create_task(stop.wait(), name="worker-stop"),
        ]
        logger.info("worker_ready queue=payments.new")
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await broker.stop()


async def run_worker(settings: Settings, stop: asyncio.Event | None = None) -> None:
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(name, stop.set)
    database = Database(settings.database_url)
    try:
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            while not stop.is_set():
                try:
                    await run_connected(settings, database, client, stop)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    logger.warning("worker_connection_failed reason=%s", type(error).__name__)
                if not stop.is_set():
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=settings.recovery_delay)
    finally:
        await database.close()


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(run_worker(settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
