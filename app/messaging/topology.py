from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments.events", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new", durable=True, arguments={"x-single-active-consumer": True}
)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await queue.bind(exchange, routing_key="payments.new")
    dlx = await broker.declare_exchange(DLX_EXCHANGE)
    dlq = await broker.declare_queue(DLQ_QUEUE)
    await dlq.bind(dlx, routing_key="payments.dlq")
