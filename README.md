# Асинхронный сервис платежей

Сервис принимает платежи через FastAPI, сохраняет их в PostgreSQL и обрабатывает асинхронно через RabbitMQ.

Платеж и outbox-событие создаются в одной транзакции. Consumer получает событие, эмулирует платежный шлюз, сохраняет результат и отправляет webhook.

```text
POST /api/v1/payments
        |
        v
PostgreSQL
payment + outbox
        |
        v
RabbitMQ -> consumer -> gateway
                      |
                      v
                  DB + webhook
```

## Стек

Python 3.12, FastAPI, Pydantic 2, SQLAlchemy async, asyncpg, PostgreSQL, RabbitMQ, FastStream, Alembic, Docker Compose, pytest.

## Возможности

- асинхронная обработка платежей;
- идемпотентность по `Idempotency-Key`;
- transactional outbox;
- webhook с retry;
- DLQ для неуспешных сообщений;
- восстановление обработки после рестартов;
- интеграционные тесты с PostgreSQL и RabbitMQ.

## Запуск

Нужны Docker и Docker Compose v2.

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose --profile demo up -d --build
docker compose ps
```

Основные адреса:

| Сервис | Адрес |
| --- | --- |
| API | `http://localhost:8000` |
| Health | `http://localhost:8000/health` |
| RabbitMQ Management | `http://localhost:15672` |
| Demo webhook receiver | `http://localhost:8080/events` |

Порты и credentials можно изменить в `.env`.

Без demo receiver:

```bash
docker compose up -d --build
```

## API

Все маршруты требуют заголовок:

```text
X-API-Key: <API_KEY>
```

### Создание платежа

```http
POST /api/v1/payments
```

Также нужен заголовок:

```text
Idempotency-Key: order-1001
```

Пример тела:

```json
{
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1001",
  "metadata": {
    "order_id": "1001"
  },
  "webhook_url": "http://webhook-receiver:8080/webhook"
}
```

Успешный ответ:

```json
{
  "payment_id": "74af37b4-4cbc-48c7-8277-7a8ca8ca8e83",
  "status": "pending",
  "created_at": "2026-10-13T09:00:00Z"
}
```

HTTP status: `202 Accepted`.

Поддерживаемые валюты: `RUB`, `USD`, `EUR`.

Для денежных значений используется `Decimal`, чтобы избежать ошибок округления.

### Idempotency

Если повторить запрос с тем же `Idempotency-Key` и тем же телом, сервис вернет существующий платеж.

Если использовать тот же ключ с другим телом, API вернет `409 Conflict`.

Конкурентные запросы с одинаковым ключом защищены уникальным ограничением PostgreSQL.

### Получение платежа

```http
GET /api/v1/payments/{payment_id}
```

Ответ содержит сумму, валюту, описание, metadata, статус, даты обработки и состояние доставки webhook.

Основные HTTP-коды:

| Код | Причина |
| --- | --- |
| `401` | неверный или отсутствующий API key |
| `404` | платеж не найден |
| `409` | конфликт `Idempotency-Key` |
| `422` | ошибка валидации |
| `503` | PostgreSQL недоступен |

## Обработка платежа

После создания платеж имеет статус `pending`.

Consumer получает сообщение из RabbitMQ и вызывает эмулятор шлюза. Эмулятор работает с задержкой 2–5 секунд:

- около 90% запросов завершаются со статусом `succeeded`;
- около 10% — `failed`.

Результат сохраняется в PostgreSQL вместе с `processed_at`.

## Webhook

После обработки платежа отправляется событие:

```json
{
  "event_id": "bf634872-f412-42f0-811c-0c0b5d53d7dc",
  "event_type": "payment.completed",
  "payment_id": "74af37b4-4cbc-48c7-8277-7a8ca8ca8e83",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "metadata": {
    "order_id": "1001"
  },
  "created_at": "2026-10-13T09:00:00Z",
  "processed_at": "2026-10-13T09:00:03Z"
}
```

`event_id` одинаковый для всех повторов одного события и также передается в заголовке `Webhook-Event-Id`.

Любой ответ `2xx` считается успешной доставкой.

При ошибке выполняются три попытки:

```text
attempt 1
   |
   | 5 sec
   v
attempt 2
   |
   | 10 sec
   v
attempt 3
   |
   v
payments.dlq
```

Webhook доставляется с семантикой `at-least-once`, поэтому получателю рекомендуется дедуплицировать события по `event_id`.

## Transactional Outbox

Платеж и первое outbox-событие создаются в одной транзакции PostgreSQL.

После commit relay публикует событие в RabbitMQ. Если брокер временно недоступен, событие остается в outbox и будет опубликовано после восстановления соединения.

Это позволяет принимать платежи даже при временной недоступности RabbitMQ и не терять события между БД и брокером.

## RabbitMQ

Используются две основные очереди:

```text
payments.events -> payments.new -> consumer

payments.dlx -> payments.dlq
```

Очереди durable, сообщения публикуются persistent.

По условиям задания одновременно активен один consumer.

Посмотреть состояние очередей:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues   name messages_ready messages_unacknowledged
```

Или через RabbitMQ Management:

```text
http://localhost:15672
```

## Demo webhook receiver

При запуске с профилем `demo` доступны готовые сценарии:

| URL | Поведение |
| --- | --- |
| `/webhook` | всегда `204` |
| `/flaky` | два раза `500`, затем `204` |
| `/fail` | всегда `500` |
| `/slow` | отвечает с задержкой |

Пример URL внутри Compose:

```text
http://webhook-receiver:8080/webhook
http://webhook-receiver:8080/flaky
http://webhook-receiver:8080/fail
http://webhook-receiver:8080/slow
```

История запросов:

```text
GET http://localhost:8080/events
```

## Проверка восстановления

Можно остановить RabbitMQ:

```bash
docker compose stop rabbitmq
```

После этого создать платеж. API сохранит его и вернет `202`.

После восстановления брокера:

```bash
docker compose start rabbitmq
```

relay опубликует сохраненное событие, и обработка продолжится.

Consumer также можно перезапустить без потери состояния:

```bash
docker compose restart consumer
```

Для автоматической проверки recovery-сценариев:

```bash
uv run --frozen python -m scripts.resilience --api http://localhost:8000
```

## Тесты

Интеграционные тесты запускаются в отдельном Compose-окружении:

```bash
docker compose -f compose.test.yaml up   --build   --abort-on-container-exit   --exit-code-from tests   tests
```

После выполнения:

```bash
docker compose -f compose.test.yaml down -v
```

Локально:

```bash
uv sync --frozen
uv run --frozen pytest -q
uv run --frozen ruff check .
```

Smoke-проверка работающего сервиса:

```bash
uv run --frozen python -m scripts.smoke --api http://localhost:8000
```

## Основные настройки

| Переменная | Назначение |
| --- | --- |
| `API_KEY` | ключ доступа к API |
| `DATABASE_URL` | подключение к PostgreSQL |
| `RABBITMQ_URL` | подключение к RabbitMQ |
| `RETRY_BASE_DELAY` | базовая задержка retry |
| `WEBHOOK_TIMEOUT` | timeout webhook |
| `OUTBOX_POLL_INTERVAL` | интервал polling outbox |
| `PUBLISH_TIMEOUT` | timeout публикации |
| `RECOVERY_DELAY` | задержка восстановления worker |
| `GATEWAY_MIN_DELAY` | минимальная задержка шлюза |
| `GATEWAY_MAX_DELAY` | максимальная задержка шлюза |
| `LOG_LEVEL` | уровень логирования |

Пример значений находится в `.env.example`.

## Локальный запуск

Можно поднять только PostgreSQL и RabbitMQ:

```bash
docker compose up -d --wait postgres rabbitmq
```

После настройки переменных окружения:

```powershell
uv run --frozen alembic upgrade head

uv run --frozen uvicorn app.main:create_app   --factory   --host 127.0.0.1   --port 8000
```

Consumer запускается отдельно:

```bash
uv run --frozen python -m app.worker
```

## Ограничения

В проекте используется эмулятор платежного шлюза — реальных списаний нет.

В рамках задания также не реализованы:

- подпись webhook;
- административный redrive из DLQ;
- полноценные monitoring/metrics;
- горизонтальное масштабирование обработки;
- несколько API-клиентов.
