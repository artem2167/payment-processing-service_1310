# Асинхронный сервис платежей

FastAPI принимает платеж, PostgreSQL сохраняет его вместе с событием outbox, а один consumer RabbitMQ выполняет эмуляцию шлюза, сохраняет результат и отправляет webhook. Ответ `202 Accepted` подтверждает сохранение запроса; обработка продолжается в фоне.

```text
POST → PostgreSQL: payment + outbox → 202
                      ↓
              relay в consumer
                      ↓
             RabbitMQ payments.new
                      ↓
          шлюз → результат в БД → webhook
                      ↓
              retry outbox / DLQ
```

Стек: Python 3.12, FastAPI, Pydantic 2, SQLAlchemy 2.0 async, asyncpg, PostgreSQL 16, FastStream, RabbitMQ 4, Alembic. Прямые и транзитивные Python-зависимости зафиксированы в `uv.lock`. Образы инфраструктуры: `postgres:16.15`, `rabbitmq:4.2.9-management`; приложение собирается на `python:3.12.13-slim-bookworm`.

## Запуск в Docker

Нужны Docker Engine с Linux-контейнерами и Docker Compose v2. На Windows запустите Docker Desktop и дождитесь готовности движка. Команды ниже выполняются из корня проекта.

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose --profile demo up -d --build
docker compose ps -a
docker compose logs -f api consumer
```

В `.env.example` приведены открытые значения для локальной демонстрации. Для другого окружения замените API-ключ, логины и пароли. При изменении учетных данных также обновите `DATABASE_URL` и `RABBITMQ_URL`; специальные символы в URL нужно кодировать. При существующих volumes изменение `POSTGRES_PASSWORD` или `RABBITMQ_DEFAULT_PASS` не меняет уже созданных пользователей.

Compose сначала дожидается готовности PostgreSQL, выполняет `alembic upgrade head` отдельным одноразовым сервисом `migrate`, затем запускает API и consumer. Consumer дополнительно дожидается готовности RabbitMQ. API проверяет доступность БД; доступность брокера не обязательна для приема платежа в outbox.

Четыре основных сервиса — `postgres`, `rabbitmq`, `api`, `consumer`. `migrate` только применяет схему. Профиль `demo` добавляет отдельный приемник webhook; для запуска без демонстрационного приемника используйте `docker compose up -d --build`.

Локальные адреса при стандартных портах из `.env.example`:

| Сервис | Адрес |
| --- | --- |
| API | `http://localhost:8000` |
| Readiness API | `http://localhost:8000/health`, нужен `X-API-Key` |
| RabbitMQ Management | `http://localhost:15672`, логин/пароль из `.env` |
| Webhook-приемник | `http://localhost:8080/events` |
| PostgreSQL | `localhost:5432` |

Порты привязаны к `127.0.0.1`. Их можно изменить через `.env`; если порт API занят, задайте, например, `API_PORT=18000`. В этом случае API доступен на `http://localhost:18000`. Для webhook внутри Compose используется имя `webhook-receiver`; `localhost` в consumer указывает на сам контейнер consumer.

## API

Каждый маршрут платежного API, включая `/health` и ответы на неизвестные пути, требует заголовок `X-API-Key`. Автоматические `/docs`, `/redoc`, `/openapi.json` отключены.

### Создать платеж

`POST /api/v1/payments` требует `Idempotency-Key`. Пример подготовлен в `examples/payment.json`.

```powershell
$apiKey = (Get-Content .env | Where-Object { $_ -match '^API_KEY=' }) -replace '^API_KEY=', ''
$apiPort = (Get-Content .env | Where-Object { $_ -match '^API_PORT=' }) -replace '^API_PORT=', ''
if (-not $apiPort) { $apiPort = '8000' }
$apiUrl = "http://localhost:$apiPort"
$headers = @{ 'X-API-Key' = $apiKey; 'Idempotency-Key' = 'order-1001' }
$body = Get-Content examples/payment.json -Raw -Encoding UTF8
$payment = Invoke-RestMethod -Method Post -Uri "$apiUrl/api/v1/payments" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body ([System.Text.Encoding]::UTF8.GetBytes($body))
$payment
```

Успешный ответ — `202`:

```json
{
  "payment_id": "74af37b4-4cbc-48c7-8277-7a8ca8ca8e83",
  "status": "pending",
  "created_at": "2026-10-13T09:00:00Z"
}
```

| Поле | Правило |
| --- | --- |
| `amount` | Положительное число или decimal-строка, до 18 цифр всего и 2 после точки; `bool`, NaN/Infinity и лишние дробные знаки отклоняются. Рекомендуется строка `"1500.00"` |
| `currency` | `RUB`, `USD`, `EUR` |
| `description` | Обязательная корректная Unicode-строка длиной 1–1000 символов, без нулевого символа |
| `metadata` | JSON-объект, по умолчанию `{}`, до 16 KiB в компактном UTF-8 представлении; NaN/Infinity и нулевой символ отклоняются |
| `webhook_url` | Обязательный URL с `http` или `https`, до 2048 символов |
| `Idempotency-Key` | Обязательный заголовок длиной 1–255 символов, строка из пробелов отклоняется |

Ключ глобально уникален и не истекает. Повтор с тем же нормализованным телом возвращает `202`, прежние `payment_id`/`created_at` и текущий статус. Например, суммы `1500` и `"1500.00"` эквивалентны; порядок ключей metadata не влияет на сравнение. Новые платеж и событие не создаются, включая конкурентные запросы. То же значение ключа с другим телом возвращает `409`.

JSON-число в `amount` читается из исходного текста запроса как Decimal: даже большая допустимая сумма не округляется через binary float. Если клиент сам сериализует деньги из float, утраченные на его стороне цифры восстановить нельзя; decimal-строка остается удобным способом передачи точной суммы.

```powershell
# Повтор: тот же payment_id, новое событие не создается.
Invoke-RestMethod -Method Post -Uri "$apiUrl/api/v1/payments" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body ([System.Text.Encoding]::UTF8.GetBytes($body))

# Конфликт: ожидается HTTP 409.
$changed = $body | ConvertFrom-Json
$changed.amount = '1501.00'
Invoke-RestMethod -Method Post -Uri "$apiUrl/api/v1/payments" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body ([System.Text.Encoding]::UTF8.GetBytes(($changed | ConvertTo-Json -Depth 20)))
```

### Получить платеж

```powershell
Invoke-RestMethod -Uri "$apiUrl/api/v1/payments/$($payment.payment_id)" -Headers @{ 'X-API-Key' = $apiKey }
```

`GET /api/v1/payments/{payment_id}` возвращает `payment_id`, `amount` как строку с двумя дробными знаками, `currency`, `description`, `metadata`, `status`, `idempotency_key`, `webhook_url`, `created_at`, `processed_at` и `delivery` с полями `delivered_at`/`exhausted_at`. Даты хранятся в UTC и возвращаются в ISO 8601. До обработки `processed_at` равен `null`.

| Код | Причина |
| --- | --- |
| `401` | API-ключ отсутствует или неверен |
| `404` | Платеж с корректным UUID не найден |
| `409` | Idempotency-Key уже использован с другим телом |
| `422` | Некорректное тело, UUID или Idempotency-Key |
| `503` | База данных недоступна |

При ошибке разбора JSON или валидации Pydantic ответ `422` содержит `detail` — список ошибок с `type`, `loc` и `msg`. Исходное значение поля и внутренний контекст исключения в ответ не включаются. Некорректный Unicode безопасно представляется в диагностике. Нестандартные JSON-константы `NaN`, `Infinity` и `-Infinity` отклоняются как `json_invalid`; бесконечные значения, полученные из слишком большого показателя степени, не принимаются в платеж или metadata. Отдельная проверка `Idempotency-Key`, состоящего только из пробельных символов, возвращает `422` со строкой `detail: "Idempotency-Key must not be blank"`.

## Шлюз, webhook и повторы

Шлюз эмулируется асинхронной задержкой 2–5 секунд: в 90% случаев результат `succeeded`, в 10% — `failed`. `failed` означает бизнес-отказ шлюза; он также сохраняется с `processed_at` и отправляется получателю webhook. Сетевая ошибка уведомления не меняет сохраненный статус платежа.

Webhook — POST с JSON:

```json
{
  "event_id": "bf634872-f412-42f0-811c-0c0b5d53d7dc",
  "event_type": "payment.completed",
  "payment_id": "74af37b4-4cbc-48c7-8277-7a8ca8ca8e83",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "metadata": {"order_id": "1001"},
  "created_at": "2026-10-13T09:00:00Z",
  "processed_at": "2026-10-13T09:00:03Z"
}
```

`event_id` стабилен для платежа и одинаков во всех повторах; он также передается в заголовке `Webhook-Event-Id`. Получатель должен дедуплицировать уведомления по нему. Успешным считается любой HTTP `2xx`. Сетевые ошибки, timeout и остальные HTTP-коды вызывают повтор. Перенаправления не выполняются. Общий timeout запроса по умолчанию — 10 секунд.

Всего предусмотрены **три логические попытки**, включая первую: при ошибке попытки 1 событие следующей попытки записывается в outbox с задержкой 5 секунд, при ошибке попытки 2 — с задержкой 10 секунд. Формула: `RETRY_BASE_DELAY * 2 ** (attempt_no - 1)` после неуспешной попытки. Отложенная публикация использует `outbox.available_at`; фактический старт может быть позднее из-за интервала relay и нагрузки. После третьей ошибки фиксируется исчерпание обработки и создается событие для `payments.dlq`.

Номер попытки и переход retry/DLQ сохраняются в БД одной транзакцией до ACK исходного сообщения. Повтор старого сообщения не создает новую попытку. При уже сохраненном результате шлюз повторно не разыгрывается; при сохраненной доставке новый HTTP-запрос не выполняется. Если технический сбой исчерпал попытки до результата шлюза, платеж может остаться `pending` с `delivery.exhausted_at`; техническая ошибка не выдается за бизнес-отказ.

### Приемник для демонстрации

В профиле `demo` доступны маршруты:

| URL в `webhook_url` | Ответ |
| --- | --- |
| `http://webhook-receiver:8080/webhook` | Всегда `204` |
| `http://webhook-receiver:8080/flaky` | Два первых запроса одного `event_id` — `500`, затем `204` |
| `http://webhook-receiver:8080/fail` | Всегда `500`; сценарий трех попыток и DLQ |
| `http://webhook-receiver:8080/slow` | `204` через 20 секунд; при стандартном timeout вызывает retry/DLQ |

Приемник показывает до 1000 последних запросов: `GET http://localhost:8080/events`, включая тело, `event_id`, путь, время и номер запроса данного события. Состояние хранится в памяти и сбрасывается при рестарте. `DELETE /events` очищает журнал и счетчики.

Для другого сценария измените `webhook_url` в JSON и используйте **новый** Idempotency-Key:

```powershell
$retryBody = $body | ConvertFrom-Json
$retryBody.webhook_url = 'http://webhook-receiver:8080/flaky'
$retryHeaders = @{ 'X-API-Key' = $apiKey; 'Idempotency-Key' = 'flaky-1001' }
$retryPayment = Invoke-RestMethod -Method Post -Uri "$apiUrl/api/v1/payments" -Headers $retryHeaders -ContentType 'application/json; charset=utf-8' -Body ([System.Text.Encoding]::UTF8.GetBytes(($retryBody | ConvertTo-Json -Depth 20)))
Invoke-RestMethod -Uri http://localhost:8080/events
```

## Outbox и гарантии доставки

Создание платежа и исходного outbox-события — одна транзакция PostgreSQL. Relay внутри consumer публикует готовые события с persistent delivery, publisher confirms и обязательной маршрутизацией. Только подтвержденная маршрутизированная публикация получает `published_at`. Недоступный брокер оставляет работу в outbox; повтор публикации учитывает `available_at` и техническую задержку, не расходуя бюджет трех попыток обработки.

Durable direct exchange `payments.events` связан с durable очередью `payments.new` по routing key `payments.new`; `payments.dlx` связан с `payments.dlq` по routing key `payments.dlq`. В рабочем процессе один платежный subscriber и prefetch 1. Очередь использует `x-single-active-consumer`: при запуске второго worker активным остается один обработчик, второй ждет. Relay — фоновая задача того же процесса.

Сохраненные события доставляются **at least once** при последующем восстановлении инфраструктуры: авария после confirm до фиксации `published_at` допускает повторную публикацию. Consumer распознает уже выполненные переходы по состоянию БД. Недоступность БД не подтверждает незавершенное сообщение; worker восстанавливает соединение с задержкой.

HTTP-вызов и транзакцию БД нельзя объединить. Авария после ответа `2xx`, но до сохранения `delivered_at`, допускает повтор webhook с тем же `event_id` в той же логической попытке. Поэтому при произвольных авариях число физических HTTP-запросов может быть больше трех. При постоянно недоступном получателе доставка исчерпывается в DLQ; гарантировать успешное HTTP-получение при конечном бюджете попыток нельзя.

Поврежденные сообщения помещаются в диагностический карантин DLQ. Автоматический возврат DLQ в `payments.new` не выполняется.

### Посмотреть очереди и DLQ

Откройте [RabbitMQ Management](http://localhost:15672), войдите с учетными данными из `.env`, выберите **Queues and Streams** → `payments.dlq`. Для просмотра используйте **Get messages** с режимом **Nack message requeue true**: сообщение останется в очереди. В **Exchanges** видны `payments.events` и `payments.dlx` с bindings.

```powershell
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT id, status, current_attempt, webhook_delivered_at, exhausted_at, last_error FROM payments ORDER BY created_at DESC LIMIT 10;"'
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT payment_id, event_type, attempt_no, routing_key, available_at, published_at, publish_attempts FROM outbox ORDER BY created_at DESC LIMIT 10;"'
```

При разборе DLQ сначала проверьте ошибку, состояние платежа и приемника. Повторная отправка исходного сообщения исчерпанного платежа сама по себе не сбрасывает попытки. Административное возобновление обработки требует отдельного осознанного изменения состояния; endpoint для такого действия в задании не предусмотрен.

### Проверить отключение брокера и рестарт

```powershell
docker compose stop rabbitmq
# Создайте платеж через POST с новым Idempotency-Key: он сохранится с 202.
docker compose start rabbitmq
# После восстановления посмотрите GET, журнал приемника и outbox.
docker compose restart consumer
```

Остановка broker не удаляет outbox. Рестарт consumer не сбрасывает попытки и сохраненный результат. Named volumes сохраняют данные PostgreSQL и RabbitMQ между обычными остановками и перезапусками.

Для автоматической проверки восстановления на запущенном Compose с профилем `demo`:

В командах ниже `$apiUrl` — адрес из примера создания платежа; в новом терминале можно передать адрес напрямую, например `--api http://localhost:18000`.

```powershell
uv run --frozen python -m scripts.resilience --api $apiUrl
```

Скрипт временно останавливает RabbitMQ проекта `async-payments`, создает новый платеж и проверяет `202`/`pending`, восстанавливает брокер и дожидается доставки. Затем повторяет сценарий с остановленным consumer. После перезапуска PostgreSQL проверяет готовность существующего API и доставку нового платежа существующим consumer, без их перезапуска. Отдельный последующий рестарт API/consumer проверяет сохранность ID, результатов и дат доставки. Используются только новые ключи; существующие платежи, очередь и журнал приемника не очищаются. При ошибке или прерывании скрипт пытается запустить остановленные им сервисы. Запускайте отдельно от других проверок, поскольку они используют те же контейнеры.

## Настройки

| Переменная | Значение по умолчанию / назначение |
| --- | --- |
| `API_KEY` | Обязательный статический API-ключ |
| `DATABASE_URL` | Async URL PostgreSQL; обязателен в `.env` для Compose, при прямом запуске Python по умолчанию локальная БД `payments` |
| `RABBITMQ_URL` | AMQP URL; обязателен в `.env` для Compose, при прямом запуске Python по умолчанию локальный брокер с пользователем `payments` |
| `RETRY_BASE_DELAY` | `5`, базовая задержка retry в секундах |
| `WEBHOOK_TIMEOUT` | `10`, общий предел HTTP-запроса |
| `OUTBOX_POLL_INTERVAL` | `0.5`, интервал проверки готовых событий |
| `PUBLISH_TIMEOUT` | `5`, timeout публикации в брокер |
| `RECOVERY_DELAY` | `2`, пауза перед восстановлением worker |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2` / `5`, границы задержки эмуляции |
| `LOG_LEVEL` | `INFO` |
| `API_PORT` | `8000`, порт API на хосте в Compose |
| `WEBHOOK_RECEIVER_PORT` | `8080`, порт демонстрационного приемника на хосте |
| `POSTGRES_PORT` / `RABBITMQ_PORT` / `RABBITMQ_MANAGEMENT_PORT` | `5432` / `5672` / `15672`, инфраструктурные порты на хосте |

Измененные настройки применяются при пересоздании приложения: `docker compose up -d api consumer`. Логи содержат идентификаторы и этап обработки; секреты и полный webhook URL в них не записываются.

## Тесты и локальная разработка

Тесты работают с настоящими PostgreSQL и RabbitMQ для транзакций, конкуренции и маршрутизации. Отдельный Compose-проект тестов не использует volumes демонстрационного сервиса и не публикует инфраструктурные порты на хост:

```powershell
docker compose -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests tests
docker compose -f compose.test.yaml down -v
```

Контейнер `tests` сначала применяет миграции, затем запускает pytest; он использует Docker target `test` с зависимостями для разработки. Интеграционные тесты создают отдельную временную схему PostgreSQL и удаляют только ее. Отсутствие `TEST_DATABASE_URL`/`TEST_RABBITMQ_URL` вне этого окружения приводит к пропуску соответствующих интеграционных тестов.

Для запуска на хосте нужен Python 3.12+ и [uv](https://docs.astral.sh/uv/getting-started/installation/):

```powershell
uv sync --frozen
uv run --frozen pytest -q
uv run --frozen ruff check .
```

После запуска Compose с профилем `demo` можно проверить HTTP-сценарии одной командой:

```powershell
uv run --frozen python -m scripts.smoke --api $apiUrl
```

Проверка создает три новых платежа с уникальными ключами; проверяет авторизацию, 20 конкурентных повторов, конфликт ключа, доставку `/webhook`, три HTTP-запроса `/flaky` и исчерпание `/fail`, стабильность `event_id` и интервалы не меньше 5/10 секунд. Она читает состояние платежей и журнал приемника, не очищая существующие записи. Содержимое DLQ дополнительно можно посмотреть в Management. При измененных портах используйте `--api`/`--receiver`; для локального worker задайте `--webhook-target http://localhost:8080`.

Результаты выполненных проверок и матрица покрытия требований приведены в [VERIFICATION.md](VERIFICATION.md).

Архив исходников и его SHA-256 собираются командой `python -m scripts.package` в каталоге `output`. Состав задан явно: приложение, миграции, тесты, примеры, служебные скрипты, конфигурация, README и отчет о проверках.

Для локального API/consumer сначала поднимите инфраструктуру `docker compose up -d --wait postgres rabbitmq`, затем задайте URL с `localhost` вместо имен контейнеров. При измененных портах/credentials подставьте значения своего `.env`:

```powershell
$env:API_KEY = $apiKey
$env:DATABASE_URL = 'postgresql+asyncpg://payments:payments@localhost:5432/payments'
$env:RABBITMQ_URL = 'amqp://payments:payments@localhost:5672/'
uv run --frozen alembic upgrade head
uv run --frozen uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8000
# В другом терминале с теми же переменными:
uv run --frozen python -m app.worker
```

Для интеграционных тестов на хосте также задайте `TEST_DATABASE_URL` и `TEST_RABBITMQ_URL`. Не запускайте локальный worker одновременно с Compose consumer для одной демонстрации. Webhook для локального worker должен указывать на адрес, доступный с хоста, например `http://localhost:8080/webhook`.

## Остановка и ограничения

`docker compose down` останавливает сервисы с сохранением named volumes. **`docker compose down -v` удаляет все платежи и состояние RabbitMQ этого Compose-проекта**; используйте только для осознанной очистки локальных данных. После очистки следующий запуск снова применит миграции.

Сервис использует эмулятор, реальных списаний нет. Статический API-ключ представляет одного клиента; ключи идемпотентности не имеют срока истечения. Последовательная обработка одного consumer выбрана по ТЗ; внешние шлюз, подпись webhook, административный redrive, мониторинг и горизонтальное масштабирование здесь не реализованы.
