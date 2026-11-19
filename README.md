# Сервис процессинга платежей

Асинхронный микросервис процессинга платежей. Принимает запросы на оплату,
обрабатывает их через эмуляцию внешнего платёжного шлюза и уведомляет клиента
о результате через webhook.

## Стек

| Компонент | Технология |
|---|---|
| HTTP API | FastAPI + Pydantic v2 |
| Обработка сообщений | FastStream (RabbitMQ / aio-pika) |
| БД | PostgreSQL + SQLAlchemy 2.0 (async, asyncpg) |
| Миграции | Alembic |
| Инфраструктура | Docker + docker-compose |

## Архитектура

```
                 ┌────────────┐  1. POST /payments (X-API-Key, Idempotency-Key)
                 │   Клиент   │──────────────────────────────────────────────┐
                 └─────┬──────┘                                              │
                       │ 6. webhook                                          ▼
                       │                                          ┌───────────────────┐
                       │                                          │        API        │
                       │                                          │  (FastAPI +       │
                       │                                          │   outbox relay)   │
                       │                                          └─────────┬─────────┘
                       │                      2. платёж + событие outbox    │
                       │                         (одна транзакция)          ▼
                       │                                          ┌───────────────────┐
                       │                                          │    PostgreSQL     │
                       │                                          │ payments, outbox  │
                       │                                          └─────────┬─────────┘
                       │                          3. relay: публикация      │
                       │                                          ┌─────────▼─────────┐
                       │                                          │   RabbitMQ        │
                       └──────────────────────────────────────────│ payments.new      │
                                                                  │ payments.retry.N  │
                                                                  │ payments.dlq      │
                                                                  └─────────┬─────────┘
                                                                            │ 4. потребление
                                                                  ┌─────────▼─────────┐
                                                                  │   Потребитель     │
                                                                  │  эмуляция → save  │
                                                                  │  → webhook → retry│
                                                                  └───────────────────┘
```

### Жизненный цикл платежа

1. **Создание.** `POST /api/v1/payments` в одной транзакции пишет строку в
   `payments` и событие в `outbox`. Ответ `202 Accepted` возвращается сразу —
   клиент не ждёт обработки.
2. **Публикация (Outbox).** Фоновая задача в процессе API опрашивает таблицу
   `outbox`, публикует неотправленные события в RabbitMQ и помечает их
   опубликованными. Это гарантирует, что событие не потеряется, даже если
   RabbitMQ недоступен в момент создания платежа.
3. **Обработка.** Consumer читает сообщение из `payments.new`, эмулирует
   обработку шлюзом (2–5 сек, ~90% успех), сохраняет финальный статус
   (`succeeded` / `failed`) и отправляет webhook.
4. **Повторные попытки.** Любая ошибка (в т.ч. недоставленный webhook)
   приводит к повтору с экспоненциальной задержкой. После исчерпания попыток
   сообщение уходит в DLQ.

   На стыке «опубликовал retry → подтвердил оригинал» гарантия доставки —
   **at-least-once**: эти два действия не атомарны, поэтому падение процесса
   в этом окне может дать повторную доставку. Сообщения при этом не теряются,
   а consumer идемпотентен — для уже финального статуса эмуляция не
   запускается повторно, отправляется только webhook.

### Идемпотентность

* **На уровне API** — заголовок `Idempotency-Key` обязателен. Повторный запрос
  с тем же ключом возвращает уже созданный платёж (тот же `payment_id`) и
  помечает ответ заголовком `Idempotency-Replayed: true`. Если тот же ключ
  приходит с другим телом, возвращается `409 Conflict`. Защита работает и
  при гонке: уникальный индекс в БД ловится и запрос переиспользует платеж.
* **На уровне consumer** — перед эмуляцией проверяется статус платежа. Если он
  уже финальный (повторная доставка сообщения), эмуляция пропускается и
  повторно отправляется только webhook.

### Топология RabbitMQ

Direct-exchange `payments`:

| Очередь | Routing key | Назначение |
|---|---|---|
| `payments.new` | `payments.new` | основная очередь обработки |
| `payments.retry.1` | `payments.retry.1` | задержка 2 с, DLX обратно в `payments.new` |
| `payments.retry.2` | `payments.retry.2` | задержка 4 с, DLX обратно в `payments.new` |
| `payments.dlq` | `payments.dlq` | письма, не обработанные за `MAX_ATTEMPTS` попыток |

Retry-очереди — обычные очереди с `x-message-ttl` и
`x-dead-letter-exchange`/`x-dead-letter-routing-key`, указывающими назад в
`payments.new`. Отдельная очередь на каждую попытку даёт экспоненциальный
backoff без эффекта «head-of-line blocking». Номер попытки переносится в
заголовке `x-attempt` и сохраняется при dead-letter.

## Быстрый старт (Docker)

```bash
cp .env.example .env          # при необходимости поменяйте API_KEY
docker compose up --build
```

Поднимаются сервисы: `postgres`, `rabbitmq`, `api` (порт `8000`),
`consumer` и одноразовый `migrate`. Миграции применяются сервисом `migrate`,
который завершается до старта `api` и `consumer`. Образ собирается через `uv`
(зависимости фиксируются по `uv.lock`), запускается от непривилегированного
пользователя; все переменные из `.env.example` прокидываются в оба процесса
через единый набор в `docker-compose.yml`.

* Swagger UI: http://localhost:8000/docs
* RabbitMQ Management: http://localhost:15672 (guest / guest)

### Примеры запросов

```bash
API=http://localhost:8000
KEY=change-me-in-production

# Создание платежа (202 Accepted)
curl -s -X POST $API/api/v1/payments \
  -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: order-123" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "149.90",
        "currency": "RUB",
        "description": "Оплата заказа 123",
        "metadata": {"order_id": 123, "user_id": 7},
        "webhook_url": "https://example.com/webhooks/payments"
      }'
# => {"payment_id":"...","status":"pending","created_at":"..."}

# Повтор с тем же Idempotency-Key вернёт тот же платёж
# (в ответе будет заголовок Idempotency-Replayed: true)

# Получение информации о платеже
curl -s $API/api/v1/payments/<payment_id> -H "X-API-Key: $KEY"
```

Для локальной проверки webhook-уведомлений есть готовый приёмник:

```bash
python examples/webhook_receiver.py --port 9000
# затем укажите webhook_url=http://<host>:9000/webhook при создании платежа

# с проверкой подписи (должна совпадать с WEBHOOK_SECRET сервиса):
python examples/webhook_receiver.py --port 9000 --secret <WEBHOOK_SECRET>
```

Каждое уведомление содержит заголовок `X-Webhook-Id` (стабилен для пары
«платёж + статус», чтобы получатель мог дедуплицировать повторы) и, если
задан `WEBHOOK_SECRET`, заголовок `X-Webhook-Signature` вида
`sha256=<hex>` — HMAC-SHA256 от точного тела запроса.

По умолчанию webhook-адреса на приватные и loopback-хосты отклоняются (защита
от SSRF). Для локальной отладки запустите сервис с
`ALLOW_PRIVATE_WEBHOOK_HOSTS=true`.

### Health-check

* `GET /health/live` — liveness: процесс жив, зависимости не проверяются.
* `GET /health/ready` — readiness: `SELECT 1` к PostgreSQL и проверка
  соединения с RabbitMQ. Возвращает `503`, если хотя бы одна зависимость
  недоступна. Именно этот эндпоинт используется в `healthcheck` сервиса `api`.
* `GET /health` — сохранён для обратной совместимости, эквивалентен
  `/health/live`.

## Локальный запуск без Docker

Основной способ — [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --group dev

# поднимите postgres и rabbitmq (например, docker compose up postgres rabbitmq)
export DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
export API_KEY=change-me-in-production

uv run alembic upgrade head                    # миграции
uv run uvicorn app.api.app:app --reload         # API
uv run faststream run app.workers.payment_consumer:app  # consumer (во втором терминале)
```

`requirements.txt` и `requirements-dev.txt` оставлены как fallback для машин,
где `uv` не установлен. Они зафиксированы по текущему `uv.lock`; при изменении
зависимостей предпочтительно обновлять `pyproject.toml`/`uv.lock`:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

alembic upgrade head
uvicorn app.api.app:app --reload
faststream run app.workers.payment_consumer:app
```

## Тесты

```bash
uv sync --group dev
uv run pytest
```

Если `uv` нет — `pip install -r requirements-dev.txt && pytest`.

Юнит-тесты не требуют БД и брокера: проверяются валидация схем, логика
retry/DLQ и доставка webhook.

### E2E-тесты

Помимо юнит-тестов есть e2e-набор, который поднимает реальные PostgreSQL и
RabbitMQ через [`testcontainers`](https://testcontainers-python.readthedocs.io/),
запускает приложение и встроенный HTTP-приёмник webhook и проверяет полный цикл
`POST -> outbox -> RabbitMQ -> обработка -> webhook`:

```bash
uv sync --group dev
uv run pytest -m e2e tests/e2e
```

Требуется запущенный Docker. Набор исключён из обычного прогона `pytest`
(в `addopts` стоит `-m "not e2e"`).

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | — (обязательно) | статический ключ для заголовка `X-API-Key` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | DSN БД |
| `DATABASE_POOL_SIZE` | `10` | размер пула соединений |
| `DATABASE_MAX_OVERFLOW` | `20` | максимальный overflow пула |
| `DATABASE_POOL_RECYCLE` | `1800` | время жизни соединения, сек |
| `DATABASE_COMMAND_TIMEOUT` | `30` | таймаут команды asyncpg, сек |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | DSN RabbitMQ |
| `PAYMENTS_EXCHANGE` | `payments` | имя exchange |
| `PAYMENTS_NEW_QUEUE` | `payments.new` | основная очередь |
| `PAYMENTS_DLQ_QUEUE` | `payments.dlq` | dead letter queue |
| `RETRY_ROUTING_PREFIX` | `payments.retry` | префикс retry-очередей |
| `BASE_RETRY_DELAY_MS` | `2000` | базовая задержка retry (далее ×2) |
| `OUTBOX_POLL_INTERVAL` | `1.0` | период опроса outbox, сек |
| `OUTBOX_BATCH_SIZE` | `100` | размер батча outbox |
| `MAX_ATTEMPTS` | `3` | попыток обработки до DLQ (1–20) |
| `PROCESSING_MIN_SECONDS` | `2` | мин. время эмуляции, сек |
| `PROCESSING_MAX_SECONDS` | `5` | макс. время эмуляции, сек |
| `SUCCESS_RATE` | `0.9` | вероятность успеха эмуляции (0–1) |
| `WEBHOOK_TIMEOUT` | `10` | таймаут webhook, сек |
| `WEBHOOK_MAX_RETRIES` | `3` | локальных попыток отправки webhook (1–10) |
| `WEBHOOK_RETRY_DELAY` | `1` | базовая задержка webhook-ретраев, сек |
| `WEBHOOK_MAX_RESPONSE_BYTES` | `1048576` | максимальный размер ответа webhook |
| `WEBHOOK_SECRET` | — | общий секрет для HMAC-подписи `X-Webhook-Signature` |
| `ALLOW_PRIVATE_WEBHOOK_HOSTS` | `false` | разрешить webhook на приватные/loopback-адреса (только для локальной отладки) |
| `LOG_LEVEL` | `INFO` | уровень логирования |
| `LOG_JSON` | `false` | писать логи в JSON (в compose по умолчанию `true`) |

Значения валидируются на старте: например, `SUCCESS_RATE` вне `[0, 1]` или
`PROCESSING_MIN_SECONDS > PROCESSING_MAX_SECONDS` приводят к падению процесса,
а не к некорректной обработке.

## Структура проекта

```
app/
├── api/                      # HTTP-слой: сборка app, зависимости, роутеры
│   ├── app.py                # create_app(), lifespan, request-id middleware
│   ├── dependencies.py       # сборка сервисов на запрос
│   └── routers/payments.py   # тонкие эндпоинты
├── workers/
│   └── payment_consumer.py   # FastStream worker: retry/DLQ, без бизнес-логики
├── services/                 # бизнес-логика, не зависящая от HTTP/брокера
│   ├── payments.py           # создание, чтение, идемпотентность
│   ├── gateway.py            # Protocol шлюза + эмуляция
│   ├── processing.py         # сценарий обработки платежа
│   └── webhook.py            # доставка, подпись, снимок уведомления
├── messaging/                # RabbitMQ и outbox
│   ├── broker.py             # топология и общий брокер
│   └── outbox.py             # relay транзакционного outbox
├── db/                       # SQLAlchemy
│   ├── models.py             # ORM-модели payments и outbox
│   ├── repositories.py       # только доступ к данным
│   └── session.py            # движок, пул, сессии
├── domain/                   # ядро без внешних зависимостей
│   ├── enums.py              # PaymentStatus, Currency
│   ├── time.py               # utcnow
│   └── contracts.py          # Pydantic-контракты API и событий
├── core/                     # инфраструктура приложения
│   ├── config.py             # настройки по группам + валидация
│   ├── logging.py            # JSON/текст, request_id/payment_id
│   ├── security.py           # X-API-Key
│   └── net.py                # SSRF-проверка, включая DNS
└── __init__.py
alembic/                      # миграции
docker/                       # entrypoint-скрипты
examples/                     # приёмник webhook для ручной проверки
tests/                        # юнит-тесты (tests/e2e — e2e на testcontainers)
```

Направление зависимостей: `api`/`workers` → `services` → `db`/`messaging`/
`domain`; `domain` не импортирует ни ORM, ни FastAPI, ни брокер. Благодаря
этому consumer не знает про HTTP, а шлюз и webhook можно подменить в тестах
без реальных PostgreSQL/RabbitMQ.

## Принятые решения

* **Outbox пишется в одной транзакции с платежом** — это единственный способ
  гарантировать, что «создан платёж» и «опубликовано событие» происходят
  атомарно.
* **Эндпоинты и consumer — тонкие адаптеры.** Создание платежа, проверка
  идемпотентности и обработка вынесены в `services/payments.py` и
  `services/processing.py`; роутер и worker только транслируют вызовы.
* **Шлюз — это Protocol.** `EmulatedPaymentGateway` реализует тот же интерфейс,
  что и реальный провайдер, поэтому эмуляцию можно заменить в интеграционных
  тестах без правки consumer'а.
* **Настройки разбиты по группам** (`DatabaseSettings`, `BrokerSettings`,
  `WebhookSettings`, …) и валидируются при старте. Процесс API падает сразу,
  если не задан `API_KEY`, а не отвечает `401` на каждый запрос.
* **Один `httpx.AsyncClient` на процесс** создаётся в lifespan и переиспользуется
  всеми доставками; отправка защищена повторной SSRF-проверкой с DNS-резолвингом
  и экспоненциальным backoff с jitter.
* **Логи структурированы**, содержат `request_id` (из `X-Request-ID` или
  сгенерированный) и `payment_id`; включается JSON-формат через `LOG_JSON`.
* **Релей использует `SELECT ... FOR UPDATE SKIP LOCKED`** — можно безопасно
  запускать несколько экземпляров API.
* **Отдельная retry-очередь на каждую попытку** вместо одного TTL-гейта —
  корректный экспоненциальный backoff без блокировки «просроченных» сообщений.
* **Двойной слой ретраев**: локальные попытки webhook (`WEBHOOK_MAX_RETRIES`,
  чтобы пережить кратковременные сетевые сбои) и попытки на уровне брокера
  (`MAX_ATTEMPTS`, после которых — DLQ).
* **Повторная обработка идемпотентна**: сообщение может прийти повторно, но
  эмуляция не запускается второй раз для финального статуса — повторно
  отправляется только webhook. Строка платежа читается `SELECT ... FOR UPDATE`,
  поэтому при нескольких consumer'ах дубли доставки сериализуются.

## Что сознательно не делалось

* **Метрики и трейсинг** (Prometheus/OpenTelemetry) не добавлялись: в задании
  они не требовались. Точки расширения готовы — структурированные логи уже
  содержат контекст запроса и платежа.
* **`requirements.txt` / `requirements-dev.txt`** не удалялись и не
  генерируются автоматически: они нужны на машинах без `uv`. Источником истины
  остаются `pyproject.toml` и `uv.lock`.
