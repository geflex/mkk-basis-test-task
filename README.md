# Payment Processing Service

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
                 │   Client   │──────────────────────────────────────────────┐
                 └─────┬──────┘                                              │
                       │ 6. webhook                                          ▼
                       │                                          ┌───────────────────┐
                       │                                          │        API        │
                       │                                          │  (FastAPI +       │
                       │                                          │   outbox relay)   │
                       │                                          └─────────┬─────────┘
                       │                      2. payment + outbox event      │
                       │                         (one transaction)           ▼
                       │                                          ┌───────────────────┐
                       │                                          │    PostgreSQL     │
                       │                                          │ payments, outbox  │
                       │                                          └─────────┬─────────┘
                       │                          3. relay: publish         │
                       │                                          ┌─────────▼─────────┐
                       │                                          │   RabbitMQ        │
                       └──────────────────────────────────────────│ payments.new      │
                                                                  │ payments.retry.N  │
                                                                  │ payments.dlq      │
                                                                  └─────────┬─────────┘
                                                                            │ 4. consume
                                                                  ┌─────────▼─────────┐
                                                                  │     Consumer      │
                                                                  │  emulate → save   │
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

Поднимаются четыре сервиса: `postgres`, `rabbitmq`, `api` (порт `8000`) и
`consumer`. Миграции применяются автоматически при старте `api`.

* Swagger UI: http://localhost:8000/docs
* RabbitMQ Management: http://localhost:15672 (guest / guest)

### Примеры запросов

```bash
API=http://localhost:8000
KEY=super-secret-api-key

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
```

По умолчанию webhook-адреса на приватные и loopback-хосты отклоняются (защита
от SSRF). Для локальной отладки запустите сервис с
`ALLOW_PRIVATE_WEBHOOK_HOSTS=true`.

## Локальный запуск без Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# поднимите postgres и rabbitmq (например, docker compose up postgres rabbitmq)
export DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
export API_KEY=super-secret-api-key

alembic upgrade head                 # миграции
uvicorn app.main:app --reload        # API
faststream run app.consumer:app      # consumer (в отдельном терминале)
```

## Тесты

```bash
pip install -r requirements-dev.txt
pytest
```

Юнит-тесты не требуют БД и брокера: проверяются валидация схем, логика
retry/DLQ и доставка webhook.

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | `super-secret-api-key` | статический ключ для заголовка `X-API-Key` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | DSN БД |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | DSN RabbitMQ |
| `PAYMENTS_EXCHANGE` | `payments` | имя exchange |
| `PAYMENTS_NEW_QUEUE` | `payments.new` | основная очередь |
| `PAYMENTS_DLQ_QUEUE` | `payments.dlq` | dead letter queue |
| `RETRY_ROUTING_PREFIX` | `payments.retry` | префикс retry-очередей |
| `BASE_RETRY_DELAY_MS` | `2000` | базовая задержка retry (далее ×2) |
| `OUTBOX_POLL_INTERVAL` | `1.0` | период опроса outbox, сек |
| `OUTBOX_BATCH_SIZE` | `100` | размер батча outbox |
| `MAX_ATTEMPTS` | `3` | попыток обработки до DLQ |
| `PROCESSING_MIN_SECONDS` | `2` | мин. время эмуляции, сек |
| `PROCESSING_MAX_SECONDS` | `5` | макс. время эмуляции, сек |
| `SUCCESS_RATE` | `0.9` | вероятность успеха эмуляции |
| `WEBHOOK_TIMEOUT` | `10` | таймаут webhook, сек |
| `WEBHOOK_MAX_RETRIES` | `3` | локальных попыток отправки webhook |
| `WEBHOOK_RETRY_DELAY` | `1` | базовая задержка webhook-ретраев, сек |
| `ALLOW_PRIVATE_WEBHOOK_HOSTS` | `false` | разрешить webhook на приватные/loopback-адреса (только для локальной отладки) |

## Структура проекта

```
app/
├── main.py          # FastAPI app + запуск outbox-релея в lifespan
├── consumer.py      # FastStream consumer: обработка, retry, DLQ
├── api/payments.py  # эндпоинты создания и получения платежа
├── broker.py        # топология RabbitMQ и общий брокер
├── outbox.py        # reley транзакционного outbox
├── webhook.py       # доставка webhook с ретраями
├── repositories.py  # доступ к данным
├── models.py        # ORM-модели payments и outbox
├── schemas.py       # Pydantic-схемы
├── security.py      # проверка X-API-Key
├── config.py        # настройки из окружения
└── db.py            # async-движок и сессии
alembic/             # миграции
docker/              # entrypoint-скрипты
examples/            # приёмник webhook для ручной проверки
tests/               # юнит-тесты
```

## Принятые решения

* **Outbox пишется в одной транзакции с платежом** — это единственный способ
  гарантировать, что «создан платёж» и «опубликовано событие» происходят
  атомарно.
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
