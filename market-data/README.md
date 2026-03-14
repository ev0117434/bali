# market-data

WebSocket-коллекторы рыночных данных и мониторы. Каждый скрипт — самодостаточный процесс.

## Содержание

- [Файлы](#файлы)
- [Конфигурация](#конфигурация)
- [Архитектура](#архитектура)
- [Redis: формат данных](#redis-формат-данных)
- [Логирование](#логирование)
- [Обработка ошибок](#обработка-ошибок)
- [Тесты](#тесты)
- [Production](#production)

---

## Файлы

```
market-data/
├── common.py           # разделяемые утилиты (логи, Redis, символы)
├── binance_spot.py     # Binance Spot bookTicker → Redis
├── binance_futures.py  # Binance Futures bookTicker → Redis
├── bybit_spot.py       # Bybit Spot tickers → Redis
├── bybit_futures.py    # Bybit Linear Futures tickers → Redis
├── stale_monitor.py    # обнаружение устаревших символов
├── latency_monitor.py  # мониторинг задержек pipeline
├── requirements.txt
├── .env.example
└── tests/
    ├── conftest.py
    ├── test_common.py
    ├── test_binance_parser.py
    ├── test_bybit_parser.py
    ├── test_stale_monitor.py
    ├── test_latency_monitor.py
    ├── test_redis_write.py
    └── acceptance_check.sh
```

---

## Конфигурация

Все параметры читаются из переменных окружения. Создать `.env` из примера:

```bash
cp .env.example .env
```

| Переменная | По умолчанию | Описание |
|-----------|-------------|---------|
| `REDIS_URL` | `redis://localhost:6379/0` | Подключение к Redis |
| `REDIS_KEY_TTL` | `300` | TTL ключей в секундах. Должен быть > `STALE_THRESHOLD_SEC` |
| `SYMBOLS_PER_CONN` | `150` | Символов на одно WS-соединение (лимит биржи 200) |
| `WS_RECV_TIMEOUT` | `60` | Таймаут получения данных (сек), после которого reconnect |
| `RECONNECT_MAX_DELAY` | `60` | Максимальная задержка backoff (сек) |
| `WS_PING_INTERVAL` | `30` | Интервал ping (Binance=30, Bybit=20, задаётся per-script) |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` |
| `STALE_THRESHOLD_SEC` | `60` | Возраст данных (сек), после которого символ считается stale |
| `SCAN_INTERVAL_SEC` | `30` | Интервал сканирования stale_monitor |
| `SCAN_COUNT` | `500` | Batch size для Redis SCAN |
| `SAMPLING_INTERVAL_SEC` | `10` | Интервал отчёта latency_monitor |
| `SAMPLING_MAX_KEYS` | `200` | Ключей за один шаг SCAN (rolling cursor) |
| `ANOMALY_WARN_MS` | `1000` | Порог WARNING для end-to-end задержки |
| `ANOMALY_CRIT_MS` | `5000` | Порог CRITICAL для end-to-end задержки |

---

## Архитектура

### Жизненный цикл коллектора

```
main()
 ├── load_symbols(file)           # выход если файл пустой
 ├── symbols → chunks[150]        # по 150 символов на соединение
 ├── shutdown_event = Event()
 ├── SIGTERM/SIGINT → shutdown_event.set()
 └── asyncio.gather(ws_worker × N)

ws_worker(chunk, conn_id)
 └── loop:
      ├── websockets.connect()
      ├── subscribe(symbols)
      ├── ping_task (Bybit: каждые 20 сек)
      ├── read loop:
      │    ├── wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
      │    ├── orjson.loads()
      │    ├── parse_message()   → None если skip
      │    └── write_redis()     → pipeline: HSET + EXPIRE + PUBLISH
      └── on error: backoff и reconnect
```

### Reconnect backoff

```python
delay = min(2 ** attempt, 60) + random.uniform(0, 1)
# attempt=0 → 1–2 сек
# attempt=5 → 32–33 сек
# attempt≥6 → 60–61 сек (максимум)
```

Счётчик `attempt` сбрасывается после первого успешного сообщения от биржи.

### Буфер при Redis down

При недоступности Redis данные помещаются в `deque(maxlen=1000)`. При следующей успешной записи буфер сбрасывается в Redis. При переполнении (1000 элементов) старые данные вытесняются с логом `redis_buffer_full`.

---

## Redis: формат данных

### Ключ

```
md:{exchange}:{market}:{symbol}

Примеры:
  md:binance:spot:BTCUSDT
  md:binance:futures:ETHUSDT
  md:bybit:spot:SOLUSDT
  md:bybit:futures:BTCUSDT
```

### Поля Hash

| Поле | Тип | Описание |
|------|-----|---------|
| `bid` | str | Best bid price |
| `bid_qty` | str | Best bid quantity |
| `ask` | str | Best ask price |
| `ask_qty` | str | Best ask quantity |
| `last` | str | Last trade price (пусто у Binance) |
| `ts_exchange` | str (ms) | Timestamp от биржи (`"0"` для Binance Spot — не передаётся) |
| `ts_received` | str (ms) | Timestamp получения WS-сообщения коллектором |
| `ts_redis` | str (ms) | Timestamp записи в Redis |

TTL ключа: `REDIS_KEY_TTL` секунд (по умолчанию 300). Ключ автоматически удаляется при отсутствии обновлений.

### Pub/Sub каналы

```
md:updates:{exchange}:{market}   # новое обновление (payload: {"symbol": "...", "key": "..."})
alerts:stale                     # от stale_monitor: {"ts":..., "stale_count":..., "symbols":[...]}
```

### Пример чтения

```bash
redis-cli HGETALL md:binance:spot:BTCUSDT
# Вернёт 8 полей с актуальными данными

redis-cli TTL md:binance:spot:BTCUSDT
# Вернёт оставшееся время жизни ключа (сек)
```

---

## Специфика бирж

### Binance Spot (`binance_spot.py`)

- **URL:** `wss://stream.binance.com:9443/ws`
- **Подписка:** `{"method":"SUBSCRIBE","params":["btcusdt@bookTicker",...],"id":N}`
- **Символы в params:** lowercase + `@bookTicker`
- **Ping:** websockets обрабатывает автоматически (`ping_interval=30`)
- **ts_exchange:** всегда `"0"` — bookTicker Spot не содержит timestamp биржи

### Binance Futures (`binance_futures.py`)

- **URL:** `wss://fstream.binance.com/ws`
- **Отличие от Spot:** поле `E` (event time ms) → `ts_exchange`
- Всё остальное идентично Spot

### Bybit Spot (`bybit_spot.py`)

- **URL:** `wss://stream.bybit.com/v5/public/spot`
- **Подписка:** `{"op":"subscribe","args":["tickers.BTCUSDT",...]}`
- **Ping:** отдельная coroutine, каждые 20 сек `{"op":"ping"}`, ответ `{"op":"pong"}`
- **ts_exchange:** поле `ts` из внешней обёртки сообщения
- Обрабатываются оба типа: `snapshot` и `delta`

### Bybit Futures (`bybit_futures.py`)

- **URL:** `wss://stream.bybit.com/v5/public/linear`
- Формат данных и логика идентичны Bybit Spot

---

## Логирование

Все скрипты пишут структурированные JSON-логи в **stdout**. Формат:

```json
{
  "timestamp": "2026-03-14T12:00:00.123456Z",
  "level": "info",
  "event": "ws_connected",
  "service": "binance_spot",
  "conn_id": 0,
  "symbols_count": 150
}
```

При запуске через `run.py` логи также дублируются в файлы `logs/<script>.log`.

### Обязательные события

| event | level | Когда |
|-------|-------|-------|
| `collector_started` | INFO | Запуск: сколько символов, сколько соединений |
| `ws_connected` | INFO | WS-соединение установлено |
| `ws_subscribed` | INFO | Подписка подтверждена биржей |
| `ws_disconnected` | WARNING | Соединение разорвано (reason, code) |
| `ws_reconnecting` | WARNING | Попытка reconnect (attempt, delay_sec, error) |
| `ws_recv_timeout` | WARNING | Нет данных за WS_RECV_TIMEOUT секунд |
| `ws_error` | ERROR | Исключение в WS-цикле |
| `redis_error` | ERROR | Ошибка записи в Redis |
| `redis_recovered` | INFO | Redis снова доступен, буфер сброшен |
| `redis_buffer_full` | WARNING | Буфер переполнен, старые данные вытеснены |
| `message_parse_error` | WARNING | Не удалось распарсить WS-сообщение |
| `stale_detected` | WARNING | Символ не обновлялся > STALE_THRESHOLD_SEC сек |
| `stale_recovered` | INFO | Символ вернулся в норму |
| `stale_scan_complete` | INFO | Цикл сканирования: total_keys, stale_count |
| `latency_report` | INFO | Агрегаты задержек за цикл (min/max/avg/p95) |
| `latency_anomaly` | WARNING/CRITICAL | end-to-end задержка > порога |
| `clock_skew_detected` | INFO | ts_received < ts_exchange (норма при расхождении часов) |
| `graceful_shutdown` | INFO | Получен SIGTERM/SIGINT, завершение |

### Правило: данные в логах не пишутся

В логах никогда нет bid/ask/price. Только метаинформация: имена, счётчики, задержки.

---

## Мониторы

### stale_monitor.py

Каждые `SCAN_INTERVAL_SEC` (30) секунд сканирует Redis и находит ключи, `ts_redis` которых старше `STALE_THRESHOLD_SEC` (60) секунд.

**Grace period:** первые 2 цикла (60 сек) после старта — алерты не генерируются. Коллекторам нужно время на заполнение Redis.

**Recovery detection:** если символ был в списке stale и пропал — лог `stale_recovered`.

**Pub/Sub алерт** в канал `alerts:stale`:
```json
{
  "ts": 1710412800000,
  "stale_count": 2,
  "symbols": [
    {"key": "md:binance:spot:XYZUSDT", "age_sec": 95},
    {"key": "md:bybit:futures:ABCUSDT", "age_sec": 72}
  ]
}
```

### latency_monitor.py

Каждые `SAMPLING_INTERVAL_SEC` (10) секунд делает шаг SCAN с rolling cursor (не сбрасывается на 0 — все ключи покрываются равномерно).

**Метрики per key:**

| Метрика | Формула | Что показывает |
|---------|---------|---------------|
| `exchange_to_collector` | `ts_received − ts_exchange` | Сетевая задержка (с учётом clock skew) |
| `collector_to_redis` | `ts_redis − ts_received` | Время обработки + записи |
| `end_to_end` | `ts_redis − ts_exchange` | Полная задержка |

Для Binance Spot (`ts_exchange == "0"`) `end_to_end` и `exchange_to_collector` не вычисляются — данных о времени биржи нет.

**Отчёт `latency_report`** (per exchange:market):
```json
{
  "event": "latency_report",
  "exchange": "binance",
  "market": "futures",
  "samples": 150,
  "e2e_ms": {"min": 1.2, "max": 45.3, "avg": 4.8, "p95": 12.1},
  "collector_to_redis_ms": {"min": 0.3, "max": 8.1, "avg": 1.2, "p95": 3.4},
  "anomalies": 0
}
```

---

## Обработка ошибок

| Ситуация | Действие | Лог |
|----------|----------|-----|
| WS disconnect | Reconnect с exponential backoff | WARNING `ws_reconnecting` |
| WS recv timeout | Reconnect | WARNING `ws_recv_timeout` |
| JSON parse error | Skip сообщение | WARNING `message_parse_error` |
| Неизвестный формат | Skip сообщение | WARNING (молча, нет лога) |
| Redis down | Буфер до 1000 записей, retry при следующей записи | ERROR `redis_error` |
| Redis recovered | Сброс буфера в Redis | INFO `redis_recovered` |
| Буфер переполнен | DROP oldest | WARNING `redis_buffer_full` |
| Файл символов не найден | Exit(1) | CRITICAL |
| Файл символов пустой | Exit(1) | CRITICAL |

---

## Тесты

```bash
# Все тесты (102 штуки, fakeredis — реальный Redis не нужен)
python3 -m pytest tests/ -v

# С покрытием
python3 -m pytest tests/ --cov=. --cov-report=term-missing

# Acceptance check (33 автоматических критерия)
bash tests/acceptance_check.sh
```

### Покрытие по модулям

| Модуль | Тестов | Что тестируется |
|--------|--------|----------------|
| `test_common.py` | 18 | `load_symbols` edge cases, `now_ms`, structlog binding |
| `test_binance_parser.py` | 18 | Парсер Spot/Futures, reconnect backoff формула |
| `test_bybit_parser.py` | 17 | Snapshot/delta, ping/pong, все граничные случаи |
| `test_stale_monitor.py` | 9 | Grace period, recovery, pub/sub формат алерта |
| `test_latency_monitor.py` | 20 | `calc_stats`, `parse_latencies`, пороги аномалий |
| `test_redis_write.py` | 20 | Формат ключей, 8 полей, TTL, pub/sub, буфер |

---

## Production

### systemd (рекомендуется)

Создать unit-файл для каждого скрипта, например `/etc/systemd/system/binance-spot.service`:

```ini
[Unit]
Description=Binance Spot Market Data Collector
After=redis.service network-online.target
Wants=network-online.target

[Service]
Type=simple
User=marketdata
WorkingDirectory=/opt/bali/market-data
EnvironmentFile=/opt/bali/market-data/.env
ExecStart=/usr/bin/python3 /opt/bali/market-data/binance_spot.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable binance-spot
systemctl start binance-spot
journalctl -u binance-spot -f
```

### supervisord

```ini
[program:binance_spot]
command=python3 /opt/bali/market-data/binance_spot.py
directory=/opt/bali/market-data
autostart=true
autorestart=true
startsecs=5
stderr_logfile=/var/log/market-data/binance_spot.err
stdout_logfile=/var/log/market-data/binance_spot.log
environment=REDIS_URL="redis://localhost:6379/0"
```

### Мониторинг логов

```bash
# Все логи в реальном времени
tail -f logs/*.log

# Только ошибки
tail -f logs/*.log | grep '"level":"error\|critical"'

# JSON парсинг
tail -f logs/binance_spot.log | python3 -m json.tool

# Через jq: только stale алерты
tail -f logs/stale_monitor.log | jq 'select(.event == "stale_detected")'

# Latency статистика
tail -f logs/latency_monitor.log | jq 'select(.event == "latency_report") | .e2e_ms'
```

### Проверка Redis

```bash
# Данные по конкретному символу
redis-cli HGETALL md:binance:spot:BTCUSDT

# Количество ключей по бирже
redis-cli KEYS "md:binance:*" | wc -l

# Подписка на обновления
redis-cli SUBSCRIBE md:updates:binance:spot

# Подписка на stale алерты
redis-cli SUBSCRIBE alerts:stale
```

---

## Зависимости

| Пакет | Версия | Назначение |
|-------|--------|-----------|
| `websockets` | ≥12.0 | Асинхронные WebSocket соединения |
| `redis[hiredis]` | ≥5.0 | Redis клиент с быстрым C-парсером |
| `orjson` | ≥3.9 | Быстрый JSON (сериализация/десериализация) |
| `structlog` | ≥24.0 | Структурированные JSON логи |
| `python-dotenv` | ≥1.0 | Загрузка `.env` файла |
| `pytest` | ≥8.0 | Фреймворк тестирования |
| `pytest-asyncio` | ≥0.23 | Поддержка async тестов |
| `fakeredis` | ≥2.20 | Моки Redis для тестов |
| `pytest-cov` | ≥5.0 | Покрытие кода |
