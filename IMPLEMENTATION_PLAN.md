# План реализации: Market Data Collector v2.1

## Критика ТЗ (важно перед стартом)

Документ хороший по духу, но имеет ряд конкретных проблем:

1. **Буфер при Redis down не специфицирован** — в принципах написано "буфер при Redis down",
   но нигде нет: размер, стратегия вытеснения (FIFO/DROP), поведение при переполнении.
   → **Решение:** in-memory deque(maxlen=1000), DROP_OLDEST при переполнении, метрика dropped_count в логах.

2. **WS_RECV_TIMEOUT есть в .env, но не описан в логике ws_worker** — без него
   зависание на "тихом" дисконнекте (TCP keepalive не сработал) невозможно детектировать.
   → **Решение:** `asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)` в основном цикле.

3. **Binance Spot: ts_exchange = ts_received → latency_monitor сломан для этого рынка** —
   exchange_to_collector всегда ≈ 0ms. ТЗ признаёт, но монитор не учитывает.
   → **Решение:** при ts_exchange == ts_received — пропускать эту метрику для данного ключа.

4. **REDIS_KEY_TTL=120 сек + STALE_THRESHOLD=60 сек = дыра** — если коллектор упал,
   через 120 сек ключи исчезнут из Redis (expired). stale_monitor не найдёт их вообще
   и не даст алерт. Мониторинг слепнет именно в момент худшего сбоя.
   → **Решение:** TTL нужен ≥ 300 сек, или stale_monitor должен хранить "ожидаемый" список символов.
   Выбираем: `REDIS_KEY_TTL=300` (default), документируем в .env.example.

5. **latency_monitor: лимит 200 ключей за цикл при 1630+ символах** — покрытие ~12% за цикл.
   SCAN не даёт гарантий распределения выборки.
   → **Решение:** использовать rolling SCAN cursor (сохранять между итерациями), не сбрасывать на 0.

6. **SYMBOLS_PER_CONN=150 дублируется** — в .env есть переменная, но в описании коллекторов
   написано "хардкод 150". Нужно читать из env везде.

7. **Bybit delta updates не упомянуты** — Bybit шлёт и snapshot и delta. Достаточно обрабатывать
   оба типа одинаково (брать поля из data), т.к. оба содержат актуальные bid/ask.

8. **Нет описания формата subscribe-ответа** — Bybit отвечает `{"success":true,"op":"subscribe"}`,
   Binance — `{"result":null,"id":1}`. Нужно логировать ws_subscribed по этим ответам, иначе
   ws_subscribed никогда не будет в логах.

---

## Структура проекта (финальная)

```
market-data/
├── common.py
├── binance_spot.py
├── binance_futures.py
├── bybit_spot.py
├── bybit_futures.py
├── stale_monitor.py
├── latency_monitor.py
├── requirements.txt
├── .env.example
├── tests/
│   ├── __init__.py
│   ├── test_common.py
│   ├── test_binance_parser.py
│   ├── test_bybit_parser.py
│   ├── test_reconnect_backoff.py
│   ├── test_stale_monitor.py
│   ├── test_latency_monitor.py
│   └── conftest.py
└── dictionaries/
    └── subscribe/
        ├── binance/
        │   ├── binance_spot.txt
        │   └── binance_futures.txt
        └── bybit/
            ├── bybit_spot.txt
            └── bybit_futures.txt
```

---

## Фазы реализации

---

### ФАЗА 0 — Инфраструктура проекта
**Цель:** рабочее окружение, зависимости, конфиг.

#### 0.1 requirements.txt
```
websockets>=12.0
redis[hiredis]>=5.0
orjson>=3.9
structlog>=24.0
python-dotenv>=1.0
pytest>=8.0
pytest-asyncio>=0.23
fakeredis[aioredis]>=2.20
```

#### 0.2 .env.example
Все переменные с дефолтами и комментариями (см. раздел 8 ТЗ + исправление TTL=300).

#### 0.3 Проверка символьных файлов
```bash
wc -l dictionaries/subscribe/binance/binance_spot.txt    # ожидаем 446
wc -l dictionaries/subscribe/binance/binance_futures.txt # ожидаем 401
wc -l dictionaries/subscribe/bybit/bybit_spot.txt        # ожидаем 392
wc -l dictionaries/subscribe/bybit/bybit_futures.txt     # ожидаем 391
```

**Самопроверка фазы:**
- [ ] `pip install -r requirements.txt` — без ошибок
- [ ] `python -c "import websockets, redis, orjson, structlog"` — без ошибок
- [ ] Все 4 файла символов существуют и непустые

---

### ФАЗА 1 — common.py
**Цель:** общий модуль < 100 строк, покрытый тестами.

#### Реализация

```python
# common.py — контракт функций:

def setup_logging(service_name: str) -> structlog.BoundLogger
    # structlog JSON рендерер, уровень из LOG_LEVEL env
    # timestamper UTC ISO8601, добавляет service= в каждый лог

def load_symbols(filepath: str) -> list[str]
    # читает файл, strip + upper + дедупликация + skip пустых и #-комментариев
    # FileNotFoundError → CRITICAL лог + sys.exit(1)
    # пустой результат → CRITICAL лог + sys.exit(1)

def get_redis() -> redis.asyncio.Redis
    # URL из REDIS_URL env, decode_responses=True, hiredis

def now_ms() -> int
    # int(time.time() * 1000)

# Константы:
REDIS_URL: str         # os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY_TTL: int     # int(os.getenv("REDIS_KEY_TTL", "300"))  # исправлено с 120
SYMBOLS_PER_CONN: int  # int(os.getenv("SYMBOLS_PER_CONN", "150"))
WS_RECV_TIMEOUT: int   # int(os.getenv("WS_RECV_TIMEOUT", "60"))
RECONNECT_MAX_DELAY: int  # int(os.getenv("RECONNECT_MAX_DELAY", "60"))
```

#### Тесты: tests/test_common.py

```python
# test_load_symbols:
- load_symbols с валидным файлом → список uppercase без дублей
- load_symbols с комментариями (#) → они пропущены
- load_symbols с пустыми строками → они пропущены
- load_symbols несуществующего файла → sys.exit(1)
- load_symbols пустого файла → sys.exit(1)
- load_symbols файла только с комментариями → sys.exit(1)
- дедупликация: "BTCUSDT\nbtcusdt\n" → ["BTCUSDT"] (len=1)

# test_now_ms:
- now_ms() возвращает int
- now_ms() ≈ time.time()*1000 (delta < 10ms)
- два вызова подряд: второй >= первого

# test_setup_logging:
- возвращает structlog BoundLogger
- лог содержит поле "service"
- вывод парсится как JSON (capture stdout)
```

**Самопроверка фазы:**
- [ ] `pytest tests/test_common.py -v` — все тесты зелёные
- [ ] `wc -l common.py` — < 100 строк
- [ ] `python -c "from common import load_symbols; print(load_symbols('dictionaries/subscribe/binance/binance_spot.txt')[:3])"` — печатает 3 символа

---

### ФАЗА 2 — WS-коллекторы

#### Архитектура каждого коллектора (одинакова)

```
main()
  └── load_symbols()
  └── setup_logging()
  └── get_redis()
  └── chunks = [symbols[i:i+SYMBOLS_PER_CONN] for i in range(0, len, SYMBOLS_PER_CONN)]
  └── shutdown_event = asyncio.Event()
  └── setup SIGTERM/SIGINT handlers
  └── asyncio.gather(*[ws_worker(chunk, conn_id, redis, shutdown_event) for conn_id, chunk in enumerate(chunks)])

ws_worker(symbols, conn_id, redis, shutdown_event):
  attempt = 0
  while not shutdown_event.is_set():
    try:
      async with websockets.connect(WS_URL, ping_interval=PING_INTERVAL) as ws:
        await subscribe(ws, symbols)
        attempt = 0  # сброс после успешного подключения
        await read_loop(ws, redis, shutdown_event)
    except Exception as e:
      log WARNING ws_reconnecting
      delay = min(2**attempt, RECONNECT_MAX_DELAY) + random.uniform(0,1)
      await asyncio.sleep(delay)
      attempt += 1
  log INFO graceful_shutdown

read_loop(ws, redis, shutdown_event):
  while not shutdown_event.is_set():
    try:
      raw = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
      ts_received = now_ms()
      data = orjson.loads(raw)
      parsed = parse_message(data)  # None если не нужно писать
      if parsed:
        await write_redis(redis, parsed, ts_received)
    except asyncio.TimeoutError:
      raise  # reconnect
    except orjson.JSONDecodeError:
      log WARNING message_parse_error

write_redis(redis, parsed, ts_received):
  ts_redis = now_ms()
  key = f"md:{exchange}:{market}:{parsed['symbol']}"
  channel = f"md:updates:{exchange}:{market}"
  payload = orjson.dumps({...})
  try:
    pipe = redis.pipeline()
    pipe.hset(key, mapping={...})
    pipe.expire(key, REDIS_KEY_TTL)
    pipe.publish(channel, payload)
    await pipe.execute()
  except redis.RedisError:
    log ERROR redis_error
    # буфер: deque(maxlen=1000), retry в фоне
```

#### 2.1 binance_spot.py

**Парсер:**
```python
def parse_message(data: dict) -> dict | None:
    # Пропустить ответ на SUBSCRIBE: {"result": null, "id": 1}
    if "result" in data:
        log.info("ws_subscribed", ...)
        return None
    # Пропустить если нет поля "s"
    if "s" not in data:
        return None
    return {
        "symbol": data["s"].upper(),
        "bid": data["b"],
        "bid_qty": data["B"],
        "ask": data["a"],
        "ask_qty": data["A"],
        "last": "",
        "ts_exchange": "0",  # bookTicker не содержит ts
    }
```

**Подписка (lowercase + @bookTicker):**
```python
params = [f"{s.lower()}@bookTicker" for s in symbols]
msg = orjson.dumps({"method": "SUBSCRIBE", "params": params, "id": conn_id})
await ws.send(msg)
```

**Ping:** websockets обрабатывает автоматически. Дополнительный ping каждые 30 сек через `ping_interval=30` в connect().

#### 2.2 binance_futures.py

Отличие от spot:
```python
# ts_exchange = data.get("E", 0) — event time от биржи
"ts_exchange": str(data.get("E", 0)),
```
WS URL: `wss://fstream.binance.com/ws`

#### 2.3 bybit_spot.py

**Парсер:**
```python
def parse_message(data: dict) -> dict | None:
    # Ответ на subscribe: {"success": true, "op": "subscribe"}
    if data.get("op") == "subscribe":
        log.info("ws_subscribed", ...)
        return None
    # Ping от сервера: {"op": "ping"}
    if data.get("op") == "ping":
        # ws.send({"op": "pong"}) — обработка в read_loop
        return None
    # Heartbeat ответ: {"ret_msg": "pong", "op": "pong"} — игнорировать
    if data.get("op") == "pong":
        return None
    # Данные: topic начинается с "tickers."
    if not data.get("topic", "").startswith("tickers."):
        return None
    d = data["data"]
    return {
        "symbol": d["symbol"].upper(),
        "bid": d.get("bid1Price", ""),
        "bid_qty": d.get("bid1Size", ""),
        "ask": d.get("ask1Price", ""),
        "ask_qty": d.get("ask1Size", ""),
        "last": d.get("lastPrice", ""),
        "ts_exchange": str(data.get("ts", 0)),
    }
```

**Подписка:**
```python
args = [f"tickers.{s.upper()}" for s in symbols]
msg = orjson.dumps({"op": "subscribe", "args": args})
```

**Ping loop (отдельная task):**
```python
async def ping_loop(ws, shutdown_event):
    while not shutdown_event.is_set():
        await asyncio.sleep(20)
        await ws.send(orjson.dumps({"op": "ping"}))
```

#### 2.4 bybit_futures.py

Идентично bybit_spot.py, отличие только:
- WS URL: `wss://stream.bybit.com/v5/public/linear`

#### Тесты: tests/test_binance_parser.py, tests/test_bybit_parser.py

```python
# test_binance_spot_parser:
- валидный bookTicker → корректный dict (symbol upper, bid, ask)
- symbol нормализован в upper case
- ts_exchange = "0" (нет в bookTicker)
- subscribe-ответ {"result":null,"id":1} → None
- неизвестный формат → None
- отсутствует поле "s" → None

# test_binance_futures_parser:
- содержит поле "E" → ts_exchange = str(E)
- нет поля "E" → ts_exchange = "0"

# test_bybit_parser:
- валидный snapshot → корректный dict
- delta update (type="delta") → корректный dict (обрабатывается так же)
- {"op":"ping"} → None (+ side effect: нужно отправить pong)
- {"op":"pong","ret_msg":"pong"} → None (игнорировать)
- {"success":true,"op":"subscribe"} → None
- topic не начинается с "tickers." → None
- отсутствие bid1Price в delta → пустая строка (graceful)

# tests/test_reconnect_backoff.py:
- delay для attempt=0: 1 <= delay <= 2
- delay для attempt=5: min(32,60) <= delay <= min(32,60)+1
- delay для attempt=10: 60 <= delay <= 61 (максимум)
- attempt сбрасывается после успешного recv
```

**Самопроверка фазы:**
- [ ] `pytest tests/test_binance_parser.py tests/test_bybit_parser.py tests/test_reconnect_backoff.py -v` — зелёные
- [ ] `wc -l binance_spot.py binance_futures.py bybit_spot.py bybit_futures.py` — каждый < 250 строк
- [ ] Линтер: `python -m py_compile binance_spot.py binance_futures.py bybit_spot.py bybit_futures.py` — без ошибок

---

### ФАЗА 3 — stale_monitor.py

#### Реализация

```python
# Ключевые детали:
# - grace_period: первые 2 итерации не генерировать алерты
# - previously_stale: set() для детекта recovery
# - SCAN с cursor (не KEYS), COUNT=500

async def scan_loop():
    cycle = 0
    previously_stale: set[str] = set()

    while not shutdown_event.is_set():
        cycle += 1
        in_grace = cycle <= 2

        stale = []
        total = 0
        cursor = 0

        while True:
            cursor, keys = await redis.scan(cursor, match="md:*:*:*", count=SCAN_COUNT)
            total += len(keys)

            for key in keys:
                ts_redis = await redis.hget(key, "ts_redis")
                if ts_redis is None:
                    log.warning("missing_ts_redis", key=key)
                    continue
                age_ms = now_ms() - int(ts_redis)
                if age_ms > STALE_THRESHOLD_SEC * 1000:
                    stale.append({"key": key, "age_sec": age_ms // 1000})

            if cursor == 0:
                break

        current_stale_keys = {s["key"] for s in stale}

        # Recovery detection
        recovered = previously_stale - current_stale_keys
        for key in recovered:
            log.info("stale_recovered", key=key)

        if not in_grace:
            for s in stale:
                log.warning("stale_detected", key=s["key"], age_sec=s["age_sec"])

            if stale:
                alert = orjson.dumps({
                    "ts": now_ms(),
                    "stale_count": len(stale),
                    "symbols": stale,
                })
                await redis.publish("alerts:stale", alert)

        log.info("stale_scan_complete", total_keys=total, stale_count=len(stale), in_grace=in_grace)
        previously_stale = current_stale_keys

        await asyncio.sleep(SCAN_INTERVAL_SEC)
```

#### Тесты: tests/test_stale_monitor.py

Используем `fakeredis`:
```python
# test_stale_detection:
- Redis с ключом где ts_redis = now-90sec → stale_detected (age≈90)
- Redis с ключом где ts_redis = now-30sec → НЕ stale
- grace period: cycle=1 → нет алерта в pub/sub, нет warning лога

# test_stale_recovery:
- цикл 1: ключ stale → previously_stale = {key}
- цикл 2: ключ обновлён → stale_recovered лог

# test_missing_ts_redis:
- ключ без поля ts_redis → WARNING "missing_ts_redis", ключ не в stale

# test_scan_uses_cursor:
- mock redis.scan возвращает cursor != 0 на первом вызове
- проверить что scan вызван дважды (до cursor=0)

# test_publish_format:
- после stale detection проверить что publish вызван с корректным JSON
- JSON содержит: ts, stale_count, symbols[].key, symbols[].age_sec
```

---

### ФАЗА 4 — latency_monitor.py

#### Реализация (с rolling SCAN cursor — исправление ТЗ)

```python
async def sampling_loop():
    scan_cursor = 0  # rolling cursor — не сбрасываем на 0 каждый цикл
    samples_by_market: dict[str, list] = defaultdict(list)

    while not shutdown_event.is_set():
        keys_processed = 0

        # Один шаг SCAN (не полный обход, а шаг cursor)
        scan_cursor, keys = await redis.scan(
            scan_cursor, match="md:*:*:*", count=SAMPLING_MAX_KEYS
        )

        for key in keys:
            ts_e, ts_r, ts_rd = await redis.hmget(key, "ts_exchange", "ts_received", "ts_redis")
            if not all([ts_e, ts_r, ts_rd]):
                continue

            ts_e, ts_r, ts_rd = int(ts_e), int(ts_r), int(ts_rd)

            # Пропускаем Binance Spot (ts_exchange == ts_received — нет смысла)
            exchange_to_collector = ts_r - ts_e if ts_e != ts_r else None
            collector_to_redis = ts_rd - ts_r
            end_to_end = ts_rd - ts_e if ts_e != ts_r else None

            # clock skew
            if exchange_to_collector is not None and exchange_to_collector < 0:
                log.info("clock_skew_detected", key=key, skew_ms=exchange_to_collector)

            # parse market from key: md:{exchange}:{market}:{symbol}
            parts = key.split(":")
            market_key = f"{parts[1]}:{parts[2]}"
            samples_by_market[market_key].append({
                "e2e": end_to_end,
                "c2r": collector_to_redis,
            })

            # anomaly check
            if end_to_end is not None:
                if end_to_end > ANOMALY_CRIT_MS:
                    log.critical("latency_anomaly", key=key, e2e_ms=end_to_end)
                elif end_to_end > ANOMALY_WARN_MS:
                    log.warning("latency_anomaly", key=key, e2e_ms=end_to_end)

        # Отчёт раз в SAMPLING_INTERVAL_SEC
        for market_key, samples in samples_by_market.items():
            exchange, market = market_key.split(":")
            e2e_vals = [s["e2e"] for s in samples if s["e2e"] is not None]
            c2r_vals = [s["c2r"] for s in samples]

            def stats(vals):
                if not vals: return {}
                s = sorted(vals)
                p95_idx = int(len(s) * 0.95)
                return {"min": s[0], "max": s[-1], "avg": sum(s)/len(s), "p95": s[p95_idx]}

            log.info("latency_report",
                exchange=exchange, market=market,
                samples=len(samples),
                e2e_ms=stats(e2e_vals),
                collector_to_redis_ms=stats(c2r_vals),
            )

        samples_by_market.clear()
        await asyncio.sleep(SAMPLING_INTERVAL_SEC)
```

#### Тесты: tests/test_latency_monitor.py

```python
# test_latency_calculation:
- ts_exchange=1000, ts_received=1010, ts_redis=1012
  → e2e=12ms, collector_to_redis=2ms, exchange_to_collector=10ms

# test_clock_skew:
- ts_received < ts_exchange → clock_skew_detected лог INFO (не ошибка)
- е2е всё равно вычисляется

# test_binance_spot_skip (ts_exchange == ts_received):
- exchange_to_collector = None, e2e = None
- нет clock_skew лога
- c2r считается нормально

# test_anomaly_warning:
- e2e = 1500ms → WARNING latency_anomaly

# test_anomaly_critical:
- e2e = 6000ms → CRITICAL latency_anomaly

# test_stats_calculation:
- percentile P95: [1,2,3,...,100] → p95=95
- avg корректный
- пустой список → пустой dict (без исключений)

# test_rolling_cursor:
- cursor не сбрасывается на 0 после каждого цикла
```

---

### ФАЗА 5 — Интеграционные тесты (fakeredis)

#### tests/conftest.py

```python
import pytest
import fakeredis.aioredis

@pytest.fixture
async def redis_client():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()
```

#### tests/test_redis_write.py

```python
# test_hset_pipeline:
# Симулируем write_redis и проверяем что в fakeredis:
- HGETALL md:binance:spot:BTCUSDT → содержит все 8 полей
- TTL md:binance:spot:BTCUSDT → > 0 (expire установлен)
- pub/sub: subscribe на md:updates:binance:spot → получаем сообщение

# test_redis_key_format:
- ключ строго md:{exchange}:{market}:{symbol} (uppercase symbol)
- binance spot BTCUSDT → md:binance:spot:BTCUSDT
- bybit futures ETHUSDT → md:bybit:futures:ETHUSDT

# test_redis_fields_types:
- все 8 полей (bid, bid_qty, ask, ask_qty, last, ts_exchange, ts_received, ts_redis) присутствуют
- все значения - строки (не None)
- ts_received и ts_redis — числовые строки (int(ts) не бросает)

# test_buffer_on_redis_down:
- redis.pipeline().execute() бросает RedisError
- write_redis буферизует сообщение (deque)
- при восстановлении буфер сбрасывается в Redis
- dropped_count логируется при переполнении (maxlen=1000)
```

---

### ФАЗА 6 — Критерии приёмки (ТЗ раздел 9)

Скрипт `tests/acceptance_check.sh` для ручной/полуавтоматической проверки:

```bash
#!/bin/bash
set -e

echo "=== Acceptance Check: Market Data Collector ==="

# 1. Проверка синтаксиса всех файлов
echo "[1] Syntax check..."
python -m py_compile common.py binance_spot.py binance_futures.py bybit_spot.py bybit_futures.py stale_monitor.py latency_monitor.py
echo "    OK"

# 2. Размер файлов
echo "[2] File size check..."
for f in binance_spot.py binance_futures.py bybit_spot.py bybit_futures.py; do
  lines=$(wc -l < $f)
  [ $lines -lt 250 ] || { echo "FAIL: $f = $lines lines (max 250)"; exit 1; }
  echo "    $f: $lines lines OK"
done
lines=$(wc -l < common.py)
[ $lines -lt 100 ] || { echo "FAIL: common.py = $lines lines (max 100)"; exit 1; }
echo "    common.py: $lines lines OK"

# 3. Все unit тесты
echo "[3] Unit tests..."
pytest tests/ -v --tb=short
echo "    OK"

# 4. JSON-валидность логов (запускаем скрипт 3 сек, проверяем stdout)
echo "[4] JSON log format..."
timeout 3 python binance_spot.py 2>&1 | head -5 | python -m json.tool > /dev/null
echo "    OK"

# 5. Redis check (требует запущенного Redis и коллекторов)
echo "[5] Redis data check (manual - run collectors first)..."
echo "    Run: redis-cli HGETALL md:binance:spot:BTCUSDT"
echo "    Expected: 8 fields with non-empty values"

echo ""
echo "=== All automated checks passed ==="
```

---

## Порядок реализации и коммиты

| # | Коммит | Файлы | Проверка |
|---|--------|-------|---------|
| 1 | `feat: add project structure and requirements` | requirements.txt, .env.example | pip install |
| 2 | `feat: implement common.py with full test coverage` | common.py, tests/test_common.py | pytest test_common.py |
| 3 | `feat: implement binance_spot.py` | binance_spot.py, tests/test_binance_parser.py | pytest test_binance_parser.py |
| 4 | `feat: implement binance_futures.py` | binance_futures.py (+ тесты) | pytest |
| 5 | `feat: implement bybit_spot.py` | bybit_spot.py, tests/test_bybit_parser.py | pytest test_bybit_parser.py |
| 6 | `feat: implement bybit_futures.py` | bybit_futures.py | pytest |
| 7 | `feat: implement stale_monitor.py` | stale_monitor.py, tests/test_stale_monitor.py | pytest test_stale_monitor.py |
| 8 | `feat: implement latency_monitor.py` | latency_monitor.py, tests/test_latency_monitor.py | pytest test_latency_monitor.py |
| 9 | `test: add redis pipeline integration tests` | tests/test_redis_write.py, tests/conftest.py | pytest tests/ |
| 10 | `chore: add acceptance check script` | tests/acceptance_check.sh | bash acceptance_check.sh |

---

## Метрики качества (финальная проверка)

```
pytest tests/ --cov=. --cov-report=term-missing
```

Ожидаемое покрытие:
- `common.py`: 100%
- `binance_spot.py` / `binance_futures.py`: ≥ 80% (парсер 100%, ws-цикл через моки)
- `bybit_spot.py` / `bybit_futures.py`: ≥ 80%
- `stale_monitor.py`: ≥ 85%
- `latency_monitor.py`: ≥ 85%

```bash
# Финальные self-checks:
python -m py_compile *.py                    # нет синтаксических ошибок
wc -l *.py | sort -n                         # проверка размеров
pytest tests/ -v                             # все тесты зелёные
cat common.py | python -c "import sys; data=sys.stdin.read(); assert len(data.splitlines()) < 100"
jq . <<< $(python -c "import common; common.setup_logging('test'); import structlog; structlog.get_logger().info('ok')")
```
