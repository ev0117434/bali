# Справочник скриптов — полная техническая документация

> **Версия:** актуально на 2026-03-17
> **Язык:** Python 3.10+
> **Охват:** все скрипты репозитория — коллекторы, мониторы, сканер, snapshot-логгер, история цен, лончер, генератор словарей

---

## Содержание

1. [Общая архитектура](#1-общая-архитектура)
2. [common.py — общие утилиты](#2-commonpy--общие-утилиты)
3. [Коллекторы рыночных данных](#3-коллекторы-рыночных-данных)
   - [binance_spot.py](#31-binance_spotpy)
   - [binance_futures.py](#32-binance_futurespy)
   - [bybit_spot.py](#33-bybit_spotpy)
   - [bybit_futures.py](#34-bybit_futurespy)
   - [okx_spot.py](#35-okx_spotpy)
   - [okx_futures.py](#36-okx_futurespy)
   - [gate_spot.py](#37-gate_spotpy)
   - [gate_futures.py](#38-gate_futurespy)
4. [stale_monitor.py — монитор устаревших данных](#4-stale_monitorpy--монитор-устаревших-данных)
5. [latency_monitor.py — монитор задержек](#5-latency_monitorpy--монитор-задержек)
6. [spread_scanner.py — сканер арбитражного спреда](#6-spread_scannerpy--сканер-арбитражного-спреда)
7. [signal_snapshot.py — логгер снапшотов](#7-signal_snapshotpy--логгер-снапшотов)
8. [price_history_writer.py — запись истории цен](#8-price_history_writerpy--запись-истории-цен)
9. [price_history_reader.py — чтение истории цен](#9-price_history_readerpy--чтение-истории-цен)
10. [run.py — лончер всех процессов](#10-runpy--лончер-всех-процессов)
11. [dictionaries/main.py — генератор словарей](#11-dictionariesmainpy--генератор-словарей)
12. [Redis: ключи и каналы](#12-redis-ключи-и-каналы)
13. [Переменные окружения](#13-переменные-окружения)
14. [Форматы файлов и выходных данных](#14-форматы-файлов-и-выходных-данных)
15. [Обработка ошибок и устойчивость](#15-обработка-ошибок-и-устойчивость)

---

## 1. Общая архитектура

```
dictionaries/main.py   ←  запускается вручную, обновляет списки символов
         │
         ▼
subscribe/*.txt        ←  8 файлов (binance_spot/futures, bybit, okx, gate)
         │
         ▼  читают при старте
┌─────────────────────────────────────────────┐
│   8 WebSocket-коллекторов (asyncio)         │
│   binance_{spot,futures}  bybit_{spot,fut}  │
│   okx_{spot,futures}      gate_{spot,fut}   │
└──────────────┬──────────────────────────────┘
               │  HSET + EXPIRE + PUBLISH
               ▼
         Redis (db 0)
           md:{exchange}:{market}:{symbol}   ← текущая цена (Hash, TTL 300 с)
           md:updates:{exchange}:{market}    ← pub/sub-каналы
               │
       ┌───────┼────────────────────┐
       ▼       ▼                    ▼
stale_monitor  latency_monitor   price_history_writer
(SCAN 30 с)   (SCAN 10 с)       (pub/sub → ZADD)
                                    │
                                    ▼
                           md:hist:{ex}:{mkt}:{sym}:{chunk}
                           md:hist:config
                           (rolling 5 чанков × 20 мин = ~100 мин)
               │
               ▼
         spread_scanner
          (каждые 200 мс)
               │  PUBLISH ch:spread_signals
               ▼
         signal_snapshot
          (слушает pub/sub, пишет CSV)
```

### Ключевые цифры

| Параметр | Значение |
|---|---|
| Число коллекторов | 8 (4 биржи × 2 рынка) |
| WS-соединений (при ~600 символах) | ~18 |
| Символов в мониторинге | 600–1600+ |
| Арбитражных направлений | 12 (A–L) |
| Интервал сканирования спреда | 200 мс |
| Cooldown сигнала | 3600 с |
| TTL ключа md:* | 300 с |
| Порог stale | 60 с |
| Длина чанка истории | 1200 с (20 мин) |
| Число чанков | 5 (~100 мин суммарно) |
| TTL чанка истории | 7200 с (2 ч) |

---

## 2. common.py — общие утилиты

**Файл:** `market-data/common.py`
Импортируется всеми восемью коллекторами и мониторами.

### Константы (считываются один раз при импорте)

```python
REDIS_URL        = os.getenv("REDIS_URL",        "redis://localhost:6379/0")
REDIS_KEY_TTL    = int(os.getenv("REDIS_KEY_TTL", "300"))   # секунды
SYMBOLS_PER_CONN = int(os.getenv("SYMBOLS_PER_CONN", "150"))
WS_RECV_TIMEOUT  = int(os.getenv("WS_RECV_TIMEOUT",  "60"))  # секунды
RECONNECT_MAX_DELAY = int(os.getenv("RECONNECT_MAX_DELAY", "60"))
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
```

**Важный нюанс:** константы читаются **при импорте**, т.е. до `asyncio.run(main())`. Если `.env` загружается не до импорта `common`, переменные примут значения по умолчанию. В каждом коллекторе вверху файла стоит `sys.path.insert(0, ...)` + в `common.py` вызывается `load_dotenv()` — это гарантирует загрузку `.env` до разбора констант.

### `setup_logging(service_name: str) → structlog.BoundLogger`

Настраивает structlog с:
- форматом **JSON** (сериализатор — `orjson` для скорости)
- временны́ми метками **ISO UTC**
- автоматическим включением stack trace при исключениях
- выводом в **stdout** (run.py перехватывает stdout → файл)

Каждая строка лога — валидный JSON-объект:
```json
{"timestamp": "2026-03-16T12:00:00.123Z", "level": "info", "event": "ws_connected", "service": "binance_spot", "conn_id": 0, "symbols_count": 150}
```

### `load_symbols(filepath: str) → list[str]`

- Завершает процесс с `sys.exit(1)` если файл не найден или пуст.
- Пропускает строки, начинающиеся с `#` (комментарии), и пустые строки.
- Приводит все символы к **верхнему регистру**.
- **Дедуплицирует** символы, сохраняя порядок первого вхождения.
- Путь к файлу задаётся **относительно директории скрипта** (через `os.path.dirname(__file__)`), поэтому скрипт можно запускать из любой директории.

### `get_redis() → aioredis.Redis`

Создаёт асинхронный клиент Redis с:
- `decode_responses=True` — все ответы Redis декодируются в строки Python (не bytes)
- автоматическим retry при `ConnectionError` и `TimeoutError`

**Нюанс:** клиент создаётся один раз в `main()` и передаётся во все воркеры. При падении Redis воркеры используют in-memory буфер (см. секцию 13).

### `now_ms() → int`

Возвращает текущее время в **миллисекундах** (`int(time.time() * 1000)`). Используется для трёх меток времени в каждом сообщении: `ts_received`, `ts_redis`, и для расчётов latency.

---

## 3. Коллекторы рыночных данных

Все коллекторы следуют единой структуре:

```
main() → load_symbols() → chunking → N × ws_worker()
ws_worker() → [бесконечный цикл]
    → websockets.connect()
    → subscribe
    → [цикл сообщений]
        → recv() с timeout
        → parse_message()
        → write_redis()
```

### Общий паттерн переподключения

```
attempt=0
while not shutdown:
    try:
        connect, subscribe, read loop
        attempt = 0  ← сбрасывается при успешном подключении
    except CancelledError:
        break
    except Exception:
        delay = min(2^attempt, RECONNECT_MAX_DELAY) + random(0,1)
        log.warning("ws_reconnecting", ...)
        sleep(delay)
        attempt += 1
```

Задержки: 1с, 2с, 4с, 8с, 16с, 32с, 60с, 60с, ... (максимум 60 + до 1 сек jitter).

### Что происходит при WS-таймауте

Если за `WS_RECV_TIMEOUT` (60с по умолчанию) не пришло ни одного сообщения — inner-цикл прерывается (break), ws.close(1000) вызывается, затем начинается переподключение. **Attempt не инкрементируется** — таймаут не считается ошибкой, delay = min(2^0, 60) + jitter = 1.X сек.

> **Внимание:** WS_RECV_TIMEOUT должен быть заведомо больше интервала пинга. При `PING_INTERVAL=30` и `WS_RECV_TIMEOUT=60` — корректно. Если поставить `WS_RECV_TIMEOUT=20`, коллектор будет постоянно переподключаться.

---

### 3.1 binance_spot.py

| Параметр | Значение |
|---|---|
| WS URL | `wss://stream.binance.com:9443/ws` |
| Символьный файл | `dictionaries/subscribe/binance/binance_spot.txt` |
| Формат подписки | `{symbol}@bookTicker` (lowercase) |
| Пинг | Автоматический через websockets (`ping_interval=30`) |
| `ts_exchange` | **Всегда `"0"`** (bookTicker спота не содержит метку времени биржи) |
| Redis ключ | `md:binance:spot:{SYMBOL}` |
| Redis канал | `md:updates:binance:spot` |

**Критический нюанс — ts_exchange = "0":**
Binance Spot bookTicker не включает поле времени биржи. Коллектор явно пишет `ts_exchange = "0"`. Это корректное поведение. `latency_monitor` специально обрабатывает этот случай и не рассчитывает end-to-end latency для Binance Spot (иначе она показала бы бессмысленные нули).

**Формат сообщения от биржи:**
```json
{"u":400900217,"s":"BNBUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}
```

**Сообщение-подтверждение подписки:**
```json
{"result": null, "id": 0}
```
Это сообщение фильтруется в `parse_message()` по наличию ключа `"result"`.

---

### 3.2 binance_futures.py

Идентично binance_spot.py, за исключением:

| Параметр | binance_spot | binance_futures |
|---|---|---|
| WS URL | `wss://stream.binance.com:9443/ws` | `wss://fstream.binance.com/ws` |
| `ts_exchange` | всегда `"0"` | `str(data.get("E", 0))` — поле `E` (event time) |

**Формат сообщения Futures bookTicker:**
```json
{"e":"bookTicker","u":1234567,"E":1710000000000,"s":"BTCUSDT","b":"50000.1","B":"0.5","a":"50001.2","A":"1.2"}
```

Поле `"E"` — event time в миллисекундах (Unix timestamp ms). Именно оно используется как `ts_exchange`.

---

### 3.3 bybit_spot.py

Самый сложный коллектор. Использует **два топика** на каждый символ.

| Параметр | Значение |
|---|---|
| WS URL | `wss://stream.bybit.com/v5/public/spot` |
| Топики | `tickers.{SYMBOL}` + `orderbook.1.{SYMBOL}` |
| Пинг | Ручной JSON `{"op":"ping"}` каждые 20 с |
| SUB_BATCH_SIZE | 10 топиков на одно subscribe-сообщение (лимит Bybit) |
| Redis ключ | `md:bybit:spot:{SYMBOL}` |

**Почему два топика?**

Bybit Spot `tickers` содержит `lastPrice` и статистику, но **не содержит best bid/ask**. Best bid/ask приходят из топика `orderbook.1` (лучший уровень стакана).

Оба топика обновляют один и тот же ключ Redis. При каждом обновлении заполняются только те поля, которые доступны в данном типе сообщения. Поля с пустым значением `""` **не перезаписывают** существующие в Redis (логика в `write_redis`/`RedisWriter.write`).

**Нюанс — orderbook.1 delta vs snapshot:**

Bybit присылает `snapshot` при подписке, а затем `delta` обновления. Размер `"0"` в поле bid/ask quantity означает **удаление уровня** — в этом случае bid/ask обнуляется (`""`), и Redis-запись для соответствующего поля не обновляется. Пустая запись защищает от распространения "удалённых" уровней.

**Паттерн ответа на subscribe:**
```json
{"success": true, "ret_msg": "", "op": "subscribe", "args": ["tickers.BTCUSDT","orderbook.1.BTCUSDT"]}
```

**Нюанс — `RedisWriter` класс:**

В bybit_spot используется OOP-подход (`class RedisWriter`) с **per-worker буфером**, в отличие от других коллекторов, где буфер — глобальная переменная `_redis_buffer`. Функционально идентично, но изолировано на уровне воркера.

**Фоновые задачи воркера:**
- `_ping_loop` — шлёт `{"op":"ping"}` каждые 20 с
- `_stats_loop` — логирует кол-во сообщений каждые 60 с
- `_read_loop` — читает сообщения, все три запускаются параллельно

---

### 3.4 bybit_futures.py

Аналогично bybit_spot, но упрощённо:

| Параметр | bybit_spot | bybit_futures |
|---|---|---|
| WS URL | `stream.bybit.com/v5/public/spot` | `stream.bybit.com/v5/public/linear` |
| Топики | `tickers` + `orderbook.1` | только `tickers` |
| Bid/ask | из orderbook.1 | из `tickers` полей `bid1Price`/`ask1Price` |
| Архитектура | класс `RedisWriter` | глобальный `_redis_buffer` |

**Формат сообщения futures tickers:**
```json
{
  "topic": "tickers.BTCUSDT",
  "ts": 1710000000000,
  "data": {
    "symbol": "BTCUSDT",
    "bid1Price": "50000.1", "bid1Size": "0.5",
    "ask1Price": "50001.2", "ask1Size": "1.2",
    "lastPrice": "50000.5"
  }
}
```

Futures tickers Bybit содержат и best bid/ask и lastPrice в одном сообщении — поэтому второй топик не нужен.

---

### 3.5 okx_spot.py

| Параметр | Значение |
|---|---|
| WS URL | `wss://ws.okx.com:8443/ws/v5/public` |
| Канал | `tickers` |
| Лимит символов на соединение | 300 (OKX_CHUNK_SIZE, игнорирует SYMBOLS_PER_CONN если он больше) |
| Пинг | Простая строка `"ping"`, ответ — строка `"pong"` |
| Redis ключ | `md:okx:spot:{SYMBOL}` |

**Нормализация символов:**

OKX использует формат `BTC-USDT` (с дефисом). Система хранит символы в "нормализованном" виде без разделителей (`BTCUSDT`).

```python
_to_native("BTCUSDT")  →  "BTC-USDT"   # для подписки на WS
_normalize("BTC-USDT") →  "BTCUSDT"     # после получения сообщения
```

Обратная нормализация работает через перебор известных котируемых валют:
```python
_KNOWN_QUOTES = ["USDT", "USDC", "BTC", "ETH", "DAI", "OKB", "USDK"]
```

**Нюанс — порядок _KNOWN_QUOTES важен!** Алгоритм проверяет суффиксы слева направо. Для символа `ETHBTC`: проверяется `USDT` — нет, `USDC` — нет, `BTC` — да → `ETH-BTC`. Редкая валюта, стоящая в начале списка перед более частой, может неправильно разбить символ.

**Формат подписки:**
```json
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"},{"channel":"tickers","instId":"ETH-USDT"}]}
```

**Формат данных:**
```json
{
  "arg": {"channel": "tickers", "instId": "BTC-USDT"},
  "data": [{
    "instId": "BTC-USDT",
    "bidPx": "50000.1", "bidSz": "0.5",
    "askPx": "50001.2", "askSz": "1.2",
    "last": "50000.5",
    "ts": "1710000000000"
  }]
}
```

**Нюанс — `ts` как строка:** В OKX поле `ts` в данных — строка (не число). `parse_message` возвращает её без преобразования: `"ts_exchange": d.get("ts", "0")`.

**Нюанс — chunk_size:** При `SYMBOLS_PER_CONN=150` и `OKX_CHUNK_SIZE=300` используется `min(150, 300) = 150`. Если хотите максимальную плотность — установите `SYMBOLS_PER_CONN=300` или увеличьте `OKX_CHUNK_SIZE`.

---

### 3.6 okx_futures.py

Аналогично okx_spot, с отличиями:

| Параметр | okx_spot | okx_futures |
|---|---|---|
| instId формат | `BTC-USDT` | `BTC-USDT-SWAP` |
| `_to_native()` | `BTCUSDT → BTC-USDT` | `BTCUSDT → BTC-USDT-SWAP` |
| `_normalize()` | `BTC-USDT → BTCUSDT` | `BTC-USDT-SWAP → BTCUSDT` |
| Фильтрация | — | только сообщения с `instId.endswith("-SWAP")` |

**Почему фильтрация по `-SWAP`?** OKX может присылать сообщения других инструментальных типов (например, квартальные фьючерсы `-YYMMDD`). Фильтр гарантирует обработку только бессрочных свопов (perpetual).

---

### 3.7 gate_spot.py

| Параметр | Значение |
|---|---|
| WS URL | `wss://api.gateio.ws/ws/v4/` |
| Канал | `spot.book_ticker` |
| Лимит на subscribe | 100 символов (BATCH_SIZE) |
| Пинг | JSON `{"time": ts_sec, "channel": "spot.ping"}` |
| Redis ключ | `md:gate:spot:{SYMBOL}` |

**Нормализация символов:**

Gate.io использует формат `BTC_USDT` (подчёркивание).

```python
_to_native("BTCUSDT")  →  "BTC_USDT"
_normalize("BTC_USDT") →  "BTCUSDT"
```

**Нюанс — батчевая подписка с задержкой:**

Gate.io не поддерживает более 100 символов в одном subscribe. Коллектор шлёт несколько сообщений с паузой 20 мс между ними:
```python
for i in range(0, len(native), BATCH_SIZE):
    await ws.send(sub_msg)
    await asyncio.sleep(0.02)   # ← важно, без паузы gate может дропнуть
```

**Нюанс — ts_exchange в секундах:**

Gate.io возвращает `time` в **секундах** (не миллисекундах!). Коллектор умножает на 1000:
```python
ts_exchange = str(data.get("time", 0) * 1000)
```

**Нюанс — нет lastPrice:**

`spot.book_ticker` не содержит `lastPrice`. Поле `last` в Redis для Gate Spot всегда остаётся пустым (как оно было в Redis до первой записи, или как его перезаписал другой источник — этого не происходит, т.к. каждая биржа пишет в свой ключ).

---

### 3.8 gate_futures.py

Аналогично gate_spot:

| Параметр | gate_spot | gate_futures |
|---|---|---|
| WS URL | `wss://api.gateio.ws/ws/v4/` | `wss://fx-ws.gateio.ws/v4/ws/usdt` |
| Канал | `spot.book_ticker` | `futures.book_ticker` |
| Пинг-канал | `spot.ping` | `futures.ping` |

**Нюанс — разные URL:** Спот и фьючерсы Gate.io обслуживаются на **разных хостах** — это одно из важных отличий от других бирж.

---

## 4. stale_monitor.py — монитор устаревших данных

**Файл:** `market-data/stale_monitor.py`

### Что делает

Каждые `SCAN_INTERVAL_SEC` секунд выполняет SCAN Redis по паттерну `md:*:*:*`, читает поле `ts_redis` из каждого ключа, и определяет, устарели ли данные.

### Параметры

| Переменная | Умолч. | Описание |
|---|---|---|
| `STALE_THRESHOLD_SEC` | 60 | Порог устаревания в секундах |
| `SCAN_INTERVAL_SEC` | 30 | Интервал между сканированиями |
| `SCAN_COUNT` | 500 | Хинт для Redis SCAN (не гарантирует ровно 500) |
| `GRACE_CYCLES` | 2 | Число начальных циклов без алертов |

### Жизненный цикл

```
Цикл 1 (grace)  → SCAN → находим stale → НЕ алертим → запоминаем stale_keys
Цикл 2 (grace)  → SCAN → находим stale → НЕ алертим → проверяем восстановление
Цикл 3+         → SCAN → stale → log.warning + publish "alerts:stale"
```

**Grace period:** Первые 2 цикла (60 с при SCAN_INTERVAL=30) после старта мониторинг молчит. Это необходимо, потому что при запуске системы коллекторы ещё не подключились к биржам и не записали данные — если алертить сразу, все ключи будут выглядеть устаревшими (их ещё нет).

**Детектирование восстановления:**

```python
recovered = previously_stale - current_stale_keys
for key in recovered:
    log.info("stale_recovered", key=key)
```

Если ключ был устаревшим на прошлом цикле, но стал свежим — логируется `stale_recovered`. Это позволяет отслеживать временные разрывы соединения.

**Нюанс — SCAN_COUNT vs реальное число ключей:**

`SCAN_COUNT=500` — это **хинт** для Redis, а не гарантия. Redis может вернуть больше или меньше ключей за итерацию в зависимости от структуры hash-таблицы. Для корректной работы важен только `cursor == 0` — признак завершения полного прохода.

**Нюанс — REDIS_KEY_TTL vs STALE_THRESHOLD_SEC:**

`REDIS_KEY_TTL` (300с по умолчанию) **должен быть больше** `STALE_THRESHOLD_SEC` (60с). Если TTL меньше порога, ключи будут исчезать из Redis раньше, чем монитор успеет их пометить как stale — монитор никогда не увидит устаревших ключей, но данные фактически могут быть устаревшими.

**Нюанс — ключ без ts_redis:**

Если ключ существует, но не содержит поле `ts_redis` (повреждённая запись) — монитор пропускает его (`continue`). Это защита от паники при нестандартных ключах в Redis.

### Алерт в Redis pub/sub

```json
{
  "ts": 1710000000000,
  "stale_count": 3,
  "symbols": [
    {"key": "md:binance:spot:XYZUSDT", "age_sec": 120},
    ...
  ]
}
```

Публикуется в канал `alerts:stale`.

---

## 5. latency_monitor.py — монитор задержек

**Файл:** `market-data/latency_monitor.py`

### Что измеряет

Три метрики задержки (в мс) для каждого сообщения:

| Метрика | Формула | Смысл |
|---|---|---|
| `exchange_to_collector` | `ts_received - ts_exchange` | Время от биржи до нашего кода |
| `collector_to_redis` | `ts_redis - ts_received` | Время записи в Redis |
| `end_to_end` | `ts_redis - ts_exchange` | Полное время от биржи до Redis |

### Алгоритм — rolling SCAN cursor

В отличие от `stale_monitor`, который делает полный SCAN за один цикл, `latency_monitor` использует **rolling cursor**: каждые 10 с берёт следующие `SAMPLING_MAX_KEYS` ключей, не сбрасывая cursor в 0. Когда cursor обнуляется (полный проход), выдаётся отчёт.

```
Итерация 1: cursor=0  → cursor=847,  ключи [0:200]
Итерация 2: cursor=847→ cursor=1543, ключи [200:400]
...
Итерация N: cursor=XY → cursor=0,    ключи [последние]
→ отчёт → cursor=0, начинаем снова
```

Преимущество: нет пика нагрузки на Redis, нагрузка распределена во времени.

### Нюанс — Binance Spot и ts_exchange

```python
if ts_e == ts_r or ts_e == 0:
    # Binance Spot: ts_exchange=0, ts_received=фактическое
    return {"exchange_to_collector": None, "end_to_end": None, ...}
```

Для Binance Spot `ts_exchange="0"` — монитор пропускает расчёт `exchange_to_collector` и `end_to_end`, чтобы не публиковать вводящие в заблуждение нули.

### Нюанс — e2e только для BTCUSDT

```python
if symbol == "BTCUSDT" and lats["end_to_end"] is not None:
    e2e_by_market[market_key].append(e2e_val)
```

End-to-end latency накапливается только для символа `BTCUSDT`. Это экономит память при большом числе символов и предполагает, что BTCUSDT репрезентативен для всего рынка.

### Нюанс — clock_skew

```python
clock_skew = exchange_to_collector < 0
```

Если `ts_exchange > ts_received` — часы коллектора отстают от часов биржи. Логируется как `clock_skew_detected`. **Это не ошибка**, а информационное предупреждение. При расхождении > 1с рекомендуется синхронизировать NTP.

### Отчёт (latency_report)

```json
{
  "event": "latency_report",
  "exchange": "binance",
  "market": "futures",
  "samples": 200,
  "e2e_ms": {"min": 12.3, "max": 450.7, "avg": 45.2, "p95": 180.1},
  "collector_to_redis_ms": {"min": 0.1, "max": 2.3, "avg": 0.4, "p95": 1.1},
  "anomalies": 0
}
```

### Пороги аномалий

| Порог | Действие |
|---|---|
| `ANOMALY_WARN_MS` (1000) | `log.warning("latency_anomaly")` |
| `ANOMALY_CRIT_MS` (5000) | `log.critical("latency_anomaly")` |

---

## 6. spread_scanner.py — сканер арбитражного спреда

**Файл:** `spread-scanner/spread_scanner.py`

### Концепция

Каждые 200 мс сканирует все пары из 12 арбитражных направлений. Для каждой пары вычисляет спред:

```
spread_pct = (sell_bid - buy_ask) / buy_ask * 100
```

Если `spread_pct >= MIN_SPREAD_PCT` и нет cooldown — записывает сигнал.

### 12 направлений (A–L)

| Направление | Покупка (spot ask) | Продажа (futures bid) |
|---|---|---|
| A | binance spot | bybit futures |
| B | bybit spot | binance futures |
| C | okx spot | binance futures |
| D | binance spot | okx futures |
| E | okx spot | bybit futures |
| F | bybit spot | okx futures |
| G | gate spot | binance futures |
| H | binance spot | gate futures |
| I | gate spot | bybit futures |
| J | bybit spot | gate futures |
| K | gate spot | okx futures |
| L | okx spot | gate futures |

### Цикл `_cycle()`

```
1. Один Redis pipeline с 2×N HMGET:
   - HMGET md:{buy_ex}:{buy_mkt}:{sym}  ask ask_qty ts_redis
   - HMGET md:{sell_ex}:{sell_mkt}:{sym} bid bid_qty ts_redis
   (все N пар всех 12 направлений — за один round-trip)

2. calc_spread() для каждой пары — чистая функция

3. Если spread_pct >= MIN_SPREAD_PCT И нет cooldown:
   - Устанавливаем cooldown[(direction, symbol)] = now_ms()
   - Формируем CSV-строку
   - Собираем в to_write список

4. Второй pipeline:
   - SET sig:spread:{direction}:{symbol} → JSON (TTL SIGNAL_TTL)
   - PUBLISH ch:spread_signals → JSON

5. Пишем в:
   - logs/spread_scanner.log (каждый цикл, статистика)
   - signals/spread_signals.csv (только сигналы)
   - signals/spread_signals_anomalies.csv (только аномалии)
```

**Нюанс — stale check в сканере:**

Переменная `STALE_THRESHOLD_MS` в сканере использует имя `STALE_THRESHOLD_SEC` из env, но хранит значение уже в **миллисекундах**:
```python
STALE_THRESHOLD_MS = int(os.getenv("STALE_THRESHOLD_SEC", "300")) * 1000
```
По умолчанию `300 * 1000 = 300000 мс = 5 минут`. Это отдельный параметр от `STALE_THRESHOLD_SEC` в `stale_monitor.py` (60с)! Сканер использует более мягкий порог — цены, устаревшие менее чем на 5 минут, считаются пригодными для расчёта спреда.

**Нюанс — cooldown:** Cooldown не блокирует сканирование — он блокирует только **запись сигнала**. Спред продолжает вычисляться каждые 200 мс, счётчик `suppressed` увеличивается. Это позволяет отслеживать динамику спреда без спама файлов.

**Нюанс — очистка cooldown:** Словарь `_cooldown` очищается каждые 1000 циклов (~3 мин при 200 мс интервале). При большом числе пар словарь может разрастаться, но очистка по истечении `COOLDOWN_MS` происходит автоматически.

**Нюанс — SIGHUP для перезагрузки символов:**

```python
loop.add_signal_handler(signal.SIGHUP, _sighup)
```

Отправьте `kill -HUP <pid>` чтобы перезагрузить списки символов из файлов без остановки сканера. Удобно после обновления `dictionaries/`.

### Prometheus метрики (опционально)

Если установлен `prometheus-client` и `ENABLE_PROMETHEUS=1`:

| Метрика | Тип | Описание |
|---|---|---|
| `spread_signals_total` | Counter | Число сигналов по направлению |
| `spread_pct` | Gauge | Текущий спред % по (direction, symbol) |
| `spread_cycles_total` | Counter | Общее число циклов |

Экспортируются на `http://0.0.0.0:{PROMETHEUS_PORT}` (по умолчанию 9091).

### Ротация файлов

| Файл | Максимум | Архивов |
|---|---|---|
| `logs/spread_scanner.log` | 50 МБ | 5 |
| `signals/spread_signals.csv` | 100 МБ | 10 |
| `signals/spread_signals_anomalies.csv` | 100 МБ | 10 |

---

## 7. signal_snapshot.py — логгер снапшотов

**Файл:** `spread-scanner/signal_snapshot.py`

### Что делает

Подписывается на Redis-канал `ch:spread_signals`. При получении сигнала запускает asyncio-задачу (`snapshot_task`), которая в течение `SNAPSHOT_DURATION_S` секунд (по умолчанию 3500 ≈ 58 минут) каждые `SNAPSHOT_INTERVAL_S` секунд (0.3 с) читает текущие цены из Redis и дописывает строку в CSV-файл.

### Структура файлов снапшотов

```
signals/
  binance_s_bybit_f/       ← direction A
    BTCUSDT_1710000000000.csv
    ETHUSDT_1710000001234.csv
  bybit_s_binance_f/       ← direction B
    ...
```

### Формат CSV

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
```

| Строка | Описание |
|---|---|
| Первая | Исходный сигнал (цены из сигнала, `spread_pct` из сигнала) |
| Последующие | Текущие цены из Redis каждые 0.3 с, `spread_pct` пересчитывается |

**Нюанс — файл открыт на всё время снапшота:**

Файл открывается один раз с `buffering=1` (строчная буферизация). Каждая строка немедленно флашится на диск. Это важно: при падении процесса данные до последней строки будут сохранены.

**Нюанс — параллельные задачи:**

Каждый сигнал порождает отдельную asyncio-задачу. При частых сигналах может быть много параллельных задач. Задачи хранятся в `set[Task]` с автоочисткой через `done_callback(tasks.discard)`. При завершении работы (`finally`) ждём все оставшиеся задачи через `asyncio.gather(*tasks)`.

**Нюанс — 3500 секунд:**

Значение 3500с (~58.3 мин) выбрано чуть меньше 1 часа — это максимальный период cooldown в сканере. Снапшот захватывает почти полный жизненный цикл сигнала.

**Нюанс — Redis недоступен во время снапшота:**

При ошибке Redis в `snapshot_task` логируется ошибка, задача спит `SNAPSHOT_INTERVAL_S` и пробует снова — без прерывания всего снапшота. Строки за период недоступности Redis будут пропущены в файле.

---

## 8. price_history_writer.py — запись истории цен

**Файл:** `market-data/price_history_writer.py`
**Тип:** отдельный сервис (запускается через `run.py`)

### Назначение

Хранит непрерывную историю цен в Redis в виде скользящего окна из 5 чанков по 20 минут (~100 минут суммарно). Обновляет историю на каждое изменение цены по всем 8 источникам.

### Принцип работы

```
8 pub/sub-каналов md:updates:{exchange}:{market}
          │
          ▼  (asyncio.create_task — не блокирует слушатель)
    HGETALL md:{exchange}:{market}:{symbol}
          │
          ▼
    ZADD md:hist:{exchange}:{market}:{symbol}:{chunk_num}   score=ts_received_ms
    EXPIRE … CHUNK_TTL_SEC
```

### Схема ключей Redis

#### Ключи данных (Sorted Set)

```
md:hist:{exchange}:{market}:{symbol}:{chunk_num}
```

| Атрибут | Описание |
|---|---|
| Тип | Sorted Set (ZSET) |
| Score | `ts_received_ms` — Unix timestamp в миллисекундах |
| Member | JSON-строка с ценовыми полями (см. ниже) |
| TTL | `CHUNK_DURATION_SEC × (MAX_CHUNKS + 1)` = 7200 с |

Поля JSON-члена:

| Поле | Описание |
|---|---|
| `bid` | Лучший бид |
| `ask` | Лучший аск |
| `bid_qty` | Количество по биду |
| `ask_qty` | Количество по аску |
| `last` | Последняя цена |
| `ts_exchange` | Метка времени биржи (ms, строка) |
| `ts_received` | Метка времени получения коллектором (ms, строка) |
| `ts_redis` | Метка времени записи в Redis (ms, строка) |

#### Конфигурационный ключ (Hash)

```
md:hist:config
```

| Поле | Значение | Описание |
|---|---|---|
| `chunk_duration_sec` | `"1200"` | Длина одного чанка в секундах |
| `max_chunks` | `"5"` | Число хранимых чанков |
| `chunk_ttl_sec` | `"7200"` | TTL каждого ZSET-ключа |
| `sources` | `"binance:spot,binance:futures,..."` | Все 8 источников |
| `key_pattern` | `"md:hist:{exchange}:{market}:{symbol}:{chunk_num}"` | Шаблон ключа |
| `config_key` | `"md:hist:config"` | Ключ этого же хэша |
| `formula` | `"chunk_num = int(ts_received_ms / chunk_duration_ms)"` | Формула номера чанка |
| `active_formula` | `"range(current_chunk - max_chunks + 1, current_chunk + 1)"` | Активные чанки |
| `score_field` | `"ts_received_ms"` | Поле, используемое как score |
| `member_format` | `"JSON: bid, ask, bid_qty, ..."` | Формат члена ZSET |
| `note` | человекочитаемое описание | — |

### Логика чанков

```
CHUNK_DURATION_MS = 1_200_000  (20 мин × 60 с × 1000 мс)

chunk_num = ts_received_ms // CHUNK_DURATION_MS

Чанк N покрывает:
  start_ms = N × 1_200_000
  end_ms   = (N+1) × 1_200_000

Активные 5 чанков в любой момент:
  current = int(time.time() * 1000) // CHUNK_DURATION_MS
  active  = range(current - 4, current + 1)
```

**Ротация без явного удаления:**

Когда `chunk_num` вырастает (каждые 20 мин), писатель просто начинает писать в новый ключ. Ключ чанка `N-5` истекает по TTL (7200 с = 2 часа). Явный DEL не нужен — Redis убирает старые данные сам.

```
t=0 мин  │ пишем в chunk 4217   [4217, —, —, —, —]
t=20 мин │ пишем в chunk 4218   [4217, 4218, —, —, —]
t=40 мин │ пишем в chunk 4219   [4217, 4218, 4219, —, —]
t=60 мин │ пишем в chunk 4220   [4217, 4218, 4219, 4220, —]
t=80 мин │ пишем в chunk 4221   [4217, 4218, 4219, 4220, 4221]  ← 5 чанков
t=100 мин│ пишем в chunk 4222   [      4218, 4219, 4220, 4221, 4222]  ← 4217 истёк по TTL
```

### Параметры окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `PRICE_CHUNK_DURATION_SEC` | `1200` | Длина чанка в секундах |
| `PRICE_MAX_CHUNKS` | `5` | Число хранимых чанков |
| `PRICE_STATS_INTERVAL_SEC` | `60` | Интервал логирования статистики |

### Логируемые события

| Событие | Уровень | Когда |
|---|---|---|
| `hist_config_written` | INFO | Старт: `md:hist:config` записан в Redis |
| `price_history_writer_started` | INFO | Подписка на все 8 каналов завершена |
| `history_stats` | INFO | Каждые `PRICE_STATS_INTERVAL_SEC` секунд: кол-во записей, текущий чанк |
| `history_write_error` | ERROR | Ошибка при обработке одного тика |
| `price_history_writer_stopped` | INFO | Корректное завершение |

---

## 9. price_history_reader.py — чтение истории цен

**Файл:** `market-data/price_history_reader.py`
**Тип:** библиотека (не запускается сама по себе, импортируется)

### Назначение

Предоставляет готовые функции для чтения данных из скользящего окна чанков. Импортируется любым скриптом, которому нужна история цен.

### Быстрый старт

```python
import redis.asyncio as aioredis
from price_history_reader import (
    get_history,       # полная история ~100 мин
    get_latest,        # последние N тиков
    get_range,         # произвольный time range
    get_history_multi, # несколько символов за один pipeline
    get_chunk_info,    # активные чанки без обращения к Redis
)

redis = aioredis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

# Полная история одного символа (~100 мин, 5 чанков)
history = await get_history(redis, "binance", "spot", "BTCUSDT")

# Только последние 20 минут
recent = await get_history(redis, "binance", "spot", "BTCUSDT", n_chunks=1)

# Последний тик
last = await get_latest(redis, "binance", "spot", "BTCUSDT", count=1)

# Произвольный диапазон по времени
data = await get_range(redis, "bybit", "futures", "ETHUSDT",
                       start_ms=1_700_000_000_000,
                       end_ms=1_700_001_200_000)

# Несколько символов — один pipeline round-trip
bulk = await get_history_multi(redis, "binance", "spot",
                               ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
# → {"BTCUSDT": [...], "ETHUSDT": [...], "SOLUSDT": [...]}

# Информация о чанках без Redis
info = get_chunk_info()
```

### Формат возвращаемых записей

Каждая запись в списке истории — словарь:

```python
{
    # Ценовые данные (строки, как хранятся в Redis)
    "bid":         "50000.10",
    "ask":         "50001.20",
    "bid_qty":     "0.500",
    "ask_qty":     "1.200",
    "last":        "50000.50",   # может быть "" для Gate Spot и Binance Spot
    "ts_exchange": "1710000000000",  # "0" для Binance Spot
    "ts_received": "1710000000050",
    "ts_redis":    "1710000000051",

    # Метаданные чанка (добавляются reader-ом)
    "chunk_num":      4221,
    "chunk_start_ms": 5065200000000,   # включительно
    "chunk_end_ms":   5065201200000,   # не включительно
}
```

### Справочник функций

#### `get_chunk_info(n_chunks=5) → dict`

Информация об активных чанках. **Не обращается к Redis** — вычисляется из системного времени.

```python
info = get_chunk_info()
# {
#   "current_chunk":      4221,
#   "active_chunks":      [4217, 4218, 4219, 4220, 4221],
#   "chunk_duration_sec": 1200,
#   "max_chunks":         5,
#   "total_history_min":  100,
#   "chunk_details": [
#     {"chunk_num": 4217,
#      "start_ms":  ..., "end_ms": ...,
#      "start_iso": "2026-03-17T04:40:00Z",
#      "end_iso":   "2026-03-17T05:00:00Z"},
#     ...
#   ]
# }
```

#### `get_history(redis, exchange, market, symbol, n_chunks=5) → list[dict]`

Полная история символа: до `n_chunks × 20` минут данных. Один pipeline — один round-trip в Redis. Результат отсортирован от старых к новым.

#### `get_latest(redis, exchange, market, symbol, count=1) → list[dict]`

Последние `count` тиков. Ищет от нового чанка к старому. Результат отсортирован от старых к новым.

#### `get_range(redis, exchange, market, symbol, start_ms, end_ms) → list[dict]`

Все тики в диапазоне `[start_ms, end_ms]`. Автоматически вычисляет нужные чанки — работает через границу чанков.

#### `get_history_multi(redis, exchange, market, symbols, n_chunks=5) → dict[str, list[dict]]`

История для списка символов одного источника. Все чанки всех символов читаются в одном пайплайне. Эффективен при запросе 10+ символов одновременно.

### Вспомогательные функции

| Функция | Описание |
|---|---|
| `current_chunk_num() → int` | Текущий номер чанка (из системного времени) |
| `chunk_time_range_ms(n) → (start_ms, end_ms)` | Временной диапазон чанка N |
| `active_chunk_nums(n_chunks=5) → list[int]` | Список активных номеров чанков |

### Константы модуля

```python
CHUNK_DURATION_SEC = 1200      # 20 минут
MAX_CHUNKS         = 5
CHUNK_DURATION_MS  = 1_200_000
CHUNK_TTL_SEC      = 7200
```

> **Важно:** константы захардкожены и должны соответствовать значениям в `price_history_writer.py` (или переменным окружения `PRICE_CHUNK_DURATION_SEC` / `PRICE_MAX_CHUNKS`). Если вы изменили настройки writer-а через env — обновите константы reader-а.

### Три способа обнаружения схемы для нового скрипта

**Вариант A** — `import price_history_reader` (рекомендуется):
```python
from price_history_reader import get_history
```

**Вариант B** — inline-сниппет (нет зависимостей, просто скопировать):
```python
CHUNK_DURATION_MS = 1_200_000
MAX_CHUNKS = 5

current = int(time.time() * 1000) // CHUNK_DURATION_MS
for n in range(current - MAX_CHUNKS + 1, current + 1):
    key = f"md:hist:{exchange}:{market}:{symbol}:{n}"
    records = await redis.zrange(key, 0, -1, withscores=True)
```

**Вариант C** — читать `md:hist:config` из Redis (runtime self-discovery):
```python
cfg = await redis.hgetall("md:hist:config")
pattern  = cfg["key_pattern"]   # "md:hist:{exchange}:{market}:{symbol}:{chunk_num}"
formula  = cfg["formula"]       # "chunk_num = int(ts_received_ms / chunk_duration_ms)"
duration = int(cfg["chunk_duration_sec"])  # 1200
n_chunks = int(cfg["max_chunks"])          # 5
```

---

## 10. run.py — лончер всех процессов

**Файл:** `run.py` (корень репозитория)

### Что делает

Запускает до 12 дочерних процессов через `subprocess.Popen`, перенаправляет их stdout/stderr в общий лог и в отдельные файлы с ротацией.

### Порядок скриптов

```python
SCRIPTS = [
    "binance_spot", "binance_futures",
    "bybit_spot",   "bybit_futures",
    "okx_spot",     "okx_futures",
    "gate_spot",    "gate_futures",
    "stale_monitor", "latency_monitor",
    "spread_scanner", "signal_snapshot",
]
```

Скрипты запускаются **последовательно** с паузой 100 мс между стартами, чтобы не перегрузить Redis при инициализации.

### Нюанс — startup_cleanup

При старте (если не передан `--no-cleanup`):
1. Показывает обратный отсчёт 3 секунды (можно прервать Ctrl+C)
2. Удаляет папку `logs/`
3. Удаляет папку `signals/`
4. Вызывает `FLUSHDB` — **очищает текущую базу Redis**

> **Предупреждение:** `FLUSHDB` удаляет **все ключи** в выбранной базе Redis (db 0 по умолчанию). Если Redis используется для других задач — используйте `--no-cleanup`.

### Нюанс — PYTHONPATH

```python
env["PYTHONPATH"] = (
    script_dir + os.pathsep + MARKET_DATA_DIR + os.pathsep + env.get("PYTHONPATH", "")
)
```

Для `spread_scanner` и `signal_snapshot` (которые находятся в `spread-scanner/`) PYTHONPATH включает и `spread-scanner/`, и `market-data/` — это позволяет им делать `from common import ...`.

### Нюанс — stderr → stdout

```python
proc = subprocess.Popen(
    ...,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,  # ← stderr идёт в тот же поток
)
```

Stderr и stdout объединены в один поток. Это упрощает мониторинг, но не позволяет разделить их в файлах логов.

### Обработка сигналов и завершение

```
Ctrl+C / SIGTERM → handle_signal() → SIGTERM всем дочерним процессам
                 → ждём 10 секунд (grace period)
                 → если процесс не завершился → SIGKILL
```

Коллекторы при получении SIGTERM устанавливают `shutdown_event`, дожидаются завершения текущей итерации recv(), закрывают WS и Redis. Ожидаемое время остановки: до `WS_RECV_TIMEOUT` (60с) — **это больше grace period**. Поэтому при принудительном Ctrl+C некоторые коллекторы могут получить SIGKILL, не дождавшись таймаута recv.

> Совет: увеличьте grace period или уменьшите WS_RECV_TIMEOUT если нужно чистое завершение.

### Нюанс — процессы не перезапускаются

Если дочерний процесс упал, run.py логирует код возврата и продолжает работу с оставшимися процессами. Перезапуск — задача внешнего процессного менеджера (systemd, supervisord).

### Ротация логов run.py

| Файл | Максимум | Архивов | Итого |
|---|---|---|---|
| `logs/{script}.log` | 50 МБ | 5 | ~300 МБ/скрипт |
| `logs/run.log` | 50 МБ | 5 | ~300 МБ |

### Аргументы командной строки

| Аргумент | Описание |
|---|---|
| `--only SCRIPT [SCRIPT...]` | Запустить только указанные скрипты |
| `--no-monitors` | Не запускать stale_monitor и latency_monitor |
| `--logs-dir DIR` | Другая папка для логов (по умолчанию `logs/`) |
| `--no-cleanup` | Не очищать Redis и не удалять logs/signals/ при старте |

---

## 11. dictionaries/main.py — генератор словарей

**Файл:** `dictionaries/main.py`
Запускается **вручную** перед первым запуском системы и при обновлении торговых пар.

### Конвейер

```
1. Binance REST API   → data/binance_spot_raw.txt, data/binance_futures_raw.txt
2. Binance WS (60 с)  → data/binance_spot_active.txt, data/binance_futures_active.txt
3. Bybit REST API     → data/bybit_spot_raw.txt, ...
4. Bybit WS (60 с)    → data/bybit_spot_active.txt, ...
5. OKX REST API       → data/okx_spot_raw.txt, ...
6. OKX WS (60 с)      → data/okx_spot_active.txt, ...
7. Gate REST API      → data/gate_spot_raw.txt, ...
8. Gate WS (60 с)     → data/gate_spot_active.txt, ...
9. Intersection       → combination/*.txt (12 файлов)
10. Union/keyword     → subscribe/**/*.txt (8 файлов)
11. Итоговый отчёт
```

### Формирование combination файлов

Intersection (пересечение) активных символов двух рынков:

```python
intersection = sorted(set_a & set_b)
```

Пример: `binance_spot_bybit_futures.txt` = символы, которые есть **и** на Binance Spot, **и** на Bybit Futures (оба рынка активны по WS).

### Формирование subscribe файлов

Subscribe-файлы — **объединения** (union) всех combination-файлов, использующих данный рынок. Например, `subscribe/binance/binance_spot.txt` содержит все символы из всех combination-файлов, где есть `binance_spot`: A, D, G, H.

### Нюанс — WS-валидация

60 секунд прослушивания WS — это минимальное время для определения активности. За 60 с должно прийти хотя бы одно обновление по каждому символу. Тихие символы (неликвиды) отсеиваются.

### Нюанс — данные в data/

Промежуточные файлы в `data/` — кэш REST-ответов. При повторном запуске `main.py` REST-запросы делаются заново (нет кэширования между запусками).

---

## 12. Redis: ключи и каналы

### Ключи данных (Hash)

```
md:{exchange}:{market}:{symbol}
```

**Поля:**

| Поле | Тип | Описание | Нюанс |
|---|---|---|---|
| `ts_exchange` | str (ms) | Время на бирже | Binance Spot: всегда "0"; Gate: секунды×1000 |
| `ts_received` | str (ms) | Время получения | Устанавливается сразу после `ws.recv()` |
| `ts_redis` | str (ms) | Время записи в Redis | Устанавливается перед `pipeline.execute()` |
| `bid` | str | Лучший бид | Может отсутствовать (не все источники) |
| `bid_qty` | str | Количество по биду | Может отсутствовать |
| `ask` | str | Лучший аск | Не обновляется если пустой |
| `ask_qty` | str | Количество по аску | Не обновляется если пустой |
| `last` | str | Последняя цена | Gate: всегда отсутствует; Binance Spot: отсутствует |

**TTL:** Устанавливается при каждой записи через `EXPIRE`, значение `REDIS_KEY_TTL` секунд (300 по умолчанию). TTL обновляется при каждом обновлении — ключ живёт пока данные свежие.

### Pub/Sub каналы

| Канал | Издатель | Подписчики | Формат |
|---|---|---|---|
| `md:updates:{exchange}:{market}` | коллекторы | — (зарезервировано) | `{"symbol": "BTCUSDT", "key": "md:binance:spot:BTCUSDT"}` |
| `ch:spread_signals` | spread_scanner | signal_snapshot | JSON сигнала |
| `alerts:stale` | stale_monitor | внешние системы | JSON с списком stale ключей |

### Ключи истории цен (Sorted Set)

```
md:hist:{exchange}:{market}:{symbol}:{chunk_num}
```

| Атрибут | Описание |
|---|---|
| Score | `ts_received_ms` (Unix ms) |
| Member | JSON: bid, ask, bid_qty, ask_qty, last, ts_exchange, ts_received, ts_redis |
| TTL | 7200 с (авто-истекает, явный DEL не нужен) |
| Издатель | `price_history_writer.py` |

Формула номера чанка: `chunk_num = ts_received_ms // 1_200_000`

Активные чанки: `range(current_chunk - 4, current_chunk + 1)` (5 штук, ~100 мин)

### Конфигурация истории цен (Hash)

```
md:hist:config
```

Хранит параметры схемы (формулу, шаблон ключа, список источников и т.д.). Записывается `price_history_writer.py` при старте. Используется для runtime self-discovery.

### Ключи сигналов

```
sig:spread:{direction}:{symbol}   TTL=SIGNAL_TTL (60 с)
```

Значение — JSON сигнала. Используется для дедупликации и быстрого чтения последнего сигнала.

---

## 13. Переменные окружения

Все переменные читаются из `market-data/.env` (загружается через `load_dotenv()` в `common.py`).

### Общие (common.py)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | URL Redis |
| `REDIS_KEY_TTL` | `300` | TTL ключей md:* в секундах. **Должен быть > STALE_THRESHOLD_SEC** |
| `SYMBOLS_PER_CONN` | `150` | Символов на одно WS-соединение |
| `WS_RECV_TIMEOUT` | `60` | Таймаут recv() в секундах. **Должен быть > WS_PING_INTERVAL** |
| `RECONNECT_MAX_DELAY` | `60` | Максимальная задержка переподключения |
| `LOG_LEVEL` | `INFO` | Уровень логирования: DEBUG, INFO, WARNING |

### Коллекторы

| Переменная | По умолчанию | Описание |
|---|---|---|
| `WS_PING_INTERVAL` | 30 (Binance), 20 (Bybit/Gate), 25 (OKX) | Интервал пинга в секундах |
| `WS_STATS_INTERVAL` | `60` | Интервал статистики (только Bybit Spot) |

### stale_monitor

| Переменная | По умолчанию | Описание |
|---|---|---|
| `STALE_THRESHOLD_SEC` | `60` | Порог устаревания |
| `SCAN_INTERVAL_SEC` | `30` | Интервал сканирования |
| `SCAN_COUNT` | `500` | Хинт для SCAN |

### latency_monitor

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SAMPLING_INTERVAL_SEC` | `10` | Интервал sampling |
| `SAMPLING_MAX_KEYS` | `200` | Ключей за итерацию SCAN |
| `ANOMALY_WARN_MS` | `1000` | Порог предупреждения |
| `ANOMALY_CRIT_MS` | `5000` | Критический порог |

### spread_scanner

| Переменная | По умолчанию | Описание |
|---|---|---|
| `MIN_SPREAD_PCT` | `1.0` | Минимальный спред для сигнала, % |
| `ANOMALY_SPREAD_PCT` | `300.0` | Аномальный спред (дополнительный файл) |
| `SCAN_INTERVAL_MS` | `200` | Интервал сканирования в мс |
| `STALE_THRESHOLD_SEC` | `300` | Порог устаревания данных для сканера (в env это секунды, но используется ×1000) |
| `SIGNAL_TTL` | `60` | TTL сигнального ключа в Redis |
| `SIGNAL_COOLDOWN_SEC` | `3600` | Cooldown по (direction, symbol) |
| `ENABLE_PROMETHEUS` | `0` | Включить Prometheus (`1` или `0`) |
| `PROMETHEUS_PORT` | `9091` | Порт метрик |

### signal_snapshot

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SNAPSHOT_INTERVAL_S` | `0.3` | Интервал записи строк |
| `SNAPSHOT_DURATION_S` | `3500` | Длительность снапшота в секундах |

### price_history_writer

| Переменная | По умолчанию | Описание |
|---|---|---|
| `PRICE_CHUNK_DURATION_SEC` | `1200` | Длина одного чанка (20 мин). Изменение требует пересчёта TTL и обновления констант в reader |
| `PRICE_MAX_CHUNKS` | `5` | Число хранимых чанков (~100 мин окно). TTL = `CHUNK_DURATION_SEC × (MAX_CHUNKS + 1)` |
| `PRICE_STATS_INTERVAL_SEC` | `60` | Как часто логировать статистику записей |

### run.py

| Переменная | По умолчанию | Описание |
|---|---|---|
| `CLEANUP_DELAY` | `3` | Секунды до очистки при старте |

---

## 14. Форматы файлов и выходных данных

### JSON-логи (structlog, stdout)

Все скрипты пишут структурированные JSON-логи. Пример:

```json
{"timestamp":"2026-03-16T12:00:00.123Z","level":"info","event":"ws_connected","service":"bybit_spot","conn_id":2,"attempt":0,"sub_batches":4}
{"timestamp":"2026-03-16T12:00:05.456Z","level":"warning","event":"ws_recv_timeout","service":"okx_futures","conn_id":0,"timeout_sec":60}
{"timestamp":"2026-03-16T12:01:00.000Z","level":"info","event":"stale_scan_complete","service":"stale_monitor","cycle":3,"total_keys":1247,"stale_count":0,"in_grace":false}
```

### signals/spread_signals.csv

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,50000.1,50600.5,1.1992,1710000000000
```

Без заголовка в файле. Структура подразумевается по позиции столбца.

### signals/spread_signals_anomalies.csv

Такой же формат, но только строки со `spread_pct >= ANOMALY_SPREAD_PCT` (300%).

### signals/{dir_name}/{SYMBOL}_{ts_signal}.csv

```
binance,bybit,BTCUSDT,50000.1,50600.5,1.1992,1710000000000
binance,bybit,BTCUSDT,50000.2,50600.3,1.1974,1710000000300
binance,bybit,BTCUSDT,50000.4,50601.1,1.2013,1710000000600
...
```

Первая строка — сигнал (из spread_scanner), последующие — снапшоты каждые 0.3 с.

### logs/spread_scanner.log

Каждая строка — JSON с информацией о цикле:

```json
{"ts":1710000000000,"cycle":1,"pairs":1500,"signals":2,"suppressed":0,"elapsed_ms":45}
{"ts":1710000200000,"cycle":1001,"pairs":1500,"signals":0,"suppressed":1,"elapsed_ms":38}
```

---

## 15. Обработка ошибок и устойчивость

### In-memory буфер при падении Redis

Все коллекторы (кроме bybit_spot, где буфер — per-worker) имеют глобальный `_redis_buffer: deque(maxlen=1000)`.

**Поведение:**
- Redis недоступен → `write_redis()` падает → сообщение кладётся в буфер
- Буфер заполнен (1000 сообщений) → старейшие удаляются, логируется `redis_buffer_full`
- Redis восстановился → при следующем успешном `write_redis()` буфер вымывается (FIFO)
- При вымывании публикация в pub/sub **не производится** (только HSET + EXPIRE)

> **Нюанс:** Данные в буфере могут быть устаревшими к моменту вымывания. Стоит учитывать при анализе задержек.

### WS-переподключение с exponential backoff

```
delay = min(2^attempt, RECONNECT_MAX_DELAY) + random(0, 1)
```

| Попытка | Задержка |
|---|---|
| 0 | ~1 с |
| 1 | ~2 с |
| 2 | ~4 с |
| 3 | ~8 с |
| 4 | ~16 с |
| 5 | ~32 с |
| 6+ | ~60 с |

Random jitter [0,1) предотвращает синхронное переподключение всех воркеров одновременно.

### Обработка повреждённых сообщений

- `JSONDecodeError` → `log.warning("message_parse_error")`, сообщение пропускается, цикл продолжается
- Отсутствие обязательных полей в parse_message → возвращается `None`, запись пропускается
- Пустые поля bid/ask → не перезаписывают существующие данные в Redis

### Graceful shutdown

Все скрипты перехватывают `SIGTERM` и `SIGINT`:
1. Устанавливают `shutdown_event`
2. Завершают текущую итерацию
3. Закрывают WebSocket соединения
4. Закрывают Redis клиент (`aclose()`)
5. Логируют `graceful_shutdown`

---

## Обязательные события лога

| Событие | Уровень | Когда |
|---|---|---|
| `collector_started` | INFO | Старт скрипта, до подключения |
| `ws_connected` | INFO | Успешное WS-подключение |
| `ws_subscribed` | INFO | Подтверждение подписки от биржи |
| `ws_reconnecting` | WARNING | Начало переподключения |
| `ws_recv_timeout` | WARNING | Таймаут recv() |
| `ws_worker_stopped` | INFO | Воркер завершил работу |
| `redis_error` | ERROR | Ошибка записи в Redis |
| `redis_buffer_full` | WARNING | Буфер переполнен |
| `redis_recovered` | INFO | Redis восстановился, буфер вымыт |
| `graceful_shutdown` | INFO | Чистое завершение |
| `stale_detected` | WARNING | Найден устаревший ключ |
| `stale_recovered` | INFO | Ключ обновился (восстановился) |
| `latency_report` | INFO | Периодический отчёт о задержках |
| `clock_skew_detected` | INFO | Часы коллектора отстают от биржи |
| `latency_anomaly` | WARNING/CRITICAL | Аномальная задержка |
| `spread_signal` | INFO | Найден арбитражный сигнал |
| `snapshot_started` | INFO | Начат снапшот |
| `snapshot_done` | INFO | Снапшот завершён |
| `hist_config_written` | INFO | price_history_writer: md:hist:config записан |
| `price_history_writer_started` | INFO | price_history_writer: подписка на pub/sub готова |
| `history_stats` | INFO | price_history_writer: периодическая статистика записей |
| `history_write_error` | ERROR | price_history_writer: ошибка при записи тика |
| `price_history_writer_stopped` | INFO | price_history_writer: корректное завершение |
