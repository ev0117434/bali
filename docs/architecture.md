# Архитектура системы

## Обзор

```
┌──────────────────────────────────────────────────────────────────┐
│                    dictionaries/main.py                          │
│  (запускается вручную, обновляет списки символов)                │
│                                                                  │
│  Binance REST+WS  Bybit REST+WS  OKX REST+WS  Gate.io REST+WS   │
│        ↓                ↓             ↓              ↓           │
│   combination/      (12 пересечений пар между биржами)           │
│        ↓                                                         │
│   subscribe/        binance/ bybit/ okx/ gate/                   │
└───────────────────────────┬──────────────────────────────────────┘
                            │ читают при старте
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Биржи (WS)                              │
│  Binance Spot/Fut  Bybit Spot/Fut  OKX Spot/Fut  Gate Spot/Fut  │
└──┬──────────────────────────────────────────────────────────────┘
   │  8 коллекторов (N WS-соединений каждый)
   ▼
HSET md:{exchange}:{market}:{symbol}  +  EXPIRE TTL  +  PUBLISH md:updates:*
   │
   ▼
┌──────────┐
│  Redis   │  md:*:*:*       (Hash, TTL 300s)   ← текущая цена
│          │  md:hist:*:*:*  (List, no TTL)     ← история 100 мин
└────┬─────┘
     │
     ├──► stale_monitor    (SCAN + HGET, алерт если нет обновлений > 60s)
     ├──► latency_monitor  (SCAN + HMGET, считает e2e задержку)
     ├──► price_history    (psubscribe md:updates:*, пишет чанки 20 мин)
     │
     └──► spread_scanner   (pipeline HMGET каждые 200ms)
               │
               │  спред ≥ MIN_SPREAD_PCT
               ├──► SET  sig:spread:{direction}:{symbol}  (TTL 60s)
               ├──► PUBLISH ch:spread_signals  {json}
               └──► signals/spread_signals.csv  (CSV-строка)
                         │
                         ▼  pub/sub
                   signal_snapshot
                         │
                         └──► signals/{dir_name}/{SYMBOL}_{ts}.csv
                              (снапшот каждые 0.3с, 3500с)
```

## Поток данных

```
Биржа WS-сообщение
    │
    ├── ts_received = now_ms()
    │
    ▼
parse_message()           # извлечь bid/ask/symbol/ts_exchange
    │
    ▼
write_redis() — pipeline:
    ├── HSET md:{ex}:{mkt}:{sym}  {8 fields}
    ├── EXPIRE ... TTL
    └── PUBLISH md:updates:{ex}:{mkt}  {"symbol":..., "key":...}
```

## Соединения с биржей

Каждый коллектор открывает несколько параллельных WS-соединений:

```
N символов / SYMBOLS_PER_CONN(150) = K соединений

Binance Spot:    ~270 / 150 = 2 соединения
Binance Futures: ~310 / 150 = 3 соединения
Bybit Spot:      ~310 / 150 = 3 соединения
Bybit Futures:   ~270 / 150 = 2 соединения
OKX Spot:        ~270 / 150 = 2 соединения
OKX Futures:     ~270 / 150 = 2 соединения
Gate.io Spot:    ~270 / 150 = 2 соединения
Gate.io Futures: ~270 / 150 = 2 соединения

Итого: ~18 WS-соединений одновременно
```

Каждое соединение — отдельная asyncio.Task. Падение одного не влияет на остальные.

## Модуль spread-scanner

### spread_scanner.py

Запускается автоматически через `run.py`. Сканирует 12 направлений арбитража:

| Ключ | Направление |
|------|------------|
| A | binance_spot(ask) → bybit_futures(bid) |
| B | bybit_spot(ask) → binance_futures(bid) |
| C | okx_spot(ask) → binance_futures(bid) |
| D | binance_spot(ask) → okx_futures(bid) |
| E | okx_spot(ask) → bybit_futures(bid) |
| F | bybit_spot(ask) → okx_futures(bid) |
| G | gate_spot(ask) → binance_futures(bid) |
| H | binance_spot(ask) → gate_futures(bid) |
| I | gate_spot(ask) → bybit_futures(bid) |
| J | bybit_spot(ask) → gate_futures(bid) |
| K | gate_spot(ask) → okx_futures(bid) |
| L | okx_spot(ask) → gate_futures(bid) |

Каждый цикл (по умолчанию 200 мс): один Redis pipeline → HMGET всех пар → расчёт спреда.
Cooldown по умолчанию: 3600 сек на (direction, symbol).
Поддерживает SIGHUP для перезагрузки файлов символов без перезапуска.

**Файлы сигналов:**

```
signals/spread_signals.csv              # все сигналы
signals/spread_signals_anomalies.csv    # спред > ANOMALY_SPREAD_PCT (300%)
```

**Формат строки в CSV:**
```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,45000.1,45451.5,1.0023,1741234567890
```

**Формат сигнала в Redis (pub/sub и SET):**
```json
{
  "symbol":        "BTCUSDT",
  "direction":     "A",
  "buy_exchange":  "binance",
  "buy_market":    "spot",
  "buy_ask":       "45000.1",
  "buy_ask_qty":   "0.532",
  "sell_exchange": "bybit",
  "sell_market":   "futures",
  "sell_bid":      "45451.5",
  "sell_bid_qty":  "1.2",
  "spread_pct":    1.0023,
  "ts_signal":     1741234567890
}
```

Redis-ключ последнего сигнала: `sig:spread:{direction}:{symbol}` (TTL 60s)
Redis-канал: `ch:spread_signals`

### signal_snapshot.py

Подписывается на `ch:spread_signals`. При получении сигнала запускает asyncio-задачу,
которая **3500 секунд каждые 0.3 сек** читает текущие цены из Redis и пишет строку в файл.

**Файловая структура:**
```
signals/
  binance_s_bybit_f/          ← направление A
    BTCUSDT_1741234567890.csv
    ETHUSDT_1741234567891.csv
  bybit_s_binance_f/          ← направление B
  okx_s_binance_f/            ← C
  binance_s_okx_f/            ← D
  okx_s_bybit_f/              ← E
  bybit_s_okx_f/              ← F
  gate_s_binance_f/           ← G
  binance_s_gate_f/           ← H
  gate_s_bybit_f/             ← I
  bybit_s_gate_f/             ← J
  gate_s_okx_f/               ← K
  okx_s_gate_f/               ← L
```

**Формат строк:** такой же CSV как у spread_scanner:
```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
```
Первая строка — исходный сигнал (цены в момент его обнаружения).
Последующие строки — текущие цены на каждом тике 0.3 сек.

Несколько сигналов на разные символы/направления обрабатываются параллельно (asyncio tasks).

### Как подписаться на сигналы из другого скрипта

```python
import asyncio, orjson
from common import get_redis

async def main():
    redis  = get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe("ch:spread_signals")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        sig = orjson.loads(message["data"])
        # sig содержит: symbol, direction, buy_exchange, sell_exchange,
        #               buy_ask, sell_bid, spread_pct, ts_signal

asyncio.run(main())
```

Проверка в терминале:
```bash
redis-cli subscribe ch:spread_signals
```

## Модуль dictionaries

Запускается вручную (`python3 dictionaries/main.py`) для актуализации списков символов.

### Пайплайн

```
1. REST API (4 биржи параллельно)
      Binance /exchangeInfo
      Bybit   /instruments-info
      OKX     /instruments
      Gate.io /currency_pairs + /contracts
          ↓
   Все USDT/USDC пары → {exchange}/data/

2. WebSocket-валидация (4 биржи параллельно, 60 сек)
      Подписка на все пары → наблюдение
          ↓
   Активные пары → {exchange}/data/{exchange}_{market}_active.txt

3. Пересечения (12 комбинаций)
      Для каждой пары бирж: A_spot ∩ B_futures
          ↓
   combination/*.txt

4. Subscribe-файлы (8 файлов)
          ↓
   subscribe/{exchange}/{exchange}_{market}.txt
```

### Нормализация форматов

| Биржа | Нативный формат | Нормализованный |
|-------|----------------|----------------|
| Binance | `BTCUSDT` | `BTCUSDT` |
| Bybit | `BTCUSDT` | `BTCUSDT` |
| OKX | `BTC-USDT`, `BTC-USDT-SWAP` | `BTCUSDT` |
| Gate.io | `BTC_USDT` | `BTCUSDT` |

## История цен (price_history.py)

```
psubscribe md:updates:*
       │
       ▼  (exchange, market, symbol) → буфер 100мс / 200 шт
pipeline HMGET  bid, ask, ts_redis
       │
       ▼
chunk_num = unix_time // 1200
slot      = chunk_num % 5

Если chunk_num изменился:
  DEL md:hist:{exchange}:{market}:{symbol}:{slot}   ← стираем старый чанк

RPUSH md:hist:{exchange}:{market}:{symbol}:{slot}  "{ts}:{bid}:{ask}"
```

Всегда доступны **5 чанков по 20 минут** (100 минут истории) для каждого из 8 источников и каждого символа. Слоты 0–4 ротируются циклически.

## Отказоустойчивость

```
Сценарий              Поведение
─────────────────────────────────────────────────────────
WS disconnect         reconnect с backoff (1s → 60s max)
WS recv timeout       reconnect (нет данных 60 сек)
Redis down            буфер deque(1000), запись при recovery
Коллектор упал        run.py логирует, другие продолжают
Символ stale          stale_monitor алерт через 60 сек
price_history упал    hist-ключи остаются, при старте продолжается запись
```

## Мониторинг

```
stale_monitor ──SCAN──► Redis ──ts_redis──► delta > 60s? ──► PUBLISH alerts:stale
                                                          └──► log WARNING stale_detected

latency_monitor ─SCAN─► Redis ──ts fields──► stats(min/avg/p95) ──► log INFO latency_report
                                          └──► e2e > 1000ms? ──► log WARNING latency_anomaly
                                          └──► e2e > 5000ms? ──► log CRITICAL latency_anomaly

price_history ─psubscribe─► md:updates:* ──► HMGET bid/ask ──► RPUSH md:hist:*:{slot}
                                                             └──► DEL при смене чанка
```
