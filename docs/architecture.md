# Архитектура системы

## Обзор

```
┌─────────────────────────────────────────────────────────┐
│                      Биржи (WS)                         │
│  Binance Spot   Binance Futures   Bybit Spot   Bybit Fut │
└────────┬───────────────┬──────────────┬──────────┬──────┘
         │               │              │          │
         ▼               ▼              ▼          ▼
   binance_spot   binance_futures  bybit_spot  bybit_futures
    (3 conn)        (3 conn)        (3 conn)    (3 conn)
         │               │              │          │
         └───────────────┴──────────────┴──────────┘
                                │
                    Redis Pipeline (HSET + EXPIRE + PUBLISH)
                                │
                          ┌─────▼──────┐
                          │   Redis    │
                          │            │
                          │  md:*:*:*  │  ← Hash keys (TTL 300s)
                          │            │
                          └─────┬──────┘
                                │
               ┌────────────────┼──────────────────┐
               │                │                   │
         stale_monitor   latency_monitor      consumers
          (SCAN+HGET)    (SCAN+HMGET)       (SUBSCRIBE)
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

Binance Spot:    446 / 150 = 3 соединения (150 + 150 + 146)
Binance Futures: 401 / 150 = 3 соединения (150 + 150 + 101)
Bybit Spot:      392 / 150 = 3 соединения (150 + 150 + 92)
Bybit Futures:   391 / 150 = 3 соединения (150 + 150 + 91)

Итого: 12 WS-соединений одновременно
```

Каждое соединение — отдельная asyncio.Task. Падение одного не влияет на остальные.

## Отказоустойчивость

```
Сценарий              Поведение
─────────────────────────────────────────────────────────
WS disconnect         reconnect с backoff (1s → 60s max)
WS recv timeout       reconnect (нет данных 60 сек)
Redis down            буфер deque(1000), запись при recovery
Коллектор упал        run.py логирует, другие продолжают
Символ stale          stale_monitor алерт через 60 сек
```

## Мониторинг

```
stale_monitor ──SCAN──► Redis ──ts_redis──► delta > 60s? ──► PUBLISH alerts:stale
                                                           └──► log WARNING stale_detected

latency_monitor ─SCAN─► Redis ──ts fields──► stats(min/avg/p95) ──► log INFO latency_report
                                          └──► e2e > 1000ms? ──► log WARNING latency_anomaly
                                          └──► e2e > 5000ms? ──► log CRITICAL latency_anomaly
```

## Масштабирование

Текущая архитектура рассчитана на одну машину. При необходимости горизонтального масштабирования:

- **Несколько Redis** — изменить `REDIS_URL` в каждом скрипте. Коллекторы stateless.
- **Разделение символов** — отредактировать `subscribe/*.txt` файлы, запустить несколько экземпляров.
- **Добавить биржу** — новый коллектор по аналогии с существующими + новые файлы в `dictionaries/subscribe/`.
