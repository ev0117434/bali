# Troubleshooting

## Коллектор не запускается

**`symbols_file_not_found` или `symbols_file_empty` в логах → Exit(1)**

Скрипт не нашёл файл символов или он пустой.

```bash
# Проверить что файл существует
ls -la dictionaries/subscribe/binance/binance_spot.txt

# Запустить из правильной директории
cd /path/to/bali
python3 run.py
```

---

## Redis недоступен

**Лог: `redis_error`**

Коллектор продолжает работать и буферизует до 1000 последних сообщений. При восстановлении Redis буфер сбрасывается автоматически (`redis_recovered`).

```bash
# Проверить Redis
redis-cli ping

# Проверить URL в .env
cat market-data/.env | grep REDIS_URL
```

---

## Нет данных в Redis через 30+ секунд после старта

1. **Проверить лог коллектора:**
```bash
tail -50 logs/binance_spot.log | python3 -m json.tool
```

2. **Нет `ws_connected`?** — проблема с сетью или блокировка по IP.

3. **Есть `ws_connected`, но нет данных?** — проблема с подпиской. Искать `ws_subscribed` в логах.

4. **Много `ws_reconnecting` с `HTTP 403`?** — биржа блокирует IP (VPN, rate limit).

5. **Проверить напрямую:**
```bash
redis-cli KEYS "md:binance:spot:*" | head -5
redis-cli HGETALL md:binance:spot:BTCUSDT
```

---

## stale_monitor сразу алертит после старта

Нормальное поведение, если коллекторы ещё не успели заполнить Redis. `stale_monitor` имеет grace period 2 цикла (60 сек) — алерты не генерируются.

Если алерты идут после 60 сек — коллекторы действительно не пишут данные. Смотреть логи коллекторов.

---

## `REDIS_KEY_TTL` vs `STALE_THRESHOLD_SEC`

**Требование: `REDIS_KEY_TTL` > `STALE_THRESHOLD_SEC`**

Иначе ключи истекают раньше, чем stale_monitor их обнаружит — мониторинг слепнет.

```
По умолчанию:
  REDIS_KEY_TTL=300, STALE_THRESHOLD_SEC=60 ✓

Плохо:
  REDIS_KEY_TTL=60, STALE_THRESHOLD_SEC=60  ← ключ исчезает раньше алерта
```

---

## latency_monitor показывает огромный e2e для Binance Spot

Это нормально. Binance Spot bookTicker не передаёт timestamp биржи, поэтому `ts_exchange` устанавливается в `"0"`. `latency_monitor` пропускает `end_to_end` расчёт для таких ключей. В отчёте будет только `collector_to_redis_ms`.

---

## `clock_skew_detected` в логах

**Не ошибка.** Означает что `ts_received < ts_exchange` — часы коллектора отстают от часов биржи. Метрика `exchange_to_collector` будет отрицательной.

Если расхождение большое (> 1 сек) — синхронизировать NTP:
```bash
timedatectl status
sudo systemctl restart systemd-timesyncd
```

---

## Процессы не останавливаются по Ctrl+C

Нормальное поведение если скрипты уже в backoff-паузе (`asyncio.sleep`). Они завершатся в течение задержки reconnect (макс 60 сек) или по SIGKILL через grace period 10 сек.

```bash
# Принудительная остановка
kill -KILL $(pgrep -f "binance_spot.py")
```

---

## Нет файлов сигналов в signals/

**`signals/spread_signals.csv` не создаётся:**

1. Сканер запущен, но не находит спредов — нормально при спокойном рынке. Проверить `MIN_SPREAD_PCT` в `.env` (по умолчанию 1.0%).
2. Сканер не запущен:
```bash
ps aux | grep spread_scanner
tail -20 logs/spread_scanner.log
```
3. Файлы combination пустые:
```bash
wc -l dictionaries/combination/*.txt
```

---

## signal_snapshot не создаёт файлы снапшотов

**Папки `signals/{direction}/` не создаются:**

1. Нет сигналов → нет снапшотов. Сначала убедиться что `spread_signals.csv` пополняется.
2. `signal_snapshot` не подписан на канал. Проверить лог:
```bash
tail -20 logs/signal_snapshot.log
```
3. Проверить что канал активен:
```bash
redis-cli subscribe ch:spread_signals
# Дождаться сигнала или подать вручную для теста:
redis-cli publish ch:spread_signals '{"symbol":"BTCUSDT","direction":"A","buy_exchange":"binance","buy_market":"spot","buy_ask":"45000","buy_ask_qty":"1","sell_exchange":"bybit","sell_market":"futures","sell_bid":"45500","sell_bid_qty":"1","spread_pct":1.11,"ts_signal":1741234567890}'
```

---

## Формат файлов снапшотов

Каждый файл `signals/{dir_name}/{SYMBOL}_{ts}.csv`:
- Первая строка — цены в момент обнаружения сигнала
- Последующие строки — текущие цены из Redis (каждые 0.3 сек, 3500 сек)

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,45000.1,45451.5,1.0023,1741234567890
binance,bybit,BTCUSDT,45001.0,45452.0,1.0022,1741234568190
binance,bybit,BTCUSDT,44999.5,45450.0,1.0012,1741234568490
...
```

Если цены недоступны в Redis (ключ истёк или коллектор упал) — поля `ask_spot` и `bid_futures` будут пустыми, `spread_pct` тоже.

---

## Логи не в JSON формате

Проверить `LOG_LEVEL` в `.env`. При `LOG_LEVEL=DEBUG` могут появляться служебные строки от сторонних библиотек.

Парсить только строки начинающиеся с `{`:
```bash
tail -f logs/binance_spot.log | grep '^{' | jq .
```
