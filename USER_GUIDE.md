# User Guide — Руководство пользователя

> Система мониторинга рыночных данных и поиска арбитражного спреда
> Binance · Bybit · OKX · Gate.io

---

## Содержание

1. [Требования и установка](#1-требования-и-установка)
2. [Первичная настройка](#2-первичная-настройка)
3. [Обновление словарей (торговые пары)](#3-обновление-словарей-торговые-пары)
4. [Запуск системы](#4-запуск-системы)
5. [Проверка работоспособности](#5-проверка-работоспособности)
6. [Интерпретация вывода и логов](#6-интерпретация-вывода-и-логов)
7. [Работа с сигналами](#7-работа-с-сигналами)
8. [Управление запущенной системой](#8-управление-запущенной-системой)
9. [Типичные сценарии использования](#9-типичные-сценарии-использования)
10. [Диагностика проблем](#10-диагностика-проблем)
11. [Настройка параметров](#11-настройка-параметров)
12. [Продакшн-деплой](#12-продакшн-деплой)

---

## 1. Требования и установка

### Системные требования

- Python **3.10** или выше
- Redis **6.0** или выше (рекомендуется 7.x)
- ОС: Linux (рекомендуется), macOS
- RAM: минимум 512 МБ, рекомендуется 2 ГБ
- Диск: 5–10 ГБ для логов и снапшотов за сутки
- Сеть: стабильное интернет-соединение с низкой задержкой

### Установка Python-зависимостей

```bash
# Установка зависимостей для сборщиков данных и сканера
pip install -r market-data/requirements.txt
pip install -r spread-scanner/requirements.txt

# Если используется arb_terminal (опционально)
pip install -r arb_terminal/requirements.txt
```

> **Рекомендуется** использовать виртуальное окружение:
> ```bash
> python3 -m venv venv
> source venv/bin/activate
> pip install -r market-data/requirements.txt -r spread-scanner/requirements.txt
> ```

### Установка и запуск Redis

```bash
# Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis-server
sudo systemctl enable redis-server

# macOS
brew install redis
brew services start redis

# Проверка
redis-cli ping    # должно вернуть: PONG
```

---

## 2. Первичная настройка

### Создание файла конфигурации

```bash
cp market-data/.env.example market-data/.env
```

Откройте `market-data/.env` и проверьте/измените параметры:

```dotenv
# Обязательно: URL вашего Redis
REDIS_URL=redis://localhost:6379/0

# TTL ключей (ДОЛЖЕН быть больше STALE_THRESHOLD_SEC)
REDIS_KEY_TTL=300

# Уровень логирования (INFO в продакшне, DEBUG при отладке)
LOG_LEVEL=INFO

# Параметры сети
SYMBOLS_PER_CONN=150
WS_RECV_TIMEOUT=60
RECONNECT_MAX_DELAY=60
```

**Параметры, которые чаще всего изменяют:**

| Параметр | Когда изменять |
|---|---|
| `REDIS_URL` | Redis на другом хосте или с паролем: `redis://:password@host:6379/0` |
| `MIN_SPREAD_PCT` | Понизить для тестирования (0.1), повысить для уменьшения сигналов (2.0) |
| `SYMBOLS_PER_CONN` | Уменьшить при лимитах биржи, увеличить для снижения числа соединений |
| `LOG_LEVEL` | `DEBUG` для детальной диагностики |
| `HIST_CHUNK_SECONDS` | Укоротить окно (например, 600 = 10 мин) для теста ротации чанков |
| `HIST_MAX_CHUNKS` | Увеличить для более глубокой истории (5 × чанк) |

---

## 3. Обновление словарей (торговые пары)

Перед первым запуском и периодически (рекомендуется еженедельно) необходимо обновить списки торговых пар.

```bash
cd dictionaries
python3 main.py
```

Процесс занимает **около 10 минут** (каждая биржа: REST-запрос + 60 с WS-прослушивание × 4 биржи).

### Что происходит

1. Запрашиваются все торговые пары по REST API каждой биржи
2. По каждой бирже открывается WebSocket и в течение 60 с слушаются обновления
3. Символы, по которым пришло хотя бы одно обновление — считаются **активными**
4. Формируются файлы пересечений (`combination/`) и подписок (`subscribe/`)

### Ожидаемый результат

```
dictionaries/
  combination/
    binance_spot_bybit_futures.txt     # ~400-600 символов
    bybit_spot_binance_futures.txt
    ... (12 файлов)
  subscribe/
    binance/
      binance_spot.txt                 # ~600-800 символов
      binance_futures.txt
    bybit/...
    okx/...
    gate/...
```

### Проверка словарей

```bash
# Количество символов в каждом файле подписки
wc -l dictionaries/subscribe/*/binance*.txt

# Количество символов в combination-файлах
wc -l dictionaries/combination/*.txt
```

> **Важно:** После обновления словарей необходим перезапуск коллекторов (или всей системы через run.py).

---

## 4. Запуск системы

### Полный запуск (рекомендуется)

```bash
python3 run.py
```

Что произойдёт:
1. **Обратный отсчёт 3 секунды** с предупреждением об очистке данных
2. Удаление папок `logs/` и `signals/`
3. Очистка Redis (`FLUSHDB`)
4. Запуск 13 процессов

> **Осторожно:** `FLUSHDB` очищает базу Redis целиком. Если Redis используется для других задач — используйте `--no-cleanup`.

### Запуск без очистки данных

```bash
python3 run.py --no-cleanup
```

Используйте при перезапуске после сбоя, когда хотите сохранить накопленные логи.

### Запуск без мониторов

```bash
python3 run.py --no-monitors
```

Запускает 10 скриптов (без stale_monitor и latency_monitor). Полезно при разработке.

### Запуск отдельных скриптов

```bash
# Запустить только Binance и мониторы
python3 run.py --only binance_spot binance_futures stale_monitor latency_monitor

# Запустить только сканер и снапшотер
python3 run.py --only spread_scanner signal_snapshot
```

### Логи в отдельной папке

```bash
python3 run.py --logs-dir /var/log/market-data/
```

### Запуск отдельного скрипта вручную (для отладки)

```bash
cd market-data
python3 binance_spot.py
```

---

## 5. Проверка работоспособности

### Ожидаемый вывод при старте

```
[run.py] Очистка данных через 3 сек... (Ctrl+C для отмены)
[run.py]   3...
[run.py]   2...
[run.py]   1...
[run.py] Удалена папка логов:    /path/to/logs
[run.py] Удалена папка сигналов: /path/to/signals
[run.py] Redis очищен (FLUSHDB): redis://localhost:6379/0
------------------------------------------------------------
Запуск 13 скриптов: binance_spot, binance_futures, ..., price_history, ...
  Запущен binance_spot (PID 1234) → logs/binance_spot.log
  Запущен binance_futures (PID 1235) → logs/binance_futures.log
  ...
  Запущен price_history (PID 1245) → logs/price_history.log
  ...
```

Через несколько секунд вы должны увидеть цветные строки от каждого коллектора:

```
[binance_spot]     {"event":"ws_connected","conn_id":0,"symbols_count":150}
[bybit_spot]       {"event":"ws_connected","attempt":0,"sub_batches":30}
[okx_futures]      {"event":"ws_connected","conn_id":0}
```

### Проверка данных в Redis

```bash
# Количество ключей с данными
redis-cli DBSIZE

# Посмотреть данные по конкретному символу
redis-cli HGETALL md:binance:spot:BTCUSDT
redis-cli HGETALL md:bybit:futures:BTCUSDT

# Живой мониторинг обновлений
redis-cli SUBSCRIBE md:updates:binance:spot

# Ключи биржи
redis-cli KEYS "md:binance:*" | wc -l
redis-cli KEYS "md:bybit:*" | wc -l
redis-cli KEYS "md:okx:*" | wc -l
redis-cli KEYS "md:gate:*" | wc -l
```

### Проверка через 30-60 секунд после запуска

```bash
# Redis должен содержать тысячи ключей
redis-cli DBSIZE        # ожидается >1000

# TTL ключа должен регулярно обновляться (меняться от 300 вниз)
redis-cli TTL md:binance:spot:BTCUSDT

# Проверить что данные свежие (ts_redis в мс, должен быть недавним)
redis-cli HGET md:binance:futures:BTCUSDT ts_redis
```

### Проверка price_history

```bash
# Через 30-60 с после запуска — история начинает заполняться
SLOT=$(($(date +%s) / 1200 % 5))
echo "Текущий слот: $SLOT"

# Посмотреть первые несколько записей по BTCUSDT Binance Spot
redis-cli LRANGE "md:hist:binance:spot:BTCUSDT:$SLOT" 0 4

# Количество записей в чанке (растёт непрерывно в течение 20 мин)
redis-cli LLEN "md:hist:binance:spot:BTCUSDT:$SLOT"

# Убедиться что ключи истории существуют
redis-cli KEYS "md:hist:*" | wc -l

# Последняя запись: "{ts_ms}:{bid}:{ask}"
redis-cli LINDEX "md:hist:binance:spot:BTCUSDT:$SLOT" -1

# Реестр слотов: временно́й диапазон каждого слота
redis-cli HGETALL md:hist:registry

# Проверить лог price_history
tail -20 logs/price_history.log
```

**Ожидаемый вывод `LRANGE`:**
```
1) "1741234567890:42500.10:42501.20"
2) "1741234568001:42500.15:42501.25"
3) "1741234568110:42500.05:42501.10"
```

**Ожидаемый вывод `HGETALL md:hist:registry`:**
```
1) "2"
2) "1453:1741237200000"
3) "3"
4) "1449:1741230000000"
...
```
Формат значения: `"{chunk_num}:{start_ts_ms}"`. Текущий слот — тот, чей `chunk_num` совпадает с `$(date +%s) / 1200`.

**Ожидаемые строки лога при старте:**
```json
{"event":"price_history_started","chunk_seconds":1200,"max_chunks":5,"batch_size":200,"batch_timeout_ms":100}
{"event":"subscribed","pattern":"md:updates:*","chunk_seconds":1200,"max_chunks":5}
```

**Ожидаемые строки при смене чанка (каждые 20 мин):**
```json
{"event":"chunk_rotated","source":"binance:spot","symbol":"BTCUSDT","new_chunk":1234,"slot":4}
{"event":"registry_updated","slot":4,"chunk_num":1234,"start_ts_ms":1741237200000}
```
`registry_updated` выводится один раз на ротацию слота (не для каждого символа). `chunk_rotated` — для каждого символа (уровень DEBUG).

Чтобы увидеть `chunk_rotated`:
```bash
LOG_LEVEL=DEBUG python3 run.py --only price_history --no-cleanup
```

### Что означает нормальная работа

- Все 13 процессов запущены и пишут в лог
- `redis-cli DBSIZE` растёт до нескольких тысяч ключей
- `stale_monitor` не сообщает о stale ключах (после grace period ~60 с)
- `latency_monitor` публикует `latency_report` каждые ~10 с
- `redis-cli KEYS "md:hist:*" | wc -l` возвращает > 0 в течение минуты после старта
- `redis-cli LLEN md:hist:binance:spot:BTCUSDT:$SLOT` непрерывно растёт
- `redis-cli HGETALL md:hist:registry` возвращает до 5 записей (заполняется постепенно — по одной на каждый новый чанк)

---

## 6. Интерпретация вывода и логов

### Цвета в терминале

| Цвет | Скрипт |
|---|---|
| Синий | binance_spot |
| Голубой | binance_futures |
| Зелёный | bybit_spot |
| Жёлтый | bybit_futures |
| Белый | okx_spot |
| Светло-серый | okx_futures |
| Тёмно-зелёный | gate_spot |
| Циан | gate_futures |
| Фиолетовый | stale_monitor |
| Красный | latency_monitor |
| Оранжевый | spread_scanner |
| Пурпурный | signal_snapshot |
| Серый | price_history |

### Ключевые события лога

**Нормальный старт коллектора:**
```json
{"event":"collector_started","symbols_count":623,"connections":5}
{"event":"ws_connected","conn_id":0,"symbols_count":150}
{"event":"ws_subscribed","response":"None"}
```

**Переподключение (нормально при нестабильной сети):**
```json
{"event":"ws_reconnecting","attempt":0,"delay_sec":1.3,"error":"ConnectionClosedError"}
{"event":"ws_connected","conn_id":0,"symbols_count":150}
```

**Отчёт о задержках (каждые ~10–60 с):**
```json
{"event":"latency_report","exchange":"bybit","market":"futures","samples":200,
 "e2e_ms":{"min":12.3,"max":180.5,"avg":45.2,"p95":95.1},
 "collector_to_redis_ms":{"min":0.1,"max":2.1,"avg":0.4,"p95":1.0},
 "anomalies":0}
```

**Арбитражный сигнал:**
```json
{"event":"spread_signal","symbol":"XYZUSDT","direction":"binance_s_bybit_f",
 "spread_pct":1.45,"buy_ask":"0.12345","sell_bid":"0.12524"}
```

**Предупреждение об устаревших данных:**
```json
{"event":"stale_detected","key":"md:gate:spot:XYZUSDT","age_sec":120}
```

**Нормальный старт price_history:**
```json
{"event":"price_history_started","chunk_seconds":1200,"max_chunks":5,"batch_size":200,"batch_timeout_ms":100}
{"event":"subscribed","pattern":"md:updates:*"}
```

**Ошибка в price_history (например, Redis недоступен):**
```json
{"event":"batch_error","error":"Connection refused","exc_info":true}
```

### Понимание Binance Spot latency

Для Binance Spot поле `ts_exchange` всегда равно `"0"` — это нормально, не ошибка. Latency monitor не будет показывать `e2e_ms` для `binance:spot` — только `collector_to_redis_ms`.

### Понимание clock_skew_detected

```json
{"event":"clock_skew_detected","key":"md:okx:futures:BTCUSDT","skew_ms":-50}
```

Означает, что часы биржи опережают ваши часы на 50 мс. Небольшие значения (< 100 мс) — норма. При > 1000 мс рекомендуется синхронизировать NTP:

```bash
sudo ntpdate -u pool.ntp.org
# или
sudo systemctl restart systemd-timesyncd
```

---

## 7. Работа с сигналами

### Где находятся сигналы

```
signals/
  spread_signals.csv                  ← все сигналы
  spread_signals_anomalies.csv        ← аномальные спреды (>300%)
  binance_s_bybit_f/                  ← снапшоты направления A
    BTCUSDT_1710000000000.csv
    ETHUSDT_1710000001000.csv
  bybit_s_binance_f/                  ← снапшоты направления B
    ...
```

### Формат spread_signals.csv

```
binance,bybit,BTCUSDT,50000.10,50600.50,1.1992,1710000000000
```

| Позиция | Поле | Значение |
|---|---|---|
| 1 | spot_exch | Биржа покупки (spot) |
| 2 | fut_exch | Биржа продажи (futures) |
| 3 | symbol | Символ |
| 4 | ask_spot | Цена покупки на споте |
| 5 | bid_futures | Цена продажи на фьючерсе |
| 6 | spread_pct | Спред в % |
| 7 | ts | Unix timestamp в мс |

### Формат снапшот-файла

Первая строка — момент сигнала, последующие — эволюция цен каждые 0.3 с:

```
binance,bybit,BTCUSDT,50000.10,50600.50,1.1992,1710000000000   ← сигнал
binance,bybit,BTCUSDT,50000.20,50601.00,1.1997,1710000000300   ← 0.3 с спустя
binance,bybit,BTCUSDT,50000.15,50599.80,1.1986,1710000000600   ← 0.6 с спустя
...
```

### Просмотр последних сигналов

```bash
# Последние 20 сигналов
tail -20 signals/spread_signals.csv

# Сигналы за последний час
awk -F',' -v cutoff=$(($(date +%s%3N) - 3600000)) '$7 > cutoff' signals/spread_signals.csv

# Топ символов по числу сигналов
cut -d',' -f3 signals/spread_signals.csv | sort | uniq -c | sort -rn | head -20

# Сигналы конкретного направления (binance→bybit)
grep "^binance,bybit" signals/spread_signals.csv
```

### Проверка активных сигналов в Redis

```bash
# Список текущих активных сигналов (TTL 60 с)
redis-cli KEYS "sig:spread:*"

# Данные конкретного сигнала
redis-cli GET "sig:spread:A:BTCUSDT"

# Подписаться на новые сигналы в реальном времени
redis-cli SUBSCRIBE ch:spread_signals
```

---

## 8. Управление запущенной системой

### Остановка

```bash
Ctrl+C
# или
kill -TERM $(pgrep -f "run.py")
```

После сигнала остановки:
1. run.py шлёт SIGTERM всем дочерним процессам
2. Ожидает 10 секунд (grace period)
3. Если процесс не завершился — SIGKILL

### Перезагрузка списков символов без перезапуска

Если вы обновили словари через `dictionaries/main.py`:

```bash
# Найти PID spread_scanner
pgrep -f "spread_scanner.py"

# Отправить SIGHUP для перезагрузки символов
kill -HUP <PID>
```

> **Только для spread_scanner.** Коллекторы не поддерживают SIGHUP — им нужен полный перезапуск.

### Мониторинг процессов

```bash
# Список процессов
pgrep -af "python3.*\.py"

# Потребление памяти
ps aux | grep python3 | grep -v grep

# Наблюдение за логом
tail -f logs/binance_spot.log | python3 -c "import sys,json; [print(json.dumps(json.loads(l),indent=2)) for l in sys.stdin]"

# Простой просмотр
tail -f logs/spread_scanner.log
```

### Просмотр алертов о stale данных

```bash
redis-cli SUBSCRIBE alerts:stale
```

---

## 9. Типичные сценарии использования

### Сценарий 1: Первый запуск с нуля

```bash
# 1. Установка зависимостей
pip install -r market-data/requirements.txt -r spread-scanner/requirements.txt

# 2. Настройка конфигурации
cp market-data/.env.example market-data/.env
nano market-data/.env    # Проверьте REDIS_URL

# 3. Обновление словарей (10 минут)
cd dictionaries && python3 main.py && cd ..

# 4. Запуск системы
python3 run.py
```

### Сценарий 2: Запуск только одной биржи для тестирования

```bash
python3 run.py --only binance_spot binance_futures stale_monitor latency_monitor spread_scanner signal_snapshot --no-cleanup
```

### Сценарий 3: Отладка конкретного коллектора

```bash
# Запустить только один коллектор с DEBUG логами
cd market-data
LOG_LEVEL=DEBUG python3 bybit_spot.py

# В другом терминале — наблюдать данные в Redis
watch -n 1 "redis-cli HGETALL md:bybit:spot:BTCUSDT"
```

### Сценарий 4: Поиск сигналов с низким порогом

Для тестирования установите минимальный порог спреда:

```bash
MIN_SPREAD_PCT=0.1 python3 run.py --only spread_scanner signal_snapshot --no-cleanup
```

> Временно: изменение через переменную окружения не сохраняется в `.env`.

### Сценарий 5: Перезапуск после сбоя

```bash
# Перезапуск без очистки данных
python3 run.py --no-cleanup
```

### Сценарий 6: Только сбор данных, без сканера

```bash
python3 run.py --only binance_spot binance_futures bybit_spot bybit_futures \
               okx_spot okx_futures gate_spot gate_futures \
               stale_monitor latency_monitor
```

### Сценарий 7: Проверка работы price_history изолированно

```bash
# 1. Убедиться, что коллекторы пишут данные
python3 run.py --only binance_spot price_history --no-cleanup

# 2. В другом терминале — наблюдать рост истории в реальном времени
SLOT=$(($(date +%s) / 1200 % 5))
watch -n 2 "redis-cli LLEN md:hist:binance:spot:BTCUSDT:$SLOT"

# 3. Просмотр последних 5 записей
watch -n 2 "redis-cli LRANGE md:hist:binance:spot:BTCUSDT:$SLOT -5 -1"
```

### Сценарий 8: Чтение истории из Python

```python
import time
import redis

r = redis.Redis(decode_responses=True)

# Текущий слот
slot = (int(time.time()) // 1200) % 5

# Читаем всю текущую историю Binance Spot BTCUSDT
entries = r.lrange(f"md:hist:binance:spot:BTCUSDT:{slot}", 0, -1)
for e in entries[-5:]:  # последние 5
    ts, bid, ask = e.split(":")
    print(f"ts={ts}  bid={bid}  ask={ask}")

# Читаем все 5 слотов (полная история ~100 мин)
all_entries = []
for s in range(5):
    all_entries += r.lrange(f"md:hist:binance:spot:BTCUSDT:{s}", 0, -1)

# Сортируем по времени (ts в начале строки)
all_entries.sort(key=lambda x: int(x.split(":")[0]))
print(f"Всего точек в истории: {len(all_entries)}")
```

---

## 10. Диагностика проблем

### Нет данных в Redis через 60+ секунд

**Диагностика:**
```bash
redis-cli DBSIZE                          # должно быть > 100
tail -20 logs/binance_spot.log            # есть ли ws_connected?
grep "ws_reconnecting" logs/*.log | tail -20
```

**Причины:**
- Файл символов не найден → `{"event":"symbols_file_not_found",...}` → запустите `dictionaries/main.py`
- Redis недоступен → `{"event":"redis_error",...}` → проверьте `redis-cli ping`
- Ошибка сети → постоянные `ws_reconnecting` → проверьте интернет-соединение

### stale_monitor сообщает об устаревших данных

```bash
redis-cli SUBSCRIBE alerts:stale
```

**Если stale-алерты сразу после запуска** — это нормально, grace period 2 цикла (~60 с).

**Если stale-алерты после 2+ минут работы:**
1. Проверьте логи соответствующего коллектора на `ws_reconnecting`
2. Убедитесь что `REDIS_KEY_TTL > STALE_THRESHOLD_SEC` (300 > 60 — OK)

### Нет сигналов в signals/spread_signals.csv

**Проверка:**
```bash
ls -la signals/              # папка создана?
tail -5 logs/spread_scanner.log  # есть ли записи о циклах?

# Проверить, что сканер видит данные
redis-cli KEYS "md:*" | wc -l   # должно быть > 1000
```

**Возможные причины:**
- `MIN_SPREAD_PCT` слишком высокий → попробуйте `MIN_SPREAD_PCT=0.1`
- Данные устаревшие → сканер использует порог 5 минут, убедитесь что коллекторы работают
- Мало символов в combination-файлах → обновите словари

### price_history не пишет историю

**Диагностика:**
```bash
# Ключи истории есть?
redis-cli KEYS "md:hist:*" | wc -l   # ожидается > 0 через 30-60 с после старта

# Процесс запущен?
pgrep -af "price_history"

# Лог
tail -20 logs/price_history.log
```

**Причины:**
- Нет события `subscribed` в логе → сервис не подключился к Redis → проверьте `redis-cli ping`
- `DBSIZE` растёт, но `md:hist:*` пустой → коллекторы работают, но pub/sub не доходит → проверьте `redis-cli SUBSCRIBE md:updates:binance:spot` (должны приходить сообщения)
- Частые `batch_error` → временная недоступность Redis → само восстановится, но будут "дыры" в истории

**Проверка pub/sub вручную:**
```bash
# Должны приходить сообщения вида {"symbol":"BTCUSDT","key":"md:binance:spot:BTCUSDT"}
redis-cli PSUBSCRIBE "md:updates:*"
```

### signal_snapshot не создаёт файлы

**Проверка:**
```bash
ls -la signals/binance_s_bybit_f/    # папка существует?
tail -5 logs/signal_snapshot.log
redis-cli SUBSCRIBE ch:spread_signals  # приходят ли сигналы?
```

`signal_snapshot` создаёт файлы только при получении сигнала через pub/sub. Если `spread_signals.csv` заполняется, но снапшоты не создаются — проверьте, что signal_snapshot запущен.

### Процесс завершился с ненулевым кодом

```bash
# Найти скрипт, который упал
grep "завершился с кодом" logs/run.log

# Просмотреть лог упавшего скрипта
tail -50 logs/binance_futures.log
```

Коды возврата:
- **0** — нормальное завершение
- **1** — ошибка конфигурации (файл символов не найден, пустой файл)
- **другое** — необработанное исключение

### Высокое потребление CPU

```bash
top -p $(pgrep -d',' -f "python3.*\.py")
```

Нормальное потребление — 1–5% на коллектор. Если один процесс потребляет >20%:
- `spread_scanner` с очень маленьким `SCAN_INTERVAL_MS` (< 50 мс)
- `latency_monitor` с очень маленьким `SAMPLING_INTERVAL_SEC` (< 2 с)

### Очень большие файлы логов

Ротация настроена автоматически (50 МБ/файл, 5 архивов = 300 МБ на скрипт). Если диск заполнился:

```bash
# Общий размер логов
du -sh logs/

# Самые большие файлы
ls -la logs/ | sort -k5 -rn | head -10

# Принудительная очистка (осторожно!)
rm logs/*.log.*     # удаляет только ротированные архивы
```

---

## 11. Настройка параметров

### Снижение нагрузки на сеть

```dotenv
SYMBOLS_PER_CONN=300      # больше символов на соединение (меньше соединений)
WS_PING_INTERVAL=45       # реже пинговать
SAMPLING_INTERVAL_SEC=30  # реже собирать latency
SCAN_INTERVAL_SEC=60      # реже проверять stale
```

### Ускорение реакции на сигналы

```dotenv
SCAN_INTERVAL_MS=100       # сканировать каждые 100 мс вместо 200
SIGNAL_COOLDOWN_SEC=600    # cooldown 10 минут вместо часа
```

### Уменьшение числа сигналов

```dotenv
MIN_SPREAD_PCT=2.0         # только спреды > 2%
SIGNAL_COOLDOWN_SEC=7200   # cooldown 2 часа
```

### Увеличение числа сигналов (для исследования рынка)

```dotenv
MIN_SPREAD_PCT=0.3
SIGNAL_COOLDOWN_SEC=300    # cooldown 5 минут
```

### Redis с паролем

```dotenv
REDIS_URL=redis://:your_password@localhost:6379/0

# Redis на другом хосте
REDIS_URL=redis://:password@192.168.1.100:6379/0
```

### Максимальная детализация логов

```dotenv
LOG_LEVEL=DEBUG
```

> **Предупреждение:** DEBUG-логи в продакшне резко увеличивают объём логов и нагрузку на I/O.

---

## 12. Продакшн-деплой

### systemd (рекомендуется)

Создайте файл `/etc/systemd/system/market-data.service`:

```ini
[Unit]
Description=Market Data Collector
After=network.target redis.service
Requires=redis.service

[Service]
Type=simple
User=market-data
WorkingDirectory=/opt/market-data
ExecStart=/opt/market-data/venv/bin/python3 run.py --logs-dir /var/log/market-data --no-cleanup
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable market-data
sudo systemctl start market-data
sudo journalctl -fu market-data    # просмотр логов
```

### supervisord

Создайте `/etc/supervisor/conf.d/market-data.conf`:

```ini
[program:market-data]
command=/opt/market-data/venv/bin/python3 run.py --logs-dir /var/log/market-data --no-cleanup
directory=/opt/market-data
user=market-data
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=30
stdout_logfile=/var/log/supervisor/market-data.log
stderr_logfile=/var/log/supervisor/market-data-err.log
```

```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl status market-data
```

### Ротация системных логов (logrotate)

Создайте `/etc/logrotate.d/market-data`:

```
/var/log/market-data/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    sharedscripts
    postrotate
        # run.py сам управляет ротацией через RotatingFileHandler
        # здесь можно добавить внешнюю ротацию если нужно
    endscript
}
```

### Мониторинг в Prometheus + Grafana (опционально)

Включите Prometheus-экспорт в spread_scanner:

```dotenv
ENABLE_PROMETHEUS=1
PROMETHEUS_PORT=9091
```

Добавьте в `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'spread_scanner'
    static_configs:
      - targets: ['localhost:9091']
```

Доступные метрики:
- `spread_signals_total` — счётчик сигналов по направлению
- `spread_pct` — текущий спред по символу
- `spread_cycles_total` — число циклов сканирования

### Рекомендации по безопасности

1. **Redis**: не открывайте Redis наружу без пароля и TLS
2. **Сеть**: запускайте систему на выделенном сервере рядом с биржами (AWS Tokyo / Singapore)
3. **Файлы**: ограничьте права доступа к `.env` содержащему API-ключи: `chmod 600 market-data/.env`
4. **Логи**: не записывайте API-ключи в логи (система их не использует, но проверьте `.env`)

### Регулярное обслуживание

| Задача | Периодичность | Команда |
|---|---|---|
| Обновление словарей | Еженедельно | `cd dictionaries && python3 main.py` |
| Очистка старых логов | Ежемесячно | `find logs/ -name "*.log.*" -mtime +30 -delete` |
| Очистка старых снапшотов | По необходимости | `find signals/ -name "*.csv" -mtime +7 -delete` |
| Проверка размера диска | Ежедневно | `du -sh logs/ signals/` |

---

## Быстрый справочник команд

```bash
# Старт
python3 run.py

# Старт без очистки
python3 run.py --no-cleanup

# Остановка
Ctrl+C  # или  kill -TERM $(pgrep -f "python3 run.py")

# Обновление пар
cd dictionaries && python3 main.py && cd ..

# Проверка Redis
redis-cli DBSIZE
redis-cli HGETALL md:binance:spot:BTCUSDT

# Просмотр сигналов
tail -f signals/spread_signals.csv

# Просмотр логов
tail -f logs/spread_scanner.log

# Живой мониторинг сигналов
redis-cli SUBSCRIBE ch:spread_signals

# Перезагрузка символов сканера (без рестарта)
kill -HUP $(pgrep -f "spread_scanner.py")

# Статистика ключей по биржам
for ex in binance bybit okx gate; do
  echo "$ex: $(redis-cli KEYS "md:$ex:*" | wc -l) keys"
done

# История цен — текущий чанк
SLOT=$(($(date +%s) / 1200 % 5))
redis-cli LLEN "md:hist:binance:spot:BTCUSDT:$SLOT"
redis-cli LRANGE "md:hist:binance:spot:BTCUSDT:$SLOT" -5 -1

# Реестр слотов (временно́й диапазон каждого слота)
redis-cli HGETALL md:hist:registry

# Всего ключей истории
redis-cli KEYS "md:hist:*" | wc -l
```
