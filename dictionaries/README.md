# dictionaries

Система генерации и хранения списков торговых символов для подписки на WebSocket-стримы.

## Содержание

- [Структура](#структура)
- [Концепция](#концепция)
- [Пайплайн генерации](#пайплайн-генерации)
- [Готовые файлы подписки](#готовые-файлы-подписки)
- [Скрипты](#скрипты)
- [Stable ID маппинги](#stable-id-маппинги)
- [Обновление списков](#обновление-списков)

---

## Структура

```
dictionaries/
├── subscribe/                  # ГОТОВЫЕ СПИСКИ для коллекторов (только эти читаются run.py)
│   ├── all_symbols.txt         # объединение всех символов
│   ├── binance/
│   │   ├── binance_spot.txt    # 446 символов
│   │   └── binance_futures.txt # 401 символ
│   ├── bybit/
│   │   ├── bybit_spot.txt      # 392 символа
│   │   └── bybit_futures.txt   # 391 символ
│   ├── mexc/
│   ├── okx/
│   └── okx/with_mapping/      # OKX с оригинальными именами (BTC-USDT-SWAP)
│
├── all_pairs/                  # полные списки всех пар с каждой биржи
│   ├── binance/
│   ├── bybit/
│   ├── mexc/
│   └── okx/
│       └── source.txt          # список названий источников
│
├── combination/                # пересечения пар между биржами (12 направлений)
│   ├── binance_spot_bybit_futures.txt
│   ├── binance_spot_mexc_futures.txt
│   └── ...
│
├── temp/                       # временные файлы при сборе полных списков
│
├── configs/                    # stable integer id-маппинги
│   ├── symbols.tsv             # symbol_id \t SYMBOL
│   └── sources.tsv             # source_id \t source_name
│
└── scripts/                    # утилиты генерации
    ├── all_pairs_temp/         # скачивание полных списков с бирж
    ├── valid_all_pairs/        # валидация пар через REST API
    ├── combination.py          # генерация пересечений (12 комбинаций)
    ├── sub_pairs.py            # сборка subscribe/ из combination/
    ├── norm_maker.py           # нормализация имён символов
    ├── common_list.py          # вспомогательные функции
    └── okx_mapping.py          # маппинг OKX имён (BTC-USDT → BTCUSDT)
```

---

## Концепция

Идея: коллектор не должен знать, какие пары активны на бирже прямо сейчас. Он читает готовый список из файла и подписывается на него. Генерация списков — отдельный пайплайн.

**Почему пересечения, а не просто "все пары":**
Системе интересны только пары, которые торгуются хотя бы на двух рынках одновременно (например, spot на Binance + futures на Bybit). Это позволяет искать арбитражные расхождения. Пары, которых нет на обеих сторонах, исключаются.

---

## Пайплайн генерации

```
Биржи REST API
     │
     ▼
[all_pairs_temp/*.py]         # скачать полные списки → temp/all_pairs/
     │
     ▼
[valid_all_pairs/*.py]        # валидировать через REST → all_pairs/
     │
     ▼
[combination.py]              # пересечь попарно (12 комбинаций) → combination/
     │
     ▼
[sub_pairs.py]                # собрать subscribe/ по биржам
     │
     ▼
subscribe/{exchange}/*.txt    # ← читают коллекторы
     │
     ▼
[main.py]                     # обновить stable id-маппинги → configs/
```

---

## Готовые файлы подписки

Файлы в `subscribe/` — единственные, которые читают коллекторы. Формат: одна строка = один символ, uppercase, без заголовков.

```
BTCUSDT
ETHUSDT
SOLUSDT
...
```

Текущий размер:

| Файл | Символов |
|------|----------|
| `binance/binance_spot.txt` | 446 |
| `binance/binance_futures.txt` | 401 |
| `bybit/bybit_spot.txt` | 392 |
| `bybit/bybit_futures.txt` | 391 |

Файлы поддерживают комментарии (`# текст`) и пустые строки — `common.load_symbols()` их пропускает.

---

## Скрипты

### `scripts/all_pairs_temp/*.py`

Скачивают полные списки активных торговых пар через REST API биржи. Результат — в `temp/all_pairs/`. Запускаются редко (при обновлении).

```bash
python3 scripts/all_pairs_temp/binance_all_pairs_temp.py
python3 scripts/all_pairs_temp/bybit_all_pairs_temp.py
```

### `scripts/valid_all_pairs/*.py`

Валидируют пары из `temp/` через REST API и записывают только активные в `all_pairs/`.

### `scripts/combination.py`

Берёт 8 списков из `all_pairs/` (4 биржи × spot/futures) и строит 12 попарных пересечений в `combination/`.

```
binance_spot  ∩ bybit_futures    → combination/binance_spot_bybit_futures.txt
binance_spot  ∩ mexc_futures     → combination/binance_spot_mexc_futures.txt
...
```

Нормализация имён встроена: `BTC-USDT-SWAP` (OKX) → `BTCUSDT`.

```bash
python3 scripts/combination.py
# Выводит количество пар в каждом пересечении
```

### `scripts/sub_pairs.py`

Объединяет `combination/*.txt` в итоговые файлы `subscribe/{exchange}/{exchange}_{market}.txt`. Каждый символ попадает в списки обеих бирж участвующих направлений.

```bash
python3 scripts/sub_pairs.py
# Создаёт/обновляет subscribe/ для binance, bybit, mexc, okx
```

### `scripts/norm_maker.py`

Нормализует формат пар (убирает разделители, суффиксы `-SWAP`, `-PERP` и т.д.).

### `scripts/okx_mapping.py`

Создаёт маппинг `BTCUSDT → BTC-USDT-SWAP` для OKX, т.к. OKX API требует оригинальные имена. Результат в `subscribe/okx/with_mapping/`.

---

## Stable ID маппинги

`configs/symbols.tsv` и `configs/sources.tsv` хранят целочисленные id для символов и бирж.

**Зачем:** внешние системы (БД, аналитика) могут ссылаться на символ по id вместо строки — id никогда не меняются, даже если символ убрали из активных пар.

Формат `symbols.tsv`:
```
0	1INCHUSDT
1	2ZUSDT
2	AAVEUSDT
...
```

Формат `sources.tsv`:
```
0	binance_spot
1	binance_futures
2	bybit_spot
3	bybit_futures
...
```

### `main.py`

Обновляет `configs/symbols.tsv` и `configs/sources.tsv` на основе `subscribe/all_symbols.txt` и `all_pairs/source.txt`.

**Важное свойство:** символы никогда не удаляются из `symbols.tsv`. Если символ пропал из активных пар — он остаётся в маппинге с прежним id. Это гарантирует стабильность id для внешних ссылок.

```bash
python3 main.py
# OK
# symbols from: .../all_symbols.txt (1234 in txt, 1234 total ids)
# sources from: .../source.txt (8)
```

---

## Обновление списков

При необходимости обновить символы (раз в неделю / при добавлении новых пар):

```bash
cd dictionaries

# 1. Скачать актуальные списки с бирж
python3 scripts/all_pairs_temp/binance_all_pairs_temp.py
python3 scripts/all_pairs_temp/bybit_all_pairs_temp.py
# ... и т.д.

# 2. Валидировать
python3 scripts/valid_all_pairs/binance_validator.py
python3 scripts/valid_all_pairs/bybit_validator.py

# 3. Построить пересечения
python3 scripts/combination.py

# 4. Собрать списки подписки
python3 scripts/sub_pairs.py

# 5. Обновить id-маппинги
python3 main.py

# 6. Перезапустить коллекторы (новые файлы читаются при старте)
# kill -TERM <pids>  или  python3 run.py
```
