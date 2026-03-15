# Документация бирж для Arbitrage Terminal
# Binance · Bybit · Gate.io · OKX
# Фокус: спот покупка + фьючерс шорт через ccxt (Python)

---

## ОБЩИЕ ПРИНЦИПЫ CCXT

### Два инстанса на биржу
Для каждой биржи создаются ДВА отдельных инстанса ccxt — spot и futures.
Это обязательно, потому что `defaultType` определяет какой API endpoint используется.

```python
# Спот
spot = ccxt.binance({
    'apiKey': '...',
    'secret': '...',
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})

# Фьючерсы (USDT-margined linear perp)
futures = ccxt.binance({
    'apiKey': '...',
    'secret': '...',
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},  # swap = perpetual futures
})
```

### Символы
```
Спот:    BASE/QUOTE        → THE/USDT
Фьючерс: BASE/QUOTE:QUOTE  → THE/USDT:USDT  (USDT-margined linear perp)
```

### Округление (КРИТИЧНО для всех бирж)
Биржа отклонит ордер если amount или price не соответствуют precision.
```python
market = exchange.market(symbol)

# precision может быть int (кол-во знаков после запятой) ИЛИ float (step size)
# ccxt сам обрабатывает оба варианта через:
amount = exchange.amount_to_precision(symbol, raw_amount)
price = exchange.price_to_precision(symbol, raw_price)
```

### load_markets()
Вызывать ОДИН раз на инстанс. Кешируется автоматически.
```python
exchange.load_markets()
# Повторный вызов ничего не делает (использует кеш)
```

---

## 1. BINANCE

### Аутентификация
```python
spot = ccxt.binance({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})

futures = ccxt.binance({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
})
```

### Настройка перед торговлей фьючерсами

**Position Mode (One-way vs Hedge):**
- По умолчанию One-way mode (net) — нам подходит
- В One-way: `sell` открывает/увеличивает шорт, `buy` закрывает шорт
- Проверить/установить:
```python
# Если нужен hedge mode (не нужен для нашей задачи):
# futures.set_position_mode(True)   # hedged
# futures.set_position_mode(False)  # one-way (default)
```

**Margin mode (Cross vs Isolated):**
```python
try:
    futures.set_margin_mode('cross', 'THE/USDT:USDT')
except Exception as e:
    # "No need to change margin type" — уже установлен, это OK
    if 'No need to change' not in str(e):
        raise
```

**Leverage:**
```python
futures.set_leverage(1, 'THE/USDT:USDT')
# ВАЖНО: set_leverage работает ТОЛЬКО с фьючерсными символами (BASE/QUOTE:QUOTE)
# НЕ с спотовыми (BASE/QUOTE) — будет ошибка NotSupported
```

### Открытие позиции

**Спот — лимитная покупка:**
```python
spot.load_markets()
order = spot.create_limit_buy_order(
    symbol='THE/USDT',
    amount=427.0,          # количество токенов
    price=0.23415,         # цена за токен
)
# order['id'] — ID ордера
# order['status'] — 'open', 'closed', 'canceled'
```

**Фьючерс — лимитный шорт (продажа для открытия):**
```python
futures.load_markets()
order = futures.create_limit_sell_order(
    symbol='THE/USDT:USDT',
    amount=423.0,          # количество контрактов (в базовой валюте)
    price=0.23585,
)
```

### Закрытие позиции

**Спот — маркет продажа:**
```python
order = spot.create_market_sell_order('THE/USDT', amount)
```

**Фьючерс — маркет покупка (закрыть шорт):**
```python
order = futures.create_market_buy_order('THE/USDT:USDT', amount)
# В one-way mode buy автоматически закрывает шорт
```

### Проверка статуса ордера
```python
order = exchange.fetch_order(order_id, symbol)
# order['status']: 'open' | 'closed' | 'canceled' | 'expired'
# order['filled']: заполненное количество
# order['average']: средняя цена исполнения
# order['cost']: total cost (filled * average)
```

### Получение текущей цены
```python
ticker = exchange.fetch_ticker(symbol)
# ticker['bid'], ticker['ask'], ticker['last']
```

### Нюансы Binance
- `recvWindow`: по умолчанию 5000мс, можно увеличить при проблемах с синхронизацией времени
- Rate limits: ccxt `enableRateLimit=True` обрабатывает автоматически
- Futures amount в базовой валюте (не контракты)
- Минимальный размер ордера: проверять через `market['limits']['amount']['min']`
- Binance может требовать `{'newOrderRespType': 'FULL'}` в params для полного ответа

### Частые ошибки
```
-4046: "No need to change margin type"  → margin mode уже установлен, OK
-2019: "Margin is insufficient"         → недостаточно средств
-1111: "Precision is over the maximum"  → неправильное округление
-4131: "The counterparty's best price does not meet the PERCENT_PRICE filter" → цена далеко от маркета
```

---

## 2. BYBIT

### Аутентификация
```python
spot = ccxt.bybit({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})

futures = ccxt.bybit({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
})
```

### Unified Trading Account (UTA)
Bybit использует Unified Trading Account — спот и деривативы в одном аккаунте.
Это упрощает работу, но есть нюансы:
- Баланс общий для spot и futures
- Не нужно переводить средства между аккаунтами

### Настройка фьючерсов

**Position mode:**
```python
# По умолчанию One-Way (Merged) — нам подходит
# Для Hedge mode:
# futures.set_position_mode(True)  # dual side
```

**Margin mode и Leverage:**
```python
try:
    futures.set_margin_mode('cross', 'THE/USDT:USDT')
except:
    pass  # уже установлен

futures.set_leverage(1, 'THE/USDT:USDT')
```

### Открытие позиции

**Спот — лимитная покупка:**
```python
order = spot.create_limit_buy_order('THE/USDT', amount, price)
```

**Фьючерс — лимитный шорт:**
```python
order = futures.create_limit_sell_order('THE/USDT:USDT', amount, price)
```

### Закрытие позиции

**Спот — маркет продажа:**
```python
order = spot.create_market_sell_order('THE/USDT', amount)
```

**Фьючерс — маркет покупка (закрыть шорт):**
```python
order = futures.create_market_buy_order('THE/USDT:USDT', amount)
```

### Нюансы Bybit
- API v5 (текущая версия в ccxt)
- `category`: ccxt автоматически определяет 'spot' или 'linear' из символа
- Иногда нужно передать `{'category': 'linear'}` в params если ccxt не определяет
- UTA account: один баланс, но нужно следить за доступной маржу
- Amount для фьючерсов в базовой валюте
- `create_market_buy_order` для фьючерсов — amount в базовой валюте (не в USDT)

### Частые ошибки
```
110007: "Insufficient available balance"         → недостаточно средств
110001: "Order does not exist"                   → ордер не найден
10001:  "params error"                           → неверный символ или параметр
110043: "Set margin mode failed"                 → позиция открыта, нельзя менять
110025: "Position is not sufficient"             → закрываете больше чем есть
```

---

## 3. GATE.IO

### Аутентификация
```python
spot = ccxt.gateio({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})

futures = ccxt.gateio({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
})
```

### КРИТИЧНО: Gate.io settle параметр
Gate.io futures API требует параметр `settle` (валюта расчёта).
ccxt обычно определяет его автоматически из символа `THE/USDT:USDT`, но иногда нужно явно:
```python
# Обычно достаточно правильного символа:
futures.create_limit_sell_order('THE/USDT:USDT', amount, price)

# Если нужно явно указать settle:
futures.create_limit_sell_order('THE/USDT:USDT', amount, price, {'settle': 'usdt'})
```

### КРИТИЧНО: Gate.io futures amount
Gate.io фьючерсы работают с КОНТРАКТАМИ, не с базовой валютой!
```python
market = futures.market('THE/USDT:USDT')
contract_size = market['contractSize']  # например 1.0
# Если contractSize = 1, то amount 100 = 100 THE
# Если contractSize = 10, то amount 10 = 100 THE

# ccxt обычно конвертирует автоматически, но ПРОВЕРЯТЬ:
# amount в create_order — в базовой валюте (ccxt конвертирует в контракты)
```

### Настройка фьючерсов

**Leverage:**
```python
futures.set_leverage(1, 'THE/USDT:USDT')
```

**Margin mode:**
```python
# Gate.io поддерживает cross и isolated
try:
    futures.set_margin_mode('cross', 'THE/USDT:USDT')
except:
    pass
```

### Открытие позиции

**Спот — лимитная покупка:**
```python
order = spot.create_limit_buy_order('THE/USDT', amount, price)
```

**Фьючерс — лимитный шорт:**
```python
order = futures.create_limit_sell_order('THE/USDT:USDT', amount, price)
```

### Закрытие позиции

**Спот — маркет продажа:**
```python
order = spot.create_market_sell_order('THE/USDT', amount)
```

**Фьючерс — маркет покупка (закрыть шорт):**
```python
# ВНИМАНИЕ: Gate.io маркет ордера на фьючерсах могут требовать price=0 или IOC
order = futures.create_market_buy_order('THE/USDT:USDT', amount)
```

### Нюансы Gate.io
- Символы в нативном API: `BTC_USDT` (с подчёркиванием), ccxt конвертирует автоматически
- `settle` параметр: обычно 'usdt' для USDT-margined
- Contract size: ОБЯЗАТЕЛЬНО проверить `market['contractSize']`
- Маркет ордера: Gate.io фьючерсы не поддерживают true market orders — ccxt эмулирует через IOC limit по лучшей цене
- `defaultType`: использовать 'swap' для перпов, 'future' для экспирационных
- UID (user id): для некоторых эндпоинтов может потребоваться `'uid': '...'` в конфиге

### Частые ошибки
```
BALANCE_NOT_ENOUGH:     → недостаточно средств
ORDER_NOT_FOUND:        → ордер не найден  
INVALID_PARAM_VALUE:    → неверные параметры (проверить settle, amount)
CONTRACT_NO_COUNTER:    → нет контрагента (низкая ликвидность)
POSITION_NOT_ENOUGH:    → закрываете больше чем есть
```

---

## 4. OKX

### Аутентификация
```python
# OKX ТРЕБУЕТ 3 ключа: apiKey, secret, password (passphrase)
spot = ccxt.okx({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'password': 'xxx',       # ← PASSPHRASE, не пароль аккаунта!
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})

futures = ccxt.okx({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'password': 'xxx',
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
})
```

### КРИТИЧНО: Unified Account и tdMode
OKX имеет Unified Account с 4 режимами (выбирается в UI, не через API):
1. Spot mode
2. Futures mode
3. Multi-currency margin mode
4. Portfolio margin mode

**tdMode** (trade mode) — ОБЯЗАТЕЛЬНЫЙ параметр при размещении ордеров:
```
Спот:        tdMode = 'cash'      (обычная покупка/продажа)
Фьючерс:    tdMode = 'cross'     (cross margin)
             tdMode = 'isolated'  (isolated margin)
```

ccxt обычно устанавливает tdMode автоматически, но лучше передать явно.

### КРИТИЧНО: OKX Contract Size
OKX фьючерсы работают с КОНТРАКТАМИ!
```python
market = futures.market('THE/USDT:USDT')
contract_size = market['contractSize']  # например 10
# amount 1 = 1 контракт = contractSize единиц базовой валюты
# Если contractSize = 10 и вы передаёте amount=10 → это 100 THE, а не 10!

# ccxt ДОЛЖЕН конвертировать автоматически, но ОБЯЗАТЕЛЬНО ПРОВЕРИТЬ
# через market['contractSize'] перед отправкой
```

### Настройка фьючерсов

**Position mode:**
```python
# One-way (net) — по умолчанию, нам подходит
# Для переключения:
# futures.set_position_mode(False)  # net mode
# futures.set_position_mode(True)   # long/short mode
# ВНИМАНИЕ: все позиции должны быть закрыты для переключения!
```

**Leverage и Margin:**
```python
futures.set_leverage(1, 'THE/USDT:USDT', {
    'marginMode': 'cross',
    'posSide': 'net',      # для one-way mode
})
```

### Открытие позиции

**Спот — лимитная покупка:**
```python
order = spot.create_limit_buy_order('THE/USDT', amount, price, {
    'tdMode': 'cash',
})
```

**Фьючерс — лимитный шорт:**
```python
order = futures.create_limit_sell_order('THE/USDT:USDT', amount, price, {
    'tdMode': 'cross',
})
```

### Закрытие позиции

**Спот — маркет продажа:**
```python
order = spot.create_market_sell_order('THE/USDT', amount, params={
    'tdMode': 'cash',
})
```

**Фьючерс — маркет покупка (закрыть шорт):**
```python
order = futures.create_market_buy_order('THE/USDT:USDT', amount, params={
    'tdMode': 'cross',
})
```

### Нюансы OKX
- **password** — это passphrase при создании API key, НЕ пароль аккаунта
- **instType**: SPOT, MARGIN, SWAP (перпы), FUTURES (экспирация), OPTION
- **tdMode**: ОБЯЗАТЕЛЬНО указывать, иначе ошибки
- **posSide**: 'net' для one-way, 'long'/'short' для hedge mode
- **Contract size**: КРИТИЧНО проверять, varies per instrument
- Фьючерсный символ в нативном API: `THE-USDT-SWAP` (ccxt конвертирует THE/USDT:USDT)
- При hedge mode: sell+short открывает шорт, buy+short закрывает шорт
- При net mode (наш случай): sell открывает/увеличивает шорт, buy закрывает

### Частые ошибки
```
51000: "Parameter tdMode error"                   → не указан или неверный tdMode
51008: "Order failed. Insufficient account balance" → нет средств
51010: "Request frequency too high"               → слишком частые запросы
51100: "Trading amount does not meet the min"     → слишком маленький ордер
51113: "Position does not exist"                  → нет позиции для закрытия
51115: "Leverage exceeds max allowed"             → leverage слишком высокий
51116: "Order price is not within the price limit" → цена далеко от маркета
51006: "Insufficient balance" (isolated margin)   → мало маржи
51503: "This order type is not supported for perps" → для trigger ордеров нужен long/short mode
```

---

## СВОДНАЯ ТАБЛИЦА: ДЕЙСТВИЯ ДЛЯ НАШЕГО ТЕРМИНАЛА

### Инициализация (один раз при первом сигнале для биржи)

| Действие | Binance | Bybit | Gate.io | OKX |
|----------|---------|-------|---------|-----|
| Spot инстанс | `defaultType: 'spot'` | `defaultType: 'spot'` | `defaultType: 'spot'` | `defaultType: 'spot'` |
| Futures инстанс | `defaultType: 'swap'` | `defaultType: 'swap'` | `defaultType: 'swap'` | `defaultType: 'swap'` |
| Password/Passphrase | Нет | Нет | Нет | **ДА** (`password`) |
| Set margin mode | `set_margin_mode('cross', sym)` | `set_margin_mode('cross', sym)` | `set_margin_mode('cross', sym)` | через `set_leverage` params |
| Set leverage | `set_leverage(1, sym)` | `set_leverage(1, sym)` | `set_leverage(1, sym)` | `set_leverage(1, sym, {'marginMode':'cross','posSide':'net'})` |
| Contract size | == 1 (amount в базе) | == 1 (amount в базе) | **ПРОВЕРИТЬ** `market['contractSize']` | **ПРОВЕРИТЬ** `market['contractSize']` |

### Открытие (лимитные ордера)

| Действие | Метод | Символ | Params |
|----------|-------|--------|--------|
| Spot buy | `create_limit_buy_order` | `THE/USDT` | `{}` (OKX: `{'tdMode':'cash'}`) |
| Futures sell (short) | `create_limit_sell_order` | `THE/USDT:USDT` | `{}` (OKX: `{'tdMode':'cross'}`) |

### Закрытие (маркет ордера)

| Действие | Метод | Символ | Params |
|----------|-------|--------|--------|
| Spot sell | `create_market_sell_order` | `THE/USDT` | `{}` (OKX: `{'tdMode':'cash'}`) |
| Futures buy (close short) | `create_market_buy_order` | `THE/USDT:USDT` | `{}` (OKX: `{'tdMode':'cross'}`) |

### Мониторинг

| Действие | Метод | Все биржи одинаково |
|----------|-------|---------------------|
| Статус ордера | `fetch_order(id, symbol)` | Да |
| Текущая цена | `fetch_ticker(symbol)` | Да |
| Отмена ордера | `cancel_order(id, symbol)` | Да |

---

## ЧЕКЛИСТ ПЕРЕД ПРОДАКШН

1. [ ] API ключи с правами на спот и фьючерс трейдинг
2. [ ] Sufficient balance на спот и фьючерс аккаунтах (или unified)
3. [ ] Position mode = One-way (net) на всех биржах
4. [ ] Margin mode = cross (или isolated, по предпочтению)
5. [ ] Leverage = 1x (или нужное значение)
6. [ ] Проверить contractSize для Gate.io и OKX
7. [ ] Проверить минимальные размеры ордеров: `market['limits']['amount']['min']`
8. [ ] Проверить price precision: `market['precision']['price']`
9. [ ] Проверить amount precision: `market['precision']['amount']`
10. [ ] Тест на минимальных суммах перед реальной торговлей
11. [ ] IP whitelist для API ключей (рекомендуется)
12. [ ] Testnet/sandbox для начального тестирования (если доступен)

---

## ШАБЛОН ПОДГОТОВКИ БИРЖИ (код)

```python
import ccxt

def prepare_exchange(name, config, symbol_base):
    """
    Подготовка биржи для арбитража.
    symbol_base: 'THE/USDT' (без :USDT)
    """
    spot_sym = symbol_base
    fut_sym = f"{symbol_base}:USDT"

    # Создание инстансов
    ExClass = getattr(ccxt, name)
    
    spot_cfg = {
        'apiKey': config['apiKey'],
        'secret': config['secret'],
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'},
    }
    fut_cfg = {
        'apiKey': config['apiKey'],
        'secret': config['secret'],
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
    }
    
    # OKX needs password
    if name == 'okx':
        spot_cfg['password'] = config['password']
        fut_cfg['password'] = config['password']
    
    spot = ExClass(spot_cfg)
    futures = ExClass(fut_cfg)
    
    # Load markets
    spot.load_markets()
    futures.load_markets()
    
    # Setup futures
    try:
        futures.set_margin_mode('cross', fut_sym)
    except Exception as e:
        print(f"  margin mode: {e} (probably already set)")
    
    if name == 'okx':
        futures.set_leverage(1, fut_sym, {
            'marginMode': 'cross', 'posSide': 'net'
        })
    else:
        futures.set_leverage(1, fut_sym)
    
    # Check contract size
    market = futures.market(fut_sym)
    cs = market.get('contractSize', 1)
    if cs != 1:
        print(f"  ⚠️  contractSize = {cs} — учитывать при расчёте amount!")
    
    # Check min amounts
    spot_market = spot.market(spot_sym)
    print(f"  Spot min amount: {spot_market['limits']['amount']['min']}")
    print(f"  Spot price precision: {spot_market['precision']['price']}")
    print(f"  Futures min amount: {market['limits']['amount']['min']}")
    print(f"  Futures price precision: {market['precision']['price']}")
    
    return spot, futures
```
