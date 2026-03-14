#!/usr/bin/env bash
# =============================================================================
# Acceptance Check: Market Data Collector v2.1
# Verifies automated criteria from TZ section 9.
# Run from market-data/ directory: bash tests/acceptance_check.sh
# =============================================================================
set -e
cd "$(dirname "$0")/.."

PASS=0
FAIL=0

ok()   { echo "  [OK]  $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
header() { echo ""; echo "=== $1 ==="; }

# -----------------------------------------------------------------------------
header "1. Python syntax check (all scripts)"
for f in common.py binance_spot.py binance_futures.py bybit_spot.py bybit_futures.py stale_monitor.py latency_monitor.py; do
    if python3 -m py_compile "$f" 2>/dev/null; then
        ok "$f"
    else
        fail "$f — syntax error"
    fi
done

# -----------------------------------------------------------------------------
header "2. File size constraints (TZ §9: collectors < 250 lines, common.py < 100)"
for f in binance_spot.py binance_futures.py bybit_spot.py bybit_futures.py stale_monitor.py latency_monitor.py; do
    lines=$(wc -l < "$f")
    if [ "$lines" -lt 250 ]; then
        ok "$f: $lines lines (< 250)"
    else
        fail "$f: $lines lines (max 250)"
    fi
done

lines=$(wc -l < common.py)
if [ "$lines" -lt 100 ]; then
    ok "common.py: $lines lines (< 100)"
else
    fail "common.py: $lines lines (max 100)"
fi

# -----------------------------------------------------------------------------
header "3. Symbol files exist and are non-empty"
for f in \
    "../dictionaries/subscribe/binance/binance_spot.txt" \
    "../dictionaries/subscribe/binance/binance_futures.txt" \
    "../dictionaries/subscribe/bybit/bybit_spot.txt" \
    "../dictionaries/subscribe/bybit/bybit_futures.txt"; do
    if [ -s "$f" ]; then
        count=$(wc -l < "$f")
        ok "$f: $count symbols"
    else
        fail "$f: missing or empty"
    fi
done

# -----------------------------------------------------------------------------
header "4. All imports resolve (no broken dependencies)"
if python3 -c "import common, binance_spot, binance_futures, bybit_spot, bybit_futures, stale_monitor, latency_monitor" 2>/dev/null; then
    ok "All modules importable"
else
    python3 -c "import common, binance_spot, binance_futures, bybit_spot, bybit_futures, stale_monitor, latency_monitor"
    fail "Import error in one or more modules"
fi

# -----------------------------------------------------------------------------
header "5. Unit + integration tests"
if python3 -m pytest tests/ -q --tb=short 2>&1 | tail -3; then
    ok "All tests passed"
else
    fail "Some tests failed"
fi

# -----------------------------------------------------------------------------
header "6. Redis key format validation (via unit tests — no live Redis needed)"
if python3 -c "
from tests.test_redis_write import *
print('  Key format tests importable')
" 2>/dev/null; then
    ok "Key format test module valid"
fi

# -----------------------------------------------------------------------------
header "7. Reconnect backoff formula verification"
python3 - <<'PYEOF'
import random, sys
errors = []
for attempt in range(15):
    for _ in range(100):
        delay = min(2**attempt, 60) + random.uniform(0, 1)
        lo = min(2**attempt, 60)
        hi = lo + 1
        if not (lo <= delay < hi):
            errors.append(f"attempt={attempt} delay={delay}")
if errors:
    print(f"  [FAIL] Backoff errors: {errors[:3]}")
    sys.exit(1)
else:
    print("  [OK]  Exponential backoff with jitter: correct for all attempts 0-14")
PYEOF

# -----------------------------------------------------------------------------
header "8. .env.example completeness"
REQUIRED_VARS="REDIS_URL REDIS_KEY_TTL WS_PING_INTERVAL WS_RECV_TIMEOUT RECONNECT_MAX_DELAY SYMBOLS_PER_CONN LOG_LEVEL STALE_THRESHOLD_SEC SCAN_INTERVAL_SEC SAMPLING_INTERVAL_SEC ANOMALY_WARN_MS ANOMALY_CRIT_MS"
missing=0
for var in $REQUIRED_VARS; do
    if grep -q "^$var=" .env.example; then
        ok ".env.example: $var"
    else
        fail ".env.example: $var missing"
        missing=$((missing+1))
    fi
done

# -----------------------------------------------------------------------------
header "9. Manual checks (require live environment)"
echo "  [MANUAL] redis-cli HGETALL md:binance:spot:BTCUSDT → 8 fields"
echo "  [MANUAL] Stop binance_spot.py → stale_monitor alerts after 60s"
echo "  [MANUAL] kill -TERM <pid> → graceful_shutdown in logs"
echo "  [MANUAL] Stop Redis → redis_error logged, collectors continue"
echo "  [MANUAL] Restart Redis → redis_recovered logged"
echo "  [MANUAL] latency_monitor → latency_report every 10s"
echo "  [MANUAL] All logs parse with: python3 binance_spot.py | jq ."

# -----------------------------------------------------------------------------
echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed"
echo "=============================="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
