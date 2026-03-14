import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from latency_monitor import calc_stats, parse_latencies


# ---------------------------------------------------------------------------
# calc_stats
# ---------------------------------------------------------------------------
class TestCalcStats:
    def test_empty_list_returns_empty_dict(self):
        assert calc_stats([]) == {}

    def test_single_value(self):
        r = calc_stats([42.0])
        assert r["min"] == 42.0
        assert r["max"] == 42.0
        assert r["avg"] == 42.0
        assert r["p95"] == 42.0

    def test_multiple_values(self):
        r = calc_stats([1.0, 2.0, 3.0, 4.0, 5.0])
        assert r["min"] == 1.0
        assert r["max"] == 5.0
        assert r["avg"] == 3.0

    def test_p95_calculation(self):
        # 100 values: 1..100. p95 index = int(100*0.95)-1 = 94 → value 95
        values = [float(i) for i in range(1, 101)]
        r = calc_stats(values)
        assert r["p95"] == 95.0

    def test_p95_small_list(self):
        # 4 values: p95_idx = max(0, int(4*0.95)-1) = max(0, 2) = 2 → sorted[2]
        r = calc_stats([10.0, 20.0, 30.0, 40.0])
        assert r["p95"] == 30.0

    def test_rounding_to_2_decimals(self):
        r = calc_stats([1.0, 2.0, 3.0])
        assert r["avg"] == round(sum([1, 2, 3]) / 3, 2)

    def test_all_same_values(self):
        r = calc_stats([5.0, 5.0, 5.0])
        assert r["min"] == r["max"] == r["avg"] == r["p95"] == 5.0


# ---------------------------------------------------------------------------
# parse_latencies
# ---------------------------------------------------------------------------
class TestParseLatencies:
    def test_normal_latencies(self):
        lats = parse_latencies("1000", "1010", "1012")
        assert lats is not None
        assert lats["exchange_to_collector"] == 10
        assert lats["collector_to_redis"] == 2
        assert lats["end_to_end"] == 12
        assert lats["clock_skew"] is False

    def test_clock_skew_detected(self):
        """ts_received < ts_exchange → clock skew (valid scenario, not an error)."""
        lats = parse_latencies("1010", "1000", "1015")
        assert lats is not None
        assert lats["clock_skew"] is True
        assert lats["exchange_to_collector"] == -10
        assert lats["end_to_end"] == 5  # ts_redis - ts_exchange

    def test_binance_spot_skipped_e2e(self):
        """When ts_exchange == ts_received (Binance Spot), e2e must be None."""
        lats = parse_latencies("1000", "1000", "1005")
        assert lats is not None
        assert lats["end_to_end"] is None
        assert lats["exchange_to_collector"] is None
        assert lats["collector_to_redis"] == 5  # still measured

    def test_ts_exchange_zero_treated_as_no_exchange_ts(self):
        """ts_exchange == '0' means no exchange timestamp."""
        lats = parse_latencies("0", "1000", "1005")
        assert lats is not None
        assert lats["end_to_end"] is None
        assert lats["collector_to_redis"] == 5

    def test_invalid_string_returns_none(self):
        assert parse_latencies("abc", "1000", "1005") is None

    def test_missing_value_returns_none(self):
        assert parse_latencies(None, "1000", "1005") is None
        assert parse_latencies("1000", None, "1005") is None
        assert parse_latencies("1000", "1000", None) is None

    def test_empty_string_returns_none(self):
        assert parse_latencies("", "1000", "1005") is None

    def test_large_realistic_timestamps(self):
        """Use real-world ms timestamps."""
        base = 1710412800000
        lats = parse_latencies(str(base), str(base + 5), str(base + 8))
        assert lats["exchange_to_collector"] == 5
        assert lats["collector_to_redis"] == 3
        assert lats["end_to_end"] == 8

    def test_collector_to_redis_always_computed(self):
        """c2r must be available even when ts_exchange is 0."""
        lats = parse_latencies("0", "1710412800000", "1710412800003")
        assert lats["collector_to_redis"] == 3


# ---------------------------------------------------------------------------
# Anomaly thresholds (conceptual, no Redis needed)
# ---------------------------------------------------------------------------
class TestAnomalyThresholds:
    def test_warn_threshold_logic(self):
        import latency_monitor
        # Verify constants are sensible defaults
        assert latency_monitor.ANOMALY_WARN_MS > 0
        assert latency_monitor.ANOMALY_CRIT_MS > latency_monitor.ANOMALY_WARN_MS

    def test_e2e_1500ms_is_warn_not_crit(self):
        import latency_monitor
        e2e = 1500
        is_crit = e2e > latency_monitor.ANOMALY_CRIT_MS
        is_warn = e2e > latency_monitor.ANOMALY_WARN_MS
        assert is_warn is True
        assert is_crit is False

    def test_e2e_6000ms_is_critical(self):
        import latency_monitor
        e2e = 6000
        is_crit = e2e > latency_monitor.ANOMALY_CRIT_MS
        assert is_crit is True

    def test_e2e_500ms_is_normal(self):
        import latency_monitor
        e2e = 500
        is_warn = e2e > latency_monitor.ANOMALY_WARN_MS
        assert is_warn is False
