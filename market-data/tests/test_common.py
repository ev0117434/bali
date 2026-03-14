import os
import sys
import time
import tempfile
import pytest

# Ensure market-data dir is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import load_symbols, now_ms, setup_logging


# ---------------------------------------------------------------------------
# load_symbols
# ---------------------------------------------------------------------------
class TestLoadSymbols:
    def _write(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
        f.write(content)
        f.close()
        return f.name

    def test_basic_load(self):
        path = self._write("BTCUSDT\nETHUSDT\nBNBUSDT\n")
        result = load_symbols(path)
        assert result == ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    def test_normalizes_to_uppercase(self):
        path = self._write("btcusdt\nEthUsdt\n")
        result = load_symbols(path)
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_skips_empty_lines(self):
        path = self._write("BTCUSDT\n\n  \nETHUSDT\n")
        result = load_symbols(path)
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_skips_comments(self):
        path = self._write("# this is a comment\nBTCUSDT\n# another\nETHUSDT\n")
        result = load_symbols(path)
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_deduplication(self):
        path = self._write("BTCUSDT\nbtcusdt\nBTCUSDT\n")
        result = load_symbols(path)
        assert result == ["BTCUSDT"]
        assert len(result) == 1

    def test_only_comments_exits(self):
        path = self._write("# only comments\n# nothing else\n")
        with pytest.raises(SystemExit) as exc_info:
            load_symbols(path)
        assert exc_info.value.code == 1

    def test_empty_file_exits(self):
        path = self._write("")
        with pytest.raises(SystemExit) as exc_info:
            load_symbols(path)
        assert exc_info.value.code == 1

    def test_nonexistent_file_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            load_symbols("/nonexistent/path/symbols.txt")
        assert exc_info.value.code == 1

    def test_strips_whitespace_around_symbols(self):
        path = self._write("  BTCUSDT  \n  ETHUSDT\n")
        result = load_symbols(path)
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_real_binance_spot_file(self):
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "..", "dictionaries", "subscribe", "binance", "binance_spot.txt"
        )
        path = os.path.normpath(path)
        if os.path.exists(path):
            result = load_symbols(path)
            assert len(result) > 0
            for sym in result:
                assert sym == sym.upper()
                assert sym  # non-empty
            assert len(result) == len(set(result))  # no duplicates


# ---------------------------------------------------------------------------
# now_ms
# ---------------------------------------------------------------------------
class TestNowMs:
    def test_returns_int(self):
        assert isinstance(now_ms(), int)

    def test_approx_current_time(self):
        expected = int(time.time() * 1000)
        result = now_ms()
        assert abs(result - expected) < 50  # within 50ms

    def test_monotonic(self):
        t1 = now_ms()
        t2 = now_ms()
        assert t2 >= t1

    def test_millisecond_precision(self):
        # Should be in ms range (13 digits for current epoch)
        t = now_ms()
        assert 1_000_000_000_000 < t < 9_999_999_999_999


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------
class TestSetupLogging:
    def test_returns_bound_logger(self):
        import structlog
        logger = setup_logging("test_service")
        assert hasattr(logger, "info")
        assert hasattr(logger, "warning")
        assert hasattr(logger, "error")

    def test_service_bound(self):
        import structlog
        logger = setup_logging("my_service")
        # Verify the bound context contains the service name
        ctx = logger._context if hasattr(logger, "_context") else {}
        # structlog BoundLogger stores context via _context or via new_values
        bound = logger.bind()
        assert bound._context.get("service") == "my_service"

    def test_multiple_services_isolated(self):
        """Each call to setup_logging returns an independent bound logger."""
        logger_a = setup_logging("service_a")
        logger_b = setup_logging("service_b")
        assert logger_a._context.get("service") == "service_a"
        assert logger_b._context.get("service") == "service_b"

    def test_extra_bind_preserved(self):
        """Additional bind() calls chain context correctly."""
        logger = setup_logging("svc")
        bound = logger.bind(conn_id=3, exchange="binance")
        assert bound._context["conn_id"] == 3
        assert bound._context["exchange"] == "binance"
        assert bound._context["service"] == "svc"
