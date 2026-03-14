#!/usr/bin/env python3
"""
Validate MEXC trading pairs by observing best bid/ask updates over a time window.

- Spot (protobuf): subscribes to spot@public.aggre.bookTicker.v3.api.pb@10ms@<SYMBOL>
- Futures (JSON): subscribes to sub.ticker for each symbol (BTC_USDT)

Reads:
  /root/siro/dictionaries/temp/all_pairs/mexc/mexc_spot_all_temp.txt
  /root/siro/dictionaries/temp/all_pairs/mexc/mexc_futures_usdt_perp_all_temp.txt

Writes (overwrite):
  /root/siro/dictionaries/all_pairs/mexc/mexc_spot.txt
  /root/siro/dictionaries/all_pairs/mexc/mexc_futures.txt

Python: 3.12+
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import hashlib
import logging
import os
import pathlib
import random
import signal
import sys
import time
import urllib.request
import zipfile
from typing import Iterable, Sequence

import websockets

# These are required for Spot protobuf decode bootstrap:
#   pip install protobuf grpcio-tools
#
# We import protobuf lazily after generating pb2.

SPOT_WS_URL = "wss://wbs-api.mexc.com/ws"
FUTURES_WS_URL = "wss://contract.mexc.com/edge"

# Spot docs note: max 30 subscriptions per connection.
SPOT_MAX_SUBS_PER_CONN = 30

DEFAULT_SPOT_IN = "/root/siro/dictionaries/temp/all_pairs/mexc/mexc_spot_all_temp.txt"
DEFAULT_FUT_IN = "/root/siro/dictionaries/temp/all_pairs/mexc/mexc_futures_usdm_all_temp.txt"
DEFAULT_SPOT_OUT = "/root/siro/dictionaries/all_pairs/mexc/mexc_spot.txt"
DEFAULT_FUT_OUT = "/root/siro/dictionaries/all_pairs/mexc/mexc_futures.txt"

PROTO_REPO_ZIP = "https://github.com/mexcdevelop/websocket-proto/archive/refs/heads/main.zip"


@dataclasses.dataclass(frozen=True)
class Paths:
    spot_in: pathlib.Path
    futures_in: pathlib.Path
    spot_out: pathlib.Path
    futures_out: pathlib.Path


@dataclasses.dataclass(frozen=True)
class Config:
    duration_s: float
    spot_batch_size: int
    futures_batch_size: int
    spot_max_conns: int
    futures_max_conns: int
    ping_interval_s: float
    futures_ping_interval_s: float
    proto_cache_dir: pathlib.Path
    log_level: str


class GracefulExit(Exception):
    pass


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def read_symbols(path: pathlib.Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    out: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out


def atomic_write_lines(path: pathlib.Path, lines: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}.{int(time.time())}")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for s in lines:
            f.write(s)
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def chunked(seq: Sequence[str], n: int) -> list[list[str]]:
    return [list(seq[i : i + n]) for i in range(0, len(seq), n)]


def mexc_futures_symbol_from_spot(sym: str) -> str:
    # For USDT perpetual list input like BTCUSDT -> BTC_USDT
    if sym.endswith("USDT") and len(sym) > 4:
        return f"{sym[:-4]}_USDT"
    # Fallback: try to split last 3-4 chars; keep as-is with underscore best-effort
    if len(sym) > 4:
        return f"{sym[:-4]}_{sym[-4:]}"
    return sym


def mexc_spot_symbol_from_futures(sym: str) -> str:
    return sym.replace("_", "").upper()


def _stable_hash(s: str) -> int:
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:8], 16)


class SpotProtoDecoder:
    """
    Bootstraps MEXC Spot protobuf decoder by downloading official .proto repo
    and generating *_pb2.py via grpcio-tools.

    It then decodes binary frames into PushDataV3ApiWrapper and detects bookTicker messages.
    """

    def __init__(self, cache_dir: pathlib.Path) -> None:
        self.cache_dir = cache_dir
        self.gen_dir = cache_dir / "gen"
        self.repo_dir = cache_dir / "repo"
        self._wrapper_cls = None

    def ensure_ready(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_repo()
        self._ensure_generated()
        self._import_wrapper()

    def _ensure_repo(self) -> None:
        marker = self.repo_dir / ".ready"
        if marker.exists():
            return

        logging.getLogger("spot.proto").info("Downloading MEXC websocket-proto repo zip...")
        zip_path = self.cache_dir / "websocket-proto-main.zip"
        try:
            urllib.request.urlretrieve(PROTO_REPO_ZIP, zip_path)  # nosec - intended
        except Exception as e:
            raise RuntimeError(
                "Failed to download protobuf repo. Ensure internet access or pre-populate cache_dir."
            ) from e

        if self.repo_dir.exists():
            with contextlib.suppress(Exception):
                for p in sorted(self.repo_dir.rglob("*"), reverse=True):
                    if p.is_file():
                        p.unlink(missing_ok=True)
                    else:
                        p.rmdir()
        self.repo_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(self.cache_dir)

        # The zip extracts to websocket-proto-main/
        extracted = self.cache_dir / "websocket-proto-main"
        if not extracted.exists():
            raise RuntimeError("Unexpected zip structure: websocket-proto-main not found")

        # Move extracted content under repo_dir
        for item in extracted.iterdir():
            target = self.repo_dir / item.name
            if target.exists():
                if target.is_dir():
                    # merge not needed
                    pass
            item.replace(target)

        # Cleanup
        with contextlib.suppress(Exception):
            zip_path.unlink(missing_ok=True)
        with contextlib.suppress(Exception):
            extracted.rmdir()

        marker.write_text("ok", encoding="utf-8")

    def _ensure_generated(self) -> None:
        wrapper_py = self.gen_dir / "PushDataV3ApiWrapper_pb2.py"
        if wrapper_py.exists():
            return

        self.gen_dir.mkdir(parents=True, exist_ok=True)

        # Find all .proto files in repo_dir
        protos = [str(p) for p in self.repo_dir.rglob("*.proto")]
        if not protos:
            raise RuntimeError("No .proto files found in downloaded repo")

        logging.getLogger("spot.proto").info("Generating *_pb2.py using grpcio-tools (this is one-time)...")
        try:
            from grpc_tools import protoc  # type: ignore
        except Exception as e:
            raise RuntimeError("Missing dependency: grpcio-tools. Install: pip install grpcio-tools") from e

        # protoc args
        # -I<repo_dir> and include subdirs
        args = [
            "protoc",
            f"-I{self.repo_dir}",
            f"--python_out={self.gen_dir}",
        ] + protos

        rc = protoc.main(args)
        if rc != 0:
            raise RuntimeError(f"protoc failed with exit code {rc}")

        if not wrapper_py.exists():
            # Some repos place wrapper under a package; search for it
            found = list(self.gen_dir.rglob("PushDataV3ApiWrapper_pb2.py"))
            if not found:
                raise RuntimeError("Generated code missing PushDataV3ApiWrapper_pb2.py")
            # If found in subdir, add gen_dir to sys.path and import by package path later; keep as-is.

    def _import_wrapper(self) -> None:
        # Add gen_dir to sys.path so imports of generated modules work.
        if str(self.gen_dir) not in sys.path:
            sys.path.insert(0, str(self.gen_dir))

        # The module might be at top-level or inside a package; try both strategies.
        wrapper_mod = None
        try:
            import PushDataV3ApiWrapper_pb2  # type: ignore

            wrapper_mod = PushDataV3ApiWrapper_pb2
        except Exception:
            # search any generated module path
            candidates = list(self.gen_dir.rglob("PushDataV3ApiWrapper_pb2.py"))
            if not candidates:
                raise
            # Build import path from relative parts
            rel = candidates[0].relative_to(self.gen_dir).with_suffix("")
            dotted = ".".join(rel.parts)
            wrapper_mod = __import__(dotted, fromlist=["*"])

        self._wrapper_cls = getattr(wrapper_mod, "PushDataV3ApiWrapper", None)
        if self._wrapper_cls is None:
            raise RuntimeError("PushDataV3ApiWrapper class not found in generated module")

    def decode_symbol_if_bookticker(self, payload: bytes) -> str | None:
        """
        Returns symbol (e.g., BTCUSDT) if payload is a bookTicker update; else None.
        """
        if self._wrapper_cls is None:
            raise RuntimeError("Decoder not initialized")

        msg = self._wrapper_cls()  # type: ignore[call-arg]
        msg.ParseFromString(payload)

        # Common fields in examples: channel, symbol, and nested publicbookticker
        symbol = getattr(msg, "symbol", "") or ""
        channel = getattr(msg, "channel", "") or ""

        # Detect bookTicker payloads
        if "bookTicker" in channel:
            # The book ticker message is typically in msg.publicbookticker (protobuf field name)
            pb = getattr(msg, "publicbookticker", None)
            if pb is not None:
                # If any of bid/ask fields are present, consider it valid
                bid = getattr(pb, "bidprice", "") or getattr(pb, "bidPrice", "")
                ask = getattr(pb, "askprice", "") or getattr(pb, "askPrice", "")
                if (bid or ask) and symbol:
                    return str(symbol).upper()

        # Batch aggregation variant (rarely used here, but handle)
        batch = getattr(msg, "publicBookTickerBatch", None) or getattr(msg, "publicbooktickerbatch", None)
        if batch is not None and symbol:
            return str(symbol).upper()

        return None


async def spot_worker(
    name: str,
    symbols: list[str],
    cfg: Config,
    deadline_ts: float,
    answered: set[str],
    answered_lock: asyncio.Lock,
    stop_event: asyncio.Event,
    decoder: SpotProtoDecoder,
) -> None:
    log = logging.getLogger(f"spot.{name}")
    # Build subscription params
    subs = [f"spot@public.aggre.bookTicker.v3.api.pb@10ms@{s}" for s in symbols]

    async def sender(ws: websockets.WebSocketClientProtocol) -> None:
        # subscribe
        req = {"method": "SUBSCRIPTION", "params": subs}
        await ws.send(json_dumps(req))
        # ping loop
        while not stop_event.is_set() and time.time() < deadline_ts:
            await asyncio.sleep(cfg.ping_interval_s)
            with contextlib.suppress(Exception):
                await ws.send(json_dumps({"method": "PING"}))

    async def receiver(ws: websockets.WebSocketClientProtocol) -> None:
        while not stop_event.is_set() and time.time() < deadline_ts:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # ack/pong are usually text
            if isinstance(msg, str):
                # ignore
                continue

            if isinstance(msg, (bytes, bytearray)):
                sym = None
                try:
                    sym = decoder.decode_symbol_if_bookticker(bytes(msg))
                except Exception as e:
                    # protobuf decode errors can happen on non-market frames; log at debug
                    log.debug("protobuf decode error: %s", e)
                if sym:
                    async with answered_lock:
                        if sym not in answered:
                            answered.add(sym)
                            log.debug("answered: %s", sym)

    # connect with small jitter to reduce stampede
    await asyncio.sleep(((_stable_hash(name) % 200) / 1000.0) + random.random() * 0.05)

    while not stop_event.is_set() and time.time() < deadline_ts:
        try:
            async with websockets.connect(
                SPOT_WS_URL,
                ping_interval=None,  # we handle ping ourselves
                close_timeout=2,
                max_queue=1024,
            ) as ws:
                log.info("connected; subs=%d", len(subs))
                await asyncio.gather(sender(ws), receiver(ws))
                return
        except Exception as e:
            log.warning("connection error: %s; reconnecting soon", e)
            await asyncio.sleep(0.5)


async def futures_worker(
    name: str,
    symbols_spot_style: list[str],
    cfg: Config,
    deadline_ts: float,
    answered_spot_style: set[str],
    answered_lock: asyncio.Lock,
    stop_event: asyncio.Event,
) -> None:
    log = logging.getLogger(f"futures.{name}")
    # Convert to futures format with underscore (BTC_USDT)
    symbols_fut = [mexc_futures_symbol_from_spot(s) for s in symbols_spot_style]

    async def sender(ws: websockets.WebSocketClientProtocol) -> None:
        # Subscribe per symbol
        for sym in symbols_fut:
            req = {"method": "sub.ticker", "param": {"symbol": sym}, "gzip": False}
            await ws.send(json_dumps(req))
        # ping loop (required; server closes if no ping within 1 min)
        while not stop_event.is_set() and time.time() < deadline_ts:
            await asyncio.sleep(cfg.futures_ping_interval_s)
            with contextlib.suppress(Exception):
                await ws.send(json_dumps({"method": "ping"}))

    async def receiver(ws: websockets.WebSocketClientProtocol) -> None:
        while not stop_event.is_set() and time.time() < deadline_ts:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # Futures pushes JSON (may be bytes if gzip=true; we force gzip=false)
            if isinstance(msg, (bytes, bytearray)):
                try:
                    msg = msg.decode("utf-8", errors="replace")
                except Exception:
                    continue

            if not isinstance(msg, str):
                continue

            data = json_loads_safe(msg)
            if not isinstance(data, dict):
                continue

            ch = data.get("channel")
            if ch in ("pong",):
                continue

            if ch != "push.ticker":
                continue

            d = data.get("data")
            if not isinstance(d, dict):
                continue

            # bid1/ask1 presence
            if d.get("bid1") is None and d.get("ask1") is None:
                continue

            sym_fut = d.get("symbol") or data.get("symbol")
            if not isinstance(sym_fut, str):
                continue

            sym_spot = mexc_spot_symbol_from_futures(sym_fut)
            async with answered_lock:
                if sym_spot not in answered_spot_style:
                    answered_spot_style.add(sym_spot)
                    log.debug("answered: %s", sym_spot)

    await asyncio.sleep(((_stable_hash(name) % 200) / 1000.0) + random.random() * 0.05)

    while not stop_event.is_set() and time.time() < deadline_ts:
        try:
            async with websockets.connect(
                FUTURES_WS_URL,
                ping_interval=None,
                close_timeout=2,
                max_queue=4096,
            ) as ws:
                log.info("connected; subs=%d", len(symbols_fut))
                await asyncio.gather(sender(ws), receiver(ws))
                return
        except Exception as e:
            log.warning("connection error: %s; reconnecting soon", e)
            await asyncio.sleep(0.5)


# -------- JSON helpers (avoid importing json at module import in case user wants fastest start) --------
import json


def json_dumps(obj: object) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def json_loads_safe(s: str) -> object:
    try:
        return json.loads(s)
    except Exception:
        return None


async def run(cfg: Config, paths: Paths) -> int:
    log = logging.getLogger("main")

    spot_all = read_symbols(paths.spot_in)
    fut_all_spot_style = read_symbols(paths.futures_in)

    log.info("loaded: spot=%d futures=%d", len(spot_all), len(fut_all_spot_style))

    # Prepare Spot protobuf decoder (one-time bootstrap)
    decoder = SpotProtoDecoder(cfg.proto_cache_dir)
    decoder.ensure_ready()

    stop_event = asyncio.Event()

    # Signal handling
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    start_ts = time.time()
    deadline_ts = start_ts + cfg.duration_s

    # Batching / concurrency
    spot_batch = min(cfg.spot_batch_size, SPOT_MAX_SUBS_PER_CONN)
    spot_groups = chunked(spot_all, spot_batch)
    fut_groups = chunked(fut_all_spot_style, cfg.futures_batch_size)

    if cfg.spot_max_conns > 0:
        spot_groups = spot_groups[: cfg.spot_max_conns]
    if cfg.futures_max_conns > 0:
        fut_groups = fut_groups[: cfg.futures_max_conns]

    spot_answered: set[str] = set()
    fut_answered_spot_style: set[str] = set()
    lock = asyncio.Lock()

    tasks: list[asyncio.Task[None]] = []

    for i, group in enumerate(spot_groups, 1):
        tasks.append(
            asyncio.create_task(
                spot_worker(
                    name=f"ws{i}",
                    symbols=group,
                    cfg=cfg,
                    deadline_ts=deadline_ts,
                    answered=spot_answered,
                    answered_lock=lock,
                    stop_event=stop_event,
                    decoder=decoder,
                )
            )
        )

    for i, group in enumerate(fut_groups, 1):
        tasks.append(
            asyncio.create_task(
                futures_worker(
                    name=f"ws{i}",
                    symbols_spot_style=group,
                    cfg=cfg,
                    deadline_ts=deadline_ts,
                    answered_spot_style=fut_answered_spot_style,
                    answered_lock=lock,
                    stop_event=stop_event,
                )
            )
        )

    log.info(
        "running for %.1fs: spot_conns=%d futures_conns=%d",
        cfg.duration_s,
        len(spot_groups),
        len(fut_groups),
    )

    # Wait until deadline or stop_event
    while time.time() < deadline_ts and not stop_event.is_set():
        await asyncio.sleep(0.2)

    stop_event.set()

    # Let tasks wind down quickly
    with contextlib.suppress(Exception):
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5.0)

    # Filter preserving original order
    spot_filtered = [s for s in spot_all if s in spot_answered]
    fut_filtered = [s for s in fut_all_spot_style if s in fut_answered_spot_style]

    # Write outputs (overwrite)
    atomic_write_lines(paths.spot_out, spot_filtered)
    atomic_write_lines(paths.futures_out, fut_filtered)

    elapsed = time.time() - start_ts
    log.info(
        "done in %.2fs: spot_ok=%d/%d futures_ok=%d/%d",
        elapsed,
        len(spot_filtered),
        len(spot_all),
        len(fut_filtered),
        len(fut_all_spot_style),
    )
    return 0


def parse_args() -> tuple[Config, Paths]:
    p = argparse.ArgumentParser(
        description="Validate MEXC Spot/Futures symbols via WS best bid/ask within a time window."
    )
    p.add_argument("--duration", type=float, default=60.0, help="validation window in seconds (default: 60)")
    p.add_argument(
        "--spot-batch",
        type=int,
        default=30,
        help=f"spot symbols per WS connection (max {SPOT_MAX_SUBS_PER_CONN}, default: 30)",
    )
    p.add_argument("--futures-batch", type=int, default=50, help="futures symbols per WS connection (default: 50)")
    p.add_argument("--spot-max-conns", type=int, default=0, help="limit spot WS connections (0=unlimited)")
    p.add_argument("--futures-max-conns", type=int, default=0, help="limit futures WS connections (0=unlimited)")
    p.add_argument("--ping", type=float, default=20.0, help="spot ping interval seconds (default: 20)")
    p.add_argument("--futures-ping", type=float, default=15.0, help="futures ping interval seconds (default: 15)")
    p.add_argument(
        "--proto-cache",
        default="/root/.cache/mexc_ws_proto",
        help="cache dir for protobuf repo and generated pb2 (default: /root/.cache/mexc_ws_proto)",
    )
    p.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR (default: INFO)")

    p.add_argument("--spot-in", default=DEFAULT_SPOT_IN)
    p.add_argument("--futures-in", default=DEFAULT_FUT_IN)
    p.add_argument("--spot-out", default=DEFAULT_SPOT_OUT)
    p.add_argument("--futures-out", default=DEFAULT_FUT_OUT)

    a = p.parse_args()

    cfg = Config(
        duration_s=float(a.duration),
        spot_batch_size=max(1, min(int(a.spot_batch), SPOT_MAX_SUBS_PER_CONN)),
        futures_batch_size=max(1, int(a.futures_batch)),
        spot_max_conns=max(0, int(a.spot_max_conns)),
        futures_max_conns=max(0, int(a.futures_max_conns)),
        ping_interval_s=max(5.0, float(a.ping)),
        futures_ping_interval_s=max(5.0, float(a.futures_ping)),
        proto_cache_dir=pathlib.Path(a.proto_cache),
        log_level=str(a.log_level).upper(),
    )

    paths = Paths(
        spot_in=pathlib.Path(a.spot_in),
        futures_in=pathlib.Path(a.futures_in),
        spot_out=pathlib.Path(a.spot_out),
        futures_out=pathlib.Path(a.futures_out),
    )
    return cfg, paths


def main() -> int:
    cfg, paths = parse_args()
    setup_logging(cfg.log_level)

    try:
        return asyncio.run(run(cfg, paths))
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logging.getLogger("main").exception("fatal: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
