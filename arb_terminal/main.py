#!/usr/bin/env python3
"""
ARB TERMINAL
============
Semi-automatic spot↔futures arbitrage.

Usage:
    python arb_terminal/main.py
    python arb_terminal/main.py --size 200 --reduction 0.25
"""

import argparse
import logging
import sys
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── path fix so imports work from any CWD ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
# ─────────────────────────────────────────────────────────────────────────────

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.prompt import Confirm, Prompt

import arb_terminal.config as cfg
from arb_terminal.signal_listener import Signal, SignalListener
from arb_terminal.trade_executor import calculate_entry_prices
from arb_terminal.exchange_manager import ExchangeManager
from arb_terminal.trade_executor import TradeEntry, TradeExecutor, TradeResult

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


# ─────────────────────────────────────────────────────────────────────────────
# Rich helpers
# ─────────────────────────────────────────────────────────────────────────────


def _fmt_price(p: float) -> str:
    if p == 0:
        return "-"
    if p < 1:
        return f"{p:.6f}"
    if p < 100:
        return f"{p:.4f}"
    return f"{p:.2f}"


def _fmt_pct(p: float, color=True) -> str:
    s = f"{p:+.4f}%"
    if color:
        return f"[green]{s}[/]" if p >= 0 else f"[red]{s}[/]"
    return s


def _fmt_usdt(v: float) -> str:
    s = f"{v:+.4f} USDT"
    return f"[green]{s}[/]" if v >= 0 else f"[red]{s}[/]"


def _age_str(sig: Signal) -> str:
    age = sig.age_sec
    return f"[yellow]{age:.1f}s[/]" if age < 10 else f"[red]{age:.1f}s[/]"


# ─────────────────────────────────────────────────────────────────────────────
# Signal info panel
# ─────────────────────────────────────────────────────────────────────────────


def build_signal_panel(
    sig: Signal,
    position_size: float,
    spread_reduction: float,
) -> Panel:
    adj_buy, adj_sell, new_spread = calculate_entry_prices(sig, spread_reduction)

    # Approximate amounts (before exchange rounding)
    buy_qty_approx = position_size / adj_buy
    sell_qty_approx = position_size / adj_sell

    t = Table.grid(padding=(0, 2))
    t.add_column(style="bold cyan", justify="right")
    t.add_column()

    t.add_row("Symbol", f"[bold white]{sig.raw_symbol}[/]  direction=[cyan]{sig.direction}[/]")
    t.add_row("Age", _age_str(sig))
    t.add_row("", "")
    t.add_row(
        "Buy leg",
        f"[green]BUY SPOT[/]  {sig.buy_exchange.upper()}"
        f"  ask=[white]{_fmt_price(sig.buy_ask)}[/]"
        f"  qty_avail=[dim]{sig.buy_ask_qty:.0f}[/]",
    )
    t.add_row(
        "Sell leg",
        f"[red]SELL FUT[/]  {sig.sell_exchange.upper()}"
        f"  bid=[white]{_fmt_price(sig.sell_bid)}[/]"
        f"  qty_avail=[dim]{sig.sell_bid_qty:.0f}[/]",
    )
    t.add_row("", "")
    t.add_row(
        "Market spread",
        f"[cyan]{sig.market_spread_pct:.4f}%[/]",
    )
    t.add_row("", "")
    t.add_row(
        "ENTRY PLAN",
        f"size=[bold]{position_size:.0f} USDT/leg[/]  reduction=[bold]{spread_reduction:.2f} pp[/]",
    )
    t.add_row(
        "  Buy price",
        f"{_fmt_price(adj_buy)}  (orig {_fmt_price(sig.buy_ask)}, +0.{int(spread_reduction/2*100):02d}%)",
    )
    t.add_row(
        "  Sell price",
        f"{_fmt_price(adj_sell)}  (orig {_fmt_price(sig.sell_bid)}, -0.{int(spread_reduction/2*100):02d}%)",
    )
    t.add_row(
        "  Buy qty ~",
        f"{buy_qty_approx:.2f} {sig.symbol.split('/')[0]}  ≈ {position_size:.2f} USDT",
    )
    t.add_row(
        "  Sell qty ~",
        f"{sell_qty_approx:.2f} {sig.symbol.split('/')[0]}  ≈ {position_size:.2f} USDT",
    )
    t.add_row(
        "  New spread",
        f"[bold cyan]{new_spread:.4f}%[/]  (was {sig.market_spread_pct:.4f}%)",
    )

    return Panel(t, title="[bold yellow]⚡ SIGNAL[/]", border_style="yellow")


# ─────────────────────────────────────────────────────────────────────────────
# Order fill monitor table
# ─────────────────────────────────────────────────────────────────────────────


def build_fill_table(entry: TradeEntry, elapsed: float) -> Table:
    tbl = Table(
        "Leg", "Exchange", "Side", "Symbol",
        "Planned\nQty", "Planned\nPrice",
        "Filled\nQty", "Avg\nPrice",
        "Cost\n(USDT)", "Status",
        title=f"Order Fill Monitor  [dim]{elapsed:.0f}s[/]",
        border_style="blue",
        show_lines=True,
    )

    for info, label in [
        (entry.buy_order, "[green]BUY SPOT[/]"),
        (entry.sell_order, "[red]SELL FUT[/]"),
    ]:
        if info is None:
            continue

        status_color = {
            "closed": "[green]FILLED[/]",
            "open": "[yellow]OPEN[/]",
            "canceled": "[red]CANCELED[/]",
            "error": "[bold red]ERROR[/]",
        }.get(info.status or "", f"[yellow]OPEN[/]" if not info.status else f"[dim]{info.status}[/]")

        fill_pct = (info.filled / info.planned_amount * 100) if info.planned_amount else 0

        tbl.add_row(
            label,
            info.exchange.upper(),
            info.side.upper(),
            info.symbol,
            f"{info.planned_amount:.4f}",
            _fmt_price(info.planned_price),
            f"{info.filled:.4f}  [dim]({fill_pct:.0f}%)[/]",
            _fmt_price(info.avg_price),
            f"{info.cost:.4f}" if info.cost else "-",
            status_color if not info.error else f"[bold red]ERROR: {info.error[:40]}[/]",
        )

    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# PnL monitor
# ─────────────────────────────────────────────────────────────────────────────


def build_pnl_panel(
    entry: TradeEntry,
    sig: Signal,
    pnl: dict,
    duration: float,
) -> Panel:
    t = Table.grid(padding=(0, 2))
    t.add_column(style="bold cyan", justify="right")
    t.add_column()

    bo = entry.buy_order
    so = entry.sell_order

    t.add_row("Symbol", f"[bold white]{sig.raw_symbol}[/]")
    t.add_row(
        "Duration",
        f"{int(duration // 60)}m {int(duration % 60)}s",
    )
    t.add_row("", "")

    # Spot leg
    t.add_row(
        "Spot (long)",
        f"{sig.buy_exchange.upper()}  entry=[white]{_fmt_price(bo.avg_price if bo else 0)}[/]"
        f"  qty={bo.filled:.4f}" if bo else "n/a",
    )
    t.add_row(
        "  bid/ask",
        f"[green]{_fmt_price(pnl['spot_bid'])}[/] / [red]{_fmt_price(pnl['spot_ask'])}[/]",
    )
    t.add_row("  PnL", _fmt_usdt(pnl["buy_pnl"]))

    t.add_row("", "")

    # Futures leg
    t.add_row(
        "Futures (short)",
        f"{sig.sell_exchange.upper()}  entry=[white]{_fmt_price(so.avg_price if so else 0)}[/]"
        f"  qty={so.filled:.4f}" if so else "n/a",
    )
    t.add_row(
        "  bid/ask",
        f"[green]{_fmt_price(pnl['fut_bid'])}[/] / [red]{_fmt_price(pnl['fut_ask'])}[/]",
    )
    t.add_row("  PnL", _fmt_usdt(pnl["sell_pnl"]))

    t.add_row("", "")
    t.add_row("TOTAL PnL", _fmt_usdt(pnl["total_pnl"]))
    t.add_row(
        "Current spread",
        f"[cyan]{pnl['current_spread_pct']:.4f}%[/]"
        f"  (entry: {(sig.market_spread_pct):.4f}%)",
    )
    t.add_row("", "")
    t.add_row("Close", "[bold]Type [yellow]close[/] + Enter to close positions[/]")

    return Panel(t, title="[bold green]📈 POSITION MONITOR[/]", border_style="green")


# ─────────────────────────────────────────────────────────────────────────────
# Trade log
# ─────────────────────────────────────────────────────────────────────────────


def _fmt_order_log(info, label: str) -> str:
    if info is None:
        return f"  {label}: none"
    lines = [
        f"  {label}:",
        f"    exchange: {info.exchange} ({info.side})",
        f"    order_id: {info.order_id}",
        f"    planned:  {info.planned_amount:.6f} @ {_fmt_price(info.planned_price)}",
        f"    filled:   {info.filled:.6f} @ avg {_fmt_price(info.avg_price)}",
        f"    cost:     {info.cost:.4f} USDT",
        f"    status:   {info.status}",
    ]
    if info.error:
        lines.append(f"    ERROR:    {info.error}")
    return "\n".join(lines)


def build_log(result: TradeResult) -> str:
    sig = result.signal
    e = result.entry
    lines = [
        "=" * 60,
        "ARB TRADE LOG",
        "=" * 60,
        f"Time:        {datetime.fromtimestamp(result.open_time):%Y-%m-%d %H:%M:%S}",
        f"Symbol:      {sig.raw_symbol}",
        f"Direction:   {sig.direction}",
        "",
        "── SIGNAL ──────────────────────────────────────────────",
        f"  buy_exchange:  {sig.buy_exchange} ({sig.buy_market})",
        f"  buy_ask:       {_fmt_price(sig.buy_ask)}",
        f"  sell_exchange: {sig.sell_exchange} ({sig.sell_market})",
        f"  sell_bid:      {_fmt_price(sig.sell_bid)}",
        f"  spread_pct:    {sig.market_spread_pct:.4f}%",
        f"  ts_signal:     {sig.ts_signal}",
        "",
        "── ENTRY PARAMETERS ────────────────────────────────────",
        f"  position_size:    {result.position_size:.2f} USDT/leg",
        f"  spread_reduction: {result.spread_reduction:.2f} pp",
        f"  adj_buy_price:    {_fmt_price(e.adjusted_buy_price)}",
        f"  adj_sell_price:   {_fmt_price(e.adjusted_sell_price)}",
        "",
        "── OPEN ORDERS ─────────────────────────────────────────",
        _fmt_order_log(e.buy_order, "BUY  (spot long)"),
        _fmt_order_log(e.sell_order, "SELL (futures short)"),
        "",
        "── POSITION ────────────────────────────────────────────",
        f"  open_time:  {datetime.fromtimestamp(result.open_time):%H:%M:%S}",
        f"  close_time: {datetime.fromtimestamp(result.close_time):%H:%M:%S}" if result.close_time else "  (not closed)",
        f"  duration:   {result.duration_sec:.0f}s",
        "",
        "── CLOSE ───────────────────────────────────────────────",
        f"  buy_close_avg:  {_fmt_price(e.buy_close_avg)}",
        f"  sell_close_avg: {_fmt_price(e.sell_close_avg)}",
        "",
        "── RESULTS ─────────────────────────────────────────────",
        f"  Spot PnL:     {result.buy_pnl():+.4f} USDT",
        f"  Futures PnL:  {result.sell_pnl():+.4f} USDT",
        f"  TOTAL PnL:    {result.total_pnl():+.4f} USDT",
        "",
        "── EVENTS ──────────────────────────────────────────────",
    ]
    for ts, desc in result.events:
        lines.append(f"  {datetime.fromtimestamp(ts):%H:%M:%S.%f}"[:22] + f"  {desc}")

    lines.append("=" * 60)
    return "\n".join(lines)


def save_log(result: TradeResult, log_dir: Path) -> Path:
    ts = datetime.fromtimestamp(result.open_time or time.time())
    fname = f"trade_{result.signal.raw_symbol}_{ts:%Y%m%d_%H%M%S}.log"
    path = log_dir / fname
    path.write_text(build_log(result), encoding="utf-8")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Trade cycle
# ─────────────────────────────────────────────────────────────────────────────


def run_trade_cycle(
    sig: Signal,
    em: ExchangeManager,
    executor: TradeExecutor,
    position_size: float,
    spread_reduction: float,
    log_dir: Path,
) -> None:
    """Full lifecycle: open → fill monitor → PnL monitor → close → log."""

    result = TradeResult(
        signal=sig,
        position_size=position_size,
        spread_reduction=spread_reduction,
    )
    result.add_event("Signal accepted")

    # ── 1. Setup futures (margin mode, leverage) ──────────────────────────
    console.print(Rule("[cyan]Setting up futures[/]"))
    adj_buy, adj_sell, _ = calculate_entry_prices(sig, spread_reduction)

    for exch, side in [(sig.buy_exchange, "buy"), (sig.sell_exchange, "sell")]:
        if side == "sell":  # only futures leg needs setup
            msgs = em.setup_futures(exch, sig.symbol)
            for m in msgs:
                console.print(f"  [dim]{exch}[/]: {m}")

    # ── 2. Place orders ───────────────────────────────────────────────────
    console.print(Rule("[cyan]Placing limit orders...[/]"))
    result.add_event("Placing orders")

    entry = executor.place_orders(sig, position_size, spread_reduction)
    result.entry = entry
    result.open_time = time.time()

    # Check for placement errors
    buy_err = entry.buy_order and entry.buy_order.status == "error"
    sell_err = entry.sell_order and entry.sell_order.status == "error"

    if buy_err and sell_err:
        console.print("[bold red]Both orders FAILED. Returning to idle.[/]")
        console.print(f"  BUY  error: {entry.buy_order.error}")
        console.print(f"  SELL error: {entry.sell_order.error}")
        result.add_event("Both orders failed")
        return

    if buy_err or sell_err:
        failed = "BUY" if buy_err else "SELL"
        succeeded = "SELL" if buy_err else "BUY"
        err_msg = (entry.buy_order if buy_err else entry.sell_order).error
        console.print(f"[bold red]{failed} order FAILED:[/] {err_msg}")
        console.print(f"[yellow]{succeeded} order placed. Cancel it?[/]")
        if Confirm.ask("Cancel the successful order?", default=True):
            executor.cancel_entry(entry, sig)
            result.add_event(f"{failed} failed, {succeeded} canceled")
            return
        result.add_event(f"{failed} failed, proceeding with one leg")

    result.add_event("Orders placed")

    # ── 3. Fill monitor ───────────────────────────────────────────────────
    start_fill = time.time()
    timeout_notified = False

    with Live(console=console, refresh_per_second=2) as live:
        while not executor.both_filled(entry):
            executor.refresh_entry(entry, sig)
            elapsed = time.time() - start_fill
            live.update(build_fill_table(entry, elapsed))
            time.sleep(cfg.ORDER_POLL_INTERVAL)

            # 5-minute timeout check
            if elapsed > cfg.ORDER_TIMEOUT_SEC and not timeout_notified:
                timeout_notified = True
                live.stop()
                console.print(
                    f"[yellow]Orders not filled after {cfg.ORDER_TIMEOUT_SEC/60:.0f} min.[/]"
                )
                action = Prompt.ask(
                    "Action",
                    choices=["wait", "cancel"],
                    default="wait",
                )
                if action == "cancel":
                    executor.cancel_entry(entry, sig)
                    result.add_event("Orders canceled by operator (timeout)")
                    save_log(result, log_dir)
                    return
                live.start()

    result.add_event("Both orders filled")
    # Record fill times
    fill_time = time.time() - start_fill
    entry.buy_fill_time = fill_time
    entry.sell_fill_time = fill_time  # approximate (polled together)

    console.print(
        f"\n[bold green]✓ Both orders filled[/]  "
        f"buy avg=[white]{_fmt_price(entry.buy_order.avg_price)}[/]  "
        f"sell avg=[white]{_fmt_price(entry.sell_order.avg_price)}[/]\n"
    )

    # ── 4. PnL monitor ───────────────────────────────────────────────────
    close_event = threading.Event()

    def _input_watcher():
        """Daemon thread — waits for 'close' input."""
        while not close_event.is_set():
            try:
                line = sys.stdin.readline().strip().lower()
                if line in ("close", "c", "q", "exit"):
                    close_event.set()
                    break
            except Exception:
                close_event.set()
                break

    watcher = threading.Thread(target=_input_watcher, daemon=True)
    watcher.start()

    pos_start = time.time()
    with Live(console=console, refresh_per_second=1) as live:
        while not close_event.is_set():
            pnl = executor.fetch_pnl(entry, sig)
            duration = time.time() - pos_start
            live.update(build_pnl_panel(entry, sig, pnl, duration))
            time.sleep(cfg.PNL_POLL_INTERVAL)

    result.add_event("Close initiated by operator")

    # ── 5. Close positions ────────────────────────────────────────────────
    console.print(Rule("[red]Closing positions (market orders)...[/]"))
    buy_close, sell_close = executor.close_positions(entry, sig)

    if buy_close:
        if "error" in buy_close:
            console.print(f"[red]BUY close ERROR:[/] {buy_close['error']}")
            result.add_event(f"BUY close error: {buy_close['error']}")
        else:
            avg = entry.buy_close_avg  # already re-fetched in close_positions
            console.print(
                f"[green]BUY closed:[/] avg={_fmt_price(avg)}"
                f"  qty={buy_close.get('filled', '?')}"
            )
            result.add_event(f"BUY closed @ avg {_fmt_price(avg)}")

    if sell_close:
        if "error" in sell_close:
            console.print(f"[red]SELL close ERROR:[/] {sell_close['error']}")
            result.add_event(f"SELL close error: {sell_close['error']}")
        else:
            avg = entry.sell_close_avg  # already re-fetched in close_positions
            console.print(
                f"[green]SELL closed:[/] avg={_fmt_price(avg)}"
                f"  qty={sell_close.get('filled', '?')}"
            )
            result.add_event(f"SELL closed @ avg {_fmt_price(avg)}")

    result.close_time = time.time()
    result.add_event("Positions closed")

    # ── 6. Summary ────────────────────────────────────────────────────────
    console.print()
    t = Table.grid(padding=(0, 2))
    t.add_column(style="bold cyan", justify="right")
    t.add_column()
    t.add_row("Duration", f"{result.duration_sec:.0f}s")
    t.add_row("Spot PnL", _fmt_usdt(result.buy_pnl()))
    t.add_row("Futures PnL", _fmt_usdt(result.sell_pnl()))
    t.add_row("TOTAL PnL", _fmt_usdt(result.total_pnl()))
    console.print(Panel(t, title="[bold]Trade Result[/]", border_style="green"))

    # ── 7. Log ────────────────────────────────────────────────────────────
    log_path = save_log(result, log_dir)
    console.print(f"\n[dim]Log saved → {log_path}[/]")
    console.print(Rule())
    console.print(build_log(result))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="ARB Terminal")
    parser.add_argument("--size", type=float, default=None, help="Position size USDT/leg")
    parser.add_argument("--reduction", type=float, default=None, help="Spread reduction pp")
    args = parser.parse_args()

    console.print(
        Panel(
            "[bold white]ARB TERMINAL[/]\n"
            "[dim]Semi-automatic spot↔futures arbitrage[/]",
            border_style="bold blue",
        )
    )

    # ── Setup phase ───────────────────────────────────────────────────────
    position_size: float = args.size or float(
        Prompt.ask("Position size (USDT/leg)", default=str(cfg.POSITION_SIZE_USDT))
    )
    spread_reduction: float = args.reduction or float(
        Prompt.ask("Spread reduction (pp)", default=str(cfg.SPREAD_REDUCTION))
    )

    console.print(
        f"[dim]Settings:[/] size=[bold]{position_size:.0f} USDT[/]"
        f"  reduction=[bold]{spread_reduction:.2f} pp[/]"
        f"  channel=[bold]{cfg.REDIS_CHANNEL}[/]"
    )

    # ── Connect ───────────────────────────────────────────────────────────
    console.print(
        f"[dim]Connecting to Redis {cfg.REDIS_HOST}:{cfg.REDIS_PORT}...[/]"
    )
    listener = SignalListener(
        cfg.REDIS_HOST, cfg.REDIS_PORT, cfg.REDIS_CHANNEL, cfg.REDIS_PASSWORD
    )
    try:
        listener.connect()
    except Exception as exc:
        console.print(f"[bold red]Redis connection failed:[/] {exc}")
        sys.exit(1)

    console.print("[green]Redis connected ✓[/]  Waiting for signals...\n")

    em = ExchangeManager(cfg.EXCHANGE_KEYS)
    executor = TradeExecutor(em)

    # ── Signal loop ───────────────────────────────────────────────────────
    for sig in listener.listen():
        # Age check
        if sig.age_sec > cfg.SIGNAL_MAX_AGE_SEC:
            console.print(
                f"[dim]Signal {sig.raw_symbol} aged {sig.age_sec:.1f}s > "
                f"{cfg.SIGNAL_MAX_AGE_SEC}s — skipped[/]"
            )
            continue

        # Show signal
        console.print(build_signal_panel(sig, position_size, spread_reduction))

        # Ask operator
        try:
            go = Confirm.ask("[bold yellow]Enter trade?[/]", default=False)
        except (EOFError, KeyboardInterrupt):
            break

        if not go:
            console.print("[dim]Skipped. Waiting for next signal...[/]\n")
            continue

        # Run full trade cycle (blocking — no new signals processed until done)
        try:
            run_trade_cycle(
                sig, em, executor, position_size, spread_reduction, cfg.LOG_DIR
            )
        except KeyboardInterrupt:
            console.print(
                "\n[bold red]Ctrl+C during trade — attempting emergency close...[/]"
            )
            # Best-effort close (entry may be partially created)
            try:
                entry = result.entry if hasattr(result, "entry") else None
                if entry and (entry.buy_order or entry.sell_order):
                    executor.close_positions(entry, sig)
            except Exception:
                pass
            break
        except Exception as exc:
            console.print(f"[bold red]Unexpected error:[/] {exc}")
            import traceback
            traceback.print_exc()

        console.print("\n[green]Cycle complete. Waiting for next signal...[/]\n")

    listener.close()
    console.print("[dim]Shutdown.[/]")


def _run() -> None:
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted — bye.[/]")
        sys.exit(0)


if __name__ == "__main__":
    _run()
