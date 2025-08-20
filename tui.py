# tui.py
import asyncio
import time
from typing import List, Dict, Any
from collections import deque
from datetime import datetime

from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.table import Table
from rich.panel import Panel
from rich import box

__all__ = ["TUI", "TuiLogHandler"]


class TUI:
    def __init__(
            self,
            refresh_hz: int = 8,
            alt_screen: bool = False,
            force_color: bool = True,
            color_system: str = "auto",
            log_rows: int = 300,
            trades_rows: int = 18,
            max_open_orders: int = 80,
    ):
        self.console = Console(force_terminal=force_color, color_system=color_system)
        self.refresh_hz = max(1, int(refresh_hz))
        self.alt_screen = bool(alt_screen)

        # данные
        self.symbol = "—"; self.base = "—"; self.quote = "—"
        self.market = {"bid": None, "ask": None, "mid": None, "spread_pct": None, "min_spread": None, "state": "INIT"}
        self.plan = {"buy": None, "sell": None, "gross": None, "net": None}
        self.bank = {"cash": "—", "base_pos": "—", "equity": "—", "realized": "—", "unrealized": "—", "total": "—"}

        # очереди
        self._orders_queue: deque[Dict[str, Any]] = deque(maxlen=int(max_open_orders))
        self._log = deque(maxlen=max(50, int(log_rows)))
        self._trades = deque(maxlen=max(6, int(trades_rows)))

        # счётчики
        self.t_counters = {"total": 0, "buy": 0, "sell": 0, "maker": 0, "taker": 0}
        self.o_counters = {"active": 0, "created": 0, "canceled": 0, "rejected": 0, "filled": 0, "expired": 0}

        # скорость/диагностика
        self.speed = {"ws": 0.0, "rest_get": 0, "rest_create": 0, "rest_cancel": 0}
        self.diag = "—"

        self._running = False
        self._start_ts = time.time()

        self.colors = {
            "market": "bright_green",
            "plan": "bright_cyan",
            "bank": "bright_magenta",
            "counters": "bright_yellow",
            "orders": "bright_blue",
            "trades": "bright_red",
            "logs": "bright_white",
            "header": "bright_black",
        }

    # --- внешние апдейты ---
    def set_symbol(self, symbol: str, base: str, quote: str):
        self.symbol, self.base, self.quote = symbol, base, quote

    def update_market(self, bid, ask, mid, spread_pct, min_spread, state_text: str | None = None):
        self.market.update(bid=bid, ask=ask, mid=mid, spread_pct=spread_pct, min_spread=min_spread)
        if state_text is not None:
            self.market["state"] = state_text

    def update_plan(self, buy, sell, gross, net):
        self.plan.update(buy=buy, sell=sell, gross=gross, net=net)

    def update_bank(self, cash, base_pos, equity, realized, unrealized, total):
        self.bank.update(cash=cash, base_pos=base_pos, equity=equity, realized=realized, unrealized=unrealized, total=total)

    def update_orders(self, rows: List[Dict[str, Any]]):
        rows = rows or []
        try:
            rows.sort(key=lambda r: float(r.get("ts", 0.0)))
        except Exception:
            pass
        for r in rows:
            self.append_order(r)

    def append_order(self, row: Dict[str, Any]):
        if not isinstance(row, dict):
            return
        self._orders_queue.append({
            "ts": row.get("ts"),
            "side": row.get("side"),
            "orderId": row.get("orderId"),
            "status": row.get("status"),
            "price": row.get("price"),
            "executedQty": row.get("executedQty"),
            "cummulativeQuoteQty": row.get("cummulativeQuoteQty"),
        })

    def update_trade_counters(self, total, buy, sell, maker, taker):
        self.t_counters.update(total=total, buy=buy, sell=sell, maker=maker, taker=taker)

    def update_order_counters(self, active, created, canceled, rejected, filled, expired):
        self.o_counters.update(active=active, created=created, canceled=canceled, rejected=rejected, filled=filled, expired=expired)

    def update_speed(self, ws_rate, rest_get, rest_create, rest_cancel):
        self.speed.update(ws=ws_rate, rest_get=rest_get, rest_create=rest_create, rest_cancel=rest_cancel)

    def update_diag(self, text: str):
        self.diag = text

    def update_last_trade(self, side, qty, price, liquidity):
        self._trades.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "side": side, "qty": qty, "price": price, "liq": liquidity
        })

    def add_log(self, level: str, text: str, logger_name: str = "", created_ts: float | None = None):
        ts = datetime.fromtimestamp(created_ts).strftime("%H:%M:%S") if created_ts else datetime.now().strftime("%H:%M:%S")
        # показываем сразу — добавляем запись мгновенно, Live подтянет на ближайшем тике
        self._log.append((ts, level, text))

    # --- форматтеры ---
    @staticmethod
    def _fmt_num(v, digits=8):
        try:
            return f"{float(v):.{digits}f}"
        except Exception:
            return "—"

    @staticmethod
    def _fmt_uptime(seconds: float) -> str:
        s = int(seconds)
        h = s // 3600
        m = (s % 3600) // 60
        s2 = s % 60
        return f"{h:02d}:{m:02d}:{s2:02d}"

    # --- таблицы ---
    def _market_table(self):
        t = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        t.add_column("Bid"); t.add_column("Ask"); t.add_column("Mid")
        t.add_column("Спред %"); t.add_column("Мин %"); t.add_column("Сост.")
        bid = f"[green]{self._fmt_num(self.market['bid'])}[/green]" if self.market['bid'] is not None else "—"
        ask = f"[red]{self._fmt_num(self.market['ask'])}[/red]" if self.market['ask'] is not None else "—"
        mid = self._fmt_num(self.market['mid'])
        spv = self.market['spread_pct']; sp = f"[yellow]{spv:.5f}[/yellow]" if spv is not None else "—"
        msp = f"{self.market['min_spread']:.5f}" if self.market['min_spread'] is not None else "—"
        st = f"[cyan]{self.market.get('state','—')}[/cyan]"
        t.add_row(bid, ask, mid, sp, msp, st)
        return t

    def _plan_table(self):
        p = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        p.add_column("BUY"); p.add_column("SELL"); p.add_column("Gross %"); p.add_column("Net %")
        pb = f"[green]{self._fmt_num(self.plan['buy'])}[/green]" if self.plan['buy'] else "—"
        ps = f"[red]{self._fmt_num(self.plan['sell'])}[/red]" if self.plan['sell'] else "—"
        gross = self.plan['gross']; net = self.plan['net']
        pg = "—" if gross is None else (f"[green]{gross:.5f}[/green]" if gross >= 0 else f"[red]{gross:.5f}[/red]")
        pn = "—" if net is None else (f"[green]{net:.5f}[/green]" if net >= 0 else f"[red]{net:.5f}[/red]")
        p.add_row(pb, ps, pg, pn)
        return p

    def _bank_table(self):
        b = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        b.add_column(self.quote); b.add_column(self.base); b.add_column("Эквити")
        b.add_column("Реализ."); b.add_column("Нереализ."); b.add_column("Итого")
        def pc(v):
            try:
                fv = float(v)
                return f"[green]{fv:.8f}[/green]" if fv > 0 else (f"[red]{fv:.8f}[/red]" if fv < 0 else f"{fv:.8f}")
            except Exception:
                return v
        b.add_row(str(self.bank['cash']), str(self.bank['base_pos']), str(self.bank['equity']),
                  pc(self.bank['realized']), pc(self.bank['unrealized']), pc(self.bank['total']))
        return b

    def _counters_table(self):
        t = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        t.add_column("Сделок"); t.add_column("Buy"); t.add_column("Sell"); t.add_column("Maker"); t.add_column("Taker")
        t.add_column("Активн."); t.add_column("Созд."); t.add_column("Отмен."); t.add_column("Reject"); t.add_column("Filled"); t.add_column("Expired")
        t.add_row(
            str(self.t_counters.get('total', 0)),
            str(self.t_counters.get('buy', 0)),
            str(self.t_counters.get('sell', 0)),
            str(self.t_counters.get('maker', 0)),
            str(self.t_counters.get('taker', 0)),
            str(self.o_counters.get('active', 0)),
            str(self.o_counters.get('created', 0)),
            str(self.o_counters.get('canceled', 0)),
            str(self.o_counters.get('rejected', 0)),
            str(self.o_counters.get('filled', 0)),
            str(self.o_counters.get('expired', 0)),
        )
        return t

    def _orders_table(self):
        t = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        t.add_column("Время", width=8)
        t.add_column("Side"); t.add_column("ID"); t.add_column("Status"); t.add_column("Price")
        t.add_column("ExecQty"); t.add_column("CumQuote")
        if self._orders_queue:
            for r in list(self._orders_queue):
                ts = r.get("ts")
                ts_s = datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S") if ts else "—"
                price = r.get("price"); execq = r.get("executedQty"); cq = r.get("cummulativeQuoteQty")
                side = r.get("side", "—")
                side_col = f"[green]{side}[/green]" if side == "BUY" else (f"[red]{side}[/red]" if side == "SELL" else side)
                st = str(r.get("status", "—"))
                if st == "FILLED":
                    st_col = "[green]FILLED[/green]"
                elif st in ("CANCELED", "REJECTED", "EXPIRED"):
                    st_col = "[red]" + st + "[/red]"
                elif st == "PARTIALLY_FILLED":
                    st_col = "[yellow]PARTIALLY_FILLED[/yellow]"
                else:
                    st_col = st
                t.add_row(
                    ts_s,
                    side_col,
                    str(r.get("orderId", "—")),
                    st_col,
                    f"{price:.8f}" if isinstance(price, (int, float)) else ("—" if price is None else str(price)),
                    f"{execq:.8f}" if isinstance(execq, (int, float)) else ("—" if execq is None else str(execq)),
                    f"{cq:.8f}" if isinstance(cq, (int, float)) else ("—" if cq is None else str(cq)),
                )
        else:
            t.add_row("—", "—", "—", "—", "—", "—", "—")
        return t

    def _trades_table(self):
        t = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        t.add_column("Время", width=8); t.add_column("Side", width=6)
        t.add_column("Qty"); t.add_column("Price"); t.add_column("Liq", width=6)
        if self._trades:
            for tr in reversed(self._trades):
                side_col = f"[green]{tr['side']}[/green]" if tr["side"] == "BUY" else f"[red]{tr['side']}[/red]"
                t.add_row(tr["ts"], side_col,
                          f"{tr['qty']:.8f}" if tr["qty"] is not None else "—",
                          f"{tr['price']:.8f}" if tr["price"] is not None else "—",
                          tr["liq"] or "—")
        else:
            t.add_row("—", "—", "—", "—", "—")

        speed = Table.grid(expand=True); speed.add_column("k1"); speed.add_column("k2")
        sp = f"WS≈{self.speed['ws']:.2f}/с | REST: get={self.speed['rest_get']}, create={self.speed['rest_create']}, cancel={self.speed['rest_cancel']}"
        diag_style = "[green]" if (self.diag or "").startswith("ОК") else "[yellow]"
       # speed.add_row("[b]СКОРОСТЬ[/b]", sp)
       # speed.add_row("[b]ДИАГНОСТИКА[/b]", f"{diag_style}{self.diag}[/]")

        return Group(t, speed)

    def _logs_panel(self):
        # Новые логи — СВЕРХУ (раньше добавлялись вниз и «терялись» за пределами панели)
        logs = Table(box=box.MINIMAL, expand=True, border_style="bright_black")
        logs.add_column("Время", width=8); logs.add_column("Уровень", width=9); logs.add_column("Сообщение", overflow="fold")
        level_color = {"INFO": "white", "WARNING": "yellow", "ERROR": "red", "DEBUG": "bright_black"}
        if self._log:
            # показываем последние записи первыми
            for ts, lvl, msg in reversed(self._log):
                color = level_color.get(lvl, "white")
                logs.add_row(ts, f"[{color}]{lvl}[/]", msg)
        else:
            logs.add_row("—", "—", "—")
        return Panel(logs, title="[b]СОБЫТИЯ (логи)[/b]", title_align="left", box=box.ROUNDED, border_style=self.colors["logs"])

    # --- рендер ---
    def _render(self):
        term_h = max(30, self.console.size.height)
        header_h = 3
        main_h   = max(12, int(term_h * 0.22))
        bottom_h = max(10, int(term_h * 0.28))
        main_row_h = max(6, main_h // 2)

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=header_h),
            Layout(name="main", size=main_h),
            Layout(name="events", ratio=1),
            Layout(name="bottom", size=bottom_h),
        )

        hdr = Table.grid(expand=True)
        hdr.add_column(justify="left"); hdr.add_column(justify="center"); hdr.add_column(justify="right")
        uptime = self._fmt_uptime(time.time() - self._start_ts)
        speed = f"WS≈{self.speed['ws']:.2f}/с • REST get={self.speed['rest_get']} create={self.speed['rest_create']} cancel={self.speed['rest_cancel']}"
        diag_style = "[green]" if (self.diag or "").startswith("ОК") else "[yellow]"
        hdr.add_row(
            f"[bold]Символ:[/bold] [white]{self.symbol}[/white]",
            f"[bold]В работе:[/bold] {uptime}    {speed}",
            f"[bold]Диагностика:[/bold] {diag_style}{self.diag}[/]    [bold]Биржа:[/bold] [white]Binance Spot[/white]",
        )
        layout["header"].update(Panel(hdr, title="[b]SHADOW MM[/b]", box=box.ROUNDED, border_style=self.colors["header"]))

        layout["main"].split_column(
            Layout(name="main_row1", size=main_row_h),
            Layout(name="main_row2", size=main_row_h),
        )
        layout["main"]["main_row1"].split_row(
            Layout(Panel(self._market_table(), title="[b]РЫНОК[/b]", title_align="left", box=box.ROUNDED, border_style=self.colors["market"]), ratio=1),
            Layout(Panel(self._plan_table(),   title="[b]ПЛАН КОТИРОВАНИЯ[/b]", title_align="left", box=box.ROUNDED, border_style=self.colors["plan"]),   ratio=1),
        )
        layout["main"]["main_row2"].split_row(
            Layout(Panel(self._bank_table(),     title=f"[b]БАНК • {self.quote}/{self.base}[/b]", title_align="left", box=box.ROUNDED, border_style=self.colors["bank"]),     ratio=1),
            Layout(Panel(self._counters_table(), title="[b]СЧЁТЧИКИ[/b]",                        title_align="left", box=box.ROUNDED, border_style=self.colors["counters"]), ratio=1),
        )

        layout["events"].update(self._logs_panel())

        layout["bottom"].split_row(
            Layout(Panel(self._orders_table(), title="[b]ОТКРЫТЫЕ ОРДЕРА[/b]",  title_align="left", box=box.ROUNDED, border_style=self.colors["orders"]), ratio=1),
            Layout(Panel(self._trades_table(), title="[b]ПОСЛЕДНИЕ СДЕЛКИ[/b]", title_align="left", box=box.ROUNDED, border_style=self.colors["trades"]), ratio=1),
        )
        return layout

    async def run(self):
        self._running = True
        with Live(self._render(), refresh_per_second=self.refresh_hz, transient=False, screen=self.alt_screen, console=self.console) as live:
            while self._running:
                live.update(self._render())
                await asyncio.sleep(1.0 / self.refresh_hz)

    def stop(self):
        self._running = False


# ---- лог-хэндлер для TUI ----
import logging
class TuiLogHandler(logging.Handler):
    def __init__(self, tui, level=logging.INFO):
        super().__init__(level)
        self.tui = tui
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            # Пихаем запись в TUI сразу (потокобезопасно)
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = None
            if loop and loop.is_running():
                loop.call_soon_threadsafe(self.tui.add_log, record.levelname, msg, record.name, record.created)
            else:
                self.tui.add_log(record.levelname, msg, record.name, record.created)
        except Exception:
            pass
