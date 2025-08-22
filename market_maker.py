# market_maker.py
import asyncio
import logging
import time
from decimal import Decimal, ROUND_DOWN, InvalidOperation

from binance.enums import *
from utils import round_step, round_step_up as _round_step_up

logger = logging.getLogger(__name__)

class MarketMaker:
    def __init__(self, cfg, client_wrapper, tui=None):
        self.cfg = cfg
        self.client_wrap = client_wrapper
        self.tui = tui

        s = cfg.get('strategy', {})
        econ = cfg.get('econ', {})

        self.symbol = s.get('symbol', 'BNBUSDT')
        self.quote_asset = self._infer_quote_asset(self.symbol)
        self.base_asset = self._infer_base_asset(self.symbol)

        self.quote_size = Decimal(str(s.get('quote_size', 10.0)))
        self.target_pct = float(s.get('target_pct', 0.5))
        self.min_spread_pct = float(s.get('min_spread_pct', 0.0))
        self.cancel_timeout = float(s.get('cancel_timeout', 10.0))
        self.reorder_interval = float(s.get('reorder_interval', 1.0))
        self.depth_level = int(s.get('depth_level', 5))

        self.maker_fee_pct = float(econ.get('maker_fee_pct', s.get('maker_fee_pct', 0.1)))
        self.taker_fee_pct = float(econ.get('taker_fee_pct', s.get('taker_fee_pct', self.maker_fee_pct)))
        self.econ_min_net = float(econ.get('min_net_pct', 0.10))
        self.enforce_post_only = bool(econ.get('enforce_post_only', s.get('post_only', True)))
        self.aggressive_take = bool(s.get('aggressive_take', False))
        self.aggressive_bps = float(s.get('aggressive_bps', 0.0))
        self.allow_short = bool(s.get('allow_short', False))

        bal_cfg = cfg.get('balance_check', {})
        self.balance_check_enabled = bool(bal_cfg.get('enabled', False))

        # сервис
        self.status_poll_interval = float(s.get('status_poll_interval', 2.0))
        self.stats_interval = float(s.get('stats_interval', 30.0))
        self.ws_timeout = float(s.get('ws_timeout', 2.0))
        self.bootstrap_on_idle = bool(s.get('bootstrap_on_idle', True))
        self.rest_bootstrap_interval = float(s.get('rest_bootstrap_interval', 3.0))
        self.plan_log_interval = float(s.get('plan_log_interval', 5.0))

        # бумажный баланс
        self.start_cash = Decimal(str(s.get('paper_cash', 1000)))
        self.cash = Decimal(self.start_cash)
        self.base = Decimal('0')
        self.equity = Decimal(self.start_cash)
        self.start_equity = Decimal(self.start_cash)
        self.realized_pnl = Decimal('0')
        self.inv_cost_quote = Decimal('0')

        # символ-инфо
        self.step_size = None
        self.tick_size = None
        self.min_notional = None
        self.min_qty = None
        self.min_price = None

        # состояние/счётчики
        self.state = "INIT"
        self.open_orders = {}
        self._last_bid = None
        self._last_ask = None
        self._last_mid = None

        self._ws_msg_count = 0
        self._rest_count = {"get_order": 0, "create_order": 0, "cancel_order": 0}
        self._rate_last_ts = time.time()
        self._rate_last_ws = 0
        self._computed_ws_rate = 0.0

        self.trades_total = 0
        self.trades_buy = 0
        self.trades_sell = 0
        self.trades_maker = 0
        self.trades_taker = 0

        self.orders_created_total = 0
        self.orders_canceled_total = 0
        self.orders_rejected_total = 0
        self.orders_filled_total = 0
        self.orders_expired_total = 0

    @staticmethod
    def _infer_quote_asset(symbol: str) -> str:
        return symbol[-4:] if symbol.endswith("USDT") else symbol[-3:]

    @staticmethod
    def _infer_base_asset(symbol: str) -> str:
        return symbol[:-4] if symbol.endswith("USDT") else symbol[:-3]

    async def init_symbol_info(self):
        info = await self.client_wrap.client.get_symbol_info(self.symbol)
        if info is None:
            logger.error("Валюта '%s' не найдена.", self.symbol)
            raise ValueError(f"Symbol {self.symbol} unavailable.")

        for f in info['filters']:
            ft = f.get('filterType')
            if ft == 'LOT_SIZE':
                self.step_size = float(f['stepSize']); self.min_qty = float(f.get('minQty', 0.0))
            elif ft == 'PRICE_FILTER':
                self.tick_size = float(f['tickSize']); self.min_price = float(f.get('minPrice', 0.0))
            elif ft in ('MIN_NOTIONAL', 'NOTIONAL'):
                self.min_notional = float(f.get('minNotional') or f.get('notional') or 0.0)

        if self.step_size is None or self.tick_size is None:
            raise RuntimeError("Не обнаружены обязательные фильтры: LOT_SIZE/PRICE_FILTER.")

        logger.info(
            "Настройки %s: шаг лота=%s, шаг цены=%s, мин. нотионал=%s, мин. кол-во=%s",
            self.symbol, self.step_size, self.tick_size, self.min_notional, self.min_qty
        )
        logger.info(
            "Стратегия: символ=%s, базовая=%s, котируемая=%s, целевой спред=%.5f%%, пост-онли=%s, бюджет/сторону=%s %s",
            self.symbol, self.base_asset, self.quote_asset, self.target_pct,
            (self.enforce_post_only and not self.aggressive_take), str(self.quote_size), self.quote_asset
        )
        if self.tui:
            self.tui.set_symbol(self.symbol, self.base_asset, self.quote_asset)
            self._push_tui_trade_counters()
            self._push_tui_order_counters()

    # ---------- утилиты ----------
    def _diagnose_qty(self, price: float):
        if price <= 0:
            return {"qty": 0.0, "reason": "цена<=0"}
        initial_qty = float(self.quote_size) / float(price)
        qty = initial_qty
        reason = None
        if self.min_notional:
            min_qty_by_notional = float(self.min_notional) / float(price)
            if qty < min_qty_by_notional:
                qty = min_qty_by_notional
                reason = f"увеличили до MIN_NOTIONAL ({self.min_notional})"
        rounded = round_step(qty, self.step_size, precision=8)
        if self.min_qty and rounded < float(self.min_qty):
            return {"qty": 0.0, "reason": f"округлённый объём {rounded} < MIN_QTY {self.min_qty}",
                    "initial": initial_qty, "rounded": rounded,
                    "min_qty": self.min_qty, "min_notional": self.min_notional,
                    "notional": rounded * price}
        return {"qty": rounded, "reason": reason, "initial": initial_qty,
                "rounded": rounded, "min_qty": self.min_qty,
                "min_notional": self.min_notional, "notional": rounded * price}

    def _compute_prices(self, bid: float, ask: float):
        mid = (float(bid) + float(ask)) / 2.0
        half = self.target_pct / 200.0
        raw_buy = mid * (1.0 - half)
        raw_sell = mid * (1.0 + half)

        buy_p = round_step(raw_buy, self.tick_size, precision=8)
        sell_p = _round_step_up(raw_sell, self.tick_size, precision=8)

        if buy_p > float(bid):
            buy_p = round_step(bid, self.tick_size, precision=8)
        if sell_p < float(ask):
            sell_p = _round_step_up(ask, self.tick_size, precision=8)

        if self.aggressive_take:
            if self.aggressive_bps > 0:
                bump = 1.0 + (self.aggressive_bps / 10000.0)
                buy_p = max(buy_p, _round_step_up(ask * bump, self.tick_size, precision=8))
                sell_p = min(sell_p, round_step(bid / bump, self.tick_size, precision=8))
            else:
                buy_p = max(buy_p, _round_step_up(ask, self.tick_size, precision=8))
                sell_p = min(sell_p, round_step(bid, self.tick_size, precision=8))

        exp_gross = ((sell_p - buy_p) / buy_p) * 100 if buy_p > 0 else 0.0
        fee_total = (self.maker_fee_pct + self.taker_fee_pct) if self.aggressive_take else (2.0 * self.maker_fee_pct)
        exp_net = exp_gross - fee_total
        return buy_p, sell_p, mid, exp_gross, exp_net

    async def _check_balance(self, side: str, qty: float, price: float) -> float:
        """Ensure there are enough funds for the order and return adjusted qty."""
        if not self.balance_check_enabled:
            return qty
        try:
            bal = await self.client_wrap.check_balance(self.symbol, side, qty)
            free_base = Decimal(str(bal.get("base", 0)))
            free_quote = Decimal(str(bal.get("quote", 0)))
        except Exception as e:
            logger.warning("Не удалось получить балансы: %s", e)
            return qty

        if side == SIDE_BUY:
            need_quote = Decimal(str(price)) * Decimal(str(qty))
            if free_quote < need_quote:
                max_qty = free_quote / Decimal(str(price)) if price > 0 else Decimal('0')
                max_qty = Decimal(str(round_step(float(max_qty), self.step_size, precision=8)))
                if max_qty <= 0 or (self.min_qty and float(max_qty) < float(self.min_qty)):
                    logger.warning("Недостаточно %s: требуется %s, доступно %s", self.quote_asset, need_quote, free_quote)
                    return 0.0
                logger.warning(
                    "Недостаточно %s: требуется %s, доступно %s. Уменьшаем qty до %s",
                    self.quote_asset, need_quote, free_quote, max_qty,
                )
                return float(max_qty)
        else:
            if free_base < Decimal(str(qty)):
                max_qty = Decimal(str(round_step(float(free_base), self.step_size, precision=8)))
                if max_qty <= 0 or (self.min_qty and float(max_qty) < float(self.min_qty)):
                    logger.warning("Недостаточно %s: требуется %s, доступно %s", self.base_asset, qty, free_base)
                    return 0.0
                logger.warning(
                    "Недостаточно %s: требуется %s, доступно %s. Уменьшаем qty до %s",
                    self.base_asset, qty, free_base, max_qty,
                )
                return float(max_qty)

        return qty

    # ---------- TUI push ----------
    def _push_tui_market(self, best_bid: float, best_ask: float, mid: float, mkt_spread: float):
        if self.tui:
            self.tui.update_market(
                bid=best_bid, ask=best_ask, mid=mid,
                spread_pct=mkt_spread, min_spread=self.min_spread_pct,
                state_text=self.state,
            )

    def _push_tui_plan(self, buy_p: float, sell_p: float, exp_gross: float, exp_net: float):
        if self.tui:
            self.tui.update_plan(buy_p, sell_p, exp_gross, exp_net)

    def _push_tui_bank(self, mid: float):
        if self.tui:
            total_pnl = (self.equity - self.start_equity)
            unrealized = (total_pnl - self.realized_pnl)
            self.tui.update_bank(
                cash=str(self.cash), base_pos=str(self.base), equity=str(self.equity),
                realized=str(self.realized_pnl), unrealized=str(unrealized), total=str(total_pnl)
            )

    def _push_tui_order_event(self, row: dict):
        if self.tui and row:
            self.tui.append_order(row)

    def _push_tui_trade_counters(self):
        if self.tui:
            self.tui.update_trade_counters(
                total=self.trades_total, buy=self.trades_buy,
                sell=self.trades_sell, maker=self.trades_maker, taker=self.trades_taker
            )

    def _push_tui_order_counters(self):
        if self.tui:
            self.tui.update_order_counters(
                active=len(self.open_orders),
                created=self.orders_created_total,
                canceled=self.orders_canceled_total,
                rejected=self.orders_rejected_total,
                filled=self.orders_filled_total,
                expired=self.orders_expired_total,
            )

    def _push_tui_diag(self, text: str):
        if self.tui:
            self.tui.update_diag(text)

    def _push_tui_speed(self):
        if self.tui:
            self.tui.update_speed(
                ws_rate=self._computed_ws_rate,
                rest_get=self._rest_count["get_order"],
                rest_create=self._rest_count["create_order"],
                rest_cancel=self._rest_count["cancel_order"],
            )

    # ---------- Баланс/PnL ----------
    def _recalc_equity(self, mid: float):
        try:
            self.equity = (self.cash + self.base * Decimal(str(mid))).quantize(Decimal("0.00000001"))
        except InvalidOperation:
            self.equity = self.cash + self.base * Decimal(str(mid))

    def _log_equity(self, mid: float):
        self._recalc_equity(mid)
        total_pnl = (self.equity - self.start_equity)
        unrealized = (total_pnl - self.realized_pnl)
        logger.info(
            "БАНК: %s=%s | %s=%s | mid=%.8f -> Эквити=%s %s | PnL: реализ=%s %s, нереализ=%s %s, всего=%s %s.",
            self.quote_asset, str(self.cash), self.base_asset, str(self.base),
            mid, str(self.equity), self.quote_asset, str(self.realized_pnl), self.quote_asset,
            str(unrealized), self.quote_asset, str(total_pnl), self.quote_asset
        )
        self._push_tui_bank(mid)

    def _liquidity_flag(self, liquidity: str | None) -> str | None:
        if not liquidity:
            return None
        v = (liquidity or "").strip().upper()
        if v in ("MAKER", "M"):
            return "MAKER"
        if v in ("TAKER", "T"):
            return "TAKER"
        return None

    def _apply_fill_delta(self, side: str, prev_exec: Decimal, prev_cum_quote: Decimal,
                          exec_qty: Decimal, cum_quote: Decimal, liquidity: str | None):
        try:
            D = lambda x: x if isinstance(x, Decimal) else Decimal(str(x or 0))
            prev_exec = D(prev_exec); prev_cum_quote = D(prev_cum_quote)
            exec_qty = D(exec_qty); cum_quote = D(cum_quote)

            d_exec = exec_qty - prev_exec
            d_quote = cum_quote - prev_cum_quote
            if d_exec <= 0:
                return

            avg_px = (d_quote / d_exec) if d_exec != 0 else Decimal('0')
            side_u = (side or "").upper()

            if side_u == "BUY":
                self.cash -= d_quote
                self.base += d_exec
                self.inv_cost_quote += d_quote
                self.trades_buy += 1
            elif side_u == "SELL":
                avg_cost = (self.inv_cost_quote / self.base) if self.base > 0 else Decimal('0')
                cost_part = avg_cost * d_exec
                realized = d_quote - cost_part
                self.realized_pnl += realized
                self.cash += d_quote
                self.base -= d_exec
                self.inv_cost_quote -= cost_part
                self.trades_sell += 1

            self.trades_total += 1
            liq = self._liquidity_flag(liquidity)
            if liq == "MAKER": self.trades_maker += 1
            elif liq == "TAKER": self.trades_taker += 1

            self._push_tui_order_event({
                "ts": time.time(),
                "side": side_u,
                "orderId": "-",
                "status": "PARTIALLY_FILLED",
                "price": float(avg_px) if avg_px else None,
                "executedQty": float(d_exec),
                "cummulativeQuoteQty": float(d_quote),
            })
            self._push_tui_trade_counters()

            if self._last_mid is not None:
                self._recalc_equity(self._last_mid)
                self._push_tui_bank(self._last_mid)

            logger.info("ИСПОЛНЕНИЕ: %s Δqty=%.8f %s, Δquote=%.8f %s, avg=%.8f, liq=%s",
                        side_u, float(d_exec), self.base_asset, float(d_quote), self.quote_asset,
                        float(avg_px), (liq or "-"))
        except Exception as e:
            logger.warning("apply_fill_delta: ошибка учёта исполнения: %s", e)

    # ---------- REST bootstrap (быстрый старт, пока WS молчит) ----------
    async def _rest_bootstrap_once(self):
        try:
            t = await self.client_wrap.client.get_orderbook_ticker(symbol=self.symbol)
            best_bid = float(t['bidPrice']); best_ask = float(t['askPrice'])
            await self._on_market(best_bid, best_ask, source="REST")
            logger.info("REST-СНИМОК: best_bid=%.8f, best_ask=%.8f (limit=%s). Продолжаем логику.",
                        best_bid, best_ask, self.depth_level)
        except Exception as e:
            logger.debug("REST bootstrap ошибка: %s", e)

    async def _bootstrap_loop(self):
        # крутимся до первого сообщения из WS, далее — по желанию (bootstrap_on_idle)
        first_done = False
        while True:
            if not first_done and self._ws_msg_count == 0:
                await self._rest_bootstrap_once()
            elif self.bootstrap_on_idle:
                await self._rest_bootstrap_once()
            await asyncio.sleep(self.rest_bootstrap_interval)
            if self._ws_msg_count > 0 and not self.bootstrap_on_idle:
                break

    # ---------- сервисные циклы ----------
    async def _stats_loop(self):
        self._computed_ws_rate = 0.0
        while True:
            now = time.time()
            dt = max(1e-6, now - self._rate_last_ts)
            dws = self._ws_msg_count - self._rate_last_ws
            self._computed_ws_rate = dws / dt
            self._rate_last_ts = now
            self._rate_last_ws = self._ws_msg_count
            self._push_tui_speed()
            await asyncio.sleep(1.0)

    # ---------- WS-циклы ----------
    async def _market_depth_loop(self):
        bm = self.client_wrap.bm
        while True:
            try:
                logger.info("WS подключение: depth_socket %s (levels=%s)", self.symbol, self.depth_level)
                async with bm.depth_socket(self.symbol, depth=self.depth_level) as stream:
                    while True:
                        try:
                            msg = await asyncio.wait_for(stream.recv(), timeout=self.ws_timeout)
                            self._ws_msg_count += 1
                            bids = msg.get('bids', []); asks = msg.get('asks', [])
                            if not bids or not asks:
                                self.state = "ЖДЁМ: пустой стакан"
                                await asyncio.sleep(self.reorder_interval); continue

                            await self.client_wrap.on_book_update(self.symbol, bids, asks)
                            best_bid = float(bids[0][0]); best_ask = float(asks[0][0])
                            await self._on_market(best_bid, best_ask, source="WS")
                            await asyncio.sleep(self.reorder_interval)
                        except asyncio.TimeoutError:
                            if self._last_mid is not None:
                                self._log_equity(self._last_mid)
                            await self._refresh_open_orders()
                            await asyncio.sleep(self.reorder_interval)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("WS depth: разрыв/ошибка (%s). Переподключаемся через 1с…", e)
                await asyncio.sleep(1.0)

    async def _aggtrade_loop(self):
        bm = self.client_wrap.bm
        while True:
            try:
                logger.info("WS подключение: aggtrade_socket %s", self.symbol)
                async with bm.aggtrade_socket(self.symbol) as stream:
                    while True:
                        try:
                            tmsg = await stream.recv()
                            price = float(tmsg.get('p', 0.0))
                            qty = float(tmsg.get('q', 0.0))
                            is_buyer_maker = bool(tmsg.get('m', False))

                            await self.client_wrap.on_trade(self.symbol, price, qty, is_buyer_maker)
                            if self.tui:
                                side = "SELL" if is_buyer_maker else "BUY"
                                liq = "M" if is_buyer_maker else "T"
                                self.tui.update_last_trade(side=side, qty=qty, price=price, liquidity=liq)

                            await self._refresh_open_orders(force=True)
                        except asyncio.TimeoutError:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("WS aggTrade: разрыв/ошибка (%s). Переподключаемся через 1с…", e)
                await asyncio.sleep(1.0)

    # ---------- Основная логика ----------
    async def _on_market(self, best_bid: float, best_ask: float, source: str):
        mid = (best_bid + best_ask) / 2.0
        self._last_bid, self._last_ask, self._last_mid = best_bid, best_ask, mid
        mkt_spread = ((best_ask - best_bid) / max(1e-12, best_bid)) * 100.0

        self.state = "ОЦЕНКА"
        self._push_tui_market(best_bid, best_ask, mid, mkt_spread)
        self._log_equity(mid)

        buy_p, sell_p, _, exp_gross, exp_net = self._compute_prices(best_bid, best_ask)
        self._push_tui_plan(buy_p, sell_p, exp_gross, exp_net)

        diag = []
        if mkt_spread < self.min_spread_pct:
            diag.append(f"СПРЕД УЗКИЙ: {mkt_spread:.5f}% < min {self.min_spread_pct:.5f}%")
        if exp_net < self.econ_min_net:
            diag.append(f"ЭКОНОМИКА: net≈{exp_net:.5f}% < порога {self.econ_min_net:.5f}%")
        self._push_tui_diag(" | ".join(diag) if diag else "ОК: котируем")

        if diag:
            self.state = "ЖДЁМ"
            return

        # --- КОТИРУЕМ ---
        self.state = "КОТИРУЕМ"

        now = time.time()
        stale_sides = [side for side, d in self.open_orders.items() if now - d['ts'] > self.cancel_timeout]
        for side in stale_sides:
            try:
                await self.client_wrap.cancel_order(symbol=self.symbol, orderId=self.open_orders[side]['orderId'])
                self._rest_count["cancel_order"] += 1
                self.orders_canceled_total += 1
                logger.info("ОТМЕНА: %s id=%s (протух).", side, self.open_orders[side]['orderId'])
                self._push_tui_order_event({
                    "ts": time.time(), "side": side, "orderId": self.open_orders[side]['orderId'],
                    "status": "CANCELED", "price": self.open_orders[side].get("price"),
                    "executedQty": self.open_orders[side].get("executedQty", 0.0),
                    "cummulativeQuoteQty": self.open_orders[side].get("cummulativeQuoteQty", 0.0),
                })
            except Exception as e:
                logger.warning("Ошибка отмены %s id=%s: %s", side, self.open_orders[side]['orderId'], e)
            self.open_orders.pop(side, None)
        self._push_tui_order_counters()

        await self._refresh_open_orders()

        ordertype = ORDER_TYPE_LIMIT_MAKER if (self.enforce_post_only and not self.aggressive_take) else ORDER_TYPE_LIMIT

        # BUY
        if 'BUY' not in self.open_orders:
            diagq = self._diagnose_qty(buy_p); qty = diagq["qty"]
            qty = await self._check_balance(SIDE_BUY, qty, buy_p)
            if qty > 0:
                try:
                    order = await self.client_wrap.create_order(
                        symbol=self.symbol, side=SIDE_BUY, type=ordertype,
                        quantity=str(qty), price=str(buy_p), timeInForce=TIME_IN_FORCE_GTC
                    )
                    self._rest_count["create_order"] += 1
                    status = order.get('status', 'NEW')

                    exec_qty = Decimal(str(order.get('executedQty') or 0))
                    cum_quote = Decimal(str(order.get('cummulativeQuoteQty') or 0))
                    liq = order.get('liquidity')
                    if status != 'REJECTED':
                        self.orders_created_total += 1
                    if status == 'REJECTED':
                        self.orders_rejected_total += 1
                        logger.info("REJECT BUY (post-only пересечение): id=%s price=%.8f", order.get('orderId'), buy_p)
                    elif status == 'FILLED':
                        self.orders_filled_total += 1
                        self._apply_fill_delta('BUY', Decimal('0'), Decimal('0'), exec_qty, cum_quote, liq)
                    elif status == 'CANCELED':
                        self.orders_canceled_total += 1
                    elif status == 'EXPIRED':
                        self.orders_expired_total += 1
                    elif status == 'PARTIALLY_FILLED':
                        self._apply_fill_delta('BUY', Decimal('0'), Decimal('0'), exec_qty, cum_quote, liq)
                        self.open_orders['BUY'] = {
                            'orderId': order['orderId'], 'ts': now, 'status': status,
                            'executedQty': float(exec_qty), 'cummulativeQuoteQty': float(cum_quote),
                            'price': float(buy_p)
                        }
                        logger.info("РАЗМЕЩЁН BUY: id=%s | qty=%.8f %s | price=%.8f %s | пост-онли=%s.",
                                    order.get('orderId', '-'), float(qty), self.base_asset, buy_p, self.quote_asset,
                                    (ordertype == ORDER_TYPE_LIMIT_MAKER))
                        self._push_tui_order_counters()
                    else:
                        self.open_orders['BUY'] = {
                            'orderId': order['orderId'], 'ts': now, 'status': status,
                            'executedQty': float(exec_qty), 'cummulativeQuoteQty': float(cum_quote),
                            'price': float(buy_p)
                        }
                        logger.info("РАЗМЕЩЁН BUY: id=%s | qty=%.8f %s | price=%.8f %s | пост-онли=%s.",
                                    order.get('orderId', '-'), float(qty), self.base_asset, buy_p, self.quote_asset,
                                    (ordertype == ORDER_TYPE_LIMIT_MAKER))

                    self._push_tui_order_event({
                        "ts": now, "side": "BUY", "orderId": order.get('orderId'),
                        "status": status, "price": float(buy_p),
                        "executedQty": float(exec_qty), "cummulativeQuoteQty": float(cum_quote)
                    })
                    self._push_tui_order_counters()
                except Exception as e:
                    logger.error("Ошибка размещения BUY: %s", e)

        # SELL
        if 'SELL' not in self.open_orders:
            diagq = self._diagnose_qty(sell_p); qty = diagq["qty"]
            if self.balance_check_enabled:
                qty = await self._check_balance(SIDE_SELL, qty, sell_p)
            elif not self.allow_short:
                max_sell = float(self.base)
                qty = min(qty, max_sell) if max_sell > 0 else 0.0
            if qty > 0:
                try:
                    order = await self.client_wrap.create_order(
                        symbol=self.symbol, side=SIDE_SELL, type=ordertype,
                        quantity=str(qty), price=str(sell_p), timeInForce=TIME_IN_FORCE_GTC
                    )
                    self._rest_count["create_order"] += 1
                    status = order.get('status', 'NEW')

                    exec_qty = Decimal(str(order.get('executedQty') or 0))
                    cum_quote = Decimal(str(order.get('cummulativeQuoteQty') or 0))
                    liq = order.get('liquidity')
                    if status != 'REJECTED':
                        self.orders_created_total += 1
                    if status == 'REJECTED':
                        self.orders_rejected_total += 1
                        logger.info("REJECT SELL (post-only пересечение): id=%s price=%.8f", order.get('orderId'), sell_p)
                    elif status == 'FILLED':
                        self.orders_filled_total += 1
                        self._apply_fill_delta('SELL', Decimal('0'), Decimal('0'), exec_qty, cum_quote, liq)
                    elif status == 'CANCELED':
                        self.orders_canceled_total += 1
                    elif status == 'EXPIRED':
                        self.orders_expired_total += 1
                    elif status == 'PARTIALLY_FILLED':
                        self._apply_fill_delta('SELL', Decimal('0'), Decimal('0'), exec_qty, cum_quote, liq)
                        self.open_orders['SELL'] = {
                            'orderId': order['orderId'], 'ts': now, 'status': status,
                            'executedQty': float(exec_qty), 'cummulativeQuoteQty': float(cum_quote),
                            'price': float(sell_p)
                        }
                        logger.info("РАЗМЕЩЁН SELL: id=%s | qty=%.8f %s | price=%.8f %s | пост-онли=%s.",
                                    order.get('orderId', '-'), float(qty), self.base_asset, sell_p, self.quote_asset,
                                    (ordertype == ORDER_TYPE_LIMIT_MAKER))
                        self._push_tui_order_counters()
                    else:
                        self.open_orders['SELL'] = {
                            'orderId': order['orderId'], 'ts': now, 'status': status,
                            'executedQty': float(exec_qty), 'cummulativeQuoteQty': float(cum_quote),
                            'price': float(sell_p)
                        }
                        logger.info("РАЗМЕЩЁН SELL: id=%s | qty=%.8f %s | price=%.8f %s | пост-онли=%s.",
                                    order.get('orderId', '-'), float(qty), self.base_asset, sell_p, self.quote_asset,
                                    (ordertype == ORDER_TYPE_LIMIT_MAKER))

                    self._push_tui_order_event({
                        "ts": now, "side": "SELL", "orderId": order.get('orderId'),
                        "status": status, "price": float(sell_p),
                        "executedQty": float(exec_qty), "cummulativeQuoteQty": float(cum_quote)
                    })
                    self._push_tui_order_counters()
                except Exception as e:
                    logger.error("Ошибка размещения SELL: %s", e)

    async def _refresh_open_orders(self, force: bool = False):
        if not self.open_orders:
            self._push_tui_order_counters()
            return

        to_drop = []
        for side, d in list(self.open_orders.items()):
            try:
                o = await self.client_wrap.get_order(symbol=self.symbol, orderId=d['orderId'])
                self._rest_count["get_order"] += 1

                status = o.get('status')
                exec_qty = Decimal(str(o.get('executedQty') or 0))
                cum_quote = Decimal(str(o.get('cummulativeQuoteQty') or 0))
                liq = o.get('liquidity')

                prev_status = d.get('status')
                prev_exec = Decimal(str(d.get('executedQty') or 0))
                prev_cum_quote = Decimal(str(d.get('cummulativeQuoteQty') or 0))

                if exec_qty != prev_exec or cum_quote != prev_cum_quote:
                    self._apply_fill_delta(side, prev_exec, prev_cum_quote, exec_qty, cum_quote, liq)
                    self._push_tui_order_event({
                        "ts": time.time(), "side": side, "orderId": d.get("orderId"),
                        "status": "PARTIALLY_FILLED", "price": d.get("price"),
                        "executedQty": float(exec_qty), "cummulativeQuoteQty": float(cum_quote)
                    })

                if status != prev_status:
                    if status == 'FILLED':
                        self.orders_filled_total += 1
                    elif status == 'CANCELED':
                        self.orders_canceled_total += 1
                    elif status == 'REJECTED':
                        self.orders_rejected_total += 1
                    elif status == 'EXPIRED':
                        self.orders_expired_total += 1

                    self._push_tui_order_event({
                        "ts": time.time(), "side": side, "orderId": d.get("orderId"),
                        "status": status, "price": d.get("price"),
                        "executedQty": float(exec_qty), "cummulativeQuoteQty": float(cum_quote)
                    })

                d['status'] = status
                d['executedQty'] = float(exec_qty)
                d['cummulativeQuoteQty'] = float(cum_quote)

                if status not in ('NEW', 'PARTIALLY_FILLED'):
                    to_drop.append(side)
            except Exception as e:
                logger.warning("Ошибка get_order для %s id=%s: %s", side, d.get('orderId'), e)

        for s in to_drop:
            self.open_orders.pop(s, None)

        self._push_tui_order_counters()

    async def run(self):
        await self.init_symbol_info()
        # параллельно крутим WS + статистику + REST-bootstrap
        tasks = [
            self._market_depth_loop(),
            self._aggtrade_loop(),
            self._stats_loop(),
        ]
        # быстрый старт через REST до первого WS (и опционально в простое)
        tasks.append(self._bootstrap_loop())
        await asyncio.gather(*tasks)
