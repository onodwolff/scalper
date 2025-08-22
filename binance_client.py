# binance_client.py
import asyncio
import logging
import random
import time
from typing import Dict, Any, Optional

from binance import AsyncClient, BinanceSocketManager

logger = logging.getLogger(__name__)


class ShadowExecutor:
    """
    Упрощённая эмуляция биржи:
    - LIMIT_MAKER отклоняется, если заявка стала бы тейкером (пересекает лучший оффер/бид)
    - Частичные исполнения по потоку сделок (aggTrade) с коэффициентом alpha
    - Имитация сетевой задержки latency_ms при создании/отмене/запросе статуса
    - Имитация мгновенного ТЕЙКЕРА (если лимит-не-post-only и цена пересекает рынок)
    - Cчитаем cumQuote как сумму(fill_qty * fill_price), liquidity: MAKER/TAKER
    """
    def __init__(self, alpha: float = 0.85, latency_ms: int = 120,
                 post_only_reject: bool = True, taker_slippage_bps: float = 0.0):
        self.alpha = max(0.0, min(1.0, float(alpha)))
        self.latency = max(0, int(latency_ms))
        self.post_only_reject = bool(post_only_reject)
        self.taker_slippage_bps = float(taker_slippage_bps)

        self._id = 0
        self._orders: Dict[int, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    async def on_book_update(self, symbol: str, bids, asks):
        # сохраняем лучший бид/оффер
        try:
            self.best_bid = float(bids[0][0]) if bids else None
            self.best_ask = float(asks[0][0]) if asks else None
        except Exception:
            pass

    async def on_trade(self, symbol: str, price: float, qty: float, is_buyer_maker: bool):
        # пробуем исполнить висящие лимитки по цене сделки
        async with self._lock:
            for oid, o in list(self._orders.items()):
                if o["status"] in ("CANCELED", "FILLED", "REJECTED", "EXPIRED"):
                    continue
                side = o["side"]
                px = float(o["price"])
                remain = float(o["origQty"]) - float(o["executedQty"])
                if remain <= 0:
                    o["status"] = "FILLED"
                    continue

                match = (side == "BUY" and price <= px) or (side == "SELL" and price >= px)
                if not match:
                    continue

                # доля исполняемого объёма — от объёма сделки, но не больше остатка
                fill_qty = min(remain, qty * self.alpha)
                if fill_qty <= 0:
                    continue

                fill_quote = fill_qty * price
                o["executedQty"] = float(o["executedQty"]) + fill_qty
                o["cummulativeQuoteQty"] = float(o["cummulativeQuoteQty"]) + fill_quote
                o["liquidity"] = "MAKER"

                if abs(float(o["executedQty"]) - float(o["origQty"])) < 1e-12:
                    o["status"] = "FILLED"
                else:
                    o["status"] = "PARTIALLY_FILLED"

    async def create_order(self, *, symbol, side, type, timeInForce, quantity, price):
        await asyncio.sleep(self.latency / 1000.0)
        side = side.upper()
        type = type.upper()

        qty = float(quantity)
        px = float(price)
        if qty <= 0:
            raise ValueError("quantity must be > 0")
        if px <= 0:
            raise ValueError("price must be > 0")

        # Post-only отклонение, если бы заявка взяла ликвидность
        if type == "LIMIT_MAKER" and self.post_only_reject:
            would_take = (
                (side == "BUY" and self.best_ask is not None and px >= self.best_ask)
                or (side == "SELL" and self.best_bid is not None and px <= self.best_bid)
            )
            if would_take:
                oid = self._next_id()
                o = {
                    "symbol": symbol,
                    "orderId": oid,
                    "side": side,
                    "status": "REJECTED",
                    "type": type,
                    "timeInForce": timeInForce,
                    "price": px,
                    "origQty": qty,
                    "executedQty": 0.0,
                    "cummulativeQuoteQty": 0.0,
                    "liquidity": None,
                    "ts": time.time(),
                }
                self._orders[oid] = o
                return o

        # Непост-онли лимитка, пересекающая рынок — исполняем как ТЕЙКЕР целиком
        if type != "LIMIT_MAKER":
            cross_buy = side == "BUY" and self.best_ask is not None and px >= self.best_ask
            cross_sell = side == "SELL" and self.best_bid is not None and px <= self.best_bid
            if cross_buy or cross_sell:
                oid = self._next_id()
                # имитируем проскальзывание
                if cross_buy:
                    trade_px = self.best_ask * (1.0 + self.taker_slippage_bps / 10000.0)
                else:
                    trade_px = self.best_bid * (1.0 - self.taker_slippage_bps / 10000.0)

                o = {
                    "symbol": symbol,
                    "orderId": oid,
                    "side": side,
                    "status": "FILLED",
                    "type": type,
                    "timeInForce": timeInForce,
                    "price": px,
                    "origQty": qty,
                    "executedQty": qty,
                    "cummulativeQuoteQty": qty * trade_px,
                    "liquidity": "TAKER",
                    "ts": time.time(),
                }
                self._orders[oid] = o
                return o

        # Обычная лимитка — висит до исполнения потоком сделок
        oid = self._next_id()
        o = {
            "symbol": symbol, "orderId": oid, "side": side, "status": "NEW",
            "type": type, "timeInForce": timeInForce, "price": px,
            "origQty": qty, "executedQty": 0.0, "cummulativeQuoteQty": 0.0,
            "liquidity": None, "ts": time.time(),
        }
        self._orders[oid] = o
        return o

    async def get_order(self, *, symbol, orderId):
        await asyncio.sleep(self.latency / 1000.0)
        oid = int(orderId)
        o = self._orders.get(oid)
        if not o:
            # имитируем EXPIRED, если кто-то спрашивает о старом
            return {
                "symbol": symbol, "orderId": oid, "status": "EXPIRED",
                "executedQty": 0.0, "cummulativeQuoteQty": 0.0, "price": None, "side": None,
                "liquidity": None,
            }
        return dict(o)

    async def cancel_order(self, *, symbol, orderId):
        await asyncio.sleep(self.latency / 1000.0)
        oid = int(orderId)
        o = self._orders.get(oid)
        if not o:
            return {"symbol": symbol, "orderId": oid, "status": "CANCELED"}
        if o["status"] in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
            return dict(o)
        o["status"] = "CANCELED"
        return dict(o)


class BinanceAsync:
    def __init__(self, api_key, api_secret, paper=True, shadow=True, shadow_opts: Optional[dict] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self.shadow_enabled = bool(shadow)
        self.client: Optional[AsyncClient] = None
        self.bm: Optional[BinanceSocketManager] = None

        shadow_opts = shadow_opts or {}
        self.shadow = ShadowExecutor(
            alpha=float(shadow_opts.get("alpha", 0.85)),
            latency_ms=int(shadow_opts.get("latency_ms", 120)),
            post_only_reject=bool(shadow_opts.get("post_only_reject", True)),
            taker_slippage_bps=float(shadow_opts.get("taker_slippage_bps", 0.0)),
        )

    async def create(self):
        self.client = await AsyncClient.create(self.api_key, self.api_secret, testnet=self.paper)
        self.bm = BinanceSocketManager(self.client)
        market = "prod" if not self.paper else "testnet"
        logger.info("Binance client created: market=%s (testnet=%s), shadow=%s", market, self.paper, self.shadow_enabled)
        if self.shadow_enabled:
            logger.info("SHADOW: эмуляция ордеров локально (alpha=%.2f, latency=%dms, post_only_reject=%s, taker_slip_bps=%.2f).",
                        self.shadow.alpha, self.shadow.latency, self.shadow.post_only_reject, self.shadow.taker_slippage_bps)
        else:
            logger.info("LIVE ORDERS: заявки будут отправляться на Binance %s.", market)
        return self

    # --- обратные вызовы для маркет-датчиков ---
    async def on_book_update(self, symbol, bids, asks):
        if self.shadow_enabled:
            await self.shadow.on_book_update(symbol, bids, asks)

    async def on_trade(self, symbol, price, qty, is_buyer_maker):
        if self.shadow_enabled:
            await self.shadow.on_trade(symbol, price, qty, is_buyer_maker)

    # --- торговые методы, совместимые с MarketMaker ---
    async def create_order(self, **kwargs):
        if self.shadow_enabled:
            return await self.shadow.create_order(**kwargs)
        # реальный вызов Binance
        return await self.client.create_order(**kwargs)

    async def get_order(self, **kwargs):
        if self.shadow_enabled:
            return await self.shadow.get_order(**kwargs)
        return await self.client.get_order(**kwargs)

    async def cancel_order(self, **kwargs):
        if self.shadow_enabled:
            return await self.shadow.cancel_order(**kwargs)
        return await self.client.cancel_order(**kwargs)

    async def close(self):
        try:
            if self.client:
                await self.client.close_connection()
        except Exception:
            pass
