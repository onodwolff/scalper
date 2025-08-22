# binance_client.py
import logging
from typing import Optional

from binance import AsyncClient, BinanceSocketManager

from shadow_executor import ShadowExecutor

logger = logging.getLogger(__name__)


class BinanceAsync:
    def __init__(self, api_key, api_secret, paper=True, shadow=True, shadow_opts: Optional[dict] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self.shadow_enabled = bool(shadow)
        self.client: Optional[AsyncClient] = None
        self.bm: Optional[BinanceSocketManager] = None

        shadow_opts = shadow_opts or {}
        opts = dict(shadow_opts)
        if "taker_slippage_bps" in opts and "market_slippage_bps" not in opts:
            opts["market_slippage_bps"] = opts.pop("taker_slippage_bps")
        self.shadow = ShadowExecutor(**opts)

    async def create(self):
        self.client = await AsyncClient.create(self.api_key, self.api_secret, testnet=self.paper)
        self.bm = BinanceSocketManager(self.client)
        market = "prod" if not self.paper else "testnet"
        logger.info(
            "Binance client created: market=%s (testnet=%s), shadow=%s",
            market,
            self.paper,
            self.shadow_enabled,
        )
        if self.shadow_enabled:
            cfg = self.shadow.cfg
            logger.info(
                "SHADOW: эмуляция ордеров локально (alpha=%.2f, latency=%dms, post_only_reject=%s, market_slip_bps=%.2f)",
                cfg.alpha,
                cfg.latency_ms,
                cfg.post_only_reject,
                cfg.market_slippage_bps,
            )
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

