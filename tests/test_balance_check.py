import pytest
from binance.enums import SIDE_BUY, SIDE_SELL

from market_maker import MarketMaker


class DummyClient:
    def __init__(self, base, quote):
        self.base = base
        self.quote = quote

    async def check_balance(self, symbol, side, qty):
        return {"base": self.base, "quote": self.quote}


@pytest.mark.asyncio
async def test_buy_qty_reduced():
    cfg = {"strategy": {"symbol": "BTCUSDT"}, "balance_check": {"enabled": True}}
    mm = MarketMaker(cfg, DummyClient(base=0, quote=50))
    mm.step_size = 0.001
    mm.min_qty = 0.0
    qty = 0.02
    price = 10000
    new_qty = await mm._check_balance(SIDE_BUY, qty, price)
    assert new_qty == pytest.approx(0.005)


@pytest.mark.asyncio
async def test_sell_canceled_when_no_base():
    cfg = {"strategy": {"symbol": "BTCUSDT"}, "balance_check": {"enabled": True}}
    mm = MarketMaker(cfg, DummyClient(base=0, quote=0))
    mm.step_size = 0.001
    mm.min_qty = 0.0
    qty = 0.01
    price = 10000
    new_qty = await mm._check_balance(SIDE_SELL, qty, price)
    assert new_qty == 0
