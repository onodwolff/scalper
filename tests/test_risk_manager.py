import pytest
from binance.enums import SIDE_SELL, ORDER_TYPE_MARKET

from risk import RiskManager


class DummyClient:
    def __init__(self):
        self.created = []
        self.client = self

    async def get_account(self):
        return {
            "balances": [
                {"asset": "BTC", "free": "1.5"},
                {"asset": "USDT", "free": "100"},
                {"asset": "ETH", "free": "0"},
            ]
        }

    async def create_order(self, **kwargs):
        self.created.append(kwargs)
        return {"status": "FILLED"}


@pytest.mark.asyncio
async def test_panic_sell_all_creates_market_orders():
    dummy = DummyClient()
    rm = RiskManager(dummy)
    await rm.panic_sell_all()
    assert len(dummy.created) == 1
    order = dummy.created[0]
    assert order["symbol"] == "BTCUSDT"
    assert order["side"] == SIDE_SELL
    assert order["type"] == ORDER_TYPE_MARKET
    assert order["quantity"] == 1.5
