import pytest
from binance.enums import ORDER_TYPE_LIMIT_MAKER, ORDER_TYPE_MARKET

from market_maker import MarketMaker


class DummyClient:
    def __init__(self):
        self.orders = []

    async def create_order(self, **kwargs):
        self.orders.append(kwargs)
        return {
            'orderId': len(self.orders),
            'status': 'NEW',
            'executedQty': '0',
            'cummulativeQuoteQty': '0',
        }

    async def cancel_order(self, *args, **kwargs):
        pass


@pytest.mark.asyncio
async def test_orders_post_only_by_default():
    cfg = {'strategy': {'symbol': 'BTCUSDT', 'allow_short': True}, 'econ': {'enforce_post_only': True}}
    mm = MarketMaker(cfg, DummyClient())
    mm.step_size = 0.001
    mm.tick_size = 0.01
    mm.min_qty = 0.0
    mm.min_notional = None

    await mm._on_market(100.0, 101.0, source="TEST")

    types = [o['type'] for o in mm.client_wrap.orders]
    assert types == [ORDER_TYPE_LIMIT_MAKER, ORDER_TYPE_LIMIT_MAKER]


@pytest.mark.asyncio
async def test_aggressive_uses_market_orders():
    cfg = {
        'strategy': {'symbol': 'BTCUSDT', 'allow_short': True, 'aggressive': True, 'aggressive_bps': 5.0},
        'econ': {'enforce_post_only': True, 'min_net_pct': -100},
    }
    mm = MarketMaker(cfg, DummyClient())
    mm.step_size = 0.001
    mm.tick_size = 0.01
    mm.min_qty = 0.0
    mm.min_notional = None

    bid, ask = 100.0, 101.0
    buy_p, sell_p, *_ = mm._compute_prices(bid, ask)
    assert buy_p > ask
    assert sell_p < bid

    await mm._on_market(bid, ask, source="TEST")

    types = [o['type'] for o in mm.client_wrap.orders]
    assert types == [ORDER_TYPE_MARKET, ORDER_TYPE_MARKET]
