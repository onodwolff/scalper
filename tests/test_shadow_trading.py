import pytest

from binance_client import BinanceAsync


@pytest.mark.asyncio
async def test_shadow_executor_behaviour():
    client = BinanceAsync(
        "key",
        "secret",
        shadow=True,
        shadow_opts={
            "alpha": 0.5,
            "latency_ms": 0,
            "market_slippage_bps": 100,
            "market_latency_ms": 0,
        },
    )

    symbol = "TESTSYM"
    await client.on_book_update(symbol, [(99, 1)], [(101, 1)])

    # post-only rejection on cross
    rej = await client.create_order(
        symbol=symbol,
        side="BUY",
        type="LIMIT_MAKER",
        timeInForce="GTC",
        quantity=1,
        price=101,
    )
    assert rej["status"] == "REJECTED"

    # regular limit order
    limit = await client.create_order(
        symbol=symbol,
        side="BUY",
        type="LIMIT",
        timeInForce="GTC",
        quantity=10,
        price=100,
    )
    oid = limit["orderId"]
    assert limit["status"] == "NEW"

    # partial fill
    await client.on_trade(symbol, 100, 8, False)
    o = await client.get_order(symbol=symbol, orderId=oid)
    assert o["status"] == "PARTIALLY_FILLED"
    assert o["executedQty"] == pytest.approx(4)

    # final fill
    await client.on_trade(symbol, 100, 20, True)
    o = await client.get_order(symbol=symbol, orderId=oid)
    assert o["status"] == "FILLED"
    assert o["executedQty"] == pytest.approx(10)

    # market order with slippage
    m = await client.create_order(symbol=symbol, side="BUY", type="MARKET", quantity=2)
    assert m["status"] == "FILLED"
    assert m["liquidity"] == "TAKER"
    expected_px = 101 * 1.01
    assert m["cummulativeQuoteQty"] == pytest.approx(expected_px * 2)


@pytest.mark.asyncio
async def test_shadow_flag_routes_to_binance():
    class Dummy:
        def __init__(self):
            self.called = False

        async def create_order(self, **kwargs):
            self.called = True
            return {"status": "LIVE"}

    client = BinanceAsync("key", "secret", shadow=False)
    dummy = Dummy()
    client.client = dummy

    async def fail(**kwargs):  # should not be used
        raise AssertionError("shadow executor called")

    client.shadow.create_order = fail  # type: ignore
    res = await client.create_order(symbol="S", side="BUY", type="MARKET", quantity=1)
    assert dummy.called and res["status"] == "LIVE"

