import pytest

from binance_client import BinanceAsync


@pytest.mark.asyncio
async def test_shadow_executor_limit_and_market():
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

