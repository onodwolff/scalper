import os
import sys

import asyncio
import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from pair_scanner import PairScanner

class DummyClient:
    async def get_exchange_info(self):
        return {"symbols": [{"symbol": "AAAUSDT", "status": "TRADING", "quoteAsset": "USDT", "isSpotTradingAllowed": True}]}

    async def get_ticker(self, symbol: str):
        assert symbol == "AAAUSDT"
        return {"quoteVolume": "1000", "lastPrice": "1"}

    async def get_orderbook_ticker(self, symbol: str):
        assert symbol == "AAAUSDT"
        return {"bidPrice": "1", "askPrice": "1.001"}

    async def get_klines(self, symbol: str, interval: str, limit: int):
        return []

def test_select_best_structure():
    cfg = {"scanner": {"min_vol_usdt_24h": 0}}
    scanner = PairScanner(cfg, DummyClient())
    result = asyncio.run(scanner.select_best())
    assert set(result.keys()) == {"best", "top"}
    assert isinstance(result["best"], dict)
    assert isinstance(result["top"], list)
