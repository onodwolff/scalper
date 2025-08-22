# pair_scanner.py
import asyncio
import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import math

from binance import AsyncClient

log = logging.getLogger(__name__)

__all__ = ["scan_best_symbol", "PairScore", "PairScanner"]

@dataclass
class PairScore:
    symbol: str
    bid: float
    ask: float
    spread_bps: float
    vol_usdt_24h: float
    vol_bps_1m: float
    ok_volume: bool
    ok_notional: bool
    tick: float
    step: float
    min_notional: float
    score: float

# ---- helpers ----
def _bps(x: float) -> float:
    return x * 10_000.0

async def _get_24h(client: AsyncClient, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        return await client.get_ticker(symbol=symbol)  # 24h stats
    except Exception as e:
        log.debug("24h fail %s: %s", symbol, e)
        return None

async def _get_klines_vol_bps(client: AsyncClient, symbol: str, bars: int) -> float:
    """
    Грубая «шумовая» волатильность в bps по 1m: среднее(|Δmid|/mid_prev).
    """
    if bars <= 1:
        return 0.0
    try:
        ks = await client.get_klines(symbol=symbol, interval="1m", limit=bars)
        mids: List[float] = []
        for ohlc in ks:
            h, l, c = float(ohlc[2]), float(ohlc[3]), float(ohlc[4])
            mids.append(0.5 * (h + l) if h > 0 and l > 0 else c)
        if len(mids) < 2:
            return 0.0
        acc = 0.0
        for i in range(1, len(mids)):
            prev, cur = mids[i - 1], mids[i]
            if prev > 0:
                acc += abs(cur - prev) / prev
        return _bps(acc / max(1, len(mids) - 1))
    except Exception as e:
        log.debug("klines fail %s: %s", symbol, e)
        return 0.0

async def _gather_limited(coros, limit: int = 20):
    sem = asyncio.Semaphore(limit)
    async def wrap(coro):
        async with sem:
            return await coro
    return await asyncio.gather(*[wrap(c) for c in coros], return_exceptions=True)

def _print_table(scored: List[PairScore], top: int):
    from rich.console import Console
    from rich.table import Table
    from rich import box

    con = Console()
    tbl = Table(
        title="SCANNER: USDT-пары (спред / волатильность / объём)",
        box=box.SIMPLE_HEAVY,
    )
    for col in ["#", "Symbol", "Spread bps", "Vol(1m) bps", "24h Quote Vol", "Tick", "Step", "MinNotional", "OK", "Score"]:
        justify = "right" if col in {"#", "Spread bps", "Vol(1m) bps", "24h Quote Vol", "Tick", "Step", "MinNotional", "Score"} else "left"
        tbl.add_column(col, justify=justify)

    for i, r in enumerate(scored[:top], 1):
        ok = "YES" if (r.ok_volume and r.ok_notional) else "NO"
        tbl.add_row(
            str(i),
            r.symbol,
            f"{r.spread_bps:.2f}",
            f"{r.vol_bps_1m:.2f}",
            f"{r.vol_usdt_24h:.0f}",
            f"{r.tick:.8f}",
            f"{r.step:.8f}",
            f"{r.min_notional:.2f}",
            ok,
            f"{r.score:.5f}",
        )
    con.print(tbl)

# ---------- основной алгоритм сканера ----------
async def _scan_impl(cfg: Dict[str, Any], client: AsyncClient) -> Dict[str, Any]:
    """Сканирует спот-пары и возвращает лучшую по скору и топ-список."""
    sc = cfg.get("scanner", {})
    quote = sc.get("quote", "USDT")
    min_price = float(sc.get("min_price", 0.0001))
    min_vol_usdt = float(sc.get("min_vol_usdt_24h", 3_000_000))
    min_notional_limit = float(sc.get("min_notional", 5.0))
    min_spread_bps = float(sc.get("min_spread_bps", 5.0))
    top_by_spread = int(sc.get("top_by_spread", 25))
    vol_minutes = int(sc.get("vol_minutes", 0))
    check_volume = bool(sc.get("check_volume_24h", True))
    check_notional = bool(sc.get("check_min_notional", True))
    w_spread = float(sc.get("score", {}).get("w_spread", 1.0))
    w_vol = float(sc.get("score", {}).get("w_vol", 0.3))
    w_volume_bonus = float(sc.get("score", {}).get("w_volume_bonus", 0.0))
    whitelist = sc.get("whitelist") or []
    blacklist = set(sc.get("blacklist") or [])

    # 1) exchangeInfo -> кандидаты по котируемой валюте и мета
    ex = await client.get_exchange_info()
    symbols = []
    meta: Dict[str, Dict[str, float]] = {}
    for s in ex.get("symbols", []):
        try:
            if s.get("status") != "TRADING":
                continue
            if s.get("quoteAsset") != quote:
                continue
            sym = s.get("symbol")
            if whitelist and sym not in whitelist:
                continue
            if sym in blacklist:
                continue
            if s.get("isSpotTradingAllowed") is False:
                continue
            lot = next((f for f in s.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
            pricef = next((f for f in s.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None)
            notf = next((f for f in s.get("filters", []) if f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL")), None)
            meta[sym] = {
                "step": float(lot.get("stepSize", 0.0)) if lot else 0.0,
                "tick": float(pricef.get("tickSize", 0.0)) if pricef else 0.0,
                "min_notional": float((notf or {}).get("minNotional") or (notf or {}).get("notional") or 0.0),
            }
            symbols.append(sym)
        except Exception:
            continue

    if not symbols:
        raise RuntimeError("Сканер: нет кандидатов (фильтры слишком строгие?)")

    # 2) bookTicker -> текущий спред для всех, затем топ-N по спреду
    try:
        books_all = await client.get_orderbook_ticker()
    except Exception as e:
        raise RuntimeError(f"Сканер: get_orderbook_ticker: {e}")

    spread_rows = []  # (spread_bps, sym, bid, ask)
    for b in books_all:
        sym = b.get("symbol")
        if sym not in symbols:
            continue
        try:
            bid = float(b.get("bidPrice") or 0.0)
            ask = float(b.get("askPrice") or 0.0)
            if bid <= 0 or ask <= 0 or ask <= bid:
                continue
            spr = _bps((ask - bid) / bid)
            if spr < min_spread_bps:
                continue
            spread_rows.append((spr, sym, bid, ask))
        except Exception:
            continue

    spread_rows.sort(reverse=True)
    top_syms = [sym for _, sym, _, _ in spread_rows[:max(5, top_by_spread)]]
    if not top_syms:
        raise RuntimeError("Сканер: нет пар с мгновенным спредом >= min_spread_bps")

    spread_map = {sym: (bid, ask, spr) for spr, sym, bid, ask in spread_rows if sym in top_syms}

    # 3) 24h статистика для топов
    t24s = await _gather_limited([_get_24h(client, s) for s in top_syms], limit=20)
    vols: Dict[str, float] = {}
    ok_volume_map: Dict[str, bool] = {}
    for s, t in zip(top_syms, t24s):
        if not isinstance(t, dict):
            continue
        try:
            qv = float(t.get("quoteVolume") or 0.0)
            lp = float(t.get("lastPrice") or 0.0)
            ok_volume = qv >= min_vol_usdt
            if lp >= min_price and ((not check_volume) or ok_volume):
                vols[s] = qv
                ok_volume_map[s] = ok_volume
        except Exception:
            pass

    if not vols:
        raise RuntimeError("Сканер: не нашли пары с подходящим lastPrice/объёмом")

    # 4) волатильность по 1m
    vol_bps_map = {s: 0.0 for s in vols.keys()}
    if vol_minutes > 1:
        vols2 = await _gather_limited([_get_klines_vol_bps(client, s, vol_minutes) for s in vols.keys()], limit=10)
        for s, vb in zip(vols.keys(), vols2):
            try:
                vol_bps_map[s] = float(vb) if vb and vb == vb else 0.0
            except Exception:
                vol_bps_map[s] = 0.0

    # 5) скоринг и таблица
    scored: List[PairScore] = []
    for s, qv in vols.items():
        bid, ask, spr_bps = spread_map.get(s, (0.0, 0.0, 0.0))
        vb = vol_bps_map.get(s, 0.0)
        m = meta.get(s, {})
        min_notional = float(m.get("min_notional", 0.0))
        ok_notional = min_notional <= min_notional_limit
        ok_volume = ok_volume_map.get(s, False)

        spread_term = w_spread * spr_bps
        vol_term = w_vol * vb
        volume_term = w_volume_bonus * math.log10(qv + 1.0)
        score = spread_term + vol_term + volume_term
        if check_volume and not ok_volume:
            score *= 0.5
        if check_notional and not ok_notional:
            score *= 0.5

        scored.append(PairScore(
            symbol=s,
            bid=bid,
            ask=ask,
            spread_bps=spr_bps,
            vol_usdt_24h=qv,
            vol_bps_1m=vb,
            ok_volume=ok_volume,
            ok_notional=ok_notional,
            tick=float(m.get("tick", 0.0)),
            step=float(m.get("step", 0.0)),
            min_notional=min_notional,
            score=score,
        ))

    scored.sort(key=lambda x: x.score, reverse=True)
    best = scored[0]
    log.info(
        "SCANNER: лучший %s | спред≈%.1f bps | вола≈%.1f bps | 24h≈%.0f USDT | score=%.2f",
        best.symbol,
        best.spread_bps,
        best.vol_bps_1m,
        best.vol_usdt_24h,
        best.score,
    )

    if sc.get("print_table", False):
        _print_table(scored, top=min(len(scored), top_by_spread))

    top_list = [x.__dict__ for x in scored[:10]]
    return {"best": best.__dict__, "top": top_list}

# --- публичный API в двух стилях ---

async def scan_best_symbol(cfg: Dict[str, Any], client: AsyncClient) -> Dict[str, Any]:
    """Функция-обёртка: совместима с `from pair_scanner import scan_best_symbol`."""
    return await _scan_impl(cfg, client)

class PairScanner:
    """Класс-обёртка: совместим с `from pair_scanner import PairScanner`."""
    def __init__(self, cfg: Dict[str, Any], client: AsyncClient):
        self.cfg = cfg
        self.client = client
    async def select_best(self) -> Dict[str, Any]:
        return await _scan_impl(self.cfg, self.client)

# самотест на прямой запуск
if __name__ == "__main__":
    print("pair_scanner OK; exports:", __all__)
