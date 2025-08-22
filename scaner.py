# pair_scanner.py
"""
Сканер пар (USDT): выбирает кандидатов для MM по критериям:
  • Текущий спред (по /api/v3/ticker/bookTicker)
  • Волатильность за ~120 минут на 1m-клинах (стд. отклонение лог-доходностей)
  • 24h объём (по /api/v3/ticker/24hr)
Выводит таблицу и лучшего кандидата по интегральному скору.

Документация эндпоинтов Binance:
  - bookTicker: лучшая bid/ask для всех символов. GET /api/v3/ticker/bookTicker
  - 24hr ticker: суточная статистика, используем quoteVolume. GET /api/v3/ticker/24hr
  - klines: исторические свечи для волатильности. GET /api/v3/klines
(см. оф. доки) 
"""
import asyncio
import math
from statistics import pstdev
from decimal import Decimal
from typing import Dict, List, Tuple

from binance import AsyncClient

# ---- параметры сканера ----
QUOTE = "USDT"
TOP_BY_SPREAD = 25        # сначала берём топ по спреду (снизит нагрузку на klines)
KLINES_LIMIT = 120        # минут на волатильность
MIN_NOTIONAL = 5.0        # sanity-check для спота
MIN_VOLUME_USDT = 100000  # отсечь совсем дохлые пары
MIN_SPREAD_BPS = 1.0      # минимальный интерес (в б.п.) для первичного фильтра (0.01% = 1 б.п.)

def pct(a: float) -> float:
    return float(a) * 100.0

def logret(p0: float, p1: float) -> float:
    if p0 <= 0 or p1 <= 0: return 0.0
    return math.log(p1 / p0)

async def get_book_tickers(client: AsyncClient) -> Dict[str, Tuple[float, float]]:
    """Вернёт {symbol: (bid, ask)} для всех спот-символов."""
    books = await client.get_orderbook_ticker()
    res = {}
    for b in books:
        sym = b.get("symbol")
        if not sym:
            continue
        try:
            bid = float(b["bidPrice"]); ask = float(b["askPrice"])
            if bid > 0 and ask > 0:
                res[sym] = (bid, ask)
        except Exception:
            continue
    return res

async def get_24h_map(client: AsyncClient, symbols: List[str]) -> Dict[str, Dict]:
    """24h статистика только по нужным символам (экономим квоту)."""
    out: Dict[str, Dict] = {}
    # запросы последовательно, чтобы не улететь в weight 40 если без symbol
    for sym in symbols:
        try:
            t = await client.get_ticker(symbol=sym)
            out[sym] = t
        except Exception:
            pass
    return out

async def get_klines_vol(client: AsyncClient, symbol: str, limit: int = 120) -> float:
    """Оценка волатильности: std лог-доходностей 1m, в процентах (за период), НЕ годовая."""
    try:
        kl = await client.get_klines(symbol=symbol, interval=AsyncClient.KLINE_INTERVAL_1MINUTE, limit=limit)
        closes = [float(k[4]) for k in kl]
        if len(closes) < 3:
            return 0.0
        rets = [logret(closes[i-1], closes[i]) for i in range(1, len(closes))]
        sigma = pstdev(rets)  # pop std
        return sigma * 100.0  # в %
    except Exception:
        return 0.0

async def scan(min_spread_bps: float = MIN_SPREAD_BPS, top: int = TOP_BY_SPREAD):
    client = await AsyncClient.create()
    try:
        ex = await client.get_exchange_info()
        spot_syms = [s for s in ex["symbols"] if s.get("status") == "TRADING" and s.get("quoteAsset") == QUOTE]

        # фильтруем по базовым спотовым ограничениям
        meta = {}
        for s in spot_syms:
            sym = s["symbol"]
            lot = next((f for f in s["filters"] if f["filterType"] == "LOT_SIZE"), None)
            pricef = next((f for f in s["filters"] if f["filterType"] == "PRICE_FILTER"), None)
            notf = next((f for f in s["filters"] if f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL")), None)
            meta[sym] = {
                "step": float(lot["stepSize"]) if lot else 0.0,
                "tick": float(pricef["tickSize"]) if pricef else 0.0,
                "min_notional": float(notf.get("minNotional") or notf.get("notional") or 0.0) if notf else 0.0
            }

        # текущий спред (книга)
        books = await get_book_tickers(client)
        rows = []
        for sym, (bid, ask) in books.items():
            if not sym.endswith(QUOTE):
                continue
            spread_pct = (ask - bid) / bid * 100.0
            if spread_pct * 100.0 < min_spread_bps:  # bps-порог
                continue
            rows.append((spread_pct, sym, bid, ask))
        rows.sort(reverse=True)  # по спреду
        top_syms = [sym for _, sym, _, _ in rows[:max(5, top)]]

        # 24h объём только по топам (экономим квоту)
        tmap = await get_24h_map(client, top_syms)
        # волатильность (параллельно, но с ограничением)
        sem = asyncio.Semaphore(10)
        vol_map: Dict[str, float] = {}
        async def vol_job(sy):
            async with sem:
                vol_map[sy] = await get_klines_vol(client, sy, limit=KLINES_LIMIT)
        await asyncio.gather(*(vol_job(s) for s in top_syms))

        # итоговая таблица
        out = []
        for sp, sym, bid, ask in rows[:max(5, top)]:
            tm = tmap.get(sym, {})
            qvol = float(tm.get("quoteVolume") or 0.0)
            vol = vol_map.get(sym, 0.0)
            m = meta.get(sym, {})
            ok_notional = (m.get("min_notional") or 0.0) <= max(MIN_NOTIONAL, 5.0)
            ok_volume = qvol >= MIN_VOLUME_USDT
            score = sp * (1.0 + vol/10.0) * (1.0 if ok_volume else 0.5)  # простая эвристика
            out.append({
                "symbol": sym,
                "spread_pct": sp,
                "vol_1m_pct": vol,
                "qvol_24h": qvol,
                "tick": m.get("tick", 0.0),
                "step": m.get("step", 0.0),
                "min_notional": m.get("min_notional", 0.0),
                "ok_volume": ok_volume,
                "ok_notional": ok_notional,
                "score": score
            })

        out.sort(key=lambda r: r["score"], reverse=True)

        # печать
        from rich.console import Console
        from rich.table import Table
        from rich import box
        con = Console()
        tbl = Table(title="SCANNER: USDT-пары (спред / волатильность / объём)", box=box.SIMPLE_HEAVY)
        for col in ["#", "Symbol", "Spread %", "Vol(1m) %", "24h Quote Vol", "Tick", "Step", "MinNotional", "OK", "Score"]:
            tbl.add_column(col, justify="right" if col in {"#", "Spread %", "Vol(1m) %", "24h Quote Vol", "Tick", "Step", "MinNotional", "Score"} else "left")

        for i, r in enumerate(out[:top], 1):
            ok = "YES" if (r["ok_volume"] and r["ok_notional"]) else "NO"
            tbl.add_row(
                str(i),
                r["symbol"],
                f"{r['spread_pct']:.4f}",
                f"{r['vol_1m_pct']:.3f}",
                f"{r['qvol_24h']:.0f}",
                f"{r['tick']:.8f}",
                f"{r['step']:.8f}",
                f"{r['min_notional']:.2f}",
                ok,
                f"{r['score']:.5f}"
            )
        con.print(tbl)

        if out:
            best = out[0]
            con.print(f"[bold green]BEST:[/bold green] {best['symbol']}  "
                      f"(spread≈{best['spread_pct']:.4f}%, vol1m≈{best['vol_1m_pct']:.3f}%, "
                      f"24hQVol≈{best['qvol_24h']:.0f}, tick={best['tick']:.8f})")
        else:
            con.print("[yellow]Не найдено подходящих пар по текущим критериям.[/yellow]")

    finally:
        await client.close_connection()

if __name__ == "__main__":
    asyncio.run(scan())
