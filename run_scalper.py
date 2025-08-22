# run_scalper.py
import asyncio
import os
import yaml
import logging
from logging.handlers import RotatingFileHandler

from binance_client import BinanceAsync
from market_maker import MarketMaker
from pair_scanner import PairScanner  # <-- добавили
from risk import RiskManager

# === ЛОГГИРОВАНИЕ ===
def setup_logging(log_path: str = "bot.log", level=logging.INFO, to_console: bool = True):
    root = logging.getLogger()
    root.setLevel(level)
    # очистим прежние хендлеры
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    # файл с ротацией
    fh = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    if to_console:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        root.addHandler(ch)

    return root


def load_cfg():
    with open("config.yaml", "r", encoding="utf-8-sig") as f:
        return yaml.safe_load(f)


async def main():
    cfg = load_cfg()
    ui_cfg = cfg.get("ui", {})
    ui_enabled = bool(ui_cfg.get("enabled", True))

    # Если TUI включён — не льём сырые логи в консоль (только файл + панель «События»)
    setup_logging(to_console=not ui_enabled)
    logger = logging.getLogger(__name__)

    api = cfg.get("api", {})
    api_key = os.getenv("BINANCE_API_KEY") or api.get('key')
    api_secret = os.getenv("BINANCE_API_SECRET") or api.get('secret')
    paper = api.get('paper', True)

    shadow_cfg = cfg.get("shadow", {}) or {}
    shadow_enabled = bool(shadow_cfg.get("enabled", True))

    client_wrap = await BinanceAsync(
        api_key, api_secret,
        paper=paper,
        shadow=shadow_enabled,
        shadow_opts=shadow_cfg
    ).create()

    # === Автоскан пар (тумблером) ===
    scanner_cfg = cfg.get("scanner", {})
    if scanner_cfg.get("enabled", False):
        try:
            scanner = PairScanner(scanner_cfg, client_wrap.client)
            chosen = await scanner.select_best()
            if chosen and "best" in chosen:
                best = chosen["best"]
                sym = best.get("symbol")
                old = cfg.get("strategy", {}).get("symbol")
                cfg["strategy"]["symbol"] = sym
                logger.info(
                    "SCANNER: выбран символ %s (старый=%s). spread=%.2f bps, vol24h≈%.0f %s%s",
                    sym,
                    old,
                    best.get("spread_bps", 0.0),
                    best.get("vol_usdt_24h", 0.0),
                    scanner_cfg.get("quote", "USDT"),
                    f", vol_bps≈{best.get('vol_bps_1m', 0.0):.2f}" if best.get("vol_bps_1m") is not None else "",
                )
            else:
                logger.warning(
                    "SCANNER: не удалось выбрать символ — остаёмся на %s",
                    cfg.get("strategy", {}).get("symbol"),
                )
        except Exception as e:
            logger.warning(
                "SCANNER: ошибка выбора пары: %s — остаёмся на %s",
                e,
                cfg.get("strategy", {}).get("symbol"),
            )

    # === Risk controls ===
    risk_cfg = cfg.get("risk", {})
    risk_mgr = RiskManager(client_wrap)
    if risk_cfg.get("stop_to_usdt", False):
        await risk_mgr.panic_sell_all()
        exit_after_stop = bool(risk_cfg.get("exit_after_stop", True))
        logger.info(
            "risk.stop_to_usdt: all assets sold, %s",
            "exiting" if exit_after_stop else "continuing operation",
        )
        if exit_after_stop:
            await client_wrap.close()
            return

    # TUI
    tui = None
    try:
        if ui_enabled:
            from tui import TUI, TuiLogHandler
            tui = TUI(
                refresh_hz=int(ui_cfg.get('refresh_hz', 8)),
                alt_screen=bool(ui_cfg.get('alt_screen', True)),
                force_color=True,
                color_system="auto",
                log_rows=int(ui_cfg.get('log_rows', 300)),
                trades_rows=int(ui_cfg.get('trades_rows', 18)),
                max_open_orders=int(ui_cfg.get('max_open_orders', 80)),
            )
            th = TuiLogHandler(tui, level=logging.INFO)
            logging.getLogger().addHandler(th)
        else:
            logger.info("TUI выключена в конфиге — вывод только в консоль/файл.")
    except Exception as e:
        logger.warning("TUI недоступна (%s). Будут только обычные логи.", e)

    mm = MarketMaker(cfg, client_wrap, tui=tui)

    try:
        if tui:
            await asyncio.gather(mm.run(), tui.run())
        else:
            await mm.run()
    finally:
        await client_wrap.close()
        logging.getLogger(__name__).info("Остановка. Смотри подробные логи в файле bot.log")


if __name__ == "__main__":
    asyncio.run(main())
