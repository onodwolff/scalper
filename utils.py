# utils.py
from decimal import Decimal, ROUND_DOWN, ROUND_UP

def round_step(value: float, step: float, precision: int = 8) -> float:
    """Округлить вниз до ближайшего шага (lot/tick)."""
    v = Decimal(str(value))
    s = Decimal(str(step))
    q = (v // s) * s
    return float(q.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN))

def round_step_up(value: float, step: float, precision: int = 8) -> float:
    """Округлить вверх до ближайшего шага (lot/tick)."""
    v = Decimal(str(value))
    s = Decimal(str(step))
    if s == 0:
        return float(v)
    if (v % s) == 0:
        q = v
    else:
        q = ((v // s) + 1) * s
    return float(q.quantize(Decimal(10) ** -precision, rounding=ROUND_UP))
