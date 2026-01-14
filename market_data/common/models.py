from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Candle:
    symbol: str
    exchange: str
    timeframe: int  # minutes
    ts: datetime  # UTC
    open: float
    high: float
    low: float
    close: float
    volume: float
    atr: float = 0.0
    volatility_regime: int = 0

    def ensure_utc(self) -> None:
        if self.ts.tzinfo is None:
            self.ts = self.ts.replace(tzinfo=timezone.utc)
        else:
            self.ts = self.ts.astimezone(timezone.utc)


