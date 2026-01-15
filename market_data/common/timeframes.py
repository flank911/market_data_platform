from typing import Dict

TF_TO_MINUTES: Dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

def tf_to_minutes(tf: str) -> int:
    if tf not in TF_TO_MINUTES:
        raise ValueError(f"Unsupported timeframe: {tf}")
    return TF_TO_MINUTES[tf]




