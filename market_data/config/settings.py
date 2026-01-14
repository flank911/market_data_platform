import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List


def getenv(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value


def getenv_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


def getenv_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def getenv_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "y", "on")


def parse_utc_datetime(value: str, fallback: datetime) -> datetime:
    try:
        if len(value) == 10:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return fallback


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool


@dataclass(frozen=True)
class FeatureThresholds:
    low_pct: float
    mid_pct: float


@dataclass(frozen=True)
class QualityThresholds:
    max_gap_pct: float
    max_nan_feature_pct: float


@dataclass(frozen=True)
class AppSettings:
    exchange: str
    symbol: str
    symbols: List[str]
    base_timeframe: str
    aggregate_timeframes: List[str]
    start_date_utc: datetime
    end_date_utc: datetime
    binance_base_url: str
    backoff_initial_s: float
    backoff_max_s: float
    feature_thresholds: FeatureThresholds
    quality_thresholds: QualityThresholds
    clickhouse: ClickHouseConfig


def load_settings() -> AppSettings:
    exchange = getenv("EXCHANGE", "binance")
    symbol = getenv("SYMBOL", "BTCUSDT")
    symbols_raw = getenv("SYMBOLS", "BTCUSDT,ETHUSDT")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    base_timeframe = getenv("BASE_TIMEFRAME", "1m")
    aggregate_timeframes = getenv("AGG_TIMEFRAMES", "5m,15m,1h,4h,1d").split(",")
    aggregate_timeframes = [tf.strip() for tf in aggregate_timeframes if tf.strip()]

    default_start = datetime.utcnow().replace(tzinfo=timezone.utc)
    start_date_utc = parse_utc_datetime(getenv("START_DATE_UTC", "2021-01-01"), default_start)
    end_date_utc = parse_utc_datetime(getenv("END_DATE_UTC", datetime.utcnow().strftime("%Y-%m-%d")), default_start)

    binance_base_url = getenv("BINANCE_BASE_URL", "https://api.binance.com")
    backoff_initial_s = getenv_float("BACKOFF_INITIAL_S", 0.5)
    backoff_max_s = getenv_float("BACKOFF_MAX_S", 8.0)

    feature_thresholds = FeatureThresholds(
        low_pct=getenv_float("VOL_LOW_ATR_PCT", 0.5),
        mid_pct=getenv_float("VOL_MID_ATR_PCT", 1.5),
    )

    quality_thresholds = QualityThresholds(
        max_gap_pct=getenv_float("QUALITY_MAX_GAP_PCT", 0.5),
        max_nan_feature_pct=getenv_float("QUALITY_MAX_NAN_FEATURE_PCT", 0.5),
    )

    ch = ClickHouseConfig(
        host=getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=getenv_int("CLICKHOUSE_PORT", 8123),
        username=getenv("CLICKHOUSE_USER", "default"),
        password=getenv("CLICKHOUSE_PASSWORD", ""),
        database=getenv("CLICKHOUSE_DB", "default"),
        secure=getenv_bool("CLICKHOUSE_SECURE", False),
    )

    return AppSettings(
        exchange=exchange,
        symbol=symbol,
        symbols=symbols,
        base_timeframe=base_timeframe,
        aggregate_timeframes=aggregate_timeframes,
        start_date_utc=start_date_utc,
        end_date_utc=end_date_utc,
        binance_base_url=binance_base_url,
        backoff_initial_s=backoff_initial_s,
        backoff_max_s=backoff_max_s,
        feature_thresholds=feature_thresholds,
        quality_thresholds=quality_thresholds,
        clickhouse=ch,
    )



