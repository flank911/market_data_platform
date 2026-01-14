import logging
from datetime import datetime
from typing import List, Sequence, Set, Tuple

import clickhouse_connect

from market_data.common.models import Candle

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        secure: bool = False,
    ) -> None:
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=secure,
        )

    def create_tables(self, ddl_sql: str) -> None:
        logger.info("Ensuring ClickHouse tables exist")
        self.client.command(ddl_sql)
        logger.info("DDL executed")

    def _rows_from_candles(self, candles: Sequence[Candle]) -> List[Tuple]:
        rows: List[Tuple] = []
        for c in candles:
            rows.append(
                (
                    c.symbol,
                    c.exchange,
                    c.timeframe,
                    int(c.ts.timestamp()),
                    float(c.open),
                    float(c.high),
                    float(c.low),
                    float(c.close),
                    float(c.volume),
                    float(c.atr),
                    int(c.volatility_regime),
                )
            )
        return rows

    def select_existing_timestamps(
        self,
        symbol: str,
        timeframe: int,
        ts_from: datetime,
        ts_to: datetime,
    ) -> Set[int]:
        query = """
            SELECT toUnixTimestamp(ts) AS uts
            FROM market_candles
            WHERE symbol = {symbol:String}
              AND timeframe = {timeframe:UInt16}
              AND ts >= {from:DateTime}
              AND ts <= {to:DateTime}
        """
        params = {
            "symbol": symbol,
            "timeframe": timeframe,
            "from": ts_from,
            "to": ts_to,
        }
        result = self.client.query(query, parameters=params)
        existing: Set[int] = set()
        for row in result.result_rows:
            existing.add(int(row[0]))
        return existing

    def insert_candles_idempotent(self, candles: Sequence[Candle], chunk_size: int = 10000) -> int:
        if not candles:
            return 0
        inserted_total = 0
        start_ts = min(c.ts for c in candles)
        end_ts = max(c.ts for c in candles)
        existing = self.select_existing_timestamps(
            symbol=candles[0].symbol,
            timeframe=candles[0].timeframe,
            ts_from=start_ts,
            ts_to=end_ts,
        )
        filtered: List[Candle] = [c for c in candles if int(c.ts.timestamp()) not in existing]
        if not filtered:
            logger.info("All %d candles already present; skipping insert", len(candles))
            return 0
        for i in range(0, len(filtered), chunk_size):
            chunk = filtered[i : i + chunk_size]
            rows = self._rows_from_candles(chunk)
            self.client.insert(
                "market_candles",
                rows,
                column_names=[
                    "symbol",
                    "exchange",
                    "timeframe",
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "atr",
                    "volatility_regime",
                ],
                settings={"async_insert": 0, "wait_for_async_insert": 1},
            )
            inserted_total += len(chunk)
        logger.info("Inserted %d new candles (filtered from %d)", inserted_total, len(candles))
        return inserted_total

    def select_candles(
        self,
        symbol: str,
        timeframe: int,
        ts_from: datetime,
        ts_to: datetime,
        include_features: bool,
    ) -> List[Tuple]:
        base_cols = [
            "symbol",
            "exchange",
            "timeframe",
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        feature_cols = ["atr", "volatility_regime"]
        cols = base_cols + (feature_cols if include_features else [])
        query = f"""
            SELECT {", ".join(cols)}
            FROM market_candles
            WHERE symbol = {{symbol:String}}
              AND timeframe = {{timeframe:UInt16}}
              AND ts >= {{from:DateTime}}
              AND ts <= {{to:DateTime}}
            ORDER BY ts ASC
        """
        params = {"symbol": symbol, "timeframe": timeframe, "from": ts_from, "to": ts_to}
        result = self.client.query(query, parameters=params)
        return result.result_rows


