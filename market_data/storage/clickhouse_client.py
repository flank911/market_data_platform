import logging
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Set, Tuple

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
        statements = [s.strip() for s in ddl_sql.split(";") if s.strip()]
        for stmt in statements:
            self.client.command(stmt)
        logger.info("DDL executed")

    # =========================
    # Dataset metadata methods
    # =========================
    def dataset_exists(self, dataset_id: str) -> bool:
        query = """
            SELECT count() 
            FROM datasets 
            WHERE dataset_id = {dataset_id:String}
        """
        res = self.client.query(query, parameters={"dataset_id": dataset_id})
        return int(res.result_rows[0][0]) > 0

    def insert_dataset_metadata(
        self,
        *,
        dataset_id: str,
        symbol: str,
        timeframe: int,
        date_from: datetime,
        date_to: datetime,
        feature_set_serialized: str,
        checksum: str,
        created_at: datetime,
    ) -> None:
        if self.dataset_exists(dataset_id):
            raise ValueError(f"Dataset already exists: {dataset_id}")
        self.client.insert(
            "datasets",
            [
                (
                    dataset_id,
                    symbol,
                    int(timeframe),
                    date_from,
                    date_to,
                    feature_set_serialized,
                    checksum,
                    created_at,
                )
            ],
            column_names=[
                "dataset_id",
                "symbol",
                "timeframe",
                "date_from",
                "date_to",
                "feature_set",
                "checksum",
                "created_at",
            ],
            settings={"async_insert": 0, "wait_for_async_insert": 1},
        )

    def list_datasets(self, *, symbol: Optional[str] = None, limit: int = 1000) -> List[Dict[str, object]]:
        where = ""
        params: Dict[str, object] = {}
        if symbol:
            where = "WHERE symbol = {symbol:String}"
            params["symbol"] = symbol
        query = f"""
            SELECT
              dataset_id, symbol, timeframe, date_from, date_to, feature_set, checksum, created_at
            FROM datasets
            {where}
            ORDER BY created_at DESC
            LIMIT {{limit:Int32}}
        """
        params["limit"] = int(limit)
        res = self.client.query(query, parameters=params)
        out: List[Dict[str, object]] = []
        for r in res.result_rows:
            out.append(
                {
                    "dataset_id": r[0],
                    "symbol": r[1],
                    "timeframe": r[2],
                    "date_from": r[3],
                    "date_to": r[4],
                    "feature_set": r[5],
                    "checksum": r[6],
                    "created_at": r[7],
                }
            )
        return out

    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, object]]:
        query = """
            SELECT
              dataset_id, symbol, timeframe, date_from, date_to, feature_set, checksum, created_at
            FROM datasets
            WHERE dataset_id = {dataset_id:String}
            LIMIT 1
        """
        res = self.client.query(query, parameters={"dataset_id": dataset_id})
        if not res.result_rows:
            return None
        r = res.result_rows[0]
        return {
            "dataset_id": r[0],
            "symbol": r[1],
            "timeframe": r[2],
            "date_from": r[3],
            "date_to": r[4],
            "feature_set": r[5],
            "checksum": r[6],
            "created_at": r[7],
        }

    def insert_dataset_lineage(
        self,
        *,
        dataset_id: str,
        source_type: str,
        source_id: str,
        created_at: datetime,
    ) -> None:
        self.client.insert(
            "dataset_lineage",
            [(dataset_id, source_type, source_id, created_at)],
            column_names=["dataset_id", "source_type", "source_id", "created_at"],
            settings={"async_insert": 0, "wait_for_async_insert": 1},
        )

    def list_dataset_lineage(self, dataset_id: str) -> List[Dict[str, object]]:
        query = """
            SELECT dataset_id, source_type, source_id, created_at
            FROM dataset_lineage
            WHERE dataset_id = {dataset_id:String}
            ORDER BY created_at ASC
        """
        res = self.client.query(query, parameters={"dataset_id": dataset_id})
        return [
            {"dataset_id": r[0], "source_type": r[1], "source_id": r[2], "created_at": r[3]}
            for r in res.result_rows
        ]

    def insert_regime_model(
        self,
        *,
        model_id: str,
        dataset_id: str,
        model_type: str,
        features_serialized: str,
        params_serialized: str,
        checksum: str,
        created_at: datetime,
    ) -> None:
        self.client.insert(
            "regime_models",
            [
                (
                    model_id,
                    dataset_id,
                    model_type,
                    features_serialized,
                    params_serialized,
                    checksum,
                    created_at,
                )
            ],
            column_names=[
                "model_id",
                "dataset_id",
                "model_type",
                "features",
                "params",
                "checksum",
                "created_at",
            ],
            settings={"async_insert": 0, "wait_for_async_insert": 1},
        )

    def list_regime_models(self, *, dataset_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, object]]:
        where = ""
        params: Dict[str, object] = {}
        if dataset_id:
            where = "WHERE dataset_id = {dataset_id:String}"
            params["dataset_id"] = dataset_id
        query = f"""
            SELECT model_id, dataset_id, model_type, features, params, checksum, created_at
            FROM regime_models
            {where}
            ORDER BY created_at DESC
            LIMIT {{limit:Int32}}
        """
        params["limit"] = int(limit)
        res = self.client.query(query, parameters=params)
        return [
            {
                "model_id": r[0],
                "dataset_id": r[1],
                "model_type": r[2],
                "features": r[3],
                "params": r[4],
                "checksum": r[5],
                "created_at": r[6],
            }
            for r in res.result_rows
        ]

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

    def insert_funding_rates(self, rows: Sequence[Tuple], *, chunk_size: int = 10000) -> int:
        if not rows:
            return 0
        inserted = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            self.client.insert(
                "funding_rates",
                chunk,
                column_names=["symbol", "exchange", "timeframe", "ts", "funding_rate"],
                settings={"async_insert": 0, "wait_for_async_insert": 1},
            )
            inserted += len(chunk)
        return inserted

    def insert_open_interest(self, rows: Sequence[Tuple], *, chunk_size: int = 10000) -> int:
        if not rows:
            return 0
        inserted = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            self.client.insert(
                "open_interest",
                chunk,
                column_names=["symbol", "exchange", "timeframe", "ts", "open_interest"],
                settings={"async_insert": 0, "wait_for_async_insert": 1},
            )
            inserted += len(chunk)
        return inserted

    def insert_liquidations(self, rows: Sequence[Tuple], *, chunk_size: int = 10000) -> int:
        if not rows:
            return 0
        inserted = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            self.client.insert(
                "liquidations",
                chunk,
                column_names=[
                    "symbol",
                    "exchange",
                    "timeframe",
                    "ts",
                    "liquidation_qty",
                    "liquidation_notional",
                    "liquidation_count",
                ],
                settings={"async_insert": 0, "wait_for_async_insert": 1},
            )
            inserted += len(chunk)
        return inserted

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

    def select_ohlcv(
        self,
        *,
        symbol: str,
        timeframe: int,
        ts_from: datetime,
        ts_to: datetime,
    ) -> List[Tuple]:
        query = """
            SELECT
              ts, open, high, low, close, volume
            FROM market_candles
            WHERE symbol = {symbol:String}
              AND timeframe = {timeframe:UInt16}
              AND ts >= {from:DateTime}
              AND ts <= {to:DateTime}
            ORDER BY ts ASC
        """
        params = {"symbol": symbol, "timeframe": timeframe, "from": ts_from, "to": ts_to}
        res = self.client.query(query, parameters=params)
        return res.result_rows

    def select_funding_rates(
        self,
        *,
        symbol: str,
        timeframe: int,
        ts_from: datetime,
        ts_to: datetime,
    ) -> List[Tuple]:
        query = """
            SELECT ts, funding_rate
            FROM funding_rates
            WHERE symbol = {symbol:String}
              AND timeframe = {timeframe:UInt16}
              AND ts >= {from:DateTime}
              AND ts <= {to:DateTime}
            ORDER BY ts ASC
        """
        params = {"symbol": symbol, "timeframe": timeframe, "from": ts_from, "to": ts_to}
        res = self.client.query(query, parameters=params)
        return res.result_rows

    def select_open_interest(
        self,
        *,
        symbol: str,
        timeframe: int,
        ts_from: datetime,
        ts_to: datetime,
    ) -> List[Tuple]:
        query = """
            SELECT ts, open_interest
            FROM open_interest
            WHERE symbol = {symbol:String}
              AND timeframe = {timeframe:UInt16}
              AND ts >= {from:DateTime}
              AND ts <= {to:DateTime}
            ORDER BY ts ASC
        """
        params = {"symbol": symbol, "timeframe": timeframe, "from": ts_from, "to": ts_to}
        res = self.client.query(query, parameters=params)
        return res.result_rows

    def select_liquidations(
        self,
        *,
        symbol: str,
        timeframe: int,
        ts_from: datetime,
        ts_to: datetime,
    ) -> List[Tuple]:
        query = """
            SELECT ts, liquidation_qty, liquidation_notional, liquidation_count
            FROM liquidations
            WHERE symbol = {symbol:String}
              AND timeframe = {timeframe:UInt16}
              AND ts >= {from:DateTime}
              AND ts <= {to:DateTime}
            ORDER BY ts ASC
        """
        params = {"symbol": symbol, "timeframe": timeframe, "from": ts_from, "to": ts_to}
        res = self.client.query(query, parameters=params)
        return res.result_rows


