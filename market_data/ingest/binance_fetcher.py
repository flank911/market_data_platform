import asyncio
import logging
from typing import AsyncGenerator, Dict, List

import httpx

from market_data.config.settings import AppSettings

logger = logging.getLogger(__name__)


class BinanceFetcher:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.base_url = settings.binance_base_url.rstrip("/")
        self.futures_base_url = settings.binance_futures_base_url.rstrip("/")

    async def _request_with_backoff(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: Dict[str, str],
    ) -> List[List]:
        backoff = self.settings.backoff_initial_s
        while True:
            try:
                resp = await client.get(url, params=params, timeout=30.0)
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    logger.warning("Rate limited; sleeping for %.2fs", retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response: {data}")
                return data
            except (httpx.HTTPError, httpx.ReadTimeout) as e:
                logger.warning("HTTP error: %s; backing off %.2fs", str(e), backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.settings.backoff_max_s)

    async def fetch_klines_paginated(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
        min_sleep_s: float = 0.2,
    ) -> AsyncGenerator[List[List], None]:
        url = f"{self.base_url}/api/v3/klines"
        async with httpx.AsyncClient() as client:
            current = start_ms
            while current <= end_ms:
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": str(current),
                    "endTime": str(end_ms),
                    "limit": str(limit),
                }
                data = await self._request_with_backoff(client, url, params)
                if not data:
                    break
                yield data
                last_open_time = int(data[-1][0])
                if last_open_time == current:
                    current = last_open_time + 60_000
                else:
                    current = last_open_time + 60_000
                await asyncio.sleep(min_sleep_s)

    async def fetch_funding_rates(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> List[Dict[str, object]]:
        url = f"{self.futures_base_url}/fapi/v1/fundingRate"
        async with httpx.AsyncClient() as client:
            params = {
                "symbol": symbol,
                "startTime": str(start_ms),
                "endTime": str(end_ms),
                "limit": str(limit),
            }
            data = await self._request_with_backoff(client, url, params)
            return [
                {
                    "fundingRate": float(r["fundingRate"]),
                    "fundingTime": int(r["fundingTime"]),
                }
                for r in data
            ]

    async def fetch_open_interest_hist(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict[str, object]]:
        url = f"{self.futures_base_url}/futures/data/openInterestHist"
        async with httpx.AsyncClient() as client:
            params = {
                "symbol": symbol,
                "period": period,
                "startTime": str(start_ms),
                "endTime": str(end_ms),
                "limit": str(limit),
            }
            data = await self._request_with_backoff(client, url, params)
            return [
                {
                    "sumOpenInterest": float(r["sumOpenInterest"]),
                    "timestamp": int(r["timestamp"]),
                }
                for r in data
            ]

    async def fetch_liquidations(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> List[Dict[str, object]]:
        url = f"{self.futures_base_url}/fapi/v1/allForceOrders"
        async with httpx.AsyncClient() as client:
            params = {
                "symbol": symbol,
                "startTime": str(start_ms),
                "endTime": str(end_ms),
                "limit": str(limit),
            }
            data = await self._request_with_backoff(client, url, params)
            out: List[Dict[str, object]] = []
            for r in data:
                price = float(r.get("price", 0.0))
                qty = float(r.get("origQty", r.get("quantity", 0.0)))
                out.append(
                    {
                        "time": int(r["time"]),
                        "price": price,
                        "qty": qty,
                        "side": r.get("side", ""),
                        "notional": price * qty,
                    }
                )
            return out



