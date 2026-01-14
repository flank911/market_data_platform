from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict


@dataclass(frozen=True)
class DatasetMeta:
    dataset_id: str
    symbol: str
    timeframe: int
    date_from: datetime
    date_to: datetime
    feature_set: Dict[str, object]  # name -> version/params
    checksum: str
    created_at: datetime


