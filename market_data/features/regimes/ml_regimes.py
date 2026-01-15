from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RegimeModelArtifact:
    model_id: str
    dataset_id: str
    model_type: str
    features: List[str]
    params: Dict[str, object]
    checksum: str
    created_at: datetime
    label_counts: Dict[str, int]
    payload: Dict[str, object]


def _sha256_json(payload: Dict[str, object]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _prepare_matrix(df: pd.DataFrame, features: List[str]) -> np.ndarray:
    mat = df[features].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return mat.to_numpy()


def train_regime_model_v1(
    df: pd.DataFrame,
    *,
    dataset_id: str,
    model_type: str,
    features: List[str],
    params: Dict[str, object],
) -> Tuple[pd.Series, RegimeModelArtifact]:
    created_at = datetime.utcnow().replace(tzinfo=timezone.utc)
    model_type_norm = model_type.lower()
    X = _prepare_matrix(df, features)
    labels: np.ndarray
    payload: Dict[str, object] = {"model_type": model_type_norm, "features": features, "params": params}

    if model_type_norm == "kmeans":
        from sklearn.cluster import KMeans

        n_clusters = int(params.get("n_clusters", 4))
        random_state = int(params.get("random_state", 42))
        model = KMeans(n_clusters=n_clusters, random_state=random_state, n_init="auto")
        labels = model.fit_predict(X)
        payload["centroids"] = model.cluster_centers_.tolist()
    elif model_type_norm == "hdbscan":
        import hdbscan

        min_cluster_size = int(params.get("min_cluster_size", 50))
        min_samples = int(params.get("min_samples", 10))
        model = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples)
        labels = model.fit_predict(X)
        payload["probabilities"] = model.probabilities_.tolist()
    else:
        raise ValueError(f"Unsupported regime model type: {model_type}")

    label_counts = {str(k): int(v) for k, v in zip(*np.unique(labels, return_counts=True))}
    payload["label_counts"] = label_counts
    payload["row_count"] = int(len(labels))

    checksum = _sha256_json({"labels": labels.tolist(), "payload": payload})
    model_id = f"rm_{dataset_id}_{model_type_norm}_v1"
    artifact = RegimeModelArtifact(
        model_id=model_id,
        dataset_id=dataset_id,
        model_type=model_type_norm,
        features=features,
        params=params,
        checksum=checksum,
        created_at=created_at,
        label_counts=label_counts,
        payload=payload,
    )
    series = pd.Series(labels, index=df.index, dtype="int64").rename("regime_ml_v1")
    return series, artifact


