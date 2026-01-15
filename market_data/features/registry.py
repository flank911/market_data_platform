from __future__ import annotations


def feature_registry() -> dict:
    return {
        "atr": {"versions": ["v1"], "params": {"window": 14}},
        "volatility": {"versions": ["v1"], "params": {"window": 30}},
        "trend": {"versions": ["v1"], "params": {"short_span": 12, "long_span": 26}},
        "sessions": {"versions": ["v1"], "params": {}},
        "volatility_regime": {"versions": ["v1"], "params": {}},
        "trend_regime": {"versions": ["v1"], "params": {}},
        "cross_market": {
            "versions": ["v1"],
            "params": {"window": 20, "signals": ["funding_rate", "open_interest", "liquidations"]},
        },
        "regime_ml": {
            "versions": ["v1"],
            "params": {"type": "hdbscan", "features": ["volatility", "funding_dev", "oi_delta"]},
        },
    }


