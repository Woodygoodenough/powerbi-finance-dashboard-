from __future__ import annotations

from datetime import datetime
from typing import Sequence

import pandas as pd


def build_fact_features(prices: pd.DataFrame) -> pd.DataFrame:
    """Return fact_features_daily by selecting feature columns."""
    feature_cols = [
        "ret_1d",
        "log_ret_1d",
        "ma_20",
        "ma_50",
        "ma_200",
        "vol_20",
        "vol_60",
        "peak_to_date",
        "drawdown_pct",
        "bb_mid_20",
        "bb_up_20",
        "bb_low_20",
        "true_range",
        "atr_14",
        "trend_regime",
        "vol_regime",
    ]
    cols = ["date", "ticker", *feature_cols]
    return prices[cols].sort_values(["ticker", "date"]).reset_index(drop=True)


def build_fact_latest_snapshot(prices: pd.DataFrame) -> pd.DataFrame:
    """One row per ticker with last close and summary stats."""
    snapshots = []
    for ticker, group in prices.groupby("ticker"):
        g = group.sort_values("date")
        last = g.iloc[-1]

        def _pct_vs(offset_days: int) -> float | None:
            if len(g) <= offset_days:
                return None
            ref = g.iloc[-1 - offset_days]
            if ref["close"] == 0:
                return None
            return (last["close"] / ref["close"]) - 1.0

        def _pct_vs_date(target_date: pd.Timestamp) -> float | None:
            subset = g[g["date"] <= target_date]
            if subset.empty:
                return None
            ref = subset.iloc[-1]
            if ref["close"] == 0:
                return None
            return (last["close"] / ref["close"]) - 1.0

        ytd_start = pd.Timestamp(year=last["date"].year, month=1, day=1)
        pct_ytd = _pct_vs_date(ytd_start)

        window_1y = g[g["date"] >= last["date"] - pd.Timedelta(days=365)]
        max_dd_1y = window_1y["drawdown_pct"].min() if not window_1y.empty else None

        snapshots.append(
            {
                "ticker": ticker,
                "last_date": last["date"],
                "last_close": last["close"],
                "pct_1d": _pct_vs(1),
                "pct_1w": _pct_vs(5),
                "pct_1m": _pct_vs(21),
                "pct_ytd": pct_ytd,
                "vol_60": last.get("vol_60"),
                "max_dd_1y": max_dd_1y,
            }
        )
    return pd.DataFrame(snapshots).sort_values("ticker").reset_index(drop=True)


def build_etl_metadata(
    run_id: str,
    run_timestamp_utc: datetime,
    tickers_succeeded: Sequence[str],
    tickers_failed: Sequence[str],
    rows_written: int,
    api_calls: int,
    notes: str | None = None,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_timestamp_utc": run_timestamp_utc.isoformat(),
                "rows_written": rows_written,
                "tickers_succeeded": ",".join(sorted(tickers_succeeded)),
                "tickers_failed": ",".join(sorted(tickers_failed)),
                "api_calls": api_calls,
                "notes": notes,
            }
        ]
    )
