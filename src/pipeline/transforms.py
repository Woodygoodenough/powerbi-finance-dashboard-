from __future__ import annotations
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .alphavantage_client import AlphaVantageError
from .tickers import Ticker


def _to_date(date_str: str) -> pd.Timestamp:
    return pd.to_datetime(date_str).normalize()


def parse_equity(payload: dict[str, Any], ticker: Ticker) -> pd.DataFrame:
    # TIME_SERIES_DAILY[_ADJUSTED] keys
    series = payload.get("Time Series (Daily)") or payload.get("Time Series (Daily) ")
    if series is None:
        # Fall back to any time series key
        series_candidates = [v for k, v in payload.items() if "Time Series" in k]
        if series_candidates:
            series = series_candidates[0]
        else:
            raise AlphaVantageError(
                f"Unexpected payload for {ticker.symbol}: {payload}"
            )
    rows = []
    for date_str, metrics in series.items():
        rows.append(
            {
                "date": _to_date(date_str),
                "ticker": ticker.symbol,
                "open": float(metrics["1. open"]),
                "high": float(metrics["2. high"]),
                "low": float(metrics["3. low"]),
                "close": float(metrics["4. close"]),
                "adj_close": float(
                    metrics.get("5. adjusted close", metrics["4. close"])
                ),
                "volume": float(
                    metrics.get("6. volume", metrics.get("5. volume", np.nan))
                ),
                "currency": ticker.currency,
                "asset_class": ticker.asset_class,
                "source": payload.get("source_function", "TIME_SERIES_DAILY"),
            }
        )
    return _finalize_prices(pd.DataFrame(rows))


def parse_fx(payload: dict[str, Any], ticker: Ticker) -> pd.DataFrame:
    series = payload["Time Series FX (Daily)"]
    rows = []
    for date_str, metrics in series.items():
        rows.append(
            {
                "date": _to_date(date_str),
                "ticker": ticker.symbol,
                "open": float(metrics["1. open"]),
                "high": float(metrics["2. high"]),
                "low": float(metrics["3. low"]),
                "close": float(metrics["4. close"]),
                "adj_close": float(metrics["4. close"]),
                "volume": np.nan,
                "currency": ticker.currency,
                "asset_class": ticker.asset_class,
                "source": payload.get("source_function", "FX_DAILY"),
            }
        )
    return _finalize_prices(pd.DataFrame(rows))


def parse_crypto(payload: dict[str, Any], ticker: Ticker) -> pd.DataFrame:
    series = payload["Time Series (Digital Currency Daily)"]
    market = payload.get("market", "USD")
    rows = []
    for date_str, metrics in series.items():

        def _get(key_with_market: str, fallback_key: str) -> float:
            if key_with_market in metrics:
                return float(metrics[key_with_market])
            if fallback_key in metrics:
                return float(metrics[fallback_key])
            raise AlphaVantageError(
                f"Missing expected crypto field {key_with_market} or {fallback_key} for {ticker.symbol}"
            )

        rows.append(
            {
                "date": _to_date(date_str),
                "ticker": ticker.symbol,
                "open": _get(f"1a. open ({market})", "1. open"),
                "high": _get(f"2a. high ({market})", "2. high"),
                "low": _get(f"3a. low ({market})", "3. low"),
                "close": _get(f"4a. close ({market})", "4. close"),
                "adj_close": _get(f"4a. close ({market})", "4. close"),
                "volume": float(
                    metrics.get("5. volume", metrics.get("5. market cap", 0.0))
                ),
                "currency": market,
                "asset_class": ticker.asset_class,
                "source": payload.get("source_function", "DIGITAL_CURRENCY_DAILY"),
            }
        )
    return _finalize_prices(pd.DataFrame(rows))


def _finalize_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").drop_duplicates(subset=["date", "ticker"], keep="last")
    return df.reset_index(drop=True)


def build_dim_ticker(tickers: Iterable[Ticker]) -> pd.DataFrame:
    rows = [
        {
            "ticker": t.symbol,
            "name": t.name,
            "asset_class": t.asset_class,
            "group": t.group,
            "currency": t.currency,
            "source": "AlphaVantage",
        }
        for t in tickers
    ]
    return (
        pd.DataFrame(rows)
        .sort_values(["asset_class", "group", "ticker"])
        .reset_index(drop=True)
    )


def build_dim_date(min_date: pd.Timestamp, max_date: pd.Timestamp) -> pd.DataFrame:
    date_range = pd.date_range(min_date, max_date, freq="D", tz=None)
    df = pd.DataFrame({"date": date_range})
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["day"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.dayofweek
    df["is_month_end"] = df["date"].dt.is_month_end
    df["is_quarter_end"] = df["date"].dt.is_quarter_end
    df["is_year_end"] = df["date"].dt.is_year_end
    return df.sort_values("date").reset_index(drop=True)
