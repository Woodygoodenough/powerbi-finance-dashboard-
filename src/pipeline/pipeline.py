from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .alphavantage_client import AlphaVantageClient, AlphaVantageError, fetch_payload
from .analytics import (
    build_etl_metadata,
    build_fact_features,
    build_fact_latest_snapshot,
)
from .features import add_features
from .settings import Settings, load_settings
from .tickers import Ticker, load_tickers
from .transforms import (
    build_dim_date,
    build_dim_ticker,
    parse_crypto,
    parse_equity,
    parse_fx,
)

logger = logging.getLogger(__name__)


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _parse_payload(payload: dict, ticker: Ticker) -> pd.DataFrame:
    if ticker.asset_class == "Equity":
        return parse_equity(payload, ticker)
    if ticker.asset_class == "FX":
        return parse_fx(payload, ticker)
    if ticker.asset_class == "Crypto":
        return parse_crypto(payload, ticker)
    raise AlphaVantageError(f"Unsupported asset class {ticker.asset_class}")


def _write_csvs(outputs: Dict[str, pd.DataFrame], version_dir: Path, docs_dir: Path) -> None:
    _ensure_dirs(version_dir, docs_dir)
    for name, df in outputs.items():
        target = version_dir / f"{name}.csv"
        df.to_csv(target, index=False)
        shutil.copy2(target, docs_dir / f"{name}.csv")


def _date_only(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    copy = df.copy()
    for col in columns:
        if col in copy.columns:
            copy[col] = pd.to_datetime(copy[col]).dt.date
    return copy


def run(settings: Settings | None = None) -> None:
    settings = settings or load_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    run_timestamp = datetime.now(timezone.utc)
    run_id = run_timestamp.strftime("%Y%m%dT%H%M%SZ")

    version_dir = settings.output_dir / run_id
    raw_dir = settings.raw_data_dir / run_id
    docs_dir = settings.docs_data_dir
    _ensure_dirs(version_dir, raw_dir, docs_dir)

    tickers = load_tickers(settings.ticker_config_path)
    logger.info("Loaded %d tickers from %s", len(tickers), settings.ticker_config_path)

    client = AlphaVantageClient(settings)

    successes: list[str] = []
    failures: list[str] = []
    price_frames: list[pd.DataFrame] = []
    api_calls = 0

    try:
        for ticker in tickers:
            logger.info("Fetching %s (%s)", ticker.symbol, ticker.asset_class)
            raw_path = raw_dir / f"{ticker.symbol}.json"
            try:
                if raw_path.exists():
                    with raw_path.open("r", encoding="utf-8") as f:
                        payload = json.load(f)
                else:
                    payload = fetch_payload(client, ticker)
                    api_calls += 1
                    with raw_path.open("w", encoding="utf-8") as f:
                        json.dump(payload, f, indent=2)
                df = _parse_payload(payload, ticker)
                price_frames.append(df)
                successes.append(ticker.symbol)
            except Exception as exc:  # broad to keep pipeline running
                failures.append(ticker.symbol)
                logger.exception("Failed to process %s: %s", ticker.symbol, exc)

        if not price_frames:
            raise RuntimeError("No price data fetched; aborting.")

        prices = (
            pd.concat(price_frames, ignore_index=True)
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )

        prices_with_features = add_features(prices)

        fact_prices = prices[
            [
                "date",
                "ticker",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "asset_class",
                "currency",
                "source",
            ]
        ].sort_values(["ticker", "date"])
        fact_prices = _date_only(fact_prices, ["date"])

        fact_features_daily = build_fact_features(prices_with_features)
        fact_features_daily = _date_only(fact_features_daily, ["date"])

        fact_latest_snapshot = build_fact_latest_snapshot(prices_with_features)
        fact_latest_snapshot = _date_only(fact_latest_snapshot, ["last_date"])

        dim_ticker = build_dim_ticker(tickers)
        dim_date = build_dim_date(
            prices["date"].min().normalize(), prices["date"].max().normalize()
        )
        dim_date = _date_only(dim_date, ["date"])

        metadata = build_etl_metadata(
            run_id=run_id,
            run_timestamp_utc=run_timestamp,
            tickers_succeeded=successes,
            tickers_failed=failures,
            rows_written=len(fact_prices),
            api_calls=api_calls,
            notes="",
        )

        outputs = {
            "dim_date": dim_date,
            "dim_ticker": dim_ticker,
            "fact_prices": fact_prices,
            "fact_features_daily": fact_features_daily,
            "fact_latest_snapshot": fact_latest_snapshot,
            "etl_metadata": metadata,
        }

        _write_csvs(outputs, version_dir, docs_dir)
        logger.info(
            "Run %s complete. Success: %d; Failed: %d; Rows: %d",
            run_id,
            len(successes),
            len(failures),
            len(fact_prices),
        )
    finally:
        client.close()


if __name__ == "__main__":
    run()

