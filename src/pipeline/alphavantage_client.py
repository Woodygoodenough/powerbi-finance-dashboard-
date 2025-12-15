from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Mapping

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .settings import Settings
from .tickers import Ticker

BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageError(RuntimeError):
    """Raised when the Alpha Vantage API reports an error or returns unexpected data."""


class RateLimiter:
    """Simple per-call interval limiter suitable for Alpha Vantage free tier."""

    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = min_interval_seconds
        self._last_call = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call
        remaining = self.min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_call = time.monotonic()


@dataclass(slots=True)
class AlphaVantageClient:
    settings: Settings
    rate_limiter: RateLimiter | None = None
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self.rate_limiter = RateLimiter(
            self.settings.alpha_vantage_min_interval_seconds
        )
        self.session = requests.Session()
        retry = Retry(
            total=self.settings.max_retries,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def close(self) -> None:
        if self.session:
            self.session.close()

    def _request(self, params: Mapping[str, Any]) -> dict[str, Any]:
        if self.session is None or self.rate_limiter is None:
            raise RuntimeError("Client not initialized.")

        query = dict(params)
        query["apikey"] = self.settings.alpha_vantage_api_key

        for attempt in range(1, self.settings.max_retries + 1):
            self.rate_limiter.wait()
            response = self.session.get(
                BASE_URL, params=query, timeout=self.settings.alpha_vantage_timeout
            )
            try:
                payload = response.json()
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                raise AlphaVantageError(
                    f"Invalid JSON response: {response.text}"
                ) from exc

            if "Information" in payload:
                # Alpha Vantage sometimes uses "Information" for premium-only notice
                raise AlphaVantageError(payload["Information"])

            if response.status_code == 429 or "Note" in payload:
                # Throttled; back off and retry
                if attempt == self.settings.max_retries:
                    raise AlphaVantageError(payload.get("Note", "Rate limited"))
                time.sleep(self.settings.backoff_seconds)
                continue

            if "Error Message" in payload:
                raise AlphaVantageError(payload["Error Message"])

            return payload

        raise AlphaVantageError("Failed to fetch data after retries.")

    # --- Public fetch helpers -------------------------------------------------
    def fetch_equity_daily(self, symbol: str) -> dict[str, Any]:
        attempts = [
            {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": "full",
                "source_function": "TIME_SERIES_DAILY_ADJUSTED",
            },
            {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": "full",
                "source_function": "TIME_SERIES_DAILY",
            },
            {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": "compact",  # free tier fallback
                "source_function": "TIME_SERIES_DAILY",
            },
        ]

        last_error: AlphaVantageError | None = None
        for params in attempts:
            try:
                payload = self._request(
                    {k: v for k, v in params.items() if k != "source_function"}
                )
                payload["source_function"] = params["source_function"]
                payload["symbol"] = symbol
                return payload
            except AlphaVantageError as exc:
                last_error = exc
                continue
        raise last_error or AlphaVantageError(f"Failed to fetch {symbol}")

    def fetch_fx_daily(self, pair: str) -> dict[str, Any]:
        from_symbol, to_symbol = pair[:3], pair[3:]
        payload = self._request(
            {
                "function": "FX_DAILY",
                "from_symbol": from_symbol,
                "to_symbol": to_symbol,
                "outputsize": "full",
            }
        )
        payload["source_function"] = "FX_DAILY"
        payload["symbol"] = pair
        return payload

    def fetch_crypto_daily(self, symbol: str, market: str = "USD") -> dict[str, Any]:
        payload = self._request(
            {
                "function": "DIGITAL_CURRENCY_DAILY",
                "symbol": symbol,
                "market": market,
            }
        )
        payload["source_function"] = "DIGITAL_CURRENCY_DAILY"
        payload["symbol"] = symbol
        payload["market"] = market
        return payload


def fetch_payload(client: AlphaVantageClient, ticker: Ticker) -> dict[str, Any]:
    if ticker.asset_class == "Equity":
        return client.fetch_equity_daily(ticker.symbol)
    if ticker.asset_class == "FX":
        return client.fetch_fx_daily(ticker.symbol)
    if ticker.asset_class == "Crypto":
        market = ticker.market or "USD"
        return client.fetch_crypto_daily(ticker.symbol, market)
    raise AlphaVantageError(f"Unsupported asset class {ticker.asset_class}")
