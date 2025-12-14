from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional
import requests
from config import settings


BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageError(RuntimeError):
    """Raised when the Alpha Vantage API reports an error or returns unexpected data."""


@dataclass(slots=True)
class AlphaVantageClient:
    """Tiny convenience wrapper around the Alpha Vantage HTTP API."""

    api_key: str
    timeout: float = settings.alpha_vantage_timeout
    session: Optional[requests.Session] = None

    def __post_init__(self) -> None:
        if not self.api_key:
            raise AlphaVantageError(
                "An API key is required to create AlphaVantageClient."
            )
        if self.timeout <= 0:
            raise ValueError("timeout must be positive.")
        if self.session is None:
            self.session = requests.Session()

    def __enter__(self) -> AlphaVantageClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        if self.session:
            self.session.close()

    def _request(self, params: Mapping[str, Any]) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("Client session already closed.")

        query: MutableMapping[str, Any] = dict(params)
        query["apikey"] = self.api_key

        response = self.session.get(BASE_URL, params=query, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        if "Error Message" in payload:
            raise AlphaVantageError(payload["Error Message"])
        if "Note" in payload:
            raise AlphaVantageError(payload["Note"])

        return payload

    def time_series_daily(
        self,
        symbol: str,
        *,
        outputsize: str = "compact",
    ) -> Mapping[str, Mapping[str, str]]:
        payload = self._request(
            {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": outputsize,
            }
        )
        try:
            return payload["Time Series (Daily)"]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise AlphaVantageError(f"Unexpected response: {payload}") from exc

    def global_quote(self, symbol: str) -> Mapping[str, str]:
        payload = self._request(
            {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
            }
        )
        try:
            return payload["Global Quote"]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise AlphaVantageError(f"Unexpected response: {payload}") from exc


def create_client() -> AlphaVantageClient:
    return AlphaVantageClient(api_key=settings.alpha_vantage_api_key)
