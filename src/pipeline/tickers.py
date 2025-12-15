from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal

import yaml

AssetClass = Literal["Equity", "FX", "Crypto"]


@dataclass(slots=True)
class Ticker:
    symbol: str
    name: str | None
    asset_class: AssetClass
    group: str
    currency: str
    market: str | None = None  # quote currency for FX/Crypto where needed


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def load_tickers(path: Path) -> List[Ticker]:
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    groups = config.get("groups", {})
    tickers: list[Ticker] = []

    for group_name, group_cfg in groups.items():
        asset_class: AssetClass = group_cfg.get("asset_class", "Equity")  # type: ignore[assignment]
        currency: str = group_cfg.get("currency", "USD")
        market_default: str | None = group_cfg.get("market")
        for item in group_cfg.get("tickers", []):
            tickers.append(
                Ticker(
                    symbol=_normalize_symbol(item["symbol"]),
                    name=item.get("name"),
                    asset_class=asset_class,
                    group=group_name,
                    currency=item.get("currency", currency),
                    market=item.get("market", market_default),
                )
            )

    # determinism for reproducibility
    tickers.sort(key=lambda t: (t.asset_class, t.group, t.symbol))
    return tickers
