from __future__ import annotations

import numpy as np
import pandas as pd


def add_features(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute daily features per ticker."""
    df = prices.copy()
    df = df.sort_values(["ticker", "date"])

    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        g = group.sort_values("date").copy()
        close = g["close"]
        g["ret_1d"] = close.pct_change()
        g["log_ret_1d"] = np.log(close).diff()

        g["ma_20"] = close.rolling(20).mean()
        g["ma_50"] = close.rolling(50).mean()
        g["ma_200"] = close.rolling(200).mean()

        returns = g["log_ret_1d"].fillna(0)
        g["vol_20"] = returns.rolling(20).std() * np.sqrt(252)
        g["vol_60"] = returns.rolling(60).std() * np.sqrt(252)

        g["peak_to_date"] = close.cummax()
        g["drawdown_pct"] = close / g["peak_to_date"] - 1.0

        std_20 = close.rolling(20).std()
        g["bb_mid_20"] = g["ma_20"]
        g["bb_up_20"] = g["ma_20"] + 2 * std_20
        g["bb_low_20"] = g["ma_20"] - 2 * std_20

        prev_close = close.shift(1)
        tr = pd.concat(
            [
                g["high"] - g["low"],
                (g["high"] - prev_close).abs(),
                (g["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        g["true_range"] = tr
        g["atr_14"] = tr.rolling(14).mean()

        g["trend_regime"] = _trend_regime(g)
        g["vol_regime"] = _vol_regime(g)
        return g

    df = df.groupby("ticker", group_keys=False).apply(_per_ticker)
    return df.reset_index(drop=True)


def _trend_regime(df: pd.DataFrame) -> pd.Series:
    cond_up = (df["ma_20"] > df["ma_50"]) & (df["ma_50"] > df["ma_200"])
    cond_down = (df["ma_20"] < df["ma_50"]) & (df["ma_50"] < df["ma_200"])
    return pd.Series(
        np.select(
            [cond_up, cond_down],
            ["Up", "Down"],
            default="Sideways",
        ),
        index=df.index,
    )


def _vol_regime(df: pd.DataFrame) -> pd.Series:
    vol = df["vol_60"]
    # Compute static quantiles per ticker for determinism
    q_low = vol.quantile(0.33)
    q_high = vol.quantile(0.67)
    return pd.Series(
        np.select(
            [vol <= q_low, vol >= q_high],
            ["Low", "High"],
            default="Med",
        ),
        index=df.index,
    )

