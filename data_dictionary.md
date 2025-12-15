# Data Dictionary

## dim_date.csv
- `date` (date): Calendar date.
- `year` (int)
- `quarter` (int): 1–4.
- `month` (int)
- `week` (int): ISO week number.
- `day` (int)
- `day_of_week` (int): Monday=0.
- `is_month_end` (bool)
- `is_quarter_end` (bool)
- `is_year_end` (bool)

Grain: one row per calendar day between min and max trading dates observed.

## dim_ticker.csv
- `ticker` (str)
- `name` (str, optional)
- `asset_class` (str): Equity / FX / Crypto.
- `group` (str): Logical grouping from config.
- `currency` (str): Quote currency.
- `source` (str): Data provider.

Grain: one row per ticker.

## fact_prices.csv
- `date` (date)
- `ticker` (str)
- `open`, `high`, `low`, `close` (float)
- `adj_close` (float): Adjusted close when available; otherwise close.
- `volume` (float): May be empty for FX.
- `asset_class` (str)
- `currency` (str)
- `source` (str): API function used.

Grain: one row per (date, ticker). No forward-filling; duplicates removed deterministically.

## fact_features_daily.csv
- `date`, `ticker`
- `ret_1d` (float): Simple daily return.
- `log_ret_1d` (float)
- `ma_20`, `ma_50`, `ma_200` (float): Moving averages on close.
- `vol_20`, `vol_60` (float): Annualized log-return volatility.
- `peak_to_date` (float): Running close peak.
- `drawdown_pct` (float)
- `bb_mid_20`, `bb_up_20`, `bb_low_20` (float): Bollinger bands.
- `true_range` (float)
- `atr_14` (float): Average true range.
- `trend_regime` (str): Up/Down/Sideways (MA-based).
- `vol_regime` (str): Low/Med/High (vol_60 quantiles).

Grain: one row per (date, ticker).

## fact_latest_snapshot.csv
- `ticker` (str)
- `last_date` (date)
- `last_close` (float)
- `pct_1d`, `pct_1w`, `pct_1m`, `pct_ytd` (float): Price change vs prior 1 day/5 days/21 days/start of year.
- `vol_60` (float): Latest 60-day annualized volatility.
- `max_dd_1y` (float): Worst drawdown over trailing ~1 year.

Grain: one row per ticker (latest available date per ticker).

## etl_metadata.csv
- `run_id` (str): UTC timestamp identifier (`YYYYMMDDTHHMMSSZ`).
- `run_timestamp_utc` (str, ISO 8601)
- `rows_written` (int): fact_prices row count.
- `tickers_succeeded` (str): Comma-separated.
- `tickers_failed` (str): Comma-separated.
- `api_calls` (int)
- `notes` (str)

Grain: one row per pipeline run.

