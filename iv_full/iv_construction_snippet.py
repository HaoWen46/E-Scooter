#!/usr/bin/env python3
"""
IV construction snippet (from kill_scooters.py).

Shows how the 6 store IVs are built using one-year-lagged store counts
instead of current-month store counts.

This snippet assumes `df` already has:
  - date (datetime, month-start)
  - county, district
  - lag_n_iv (lagged total national EV registrations)
  - open_stores* columns (current-month store counts, from scooter_and_iv.csv)

And that store_counts_2018.csv is available in the input folder.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── store columns used in IV formula ─────────────────────────────────────────
STORE_COLS = [
    "open_stores7eleven", "open_storesfamilymart", "open_storeshilife",
    "open_storesOK", "open_storespxmart",
    "open_storescarrefour", "open_storesrtmart", "open_storessimplemart",
]

def build_ivs(df: pd.DataFrame, store_counts_2018_path: str) -> pd.DataFrame:
    """
    Attach 6 store IVs to df using one-year-lagged store counts.

    For each (county, district, date) row, the IV is:
        iv_X = log(lag_n_iv) * log1p(open_stores_X from 12 months prior)

    The 2018 store count panel fills the gap for 2019 rows.
    2020+ rows use their own store counts shifted back 12 months.
    """

    # --- build full store count panel (2018 from CSV + 2019+ from df) --------
    stores_current = (
        df[["county", "district", "date"] + STORE_COLS]
        .drop_duplicates(subset=["county", "district", "date"])
    )
    stores_2018 = pd.read_csv(store_counts_2018_path, parse_dates=["date"])
    stores_2018 = stores_2018[["county", "district", "date"] + STORE_COLS]

    stores_panel = (
        pd.concat([stores_2018, stores_current], ignore_index=True)
        .drop_duplicates(subset=["county", "district", "date"])
    )

    # --- shift dates forward 1 year so they merge onto the target year -------
    stores_lagged = stores_panel.copy()
    stores_lagged["date"] = stores_lagged["date"] + pd.DateOffset(years=1)
    stores_lagged = stores_lagged.rename(
        columns={c: f"lag1y_{c}" for c in STORE_COLS}
    )

    df = df.merge(stores_lagged, on=["county", "district", "date"], how="left")

    # --- IV formula ----------------------------------------------------------
    def log1p_safe(x):
        return np.log(x.clip(lower=0) + 1)

    log_lag = np.log(df["lag_n_iv"].clip(lower=1))

    df["iv_7eleven"]          = log_lag * log1p_safe(df["lag1y_open_stores7eleven"])
    df["iv_familymart"]       = log_lag * log1p_safe(df["lag1y_open_storesfamilymart"])
    df["iv_okhilife"]         = log_lag * log1p_safe(df["lag1y_open_storesOK"] + df["lag1y_open_storeshilife"])
    df["iv_pxmart"]           = log_lag * log1p_safe(df["lag1y_open_storespxmart"])
    df["iv_carrefour"]        = log_lag * log1p_safe(df["lag1y_open_storescarrefour"])
    df["iv_rtmartsimplemart"] = log_lag * log1p_safe(df["lag1y_open_storesrtmart"] + df["lag1y_open_storessimplemart"])

    return df
