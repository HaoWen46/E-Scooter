#!/usr/bin/env python3
"""
Build store counts for all 12 months of 2018, to serve as one-year lags for 2019 IVs.

Convenience stores: reconstructed month-by-month from raw CSV (opendate/changedate).
Grocery stores + gas stations: proxied from 2019 data (raw data not accessible on this server).
Output: input/store_counts_2018.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

INST  = Path("/home5/B11902156/EScooters/01-Build/03-Instruments")
INPUT = Path("/home5/B11902156/Project/input")

STORE_COLS = [
    "open_stores7eleven", "open_storesfamilymart", "open_storeshilife",
    "open_storesOK", "open_storespxmart",
    "open_storescarrefour", "open_storesrtmart", "open_storessimplemart",
    "open_storescpc",
]

# ── convenience stores ────────────────────────────────────────────────────────

def parse_roc(s):
    s = str(s).strip().zfill(7)
    try:
        return pd.Timestamp(int(s[:3]) + 1911, int(s[3:5]), int(s[5:7]))
    except:
        return pd.NaT

raw = pd.read_csv(INST / "01-ConvenienceStores/output/convenientstore_district.csv")
raw["county"] = raw["county"].str.replace("巿", "市").str.replace("台", "臺")
raw["parent"] = raw["parent"].map({
    "全家便利商店股份有限公司": "全家",
    "統一超商股份有限公司":    "7-11",
    "萊爾富國際股份有限公司":  "萊爾富",
    "來來超商股份有限公司":    "OK",
    "全聯實業股份有限公司":    "全聯",
}).fillna(raw["parent"])

raw["opendate_dt"]   = raw["opendate"].apply(parse_roc)
raw["changedate_dt"] = raw["last_change"].apply(parse_roc)
raw.loc[raw["status"] == 1, "changedate_dt"] = pd.NaT  # active stores have no close date

parent_to_col = {
    "7-11":   "open_stores7eleven",
    "全家":   "open_storesfamilymart",
    "萊爾富": "open_storeshilife",
    "OK":     "open_storesOK",
    "全聯":   "open_storespxmart",
}

months_2018 = pd.date_range("2018-01-01", "2018-12-01", freq="MS")

conv_frames = []
for month in months_2018:
    open_stores = raw[
        (raw["opendate_dt"] <= month) &
        (raw["changedate_dt"].isna() | (raw["changedate_dt"] >= month))
    ].copy()
    open_stores["store_col"] = open_stores["parent"].map(parent_to_col)
    open_stores = open_stores.dropna(subset=["store_col"])
    wide = (
        open_stores.groupby(["county", "district", "store_col"])
        .size()
        .reset_index(name="n")
        .pivot_table(index=["county", "district"], columns="store_col", values="n",
                     aggfunc="sum", fill_value=0)
        .reset_index()
    )
    wide.columns.name = None
    wide["date"] = month
    conv_frames.append(wide)

conv_2018 = pd.concat(conv_frames, ignore_index=True)
print(f"Convenience stores: {len(conv_2018)} district-month rows for 2018")

# ── grocery + gas stations: proxy from matching month in 2019 ────────────────

iv2019 = pd.read_stata(INST / "04-MergingInstruments/output/iv_for_reg.dta")
iv2019["date"] = pd.to_datetime(iv2019["app_date"]).dt.to_period("M").dt.to_timestamp()

grocery_cols = ["county", "district", "date",
                "open_storescarrefour", "open_storesrtmart",
                "open_storessimplemart", "open_storescpc"]
grocery_2019 = iv2019[[c for c in grocery_cols if c in iv2019.columns]].copy()

# shift 2019 dates back by one year → proxy for 2018
grocery_2018 = grocery_2019.copy()
grocery_2018["date"] = grocery_2018["date"] - pd.DateOffset(years=1)

# ── merge convenience + grocery/gas ──────────────────────────────────────────

# get the full district list from 2019
districts = iv2019[iv2019["date"] == "2019-01-01"][["county", "district"]].copy()

result_frames = []
for month in months_2018:
    d = districts.copy()
    d["date"] = month

    conv_m = conv_2018[conv_2018["date"] == month].drop(columns="date")
    groc_m = grocery_2018[grocery_2018["date"] == month].drop(columns="date")

    d = d.merge(conv_m, on=["county", "district"], how="left")
    d = d.merge(groc_m, on=["county", "district"], how="left")
    result_frames.append(d)

result = pd.concat(result_frames, ignore_index=True)

# rural districts with no convenience stores → genuine zero
for col in ["open_stores7eleven", "open_storesfamilymart", "open_storeshilife",
            "open_storesOK", "open_storespxmart"]:
    if col in result.columns:
        result[col] = result[col].fillna(0)

final_cols = ["county", "district", "date"] + STORE_COLS
result = result[[c for c in final_cols if c in result.columns]]
result = result.sort_values(["county", "district", "date"]).reset_index(drop=True)

dst = INPUT / "store_counts_2018.csv"
result.to_csv(dst, index=False)
print(f"Saved → {dst}  ({len(result)} rows, {result['date'].nunique()} months)")
print(result[result["district"] == "三峽區"].to_string(index=False))
