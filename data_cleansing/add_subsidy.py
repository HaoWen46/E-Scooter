# merge_append_subsidy_wide.py
# Append 6 subsidy columns from a wide multi-level header CSV to the main panel,
# then add natural-log ("ln_") columns for those six (and installed_base),
# and add exact 1-year lags for the six subsidy columns (plus ln of those lags).
# ALSO append 6 old_for_new columns from old_for_new.csv (same wide structure),
# and add ln(1+x) for those 6 new columns.
# Local = county only; Total = county + (經濟部工業局 + 行政院環保署).
# IMPORTANT: subsidy 1-year lags are computed FROM subsidy.csv before intersecting with main.
# old_for_new currently has NO lags.

import os
import sys
import pandas as pd
import numpy as np

# ================= utils =================
def fatal(msg: str):
    print(f"[FATAL] {msg}")
    sys.exit(1)

def ok(msg: str):
    print(f"[OK] {msg}")

def ckpt(msg: str):
    print(f"\n[CHECKPOINT] {msg}")

def ensure(path: str, label: str):
    if not os.path.exists(path):
        fatal(f"{label} not found: {path}")

def check_unique(df: pd.DataFrame, keys: list, name: str):
    dup = df.groupby(keys).size().reset_index(name="n")
    bad = dup[dup["n"] > 1]
    if not bad.empty:
        print(bad.head(10))
        fatal(f"{name} has duplicate rows for keys {keys}. Fix source first.")

def to_trad_tai(s: pd.Series) -> pd.Series:
    # ONLY convert 台 → 臺
    return s.astype(str).str.replace("台", "臺", regex=False)

def drop_cols_if_exist(df: pd.DataFrame, cols: list, label: str) -> pd.DataFrame:
    exist = [c for c in cols if c in df.columns]
    if exist:
        df = df.drop(columns=exist)
        ok(f"dropped existing {label} cols: {exist}")
    return df

# =============== main loader ===============
def load_main(path: str) -> pd.DataFrame:
    ensure(path, "Main CSV")
    df = pd.read_csv(path)

    if "county" not in df.columns:
        fatal("Main CSV missing 'county' column")
    if "date" not in df.columns:
        fatal("Main CSV missing 'date' column")

    # normalize date → month-start midnight
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    if df["date"].isna().any():
        n = int(df["date"].isna().sum())
        fatal(f"Main CSV has {n} invalid 'date' values")

    # join key with 臺 (don’t mutate original column)
    df["county_key"] = to_trad_tai(df["county"])
    ok(f"main shape: {df.shape[0]} × {df.shape[1]}")
    ok(f"main date range: {df['date'].min()} → {df['date'].max()}")
    return df

# ======= wide CSV header fixer =======
def _ffill_top_header(mi: pd.MultiIndex) -> pd.MultiIndex:
    """
    Forward-fill level-0 header ('重型','輕型','小型輕型') so Unnamed... get replaced
    with the last seen real label. Keep the first column as ('_meta','date').
    """
    pairs = list(mi.to_flat_index())
    lvl0 = []
    last = None
    for a, b in pairs:
        a = str(a)
        b = str(b)
        if a.startswith("Unnamed") and b.startswith("Unnamed"):
            lvl0.append("_meta")
            continue
        if a.startswith("Unnamed"):
            a = last
        else:
            last = a
        lvl0.append(a)

    lvl1 = []
    for (a, b), a2 in zip(pairs, lvl0):
        a = str(a); b = str(b)
        if a2 == "_meta" and a.startswith("Unnamed") and b.startswith("Unnamed"):
            lvl1.append("date")
        else:
            lvl1.append(b)

    return pd.MultiIndex.from_tuples(list(zip(lvl0, lvl1)))

# ======= generic wide multitable loader =======
def load_wide_multitable(
    path: str,
    main_dates: pd.Series,
    out_prefix: str,          # "subsidy" or "old_for_new"
    central_names = {"經濟部工業局", "行政院環保署"},
    intersect_after: bool = True,
    compute_lags_1y: bool = False,
) -> pd.DataFrame:
    """
    Generic reader for your 2-row-header wide tables.
    Builds 6 columns:
      {out_prefix}_w{1,2,3}_{local,total}
    Optionally adds exact 1y lags (computed from full history before intersect).
    """
    ensure(path, f"Wide CSV ({out_prefix})")
    raw = pd.read_csv(path, header=[0,1])

    # fix the multiindex header so Unnamed get forward-filled
    raw.columns = _ffill_top_header(raw.columns)

    if ("_meta", "date") not in raw.columns:
        fatal(f"{out_prefix}: Could not locate the 'date' column after header fix")

    # parse date 'YYYY-MM' → month-start
    raw[("_meta", "date")] = pd.to_datetime(
        raw[("_meta", "date")].astype(str) + "-01",
        errors="coerce"
    ).dt.normalize()
    if raw[("_meta", "date")].isna().any():
        fatal(f"{out_prefix}: wide file has invalid month strings (cannot parse)")

    # isolate data columns (drop meta/date)
    data_cols = [c for c in raw.columns if c[0] not in {"_meta"}]
    data = raw[data_cols].copy()  # MultiIndex columns: (category, unit)

    # validate categories set
    cats = set([c[0] for c in data.columns])
    expected = {"重型", "輕型", "小型輕型"}
    if not cats.issubset(expected):
        fatal(f"{out_prefix}: Unexpected categories in wide file: {sorted(cats)}")

    # numeric coercion
    data = data.apply(pd.to_numeric, errors="coerce")

    # attach date
    data[("meta", "date")] = raw[("_meta", "date")]
    data.columns = pd.MultiIndex.from_tuples(data.columns)

    # long form: (date, category, unit, value)
    long = (
        data.set_index(("meta", "date"))
            .stack(level=[0,1])  # stack (category, unit)
            .reset_index()
    )
    long.columns = ["date", "category", "unit", "value"]

    # 臺-normalized county join key
    long["county_key"] = to_trad_tai(long["unit"])

    # map category → weight tag
    cat2w = {"小型輕型": "w1", "輕型": "w2", "重型": "w3"}
    long["wtag"] = long["category"].map(cat2w)

    # split central vs local
    is_central = long["unit"].isin(central_names)

    # ---- LOCAL (county-only) wide pivot ----
    local = long[~is_central].copy()
    check_unique(local, ["county_key", "date", "category"], f"{out_prefix} local (pre-pivot)")
    local_wide = (
        local.pivot_table(
                index=["county_key", "date"],
                columns="wtag",
                values="value",
                aggfunc="first"
        )
        .rename(columns={
            "w1": f"{out_prefix}_w1_local",
            "w2": f"{out_prefix}_w2_local",
            "w3": f"{out_prefix}_w3_local"
        })
        .reset_index()
    )
    for col in [f"{out_prefix}_w1_local", f"{out_prefix}_w2_local", f"{out_prefix}_w3_local"]:
        if col not in local_wide.columns:
            local_wide[col] = np.nan

    # ---- CENTRAL (IDB + EPA sum per date & wtag) ----
    cent = long[is_central].copy()
    if not cent.empty:
        cent_sum = (
            cent.groupby(["date", "wtag"], as_index=False)["value"].sum(min_count=1)
                .pivot(index="date", columns="wtag", values="value")
                .rename(columns={"w1": "central_w1", "w2": "central_w2", "w3": "central_w3"})
                .reset_index()
        )
        for col in ["central_w1","central_w2","central_w3"]:
            if col not in cent_sum.columns:
                cent_sum[col] = 0.0
    else:
        cent_sum = pd.DataFrame({"date": local_wide["date"].unique()})
        cent_sum = cent_sum.assign(central_w1=0.0, central_w2=0.0, central_w3=0.0)

    # ---- combine local + central and compute totals ----
    combo_full = local_wide.merge(cent_sum, on="date", how="left", validate="m:1")
    for w in ("w1","w2","w3"):
        combo_full[f"{out_prefix}_{w}_total"] = (
            combo_full[f"{out_prefix}_{w}_local"].fillna(0.0).astype(float)
            + combo_full[f"central_{w}"].fillna(0.0)
        )

    # final payload before lagging — FULL history from wide file
    out_full = combo_full[[
        "county_key", "date",
        f"{out_prefix}_w1_local", f"{out_prefix}_w2_local", f"{out_prefix}_w3_local",
        f"{out_prefix}_w1_total", f"{out_prefix}_w2_total", f"{out_prefix}_w3_total",
    ]].copy()
    check_unique(out_full, ["county_key","date"], f"{out_prefix} additions (pre-lag)")

    # ---- optional EXACT 1-year lags computed from full history ----
    if compute_lags_1y:
        six = [
            f"{out_prefix}_w1_local", f"{out_prefix}_w2_local", f"{out_prefix}_w3_local",
            f"{out_prefix}_w1_total", f"{out_prefix}_w2_total", f"{out_prefix}_w3_total",
        ]
        lag = out_full[["county_key","date"] + six].copy()
        lag["date"] = lag["date"] + pd.DateOffset(years=1)   # t + 1y
        lag = lag.rename(columns={c: f"lag1y_{c}" for c in six})

        out_full = out_full.merge(
            lag[["county_key","date"] + [f"lag1y_{c}" for c in six]],
            on=["county_key","date"],
            how="left",
            validate="1:1"
        )

    # ---- intersect with main's months if requested ----
    if intersect_after:
        keep_dates = pd.to_datetime(pd.Series(main_dates)).dt.normalize().unique()
        out = out_full[out_full["date"].isin(set(keep_dates))].copy()
    else:
        out = out_full

    ok(f"{out_prefix} rows (after intersect with main={intersect_after}): {len(out)}")
    ok(f"{out_prefix} date window: {out['date'].min()} → {out['date'].max()}")
    check_unique(out, ["county_key","date"], f"{out_prefix} additions (post)")
    return out

# ============== add ln columns ==============
def add_log_cols(df: pd.DataFrame, cols: list, prefix: str = "ln_") -> pd.DataFrame:
    """
    Add base-e log(1+x) columns for each column in 'cols'.
    Non-numeric or NaN inputs → NaN. Returns df (mutates in place).
    """
    for c in cols:
        ln_c = f"{prefix}{c}"
        v = pd.to_numeric(df[c], errors="coerce")
        df[ln_c] = np.where(v >= 0, np.log1p(v.astype(float)), np.nan)
    ok(f"added ln(1+x) columns: {[f'{prefix}{c}' for c in cols]}")
    return df

# ================= main =================
def main():
    main_path = os.path.expanduser("../input/scooter_PBGN.csv")

    wide_subsidy_path = os.path.expanduser("../input/subsidy.csv")
    old_for_new_path  = os.path.expanduser("../input/old_for_new.csv")

    out_path  = os.path.expanduser("../input/scooter_PBGN_o4n.csv")

    # define expected column names up front so we can drop them if already present
    subsidy_cols = [
        "subsidy_w1_local","subsidy_w2_local","subsidy_w3_local",
        "subsidy_w1_total","subsidy_w2_total","subsidy_w3_total"
    ]

    old_for_new_cols = [
        "old_for_new_w1_local","old_for_new_w2_local","old_for_new_w3_local",
        "old_for_new_w1_total","old_for_new_w2_total","old_for_new_w3_total"
    ]
    lag_cols = [f"lag1y_{c}" for c in subsidy_cols + old_for_new_cols]

    ln_cols_existing = [f"ln_{c}" for c in (subsidy_cols + lag_cols + old_for_new_cols)]

    ckpt("Loading main panel")
    main = load_main(main_path)

    # ---- DROP any previous-run columns to avoid _x/_y ----
    ckpt("Dropping existing subsidy / old_for_new / ln columns from main (if any)")
    main = drop_cols_if_exist(main, subsidy_cols, "subsidy")
    main = drop_cols_if_exist(main, lag_cols, "lag1y_subsidy")
    main = drop_cols_if_exist(main, old_for_new_cols, "old_for_new")
    main = drop_cols_if_exist(main, ln_cols_existing, "ln_* (old runs)")

    ckpt("Loading wide subsidy table & building 6 columns + exact 1y lags FROM subsidy.csv")
    add6_subsidy = load_wide_multitable(
        wide_subsidy_path,
        main_dates=main["date"].unique(),
        out_prefix="subsidy",
        compute_lags_1y=True,
        intersect_after=True
    )

    ckpt("Loading old_for_new wide table & building 6 old_for_new columns")
    add6_old = load_wide_multitable(
        old_for_new_path,
        main_dates=main["date"].unique(),
        out_prefix="old_for_new",
        compute_lags_1y=True,
        intersect_after=True
    )

    ckpt("Merging subsidy onto main by (county_key, date)")
    merged = main.merge(
        add6_subsidy,
        on=["county_key","date"],
        how="left",
        validate="m:1"
    )

    # extra safety: if something still exists, drop before second merge
    ckpt("Dropping any old_for_new collisions before merge (paranoia)")
    merged = drop_cols_if_exist(merged, [c for c in add6_old.columns if c not in ("county_key","date")], "old_for_new collisions")

    ckpt("Merging old_for_new onto main by (county_key, date)")
    merged = merged.merge(
        add6_old,
        on=["county_key","date"],
        how="left",
        validate="m:1"
    )

    # coverage sanity
    miss_all_subsidy = merged[subsidy_cols].isna().all(axis=1).sum()
    ok(f"merged rows: {merged.shape[0]}; rows missing ALL 6 subsidy cols: {miss_all_subsidy}")

    miss_all_old = merged[old_for_new_cols].isna().all(axis=1).sum()
    ok(f"rows missing ALL 6 old_for_new cols: {miss_all_old}")

    sub_cols_for_ln = subsidy_cols + lag_cols + old_for_new_cols + ["installed_base"]

    ckpt("Adding natural-log ln(1+x) for subsidy, their 1y lags, old_for_new, and installed_base")
    merged = add_log_cols(merged, sub_cols_for_ln, prefix="ln_")

    ckpt("Saving")
    merged.to_csv(out_path, index=False)
    print(f"\n[RESULT] saved → {out_path}")
    print(f"[OK] final shape: {merged.shape}")

if __name__ == "__main__":
    main()
