import pandas as pd
import numpy as np

input_path = "../input/scooter_and_iv.csv"
output_path = "../input/scooter_no_scooter.csv"

df = pd.read_csv(input_path)

# ------------------------------------------------------------
# BUILD CLEAN MONTHLY DATE & REMOVE app_date
# ------------------------------------------------------------
if "app_date" not in df.columns:
    raise ValueError("app_date missing — cannot build date column")

df["date"] = pd.to_datetime(df["app_date"], errors="coerce")

bad = df["date"].isna().sum()
if bad > 0:
    print(f"[WARN] Dropping {bad} rows with invalid app_date")
    df = df.dropna(subset=["date"])

df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

df = df.drop(columns=["app_date"])
print("[OK] app_date removed; using clean monthly 'date' column")

# ------------------------------------------------------------
# BUILD JUST THE 6 IVs YOU NEED
# Requires: lag_n_iv already present in CSV
# ------------------------------------------------------------
if "lag_n_iv" not in df.columns:
    raise ValueError("lag_n_iv missing — cannot build IVs")

log_lag = np.log(df["lag_n_iv"].clip(lower=1))  # avoid log(0)

def log1p_safe(x):
    return np.log(x.clip(lower=0) + 1)

df["iv_7eleven"] = log_lag * log1p_safe(df["open_stores7eleven"])
df["iv_familymart"] = log_lag * log1p_safe(df["open_storesfamilymart"])
df["iv_okhilife"] = log_lag * log1p_safe(df["open_storesOK"] + df["open_storeshilife"])
df["iv_pxmart"] = log_lag * log1p_safe(df["open_storespxmart"])
df["iv_carrefour"] = log_lag * log1p_safe(df["open_storescarrefour"])
df["iv_rtmartsimplemart"] = log_lag * log1p_safe(df["open_storesrtmart"] + df["open_storessimplemart"])

# ------------------------------------------------------------
# KEEP LIST
# ------------------------------------------------------------
keep = [
    "county","district","date",
    "nstation","ln_nstation",
    "ln_lag_natsta_excl_county",

    # logs for stores
    "ln_7eleven","ln_familymart","ln_other_convenience",
    "ln_pxmart","ln_carrefour","ln_other_grocery",

    # raw store counts
    "open_stores7eleven","open_storesfamilymart","open_storeshilife",
    "open_storesOK","open_storespxmart","open_storescarrefour",
    "open_storesrtmart","open_storessimplemart",

    # DEMOGRAPHICS
    "hh_size","popdensity","median_inc","pct_female",
    "pct_between_20_29","pct_between_30_39","pct_between_40_49",
    "pct_between_50_59","pct_above_60","pct_less_hs","pct_above_college",
    "pct_executive","pct_professional","pct_technician",
    "pct_administrative","pct_service","pct_skilled",
    "pct_machinery","pct_laborer",

    # THE 6 IVs YOU WANT
    "iv_7eleven",
    "iv_familymart",
    "iv_okhilife",
    "iv_pxmart",
    "iv_carrefour",
    "iv_rtmartsimplemart",

    "disid"
]

existing_keep = [c for c in keep if c in df.columns]
missing = [c for c in keep if c not in df.columns]
dropped = [c for c in df.columns if c not in existing_keep]

print("\nKEEPING:")
for c in existing_keep:
    print(" ✓", c)

print("\nMISSING (FYI only):")
for c in missing:
    print(" ⚠", c)

print("\nDROPPING:")
for c in dropped:
    print(" ✗", c)

clean = df[existing_keep].copy()

# ------------------------------------------------------------
# DROP DUPLICATES ON PRIMARY KEYS
# ------------------------------------------------------------
dups = clean.duplicated(subset=["county", "district", "date"], keep=False)
n_dups = dups.sum()

if n_dups > 0:
    print(f"\n[WARN] Found {n_dups} duplicated (county, district, date) rows.")
    print("       Keeping first occurrence per key.")
    clean = clean.drop_duplicates(
        subset=["county", "district", "date"],
        keep="first"
    )

# ------------------------------------------------------------
# SORT PANEL
# ------------------------------------------------------------
clean = clean.sort_values(["county", "district", "date"]).reset_index(drop=True)
print("[OK] Sorted by county, district, date")

clean.to_csv(output_path, index=False)
print("\nSaved cleaned file to:", output_path)
