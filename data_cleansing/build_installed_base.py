import os
import pandas as pd
import numpy as np
import sys

def safe_read_csv(path, **kwargs):
    if not os.path.exists(path):
        sys.exit(f"[FATAL] File not found: {path}")
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        sys.exit(f"[FATAL] Failed to read {path}: {e}")

def read_installed_base(path):
    print("\n[CHECKPOINT] Reading installed_base...")
    df = safe_read_csv(path, header=[0, 1], index_col=0)
    df.index.name = "date"

    lev0 = list(df.columns.get_level_values(0))
    lev1 = list(df.columns.get_level_values(1))
    for i in range(len(lev0)):
        if pd.isna(lev0[i]) or str(lev0[i]).startswith("Unnamed"):
            lev0[i] = lev0[i - 1]
    df.columns = pd.MultiIndex.from_tuples(zip(lev0, lev1), names=["county", "district"])

    print(f"[OK] installed_base loaded: {df.shape[0]} months × {df.shape[1]} county/districts")
    return df

def build_dataset(scooter_path, installed_path, output_path):
    # --- scooter ---
    print("\n[CHECKPOINT] Reading scooter data...")
    df1 = safe_read_csv(scooter_path)

    # use existing 'date' if you've already created it in the CSV
    if "date" in df1.columns:
        df1["date"] = pd.to_datetime(df1["date"], errors="coerce").dt.normalize()
    elif "app_date" in df1.columns:
        df1["date"] = pd.to_datetime(df1["app_date"], errors="coerce").dt.normalize()
    else:
        sys.exit("[FATAL] Neither 'date' nor 'app_date' exists in scooter CSV")

    if df1["date"].isna().any():
        print(f"[WARN] {df1['date'].isna().sum()} scooter rows had invalid dates → dropped")
        df1 = df1.dropna(subset=["date"])

    # clean county/district
    for c in ["county", "district"]:
        if c not in df1.columns:
            sys.exit(f"[FATAL] '{c}' column missing in scooter CSV")
        df1[c] = df1[c].astype(str).str.strip()

    # drop any existing installed_base / ln_installed_base so merge doesn't create _x/_y
    for col in ["installed_base", "ln_installed_base"]:
        if col in df1.columns:
            print(f"[WARN] Dropping '{col}' from scooter data (will replace from PBGN)")
            df1 = df1.drop(columns=[col])

    print(f"[OK] scooter data loaded: {df1.shape[0]} rows × {df1.shape[1]} cols")

    # --- installed_base ---
    df2_wide = read_installed_base(installed_path)
    df2 = (
        df2_wide
        .stack(level=[0, 1], future_stack=True)
        .rename("installed_base")
        .reset_index()
    )
    df2["date"] = pd.to_datetime(df2["date"], errors="coerce").dt.normalize()
    df2["installed_base"] = pd.to_numeric(df2["installed_base"], errors="coerce")
    for c in ["county", "district"]:
        df2[c] = df2[c].astype(str).str.strip()

    # --- merge ---
    print("\n[CHECKPOINT] Merging...")
    df = df1.merge(df2, on=["date", "county", "district"], how="left")

    # validate merge
    missing_base = df["installed_base"].isna().sum()
    if missing_base > 0:
        sys.exit(f"[FATAL] {missing_base} rows missing installed_base after merge")

    print(f"[OK] merge done: {df.shape[0]} rows total")

    # --- ln_installed_base ---
    df["ln_installed_base"] = np.log(df["installed_base"] + 1)

    # --- drop app_date ---
    if "app_date" in df.columns:
        df = df.drop(columns=["app_date"])
        print("[OK] Dropped 'app_date' column")

    # --- save output ---
    df.to_csv(output_path, index=False)
    print(f"[RESULT] Saved merged dataset → {output_path}")

    return df

if __name__ == "__main__":
    scooter_path = os.path.expanduser("../input/scooter_no_scooter.csv")
    installed_path = os.path.expanduser("../input/PBGN_install_base.csv")
    output_path = os.path.expanduser("../input/scooter_PBGN.csv")

    df = build_dataset(scooter_path, installed_path, output_path)
    print("\n[RESULT] Sample output:")
    print(df[["county", "district", "date", "installed_base", "ln_installed_base"]].head())
