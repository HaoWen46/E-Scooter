import os
import numpy as np
import pandas as pd

def read_pivot_csv(path, index_col=0, county_name="county", district_name="district"):
    df = pd.read_csv(path, header=[0,1], index_col=index_col)
    lev0 = df.columns.get_level_values(0).to_list()
    lev1 = df.columns.get_level_values(1).to_list()
    for i in range(len(lev0)):
        if pd.isna(lev0[i]) or str(lev0[i]).startswith("Unnamed"):
            lev0[i] = lev0[i-1]
    df.columns = pd.MultiIndex.from_tuples(zip(lev0, lev1), names=[county_name, district_name])
    return df

# === paths ===
path1 = os.path.expanduser("../input/scooter_and_iv.csv")
path2 = os.path.expanduser("../input/initial_register.csv")
outdir = os.path.expanduser("../output/")
os.makedirs(outdir, exist_ok=True)

df1 = pd.read_csv(path1)
df2 = read_pivot_csv(path2)

# === preprocessing ===
df1["app_date"] = df1["app_date"].str.title()
df1["Date"] = pd.to_datetime(df1["app_date"], format="%d%b%Y", errors="coerce")

# === filter by PBGN flag directly ===
pbgn_df = df1.loc[df1["pleague"] == "PBGN"].copy()

# === monthly totals and PBGN counts ===
totals_m = df1.groupby(["county", "district", "Date"])["nscooter"].sum()
pbgn_m = pbgn_df.groupby(["county", "district", "Date"])["nscooter"].sum()

# === pivot for ordering ===
pivot_m = (pbgn_m / totals_m).reset_index(name="ratio").pivot(
    index="Date", columns=["county", "district"], values="ratio"
)

# === cumulative ratios function ===
def cumulative_ratios(totals, pbgn, pivot, name):
    df = pd.concat([totals.rename("total"), pbgn.rename("pbgn")], axis=1).fillna(0).reset_index()
    frames = []
    n_entries = len(pivot.index)

    for x in range(1, n_entries + 1):
        levels = list(pivot.index)[:x]
        timecol = [c for c in df.columns if c not in ["county","district","total","pbgn"]][0]
        mask = df[timecol].isin(levels)
        agg = df.loc[mask].groupby(["county","district"])[["pbgn","total"]].sum()

        ratio = np.where(agg["total"] == 0, np.nan, agg["pbgn"]/agg["total"])
        ratio = pd.Series(ratio, index=agg.index).reindex(df2.columns)

        row_df = ratio.to_frame().T
        row_df.index = [f"first_{x}_{name}"]
        frames.append(row_df.round(4))

    combined = pd.concat(frames,axis=0)
    combined.to_csv(os.path.join(outdir,f"ratios_first_all_{name}.csv"),encoding="utf-8-sig")
    combined.to_excel(os.path.join(outdir,f"ratios_first_all_{name}.xlsx"),float_format="%.4f")

    installed_combined = combined.mul(df2.iloc[0],axis=1).round(0)
    installed_combined.to_csv(os.path.join(outdir,f"installed_base_first_all_{name}.csv"),encoding="utf-8-sig")
    installed_combined.to_excel(os.path.join(outdir,f"installed_base_first_all_{name}.xlsx"),float_format="%.0f")

    return combined, installed_combined

# === run monthly only ===
ratios_m_out, installed_m = cumulative_ratios(totals_m, pbgn_m, pivot_m, "m")

# === print summary ===
print("\n=== Monthly ratios (PBGN only) ===")
print(ratios_m_out)
print("\n=== Monthly installed base (PBGN only) ===")
print(installed_m)
print("\nDone. Monthly results saved to", outdir)
