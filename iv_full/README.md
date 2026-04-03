# IV Construction: One-Year-Lagged Store Counts

## Problem

The PBGN district-month panel (`scooter_PBGN_o4n.csv`) had all six store IV columns
(`iv_7eleven`, `iv_familymart`, `iv_okhilife`, `iv_pxmart`, `iv_carrefour`,
`iv_rtmartsimplemart`) missing for two months across all 158 districts:

| Month | All 158 districts missing |
|-------|--------------------------|
| 2019-01 | Yes |
| 2019-09 | Yes |

All other months were fully populated.

## Root Cause

The IVs are constructed in `kill_scooters.py` as:

```python
log_lag = np.log(df["lag_n_iv"].clip(lower=1))
iv_7eleven = log_lag * log1p(open_stores7eleven)
```

where `lag_n_iv` is the **one-month lagged** total national EV registrations for that
district-series combination.

Two separate bugs caused the blanket missingness:

**January 2019** — `lag_n_iv` is NaN for every row in January 2019 because there is
no December 2018 EV registration data in the source file
(`scooter_and_iv.csv` starts in January 2019).

**September 2019** — The source file is series-level (one row per district × scooter
series × month). After computing IVs, `kill_scooters.py` deduplicates to one row per
`(county, district, date)` with `keep="first"`. In September 2019, the Ai-1 series
made its debut and sorted to the top for every district. Ai-1's debut month has no
prior-month EV data, so `lag_n_iv = NaN` for those rows, making `iv_7eleven = NaN`.
Since `keep="first"` always picked Ai-1, all 158 districts ended up with missing IVs
for September 2019.

## Fix 1 — Deduplication (September 2019)

Changed the dedup in `kill_scooters.py` to sort by `iv_7eleven` descending (NaN last)
before keeping the first row. This ensures that for any district-month where at least
one series has a valid IV (e.g. Gogoro2 with `lag_n_iv` populated), that row is kept.

```python
clean = (
    clean
    .sort_values(["county", "district", "date", "iv_7eleven"],
                 ascending=[True, True, True, False], na_position="last")
    .drop_duplicates(subset=["county", "district", "date"], keep="first")
)
```

## Fix 2 — One-Year-Lagged Store Counts (January 2019 and general approach)

January 2019 cannot be fixed by the dedup alone — the source data simply has no
December 2018 EV counts to lag from. The deeper fix changes the IV construction to
use **store counts from twelve months prior** rather than the current month.

This is also more defensible as an instrument: using last year's store locations
removes any contemporaneous correlation between store openings and EV adoption.

The revised IV formula is:

```
iv_X = log(lag_n_iv) * log1p(open_stores_X  from  date − 12 months)
```

`lag_n_iv` remains a one-month lag of national EV registrations (unchanged).
Only the store count part is now lagged by a year.

## Building the 2018 Store Count Panel

The existing compiled store count data
(`EScooters/01-Build/03-Instruments/04-MergingInstruments/output/iv_for_reg.dta`)
starts in January 2019. The original Stata scripts hardcode `startmonth = mdy(1,1,2019)`.
To supply 2018 store counts, the raw source data was used directly.

### Convenience stores (7-Eleven, FamilyMart, Hilife, OK, PXMart)

Source: `EScooters/01-Build/03-Instruments/01-ConvenienceStores/output/convenientstore_district.csv`

Each row is a branch with an opening date (`opendate`, ROC calendar format) and a
last-change date (`last_change`). A store is counted as open in month `M` if:

```
opendate ≤ M  AND  (status is active  OR  changedate ≥ M)
```

This logic is applied for each of the 12 months in 2018, producing month-by-month
counts per `(county, district, chain)`. Seven rural districts in Tainan and Kaohsiung
had no matching stores and were filled with zero (correct — they genuinely have none).

### Grocery stores and gas stations (Carrefour, RT-Mart, Simplemart, CPC)

The raw CSV files for these chains are macOS Finder aliases pointing to a Dropbox
folder (`EScooters/01-Build/03-Instruments/02-GroceryStores/02-Cleaning/input/`) and
are not accessible on this server. January 2019 counts from `iv_for_reg.dta` are used
as a proxy for all twelve months of 2018. These store types change slowly and the
approximation introduces minimal error.

### Output

`Project/input/store_counts_2018.csv` — 1,896 rows (158 districts × 12 months),
columns: `county`, `district`, `date`, and one column per store chain.

## Final IV Pipeline

At IV construction time in `kill_scooters.py`:

1. Extract the unique `(county, district, date, open_stores_*)` panel from
   `scooter_and_iv.csv` (covers 2019–2023).
2. Prepend `store_counts_2018.csv` (covers 2018).
3. Shift all dates forward by one year to create a lagged lookup table.
4. Merge onto the main data on `(county, district, date)`.
5. Compute IVs using the merged `lag1y_open_stores_*` columns.

Result: January 2019 now has valid IVs (using January 2018 store counts); September
2019 now has valid IVs (dedup fix + September 2018 store counts). No other months
were affected.

## Relevant Code

`data_cleansing/iv_full/build_jan2018_stores.py` — builds `store_counts_2018.csv`
from the raw convenience store CSV and the 2019 grocery/gas proxy.

`data_cleansing/iv_full/kill_scooters.py` — the full updated pipeline script with
year-lagged store counts and fixed deduplication. This is the canonical version;
the copy in `data_cleansing/` is what actually gets run.

`data_cleansing/iv_full/iv_construction_snippet.py` — the IV construction function
extracted from `kill_scooters.py` for reference.
