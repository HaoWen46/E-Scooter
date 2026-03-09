# -*- coding: utf-8 -*-
"""
Generate monthly station CSVs following batterystation_month.py logic.

Status logic is copied verbatim from batterystation_month.py.
Coordinates are sourced from four files (merged by station Name):
  1. gostations_with_websites.csv  — active stations (vmid + lat/lon)
  2. gostation_deleted.csv         — removed stations (vmid from url, lat/lon from map_url)
  3. gostation_removed_from_construction.csv — same structure as deleted
  4. station_big.xlsx              — additional vmid + coords for historically removed stations

Output: stations_monthly/stations_YYYY_MM.csv with columns: vmid, Longitude, Latitude
"""

import os
import re
import pandas as pd

IN_CLEANED  = "gostations_cleaned.csv"
IN_WEBSITES = "gostations_with_websites.csv"
IN_DELETED  = "gostation_deleted.csv"
IN_REMOVED  = "gostation_removed_from_construction.csv"
IN_BIG      = "station_big.xlsx"

OUT_DIR    = "stations_monthly"
START_YM   = (2019, 1)
END_YM     = (2023, 12)
SIX_CITIES = {'臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市'}


def extract_coords(map_url):
    """Extract (lat, lon) from Google Maps URL ll=lat,lon parameter."""
    if not isinstance(map_url, str):
        return None, None
    m = re.search(r'll=([\d.]+),([\d.]+)', map_url)
    if not m:
        return None, None
    return float(m.group(1)), float(m.group(2))


def vmid_from_url(url):
    """Extract vmid from URL like /gostation/vmid/{uuid}/"""
    if not isinstance(url, str) or '/vmid/' not in url:
        return None
    return url.split('/vmid/')[1].split('/')[0]


def build_coords():
    """
    Build Name → (vmid, Longitude, Latitude) from all sources.
    Priority: websites > deleted/removed > station_big.
    """
    rows = []

    # Source 1: active stations
    df_w = pd.read_csv(IN_WEBSITES)
    df_w['vmid'] = df_w['vmid'].fillna(df_w['VmId'])
    for _, r in df_w[['Name','vmid','Longitude','Latitude']].iterrows():
        rows.append({'Name': r['Name'], 'vmid': r['vmid'],
                     'Longitude': r['Longitude'], 'Latitude': r['Latitude'],
                     'priority': 0})

    # Sources 2 & 3: deleted / removed-from-construction stations
    for fpath in (IN_DELETED, IN_REMOVED):
        df = pd.read_csv(fpath)
        df['vmid'] = df['url'].apply(vmid_from_url)
        coords = df['map_url'].apply(extract_coords)
        df['Latitude']  = [c[0] for c in coords]
        df['Longitude'] = [c[1] for c in coords]
        for _, r in df[['station_name','vmid','Longitude','Latitude']].iterrows():
            rows.append({'Name': r['station_name'], 'vmid': r['vmid'],
                         'Longitude': r['Longitude'], 'Latitude': r['Latitude'],
                         'priority': 1})

    # Source 4: station_big.xlsx — additional vmid + coords (covers many deleted stations)
    # Some names in station_big differ from gostations_cleaned (spaces, dropped prefixes, swapped A/B).
    # Map cleaned names → station_big names so the join works.
    BIG_NAME_ALIASES = {
        '家樂福便利購台中南屯店站':    '家樂福便利購台中南屯店站A',
        '7-ELEVEN 鳳麟店站B':         '7-ELEVEN鳳麟店站A',   # A/B labels are swapped in station_big
        '7-ELEVEN 鳳麟店站A':         '7-ELEVEN鳳麟店站B',
        'PGO 大同延平店站':            'PGO大同延平店站',
        'Gogoro 大雅學府門市站':       '大雅學府門市站',
        'Gogoro 桃園服務中心站':       'Gogoro桃園服務中心站',
        'PGO 高雄明誠店站':            '高雄明誠店站',
        'Gogoro 光華八德店站':         'Gogoro光華八德店站',
        'Gogoro 楊梅騏轟服務中心站':   '楊梅騏轟服務中心站',
        'Gogoro 土城裕民門市站B':      'Gogoro土城裕民門市站B',
        'Gogoro 土城裕民門市站A':      'Gogoro土城裕民門市站A',
        'Gogoro 土城裕民店站':         'Gogoro土城裕民門市站B',  # same location
        'HiLife 北縣五春店站':         'HiLife北縣五春店站',
        'HiLife 三民鼎金店站':         'HiLife三民鼎金店站',
        'Gogoro新店北新服務中心站':    'Gogoro 新店北新服務中心站',
        'Gogoro三民建工門市站':        'Gogoro 三民建工門市站',
        '7-ELEVEN 城運店站':           '7-ELEVEN城運店站',
        'Gogoro 圓山服務中心站':       'Gogoro圓山服務中心站',
        'Gogoro 三重集成服務中心站':   'Gogoro三重集成服務中心站',
        'Impact Hub 站':               'Impact Hub站',
        '7-ELEVEN 環金店站':           '7-ELEVEN環金店站',
        '7-ELEVEN 海裕店站C':          '7-ELEVEN海裕店站C',
        'eReady 新北三重店站B':        'eReady新北三重店站B',
        '應安168 華視文教一場停車場站A': '華視文教一場停車場站A',
        'YAMAHA 中和連城店站A':        'YAMAHA中和連城店站A',
        'YAMAHA 中和連城店站B':        'YAMAHA中和連城店站B',
        'PGO 桃園民生店站':            'PGO桃園民生店站',
        '7-ELEVEN 蘆海店站':           '7-ELEVEN蘆海店站',
    }
    df_big = pd.read_excel(IN_BIG)
    df_big.columns = ['vmid', 'Name', 'address', 'hours', 'Latitude', 'Longitude', 'image']
    # Add alias rows: duplicate each aliased entry under the cleaned name
    alias_rows = []
    big_name_to_row = df_big.set_index('Name')
    for cleaned_name, big_name in BIG_NAME_ALIASES.items():
        if big_name in big_name_to_row.index:
            r = big_name_to_row.loc[big_name]
            alias_rows.append({'Name': cleaned_name, 'vmid': r['vmid'],
                                'Longitude': r['Longitude'], 'Latitude': r['Latitude'],
                                'priority': 2})
    rows.extend(alias_rows)
    for _, r in df_big[['Name','vmid','Longitude','Latitude']].iterrows():
        rows.append({'Name': r['Name'], 'vmid': r['vmid'],
                     'Longitude': r['Longitude'], 'Latitude': r['Latitude'],
                     'priority': 2})

    coords_df = pd.DataFrame(rows)
    coords_df = coords_df.dropna(subset=['vmid','Longitude','Latitude'])
    # Keep best (lowest priority) entry per Name
    coords_df = (coords_df.sort_values('priority')
                           .drop_duplicates('Name')
                           .drop(columns='priority'))
    return coords_df


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # ── Setup (mirrors batterystation_month.py preamble) ─────────────────────
    df_battery = pd.read_csv(IN_CLEANED)
    df_battery['id'] = df_battery.index
    df_battery = df_battery.rename(columns={'status': 'current_status'})

    # ── Status logic copied verbatim from batterystation_month.py ────────────

    # Create DataFrames for activation and deactivation events
    activate_events = pd.melt(df_battery, id_vars=['id'],
                      value_vars=['activate_time1', 'activate_time2', 'activate_time3', 'activate_time4'],
                      var_name='event', value_name='datetime')
    remove_events = pd.melt(df_battery, id_vars=['id'],
                      value_vars=['remove_time1', 'remove_time2'],
                      var_name='event', value_name='datetime')

    # Concatenate activation and deactivation DataFrames
    events = pd.concat([activate_events, remove_events])

    # Convert datetime to quarter
    events['month'] = pd.to_datetime(events['datetime']).dt.to_period('M')
    events['datetime'] = pd.to_datetime(events['datetime'])

    # Sort events by id and datetime
    events = events.sort_values(by=['id', 'datetime'])

    # Group by 'id' and 'quarter' and extract the first and last observations
    firstlast = events.groupby(['id', 'month']).agg(
        first=('event', 'first'),
        last=('event', 'last')
    ).reset_index()

    # Find the status in the beginning of the quarter and the end of the quarter
    firstlast['start_status'] = (firstlast['first'].str.startswith(('remove'))).astype(int)
    # if the first event is remove, then the station must be initially active
    firstlast['end_status'] = (firstlast['last'].str.startswith('activate')).astype(int)

    # Create a DataFrame with all quarters for each id
    all_quarters = pd.DataFrame(
        {'month': pd.period_range(start=events['month'].min(), end=events['month'].max(), freq='M')}
    )
    # Repeat each id for all quarters
    all_quarters = all_quarters.assign(key=1).merge(pd.DataFrame({'key': [1]*len(events['id'].unique()), 'id': events['id'].unique()})).drop('key', axis=1)

    # Merge with events DataFrame to fill missing quarters
    stationstatus = pd.merge(all_quarters, firstlast[['month', 'id', 'start_status', 'end_status']], on=['month', 'id'], how='left')

    # Fill the status by start and end status
    stationstatus['end_status'] = stationstatus.groupby('id')['end_status'].ffill()
    stationstatus['start_status'] = stationstatus.groupby('id')['start_status'].bfill()
    stationstatus['status'] = stationstatus['end_status'].fillna(stationstatus['start_status'])

    stationstatus = stationstatus[['id', 'month', 'status']]

    # merging back to the battery station info
    stationstatus = stationstatus.merge(df_battery[['id', 'Name', 'current_status', 'county']], on='id', how='left')

    # Fill the missing status with the current status
    cond = stationstatus['current_status'] == "已啟用"
    stationstatus['status'] = stationstatus['status'].fillna(cond.map({True: 1, False: 0}))

    # ── Filter to 6 cities only ───────────────────────────────────────────────
    stationstatus = stationstatus[stationstatus['county'].isin(SIX_CITIES)]

    # ── Join vmid + coordinates via Name (all sources) ────────────────────────
    coords = build_coords()
    stationstatus = stationstatus.merge(coords, on='Name', how='left')
    stationstatus = stationstatus.dropna(subset=['vmid', 'Longitude', 'Latitude'])

    # ── Write monthly CSVs ────────────────────────────────────────────────────
    print(f"Writing monthly CSVs to {OUT_DIR}/...")
    for y in range(START_YM[0], END_YM[0] + 1):
        for m in range(1, 13):
            if (y, m) < START_YM or (y, m) > END_YM:
                continue
            period = pd.Period(f"{y}-{m:02d}", freq='M')
            month_data = stationstatus[
                (stationstatus['month'] == period) &
                (stationstatus['status'] == 1)
            ].drop_duplicates('vmid')

            out_path = os.path.join(OUT_DIR, f"stations_{y:04d}_{m:02d}.csv")
            month_data[['vmid', 'Longitude', 'Latitude']].to_csv(out_path, index=False)
            print(f"  {out_path}: {len(month_data)} stations")

    print("Done.")


if __name__ == "__main__":
    main()
