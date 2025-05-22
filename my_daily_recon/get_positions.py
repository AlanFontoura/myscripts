import os
import pandas as pd
import numpy as np
import awswrangler as wr

output_folder = "my_daily_recon/outputs"
output_files = os.listdir(output_folder)
output_files.sort()
most_recent_recon = pd.read_csv(f"{output_folder}/{output_files[-1]}")
most_recent_recon.loc[most_recent_recon['Security ID'] == 'USD', 'Security ID'] = 'BC36B21F-D673-492F-8D1D-30956550D237'

unit_breaks = most_recent_recon.loc[
    ~most_recent_recon["Units - Reconciled"], ["Account ID", "Security ID"]
]
mv_breaks = most_recent_recon.loc[
    ~most_recent_recon["Market Value - Reconciled"], ["Account ID", "Security ID"]
]

s3_files = wr.s3.list_objects("s3://d1g1t-custodian-data-us-east-1/apx/gresham")
position_files = [file for file in s3_files if "Position.csv" in file]

units_df = pd.DataFrame()
mv_df = pd.DataFrame()
dated_folder_set = []

for file in position_files:
    dated_folder = file.split("/")[5]
    if dated_folder >= "20250319":
        print(f"Processing dated folder: {dated_folder}")
        # Read the CSV file into a DataFrame
        df = wr.s3.read_csv(file)
        df = (
            df[["AccountCode", "SecurityID", "Current", "MV_Local"]]
            .rename(
                columns={
                    "AccountCode": "Account ID",
                    "SecurityID": "Security ID",
                    "Current": "Units",
                    "MV_Local": "Market Value",
                }
            )
            .groupby(["Account ID", "Security ID"])
            .sum()
            .reset_index()
        )
        units = unit_breaks.merge(
            df[["Account ID", "Security ID", "Units"]],
            on=["Account ID", "Security ID"],
            how="left",
        ).rename(columns={"Units": dated_folder})
        mv = mv_breaks.merge(
            df[["Account ID", "Security ID", "Market Value"]],
            on=["Account ID", "Security ID"],
            how="left",
        ).rename(columns={"Market Value": dated_folder})

        if not units_df.empty:
            units_df = pd.merge(
                units_df,
                units[["Account ID", "Security ID", dated_folder]],
                on=["Account ID", "Security ID"],
                how="outer",
            )
        else:
            units_df = units[["Account ID", "Security ID", dated_folder]]
        if not mv_df.empty:
            mv_df = pd.merge(
                mv_df,
                mv[["Account ID", "Security ID", dated_folder]],
                on=["Account ID", "Security ID"],
                how="outer",
            )
        else:
            mv_df = mv[["Account ID", "Security ID", dated_folder]]
            
        dated_folder_set.append(dated_folder)
    else:
        # Skip older dated folders
        print(f"Skipping dated folder: {dated_folder} as it is older than 20250319")

units_mask = units_df[dated_folder_set].isin([0, np.nan]).all(axis=1)
mv_mask = mv_df[dated_folder_set].isin([0, np.nan]).all(axis=1)

units_df = units_df[~units_mask].fillna(0).sort_values(["Account ID", "Security ID"])
mv_df = mv_df[~mv_mask].fillna(0).sort_values(["Account ID", "Security ID"])

units_df.loc[units_df['Security ID'] == 'BC36B21F-D673-492F-8D1D-30956550D237', 'Security ID'] = 'USD'
mv_df.loc[mv_df['Security ID'] == 'BC36B21F-D673-492F-8D1D-30956550D237', 'Security ID'] = 'USD'

units_df.to_csv('my_daily_recon/outputs/units.csv', index=False)
mv_df.to_csv('my_daily_recon/outputs/mv.csv', index=False)

pd.read_pickle('/mnt/g/My Dri')