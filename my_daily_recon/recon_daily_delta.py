import os
import awswrangler as wr
import pandas as pd
from configparser import ConfigParser
import numpy as np
import logging
from base_main import BaseMain

LOG = logging.getLogger(__name__)


class ReconDailyDelta(BaseMain):
    def __init__(self):
        super().__init__()
        constants = ConfigParser()
        constants.read("my_daily_recon/inputs/my_daily_recon.ini")
        # self.client = constants.get(self.args.profile, "CLIENT")
        # self.env = constants.get(self.args.profile, "ENVIRONMENT")
        # self.region = constants.get(self.args.profile, "REGION")
        self.custodian_folder = constants.get(self.args.profile.upper(), "CUSTODIAN_FOLDER")

    def add_extra_args(self):
        self.parser.add_argument(
            "-p",
            "--profile",
            dest="profile",
            type=str,
            required=True,
            help="Client profile to be used",
        )

    def get_recent_recon_files(self):
        """
        Get the most recent reconciliation files from S3.
        """
        outpuyt_dir = "my_daily_recon/outputs/"
        files = os.listdir(outpuyt_dir)
        files = [file for file in files if "breaks_only.csv" in file]
        files.sort()
        current = pd.read_csv(os.path.join(outpuyt_dir, files[-1]))
        previous = pd.read_csv(os.path.join(outpuyt_dir, files[-2]))
        return current, previous

    def compare_recon_files(self, current, previous):
        full_recon = current.merge(
            previous, on=["Account ID", "Security ID"], indicator=True, how="outer"
        )
        delta_new = full_recon.loc[
            full_recon["_merge"] == "left_only", ["Account ID", "Security ID"]
        ]
        delta_fix = full_recon.loc[
            full_recon["_merge"] == "right_only", ["Account ID", "Security ID"]
        ]
        new_breaks = current.merge(delta_new, how="inner")
        fixed_breaks = previous.merge(delta_fix, how="inner")
        return new_breaks, fixed_breaks

    def get_transactions(self, new_breaks):
        logging.info("Fetching transaction files from S3...")
        transaction_files = wr.s3.list_objects(
            self.custodian_folder, suffix="Transaction.csv"
        )
        transaction_files = [
            file for file in transaction_files if file.split("/")[5] > "20250318"
        ]
        logging.info(f"Found {len(transaction_files)} transaction files.")
        dfl = []
        filtering_df = new_breaks[["Account ID", "Security ID"]].rename(
            columns={"Account ID": "AccountCode", "Security ID": "SecurityID1"}
        )
        
        counter = 0

        for file in transaction_files:
            counter += 1
            logging.info(f"Processing file {counter}/{len(transaction_files)}: {file}")
            df = wr.s3.read_csv(file)
            df = df.merge(filtering_df, how="inner")
            df["source"] = file.split("/")[5]
            dfl.append(df)

        transactions = (
            pd.concat(dfl, ignore_index=True)
            .sort_values(by=["AccountCode", "SecurityID1", "source"])
            .reset_index(drop=True)
        )
        return transactions

    def get_positions(self, new_breaks):
        logging.info("Fetching position files from S3...")
        position_files = wr.s3.list_objects(
            self.custodian_folder, suffix="Position.csv"
        )
        position_files = [
            file for file in position_files if file.split("/")[5] > "20250318"
        ]
        logging.info(f"Found {len(position_files)} position files.")
        dfl = []
        filtering_df = new_breaks[["Account ID", "Security ID"]].rename(
            columns={"Account ID": "AccountCode", "Security ID": "SecurityID"}
        )
        
        counter = 0

        for file in position_files:
            counter += 1
            logging.info(f"Processing file {counter}/{len(position_files)}: {file}")
            df = wr.s3.read_csv(file)
            df = df.merge(filtering_df, how="inner")
            df["source"] = file.split("/")[5]
            dfl.append(df)

        positions = (
            pd.concat(dfl, ignore_index=True)
            .sort_values(by=["AccountCode", "SecurityID", "source"])
            .reset_index(drop=True)
        )
        return positions

    def run(self):
        current, previous = self.get_recent_recon_files()
        new_breaks, fixed_breaks = self.compare_recon_files(current, previous)
        today = new_breaks.loc[0, "Date"]
        new_breaks.to_csv(f"my_daily_recon/outputs/{today}_new_breaks.csv", index=False)
        fixed_breaks.to_csv(f"my_daily_recon/outputs/{today}_fixed_breaks.csv", index=False)
        transactions = self.get_transactions(new_breaks)
        transactions.to_csv(
            f"my_daily_recon/outputs/{today}_new_break_transactions.csv", index=False
        )
        positions = self.get_positions(new_breaks)
        positions.to_csv(
            f"my_daily_recon/outputs/{today}_new_break_positions.csv", index=False
        )



if __name__ == "__main__":
    recon_daily_delta = ReconDailyDelta()
    recon_daily_delta.run()
    LOG.info("Reconciliation daily delta completed successfully.")
