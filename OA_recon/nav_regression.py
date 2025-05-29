# import logging
import os
import pandas as pd
from pathlib import Path

from base_main import BaseMain
from utils import logger_setup, NoResponseError, ChartTableFormatter

logger = logger_setup("logs", "gresham_recon")


class NAVRegression(BaseMain):

    def __init__(self):
        BaseMain.__init__(self)

    def add_extra_args(self):
        self.parser.add_argument(
            "-lv",
            "--level",
            dest="level",
            default="accounts",
            type=str,
            required=False,
            help="Hierarchy level to download data (either accounts, clients or households)",
        )

        self.parser.add_argument(
            "-be",
            "--base_env",
            dest="base_env",
            type=str,
            required=True,
            help="Base environment name (either client name or a greek)",
        )

        self.parser.add_argument(
            "-te",
            "--target_env",
            dest="target_env",
            type=str,
            required=True,
            help="Target environment name (either client name or a greek)",
        )

        self.parser.add_argument(
            "-bv",
            "--base_version",
            dest="base_version",
            type=str,
            required=True,
            help="Base version (e.g., v5.0, v5.1, etc.)",
        )

        self.parser.add_argument(
            "-tv",
            "--target_version",
            dest="target_version",
            type=str,
            required=True,
            help="Target version (e.g., v5.0, v5.1, etc.)",
        )

    def read_files(self, env, lvl):
        folder = f"OA_recon/outputs/{env}/{lvl}"
        files = [file for file in os.listdir(folder) if ".csv" in file]
        if not files:
            raise NoResponseError(f"No CSV files found in {folder}")

        dataframes = []
        for file in files:
            df = pd.read_csv(os.path.join(folder, file))
            dataframes.append(df)
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df

    def return_columns(self, cols):
        base_cols = ["Date", "Entity ID"]
        analytic_cols = cols[2:]
        for col in analytic_cols:
            for suffix in [
                self.args.base_version,
                self.args.target_version,
                "Diff",
                "Reconciled",
            ]:
                base_cols.append(f"{col} - {suffix}")
        return base_cols

    def compare_versions(self):
        base_data = self.read_files(self.args.base_env, self.args.level)
        target_data = self.read_files(self.args.target_env, self.args.level)
        cols = base_data.columns
        if base_data.empty or target_data.empty:
            raise NoResponseError(
                "One of the datasets is empty. Cannot perform comparison."
            )
        comparison = pd.merge(
            base_data,
            target_data,
            on=["Date", "Entity ID"],
            suffixes=[f" - {self.args.base_version}", f" - {self.args.target_version}"],
            how="outer",
        ).fillna(0)

        for col in cols[2:]:
            comparison[f"{col} - Diff"] = (
                comparison[f"{col} - {self.args.target_version}"]
                - comparison[f"{col} - {self.args.base_version}"]
            )
            comparison[f"{col} - Reconciled"] = abs(comparison[f"{col} - Diff"]) < 0.01
        comparison = comparison[self.return_columns(cols)]
        return comparison

    def filter_recon(self, recon):
        if recon.empty:
            logger.error("No data to filter. Please check the input files.")
            return recon
        filtered_recon = recon[~recon.filter(like="Reconciled").all(axis=1)]
        if filtered_recon.empty:
            logger.warning("Data is fully reconciled. No discrepancies found.")
        else:
            logger.info(
                f"Filtered reconciliation data contains {len(filtered_recon)} rows."
            )
        return filtered_recon

    def run(self):
        recon = self.compare_versions()
        if recon.empty:
            logger.error("No data to reconcile. Please check the input files.")
            return
        output_folder = "OA_recon/outputs"
        output_file = f"reconciliation_{self.args.base_env}_{self.args.base_version}_{self.args.target_env}_{self.args.target_version}_{self.args.level}.csv"
        recon.to_csv(os.path.join(output_folder, output_file), index=False)
        filtered_recon = self.filter_recon(recon)
        filtered_output_file = f"filtered_reconciliation_{self.args.base_env}_{self.args.base_version}_{self.args.target_env}_{self.args.target_version}_{self.args.level}.csv"
        filtered_recon.to_csv(
            os.path.join(output_folder, filtered_output_file), index=False
        )
        logger.info(f"Reconciliation completed. Results saved to {output_folder}.")


if __name__ == "__main__":
    nav_regression = NAVRegression()
    nav_regression.run()
    logger.info("NAV Regression script completed successfully.")
