from utils import logger_setup
import pandas as pd
import os
import argparse

logger = logger_setup("logs", "vnf_recon")


class OARecon:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "-b",
            "--base",
            dest="base_env",
            type=str,
            required=True,
            help="Name of the folder with the base environment files",
        )
        self.parser.add_argument(
            "-t",
            "--target",
            dest="target_env",
            type=str,
            required=True,
            help="Name of the folder with the target environment files",
        )
        self.args = self.parser.parse_args()
        self.base_columns = ["Account ID", "Date"]
        self.comparison_columns = [
            "Net Deposits",
            "Net Additions",
            "Gain",
            "Fees",
            "Expenses",
            "Market Value EoP",
            "Total Return",
        ]

    @property
    def base_files(self):
        return os.listdir(f"OA_recon/outputs/{self.args.base_env}")

    @property
    def target_files(self):
        return os.listdir(f"OA_recon/outputs/{self.args.self.args.target_env}")

    def get_vnf_data(self, file_path):
        vnf_data = pd.read_csv(file_path)
        vnf_data = vnf_data[self.base_columns + self.comparison_columns]
        vnf_data = vnf_data.rename(
            columns={
                value: f"{index+1} - {value}"
                for index, value in enumerate(self.comparison_columns)
            }
        )
        return vnf_data

    def merge_data(self, file):
        base_df = self.get_vnf_data(f"OA_recon/outputs/{self.args.base_env}/{file}")
        target_df = self.get_vnf_data(f"OA_recon/outputs/{self.args.target_env}/{file}")

        recon_df = base_df.merge(
            target_df,
            how="outer",
            on=self.base_columns,
            suffixes=[f"_{self.args.base_env}", f"_{self.args.target_env}"],
        )
        recon_df = recon_df.fillna(0)
        return recon_df

    def run_recon(self, df):
        cols = [
            f"{index+1} - {value}"
            for index, value in enumerate(self.comparison_columns)
        ]
        for col in cols:
            df[f"{col}_recon"] = round(
                abs(
                    df[f"{col}_{self.args.base_env}"]
                    - df[f"{col}_{self.args.target_env}"]
                ),
                4,
            )
        cols = df.columns.tolist()[2:]
        cols.sort()
        final_df = df[self.base_columns + cols]
        final_df = final_df[final_df["Date"] != "2020-12-31"]
        return final_df

    def filter_non_recon_entries(self, df):
        recon_cols = [
            f"{index+1} - {value}_recon"
            for index, value in enumerate(self.comparison_columns)
        ]
        non_recon_df = df[df.loc[:, recon_cols].gt(1).any(axis=1)]
        return non_recon_df

    def count_breaks(self, df):
        recon_cols = [
            f"{index + 1} - {value}_recon"
            for index, value in enumerate(self.comparison_columns)
        ]
        breaks = (
            df.groupby("Account ID")[recon_cols]
            .apply(lambda x: (abs(x) > 1).sum())
            .reset_index(drop=False)
        )
        return breaks

    def recon_values_and_flows(self):
        base_files = os.listdir(f"OA_recon/outputs/{self.args.base_env}")
        target_files = os.listdir(f"OA_recon/outputs/{self.args.target_env}")
        accounts = pd.read_csv("OA_recon/inputs/Account.csv")[
            ["AccountCode", "AccountName", "CustodianName"]
        ]
        accounts.rename(
            columns={
                "AccountCode": "Account ID",
                "AccountName": "Account Name",
                "CustodianName": "Custodian",
            },
            inplace=True,
        )
        total_files = len(base_files)
        counter = 0
        full_recon_list = []
        filtered_recon_list = []
        break_count_list = []
        for file in base_files:
            counter += 1
            logger.info(f"Reconciling file #{counter}/{total_files}")
            if file not in target_files:
                logger.info("No target data. Skip file")
                continue
            try:
                merged_df = self.merge_data(file)
                recon_df = self.run_recon(merged_df)
                full_recon_list.append(recon_df)
                filtered_recon_list.append(self.filter_non_recon_entries(recon_df))
                break_count_list.append(self.count_breaks(recon_df))
                logger.info(f"MERGED FILE {file}")
            except:
                logger.warning(f"SKIPPED FILE {file}")
                continue

        try:
            pd.concat(full_recon_list).merge(accounts, how="left").sort_values(
                ["Account ID", "Date"]
            ).to_csv(f"OA_recon/outputs/full_recon.csv", index=False)
        except ValueError:
            print("Nothing to concatenate on full recon")

        try:
            pd.concat(filtered_recon_list).merge(accounts, how="left").sort_values(
                ["Account ID", "Date"]
            ).to_csv(f"OA_recon/outputs/filtered_recon.csv", index=False)
        except ValueError:
            print("Nothing to concatenate on filtered recon")

        try:
            pd.concat(break_count_list).merge(accounts, how="left").sort_values(
                ["Account ID"]
            ).to_csv(f"OA_recon/outputs/break_count.csv", index=False)
        except ValueError:
            print("Nothing to concatenate on break count")


if __name__ == "__main__":
    work = OARecon()
    work.recon_values_and_flows()
