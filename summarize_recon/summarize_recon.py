import awswrangler as wr
import pandas as pd
from configparser import ConfigParser
import logging
from base_main import BaseMain

LOG = logging.getLogger(__name__)


class SummarizeRecon(BaseMain):
    def __init__(self):
        super().__init__()
        constants = ConfigParser()
        constants.read("summarize_recon/inputs/summarize_recon.ini")
        self.recon_folder = constants.get(self.args.profile, "RECON_FOLDER")
        self.data_folder = constants.get(self.args.profile, "DATA_FOLDER")
        self.metrics = constants.get(self.args.profile, "METRICS").split(",")

    def add_extra_args(self):
        self.parser.add_argument(
            "-p",
            "--profile",
            dest="profile",
            type=str,
            required=True,
            help="Client profile to be used",
        )

    def get_recon(self):
        """
        Get recon data from S3
        """
        recon_files = wr.s3.list_objects(self.recon_folder)
        recon_file = [
            file
            for file in recon_files
            if "archive" not in file
            and "breaking" not in file
            and "exception" not in file
        ]
        recon = pd.read_csv(recon_file[0])
        recon = self.add_security_data(recon)
        recon = self.add_security_type(recon)
        return recon

    def get_s3_data(self, file_type, **kwargs):
        files = wr.s3.list_objects(self.data_folder)
        files = [file for file in files if file_type in file]
        files.sort()
        df = pd.read_csv(files[-1], usecols=kwargs.get("usecols", None))
        return df

    def build_hierarchy(self):
        accounts = self.get_s3_data(
            "Account.csv",
            usecols=["AccountCode", "AccountName", "CustodianName", "ClientCode"],
        )
        clients = self.get_s3_data("Client.csv", usecols=["ClientID", "HouseholdID"])
        hierarchy = (
            pd.merge(
                accounts, clients, left_on="ClientCode", right_on="ClientID", how="left"
            )
            .drop(columns="ClientCode")
            .rename(
                columns={
                    "AccountCode": "Account ID",
                    "AccountName": "Account Name",
                    "CustodianName": "Custodian",
                    "ClientID": "Client ID",
                    "HouseholdID": "Household ID",
                }
            )
        )
        return hierarchy

    def get_security_data(self):
        security_data = self.get_s3_data(
            "Security.csv",
            usecols=[
                "SecurityID",
                "SecurityTypeCode",
                "Symbol",
            ],
        )
        security_data.rename(
            columns={
                "SecurityID": "Security ID",
                "SecurityTypeCode": "Security Type Code",
            },
            inplace=True,
        )
        return security_data

    def add_security_data(self, recon):
        """
        Add security data to recon
        """
        security_data = self.get_security_data()
        recon = pd.merge(
            recon,
            security_data,
            left_on="instrument",
            right_on="Security ID",
            how="left",
        )
        recon.drop(columns=["Security ID"], inplace=True)
        return recon

    def add_security_type(self, recon):
        non_marketable = ["en", "hf", "hp", "lp", "oa", "pl", "pp", "zp"]
        true_pe = ["pe", "rp"]
        recon.loc[recon["Security Type Code"].isin(non_marketable), "Security Type"] = (
            "Non-marketable"
        )
        recon.loc[recon["Security Type Code"].isin(true_pe), "Security Type"] = (
            "True PE"
        )
        recon.loc[recon["Security Type Code"] == "ca", "Security Type"] = "Cashlike"
        recon["Security Type"] = recon["Security Type"].fillna("Marketable")
        return recon

    def summarize_metric(self, recon, metric=None):
        """
        Summarize recon data
        """
        if metric == "all":
            recon["all_reconciled"] = (
                recon["units_reconciled"]
                & recon["price_reconciled"]
                & recon["mv_clean_reconciled"]
            )
            recon = recon[~recon["all_reconciled"]]
        elif metric:
            recon = recon[~recon[f"{metric}_reconciled"]]
        df = recon[["account", "Security Type"]].rename(
            columns={"account": "Account ID"}
        )
        recon_summary = (
            df.groupby(["Account ID", "Security Type"])
            .size()
            .reset_index()
            .pivot_table(
                index=["Account ID"],
                columns=["Security Type"],
                values=0,
                fill_value=0,
            )
            .reset_index()
        )
        recon_summary["Total"] = recon_summary.iloc[:, 1:].sum(axis=1)
        recon_summary = recon_summary.melt(
            id_vars=["Account ID"],
            value_vars=recon_summary.columns[1:-1],
            var_name="Security Type",
            value_name="Count",
        ).sort_values(
            by=["Account ID", "Security Type"],
            ascending=[True, True],
        )

        if metric == "all":
            recon_summary.rename(columns={"Count": "Any Breaks"}, inplace=True)
        elif metric:
            recon_summary.rename(
                columns={"Count": f"{metric.capitalize()} Breaks"}, inplace=True
            )
        else:
            recon_summary.rename(columns={"Count": "Positions"}, inplace=True)
        return recon_summary

    def summarize_recon(self, recon):
        """
        Summarize recon data
        """
        dfl = []
        for metric in self.metrics:
            recon_summary = self.summarize_metric(recon, metric)
            dfl.append(recon_summary)

        recon_summary = self.summarize_metric(recon)
        all_recon_summary = self.summarize_metric(recon, "all")
        for df in dfl:
            recon_summary = pd.merge(
                recon_summary,
                df,
                how="left",
            )
        recon_summary = pd.merge(
            recon_summary,
            all_recon_summary,
            how="left",
        )
        return recon_summary.fillna(0)

    def summarize_by_account(self, recon):
        df = (
            recon.drop(columns=["Security Type"])
            .groupby(
                ["Account ID", "Account Name", "Custodian", "Client ID", "Household ID"]
            )
            .sum()
            .reset_index()
        )
        return df

    def summarize_by_client(self, recon):
        df = (
            recon.drop(
                columns=["Security Type", "Account ID", "Account Name", "Custodian"]
            )
            .groupby(["Client ID", "Household ID"])
            .sum()
            .reset_index()
        )
        return df

    def summarize_by_household(self, recon):
        df = (
            recon.drop(
                columns=[
                    "Security Type",
                    "Account ID",
                    "Account Name",
                    "Custodian",
                    "Client ID",
                ]
            )
            .groupby(["Household ID"])
            .sum()
            .reset_index()
        )
        return df

    def add_hierarchy(self, recon, hierarchy):
        """
        Add hierarchy to recon data
        """
        recon = pd.merge(
            hierarchy,
            recon,
            how="right",
        )
        return recon

    def alternative_summary(self, recon, threshold=50):
        recon["d1g1t_mv_clean"] = recon["d1g1t_mv_clean"].fillna(0)
        recon["custodian_mv_clean"] = recon["custodian_mv_clean"].fillna(0)
        recon["mv_clean_reconciled"] = (
            abs(recon["d1g1t_mv_clean"] - recon["custodian_mv_clean"]) < threshold
        )
        return recon


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    work = SummarizeRecon()
    recon_data = work.get_recon()
    hierarchy = work.build_hierarchy()

    summary = work.summarize_recon(recon_data)
    summary = work.add_hierarchy(summary, hierarchy)
    summary_by_account = work.summarize_by_account(summary)
    summary_by_client = work.summarize_by_client(summary)
    summary_by_household = work.summarize_by_household(summary)

    summary.to_csv("summarize_recon/outputs/recon_summary_by_sec_type.csv", index=False)
    summary_by_account.to_csv(
        "summarize_recon/outputs/recon_summary_by_account.csv", index=False
    )
    summary_by_client.to_csv(
        "summarize_recon/outputs/recon_summary_by_client.csv", index=False
    )
    summary_by_household.to_csv(
        "summarize_recon/outputs/recon_summary_by_household.csv", index=False
    )

    alternative_data = work.alternative_summary(recon_data)
    alternative_summary = work.summarize_recon(alternative_data)
    alternative_summary = work.add_hierarchy(alternative_summary, hierarchy)
    alternative_summary_by_household = work.summarize_by_household(alternative_summary)
    alternative_summary_by_household.to_csv(
        "summarize_recon/outputs/recon_summary_by_household_alternative.csv",
        index=False,
    )
