import pandas as pd
from configparser import ConfigParser
import numpy as np
import logging
from base_main import BaseMain

LOG = logging.getLogger(__name__)


"""
The main purpose of this script is to generate a recon file according
to my rules.

The necessary inputs to run the code are:
- Variables: must be set on inputs/transfer_values.ini file
    * profile: client profile (set variables on the .ini file)
    * date: base date for the reconciliation (YYYY-MM-DD)
- Files
    * <ENV>-<CLIENT>-latest-tracking.csv: export from Rundeck job 'Export
    Tracking (2.28,3.x, 4.x)' located at
    http://rundeck.d1g1twealth.com:4440/project/FE/job/show/9bb0e7ae-337d-43ef-bd57-4b949ebc55f7.
    There is no need to save a local copy. The script downloads the file
    from its default location at S3. Set the proper name at the .ini file.

    * Position.csv: provided by the client. It contains the positions of
    the client at the current date. Located in custodian buckets
    (e.g.: s3://d1g1t-custodian-data-us-east-1)

    * Security.csv: provided by the client. It contains the security master
    table of the client at the current date. Located in the same folder as
    the Position.csv file.

"""


class MyDailyRecon(BaseMain):
    def __init__(self):
        super().__init__()
        constants = ConfigParser()
        constants.read("my_daily_recon/inputs/my_daily_recon.ini")
        self.client = constants.get(self.args.profile, "CLIENT")
        self.env = constants.get(self.args.profile, "ENVIRONMENT")
        self.region = constants.get(self.args.profile, "REGION")
        self.tracking_file = constants.get(self.args.profile, "TRACKING_FILE")
        self.position_file = constants.get(self.args.profile, "POSITION_FILE")
        self.security_file = constants.get(self.args.profile, "SECURITY_MASTER_FILE")
        self.usd_security = constants.get(self.args.profile, "USD_SECURITY")
        threshold_str = constants.get(self.args.profile, "THRESHOLD")
        self.threshold = dict(item.split("=") for item in threshold_str.split(","))

    def add_extra_args(self):
        self.parser.add_argument(
            "-p",
            "--profile",
            dest="profile",
            type=str,
            required=True,
            help="Client profile to be used",
        )

        self.parser.add_argument(
            "-d",
            "--date",
            dest="date",
            type=str,
            required=True,
            help="Recon date (YYYY-MM-DD)",
        )

    def get_tracking(self):
        """
        Get the tracking file from S3
        """
        tracking = pd.read_csv(self.tracking_file)
        tracking.loc[tracking["instrument"] == "USD", "mv"] = tracking.loc[
            tracking["instrument"] == "USD", "units"
        ]
        tracking = tracking[tracking["is_dead"] == "f"]
        tracking = tracking[~tracking["account"].str.contains("_")]
        tracking = tracking.dropna(subset=["units", "mv"], how="all")
        tracking = tracking[
            ["date", "account", "instrument", "scale", "units", "price", "mv"]
        ]
        tracking.rename(
            columns={
                "date": "Date",
                "account": "Account ID",
                "instrument": "Security ID",
                "scale": "Scale",
                "units": "Units - d1g1t",
                "price": "Price - d1g1t",
                "mv": "Market Value - d1g1t",
            },
            inplace=True,
        )
        return tracking

    def get_position(self):
        """
        Get the position file from S3
        """
        dte = self.args.date.replace("-", "")
        self.position_file = self.position_file.replace("YYYYMMDD", dte)
        pos = pd.read_csv(self.position_file)
        pos.rename(
            columns={
                "AccountCode": "Account ID",
                "SecurityID": "Security ID",
                "Current": "Units - Custodian",
                "Price": "Price - Custodian",
                "MV_Local": "Market Value - Custodian",
            },
            inplace=True,
        )
        pos = pos[
            [
                "Date",
                "Account ID",
                "Security ID",
                "Units - Custodian",
                "Price - Custodian",
                "Market Value - Custodian",
            ]
        ]
        pos["Price - Custodian"] = pos["Price - Custodian"].fillna(0)
        pos = (
            pos.groupby(
                ["Date", "Account ID", "Security ID", "Price - Custodian"],
                as_index=False,
            )
            .sum()
            .reset_index(drop=True)
        )
        pos["Security ID"] = pos["Security ID"].replace({self.usd_security: "USD"})
        return pos

    def get_security(self):
        """
        Get the security file from S3
        """
        dte = self.args.date.replace("-", "")
        self.security_file = self.security_file.replace("YYYYMMDD", dte)
        sec_master = pd.read_csv(
            self.security_file,
            usecols=["SecurityID", "SecurityName", "Symbol", "SecurityTypeCode"],
        )
        sec_master.rename(
            columns={
                "SecurityID": "Security ID",
                "SecurityName": "Security Name",
                "SecurityTypeCode": "Security Type",
            },
            inplace=True,
        )
        return sec_master

    def get_s3_data(self):
        """
        Get the data from S3
        """
        tracking = self.get_tracking()
        position = self.get_position()
        security = self.get_security()

        # Remove duplicates
        tracking = tracking.drop_duplicates()
        position = position.drop_duplicates()
        security = security.drop_duplicates()

        # Remove empty rows
        tracking = tracking.dropna(how="all")
        position = position.dropna(how="all")
        security = security.dropna(how="all")

        return tracking, position, security

    def merge_files(self, tracking, position, security):
        recon = tracking.merge(position, how="outer").merge(security, how="left")
        recon.loc[recon["Security ID"] == "USD", "Security Name"] = "US Dollar"
        recon.loc[recon["Security ID"] == "USD", "Security Type"] = "ca"
        recon.loc[recon["Security ID"] == "USD", "Symbol"] = "cash"

        recon["Category"] = "Marketable"
        recon.loc[recon["Security Type"] == "ca", "Category"] = "Cashlike"
        recon.loc[recon["Security Type"].isin(["pe", "rp"]), "Category"] = "True PE"
        recon.loc[
            recon["Security Type"].isin(
                ["en", "hf", "hp", "lp", "oa", "pl", "pp", "zp"]
            ),
            "Category",
        ] = "Non Marketable"
        return recon

    def add_recon_columns(self, recon):
        """
        Add the reconciliation columns to the dataframe
        """
        recon.loc[
            :,
            [
                "Units - d1g1t",
                "Units - Custodian",
                "Price - d1g1t",
                "Price - Custodian",
                "Market Value - d1g1t",
                "Market Value - Custodian",
            ],
        ] = recon.loc[
            :,
            [
                "Units - d1g1t",
                "Units - Custodian",
                "Price - d1g1t",
                "Price - Custodian",
                "Market Value - d1g1t",
                "Market Value - Custodian",
            ],
        ].fillna(
            0
        )

        for analytic in ["Units", "Price", "Market Value"]:
            recon[f"{analytic} - Diff"] = round(
                abs(recon[f"{analytic} - d1g1t"] - recon[f"{analytic} - Custodian"]), 2
            )
            recon[f"{analytic} - Reconciled"] = recon[f"{analytic} - Diff"] < float(
                self.threshold[analytic]
            )

        return recon

    def recon_adjustments(self, recon):
        recon.loc[
            recon["Category"].isin(["Non Marketable", "True PE"]),
            [
                "Units - d1g1t",
                "Units - Custodian",
                "Units - Diff",
                "Price - d1g1t",
                "Price - Custodian",
                "Price - Diff",
            ],
        ] = np.nan
        recon.loc[
            recon["Category"].isin(["Non Marketable", "True PE"]),
            ["Units - Reconciled", "Price - Reconciled"],
        ] = True
        return recon

    def generate_recon(self, tracking, position, security):
        recon = self.merge_files(tracking, position, security)
        recon = self.add_recon_columns(recon)
        recon = self.recon_adjustments(recon)
        recon = self.filter_empty_rows(recon)
        return recon

    def filter_empty_rows(self, recon):
        """
        Filter out empty rows from the reconciliation dataframe
        """
        subset_columns = [
            "Units - d1g1t",
            "Units - Custodian",
            "Market Value - d1g1t",
            "Market Value - Custodian",
        ]
        mask = recon[subset_columns].isin([0, 0.0, np.nan]).all(axis=1)
        recon = recon[~mask]
        return recon

    def split_recon(self, recon, current_date):
        recon['reconciled'] = recon['Units - Reconciled'] & recon['Market Value - Reconciled']
        recon = recon[~recon['reconciled']]
        cols = [
                # "Date",
                "Account ID",
                "Security ID",
                "Security Name",
                "Symbol",
                "Security Type",
                "Units - d1g1t",
                "Units - Custodian",
                "Units - Diff",
                "Units - Reconciled",
                "Price - d1g1t",
                "Price - Custodian",
                "Price - Diff",
                "Price - Reconciled",
                "Market Value - d1g1t",
                "Market Value - Custodian",
                "Market Value - Diff",
                "Market Value - Reconciled",
            ]
        units_and_price = [
                "Units - d1g1t",
                "Units - Custodian",
                "Units - Diff",
                "Units - Reconciled",
                "Price - d1g1t",
                "Price - Custodian",
                "Price - Diff",
                "Price - Reconciled",
            ]
        cashlike = recon.loc[recon["Category"] == "Cashlike", cols].drop(columns="Security Type")
        marketable = recon.loc[recon["Category"] == "Marketable", cols]
        non_marketable = recon.loc[recon["Category"] == "Non Marketable", cols].drop(columns=units_and_price)
        true_pe = recon.loc[recon["Category"] == "True PE", cols].drop(columns=units_and_price)

        cashlike.to_csv(f"my_daily_recon/outputs/{current_date}_{self.client}_{self.env}_cashlike_recon.csv", index=False)
        marketable.to_csv(f"my_daily_recon/outputs/{current_date}_{self.client}_{self.env}_marketable_recon.csv", index=False)
        non_marketable.to_csv(f"my_daily_recon/outputs/{current_date}_{self.client}_{self.env}_non_marketable_recon.csv", index=False)
        true_pe.to_csv(f"my_daily_recon/outputs/{current_date}_{self.client}_{self.env}_true_pe_recon.csv", index=False)


    def output_file(self, recon):
        """
        Save the reconciliation file
        """
        recon = recon[
            [
                "Date",
                "Account ID",
                "Security ID",
                "Security Name",
                "Symbol",
                "Security Type",
                "Category",
                "Units - d1g1t",
                "Units - Custodian",
                "Units - Diff",
                "Units - Reconciled",
                "Price - d1g1t",
                "Price - Custodian",
                "Price - Diff",
                "Price - Reconciled",
                "Market Value - d1g1t",
                "Market Value - Custodian",
                "Market Value - Diff",
                "Market Value - Reconciled",
            ]
        ]
        current_date = recon.iloc[0, 0]
        recon = recon.sort_values(["Account ID", "Security ID"])
        recon_file = (
            f"my_daily_recon/outputs/{current_date}_{self.client}_{self.env}_full_recon.csv"
        )
        recon.to_csv(recon_file, index=False)
        self.split_recon(recon, current_date)
        LOG.info(f"Reconciliation files saved to my_daily_recon/outputs/")

    def run(self):
        """
        Run the main function
        """
        LOG.info("Starting reconciliation file generation")

        # Get data from S3
        track, pos, sec_master = self.get_s3_data()

        # Create the reconciliation file
        recon = self.generate_recon(track, pos, sec_master)

        # Save the reconciliation file
        self.output_file(recon)

        pass


if __name__ == "__main__":
    runner = MyDailyRecon()
    runner.run()
