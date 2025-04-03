import awswrangler as wr
import pandas as pd
from configparser import ConfigParser
import logging
from base_main import BaseMain

LOG = logging.getLogger(__name__)


"""
The main purpose of this script is to generate transfer values (both in CAD and USD) for all transactions of a 
single client.

- For security transfers, the transfer value is:
    * The field 'Market Value in Transaction Currency', if populated
    * d1g1t Transaction Quantity X Market Price on Trade Date, if aforementioned field is not populated
- For cash transfers, the transfer value is d1g1t Transaction Amount

The necessary inputs to run the code are:
- Variables: must be set on inputs/transfer_values.ini file
    * client: client name
    * environment: either production or staging
    * region: either CA or US
    * base currency: base currency on FX file (either CAD or USD)    
- Files
    * <CLIENT>-fx-rate.csv: export from Django API at https://api-<CLIENT>.d1g1t<staging>.com/admin-d1g1t/main/fxrate/
    in csv format and saved into folder /inputs/. There is no need to edit the file.
    
    * <ENV>-<CLIENT>-prices.csv.gz: export from Rundeck job 'Export (ALL with FPK) Prices (2.26|2.28)' located at
    http://rundeck.d1g1twealth.com:4440/project/FE/job/show/c5efa0c6-158b-4b55-91e6-b6f6e45a5728. There is no need
    to save a local copy. The script downloads the file from its default location at S3.
    
    * <ENV>-<CLIENT>-transactions.csv.gz: export from Rundeck job 'Export Transactions (5.x)' located at
    http://rundeck.d1g1twealth.com:4440/project/FE/job/show/6e61325f-c03f-457d-b79b-b55f4b595810. There is no need
    to save a local copy. The script downloads the file from its default location at S3.
    
    * instruments-<ENV>.csv: export from Rundeck job 'Export Instruments - Generic' located at
    http://rundeck.d1g1twealth.com:4440/project/FE/job/show/eefcb4c7-a0f6-4272-ac2d-d353dd2979ca. There is no need
    to save a local copy. The script downloads the file from its default location at S3.
    
The script auto-fills both missing FX rates and security market prices with the last existing value.

Results are saved in folder /outputs/, in a file named '<CLIENT>_transfer_values_<ENVIRONMENT>.csv'

The last two columns of the csv file are TransferValueCAD and TransferValueUSD, with the transfer values for given
transactions.
"""


class TransferValues(BaseMain):
    def __init__(self):
        super().__init__()
        constants = ConfigParser()
        constants.read("transfer_values/inputs/transfer_values.ini")
        self.client = constants.get(self.args.profile, "CLIENT")
        self.env = constants.get(self.args.profile, "ENVIRONMENT")
        self.region = constants.get(self.args.profile, "REGION")
        self.base_cur = constants.get(self.args.profile, "BASE_CURRENCY")
        self.export_path = (
            f"s3://d1g1t-client-{self.region.lower()}/{self.client.lower()}/exports"
        )

    def add_extra_args(self):
        self.parser.add_argument(
            "-p",
            "--profile",
            dest="profile",
            type=str,
            required=True,
            help="Client profile to be used",
        )

    def get_fx_rates(self):
        fx_rates = pd.read_csv(
            f"transfer_values/inputs/{self.client}-fx-rate.csv"
        ).rename(columns={"date": "Trade Date", "close": "FX Rate"})

        fx_rates = (
            fx_rates[fx_rates["base__name"] == self.base_cur]
            .drop(columns="base__name")
            .pivot(index="Trade Date", columns="foreign__name", values="FX Rate")
            .sort_values("Trade Date")
            .reset_index(drop=False)
        )

        temp_fx = pd.DataFrame()
        temp_fx["Trade Date"] = pd.date_range(
            start=fx_rates["Trade Date"].min(), end=fx_rates["Trade Date"].max()
        ).strftime("%Y-%m-%d")

        total_fx = (
            temp_fx.merge(fx_rates, how="left")
            .ffill()
            .melt(
                id_vars="Trade Date",
                var_name="Transaction Currency",
                value_name="FX Rate",
            )
            .reset_index(drop=True)
        )

        return total_fx

    def adjust_fx_table(self, fx_table):
        other_currency = "CAD" if self.base_cur == "USD" else "USD"
        adjusted_table = (
            fx_table[fx_table["Transaction Currency"] == other_currency]
            .drop(columns="Transaction Currency")
            .reset_index(drop=True)
            .rename(columns={"FX Rate": "CAD/USD"})
        )
        return adjusted_table

    def get_prices(self):
        security_prices = pd.read_csv(
            f"{self.export_path}/{self.env}-{self.client.lower()}-prices.csv.gz"
        )
        security_prices = security_prices[
            ["firm_provided_key", "date", "close"]
        ].rename(
            columns={
                "firm_provided_key": "Security ID",
                "date": "Trade Date",
                "close": "Market Price",
            }
        )
        security_prices = security_prices.pivot_table(
            values="Market Price", index="Trade Date", columns="Security ID"
        )
        security_prices = security_prices.sort_values("Trade Date")
        security_prices = security_prices.ffill().reset_index(drop=False)
        security_prices = security_prices.melt(
            id_vars="Trade Date", var_name="Security ID", value_name="Market Price"
        )
        security_prices = security_prices[
            ~security_prices["Market Price"].isna()
        ].reset_index(drop=True)

        return security_prices

    def get_instruments(self):
        instruments = pd.read_csv(f"{self.export_path}/instruments-{self.env}.csv")
        currencies = instruments[["InstrumentID", "Name", "Currency"]].rename(
            columns={
                "InstrumentID": "Security ID",
                "Name": "Security Name",
                "Currency": "Security Currency",
            }
        )
        currencies = currencies.drop_duplicates()
        instruments = instruments[["InstrumentID", "UoM"]].rename(
            columns={"InstrumentID": "Security ID"}
        )
        instruments = instruments.drop_duplicates()

        return instruments, currencies

    def get_transactions(self):
        trx = pd.read_csv(
            f"{self.export_path}/{self.env}-{self.client.lower()}-transactions.csv.gz"
        )
        trx = trx[trx["Is Cancelled"] == "f"]
        types = [
            "transfer-security-in",
            "transfer-cash-in",
            "internal-transfer-cash-in",
            "deposit",
            "internal-transfer-security-out",
            "internal-transfer-security-in",
            "transfer-cash-out",
            "transfer-security-out",
            "internal-transfer-cash-out",
            "withdrawal",
        ]

        trx = trx[trx["d1g1t Transaction Type"].isin(types)].reset_index(drop=True)
        trx = trx.rename(columns={"Security": "Security ID"})

        return trx

    def create_transfer_value_cols_for_cash(self, transactions, fx_rates, cad_usd_fx):
        cash = transactions[
            ~transactions["d1g1t Transaction Type"].str.contains("security")
        ]
        cash_with_fx = cash[~cash["Trade FxRate"].isna()]
        cash_with_fx.loc[:, "TransferValueTradeCurrency"] = cash_with_fx.loc[
            :, "d1g1t Transaction Amount"
        ]
        cash_with_fx.loc[
            cash_with_fx["Transaction Currency"] == "CAD", "TransferValueCAD"
        ] = cash_with_fx.loc[
            cash_with_fx["Transaction Currency"] == "CAD", "d1g1t Transaction Amount"
        ]
        cash_with_fx.loc[
            cash_with_fx["Transaction Currency"] == "CAD", "TransferValueUSD"
        ] = (
            cash_with_fx.loc[
                cash_with_fx["Transaction Currency"] == "CAD",
                "d1g1t Transaction Amount",
            ]
            / cash_with_fx.loc[
                cash_with_fx["Transaction Currency"] == "CAD", "Trade FxRate"
            ]
        )
        cash_with_fx.loc[
            cash_with_fx["Transaction Currency"] == "USD", "TransferValueUSD"
        ] = cash_with_fx.loc[
            cash_with_fx["Transaction Currency"] == "USD", "d1g1t Transaction Amount"
        ]
        cash_with_fx.loc[
            cash_with_fx["Transaction Currency"] == "USD", "TransferValueCAD"
        ] = (
            cash_with_fx.loc[
                cash_with_fx["Transaction Currency"] == "USD",
                "d1g1t Transaction Amount",
            ]
            * cash_with_fx.loc[
                cash_with_fx["Transaction Currency"] == "USD", "Trade FxRate"
            ]
        )

        cash = cash[cash["Trade FxRate"].isna()]
        cash = cash.assign(TransferValueTradeCurrency=cash["d1g1t Transaction Amount"])
        cash = cash.merge(fx_rates, how="left")
        cash["FX Rate"] = cash["FX Rate"].fillna(1)
        cash["Trade FxRate"] = cash["Trade FxRate"].fillna(cash["FX Rate"])
        cash["FX Rate"] = cash["Trade FxRate"]
        cash = cash.merge(cad_usd_fx, how="left")

        if self.base_cur == "CAD":
            cash = cash.assign(
                TransferValueCAD=cash["TransferValueTradeCurrency"] / cash["FX Rate"]
            )
            cash = cash.assign(
                TransferValueUSD=cash["TransferValueCAD"] * cash["CAD/USD"]
            )
        else:
            cash = cash.assign(
                TransferValueUSD=cash["TransferValueTradeCurrency"] / cash["FX Rate"]
            )
            cash = cash.assign(
                TransferValueCAD=cash["TransferValueUSD"] * cash["CAD/USD"]
            )

        cash = pd.concat([cash, cash_with_fx])

        return cash

    def create_transfer_value_cols_for_sec(
        self, transactions, instruments, fx_rates, cad_usd_fx
    ):
        # Creating Transfer Value columns for security transfers
        securities = transactions[
            transactions["d1g1t Transaction Type"].str.contains("security")
        ]
        securities = securities.merge(instruments, how="left")

        # Transfers with market value
        sec_with_mv = securities[
            ~securities["Market Value in Transaction Currency"].isna()
        ]

        sec_with_mv = sec_with_mv.assign(
            TransferValueTradeCurrency=sec_with_mv[
                "Market Value in Transaction Currency"
            ]
        )
        sec_with_mv = sec_with_mv.merge(fx_rates, how="left")
        sec_with_mv["FX Rate"] = sec_with_mv["FX Rate"].fillna(1)
        sec_with_mv = sec_with_mv.merge(cad_usd_fx, how="left")

        if self.base_cur == "CAD":
            sec_with_mv = sec_with_mv.assign(
                TransferValueCAD=sec_with_mv["TransferValueTradeCurrency"]
                / sec_with_mv["FX Rate"]
            )
            sec_with_mv = sec_with_mv.assign(
                TransferValueUSD=sec_with_mv["TransferValueCAD"]
                * sec_with_mv["CAD/USD"]
            )
        else:
            sec_with_mv = sec_with_mv.assign(
                TransferValueUSD=sec_with_mv["TransferValueTradeCurrency"]
                / sec_with_mv["FX Rate"]
            )
            sec_with_mv = sec_with_mv.assign(
                TransferValueCAD=sec_with_mv["TransferValueUSD"]
                * sec_with_mv["CAD/USD"]
            )

        return sec_with_mv

    def create_cols_for_empty_transfer_value(
        self, transactions, instruments, cad_usd_fx
    ):

        securities = transactions[
            transactions["d1g1t Transaction Type"].str.contains("security")
        ]
        securities = securities.merge(instruments, how="left")

        empty_mv = securities[securities["Market Value in Transaction Currency"].isna()]
        empty_mv = empty_mv[empty_mv["d1g1t Transaction Quantity"] != 0]
        empty_mv = empty_mv[~empty_mv["Security ID"].str.contains("-legacy")]

        empty_mv = empty_mv.merge(cad_usd_fx, how="left")
        empty_mv.loc[empty_mv["Security Currency"] == "CAD", "TransferValueCAD"] = (
            empty_mv["d1g1t Transaction Quantity"]
            * empty_mv["UoM"]
            * empty_mv["Market Price"]
        )
        empty_mv.loc[empty_mv["Security Currency"] == "CAD", "TransferValueUSD"] = (
            empty_mv["d1g1t Transaction Quantity"]
            * empty_mv["UoM"]
            * empty_mv["Market Price"]
            / empty_mv["CAD/USD"]
        )

        empty_mv.loc[empty_mv["Security Currency"] == "USD", "TransferValueUSD"] = (
            empty_mv["d1g1t Transaction Quantity"]
            * empty_mv["UoM"]
            * empty_mv["Market Price"]
        )
        empty_mv.loc[empty_mv["Security Currency"] == "USD", "TransferValueCAD"] = (
            empty_mv["d1g1t Transaction Quantity"]
            * empty_mv["UoM"]
            * empty_mv["Market Price"]
            * empty_mv["CAD/USD"]
        )

        empty_mv.loc[
            empty_mv["Transaction Currency"] == "USD", "TransferValueTradeCurrency"
        ] = empty_mv.loc[empty_mv["Transaction Currency"] == "USD", "TransferValueUSD"]
        empty_mv.loc[
            empty_mv["Transaction Currency"] == "CAD", "TransferValueTradeCurrency"
        ] = empty_mv.loc[empty_mv["Transaction Currency"] == "CAD", "TransferValueCAD"]

        return empty_mv

    def generate_final_table(self, cash, sec_with_mv, empty_mv, final_cols):
        final_file = (
            pd.concat([cash, sec_with_mv, empty_mv])
            .sort_values("Trade Date")
            .reset_index(drop=True)
        )
        final_cols += [
            "CAD/USD",
            "UoM",
            "Market Price",
            "TransferValueTradeCurrency",
            "TransferValueCAD",
            "TransferValueUSD",
        ]

        final_file = final_file[final_cols]
        final_file = final_file.rename(columns={"CAD/USD": "USD/CAD"})

        negative_transactions = [
            "internal-transfer-security-out",
            "internal-transfer-cash-out",
            "transfer-security-out",
            "transfer-cash-out",
            "withdrawal",
        ]
        final_file.loc[
            final_file["d1g1t Transaction Type"].isin(negative_transactions),
            "TransferValueTradeCurrency",
        ] = (
            -1
            * final_file.loc[
                final_file["d1g1t Transaction Type"].isin(negative_transactions),
                "TransferValueTradeCurrency",
            ]
        )
        final_file.loc[
            final_file["d1g1t Transaction Type"].isin(negative_transactions),
            "TransferValueCAD",
        ] = (
            -1
            * final_file.loc[
                final_file["d1g1t Transaction Type"].isin(negative_transactions),
                "TransferValueCAD",
            ]
        )
        final_file.loc[
            final_file["d1g1t Transaction Type"].isin(negative_transactions),
            "TransferValueUSD",
        ] = (
            -1
            * final_file.loc[
                final_file["d1g1t Transaction Type"].isin(negative_transactions),
                "TransferValueUSD",
            ]
        )

        final_file[
            ["TransferValueTradeCurrency", "TransferValueCAD", "TransferValueUSD"]
        ] = round(
            final_file[
                ["TransferValueTradeCurrency", "TransferValueCAD", "TransferValueUSD"]
            ],
            4,
        )

        return final_file

    def main(self):
        fx_rates = self.get_fx_rates()
        cad_usd_fx = self.adjust_fx_table(fx_rates)
        prices = self.get_prices()
        instruments, currencies = self.get_instruments()
        transactions = self.get_transactions()
        transactions = transactions.merge(currencies, how="left").merge(
            prices, how="left"
        )
        final_cols = list(transactions.columns)
        cash = self.create_transfer_value_cols_for_cash(
            transactions, fx_rates, cad_usd_fx
        )
        sec_with_mv = self.create_transfer_value_cols_for_sec(
            transactions, instruments, fx_rates, cad_usd_fx
        )
        empty_mv = self.create_cols_for_empty_transfer_value(
            transactions, instruments, cad_usd_fx
        )
        final_df = self.generate_final_table(cash, sec_with_mv, empty_mv, final_cols)
        final_df.to_csv(
            f"transfer_values/outputs/{self.client}_transfer_values_{self.env}.csv",
            index=False,
        )


if __name__ == "__main__":
    calculator = TransferValues()
    calculator.main()
