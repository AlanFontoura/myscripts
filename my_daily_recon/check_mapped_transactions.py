import awswrangler as wr
import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument(
    "-d",
    "--date",
    dest="date",
    type=str,
    required=False,
    help="Date to check",
)

args = parser.parse_args()

args.date = args.date.replace("-", "")

custodian_file = (
    f"s3://d1g1t-custodian-data-us-east-1/apx/gresham/{args.date}/Transaction.csv"
)
custodian_data = pd.read_csv(custodian_file)
mapped_data_folder = (
    f"s3://d1g1t-dataloader-us/production/outputs/gresham/{args.date}/apx/"
)
mapped_transaction_files = wr.s3.list_objects(mapped_data_folder)
mapped_transaction_files = [
    file
    for file in mapped_transaction_files
    if "transactions" in file and ".csv" in file
]
mapped_transactions = pd.concat(
    [pd.read_csv(file) for file in mapped_transaction_files]
)

mapped_transactions["TransactionGUID"] = mapped_transactions["Origin ID"].str[:-2]
missing_mappers = mapped_transactions[
    ~mapped_transactions["TransactionGUID"].isin(custodian_data["TransactionGUID"])
]
missing_mappers = missing_mappers[
    ~missing_mappers["Custodian Account ID"].str.endswith("benchmark")
]

if not missing_mappers.empty:
    print("Missing transactions found")
    missing_mappers.to_csv(
        f"my_daily_recon/outputs/missing_mapped_transactions_{args.date}.csv",
        index=False,
    )
else:
    print("No missing transactions found.")
