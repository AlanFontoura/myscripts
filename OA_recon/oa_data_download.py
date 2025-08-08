# import logging
import os
import pandas as pd
from pathlib import Path
from multiprocess import Pool

from base_main import BaseMain
from utils import logger_setup, NoResponseError, ChartTableFormatter

logger = logger_setup("logs", "gresham_recon")


class OADataDownload(BaseMain):

    def __init__(self):
        BaseMain.__init__(self)

    def add_extra_args(self):
        self.parser.add_argument(
            "-d",
            "--date",
            dest="report_date",
            type=str,
            required=True,
            help="Report date on the format YYYY-MM-DD",
        )

        self.parser.add_argument(
            "-c",
            "--currency",
            dest="currency",
            default="CAD",
            type=str,
            required=False,
            help="Currency used in the report",
        )
        
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
            "-f",
            "--filter",
            dest="filter",
            action="store_true",
            default=False,  
            required=False,
            help="Filter the entity IDs based on VNF data",
        )

    def create_output_folder(self) -> None:
        try:
            server_name = self.args.server.replace("https://api-", "").split(".")[0]
            folder = f"OA_recon/outputs/{server_name}/{self.args.level}"
            folder_path = Path(folder)
            folder_path.mkdir(parents=True, exist_ok=True)
            print("Output folder created")
        except PermissionError as e:
            print(f"Permission denied: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    @property
    def output_folder(self):
        server_name = self.args.server.replace("https://api-", "").split(".")[0]
        return f"OA_recon/outputs/{server_name}/{self.args.level}"

    @property
    def input_file(self):
        server_name = self.args.server.replace("https://api-", "").split(".")[0]
        return f"{server_name}_{self.args.level}_entity_ids.csv"

    @property
    def payload(self):
        payload = {
            "options": {
                "time_series": "monthly",
                "single_result": True,
                "date_range": {
                    "value": "since_inception",
                    "label": "Since Inception",
                },
            },
            "control": {"selected_entities": {"accounts_or_positions": None}},
            "settings": {
                "currency": self.args.currency,
                "date": {"date": self.args.report_date, "value": "specificDate"},
            },
            "groups": {"selected": []},
            "metrics": {"selected": []},
        }
        return payload

    @property
    def vnf_entities(self):
        vnf_entities = f"vnf_{self.args.level}.csv"
        if vnf_entities in os.listdir("OA_recon/inputs") and self.args.filter:
            vnf_data = pd.read_csv(f"OA_recon/inputs/{vnf_entities}")
            return vnf_data["Portfolio Firm Provided Key"].unique().tolist()
        return None

    @property
    def entity_ids(self) -> pd.DataFrame:
        if self.input_file in os.listdir("OA_recon/inputs"):
            entities = pd.read_csv(f"OA_recon/inputs/{self.input_file}")
        else:
            entities = self.get_entity_data()
            entities.to_csv(f"OA_recon/inputs/{self.input_file}", index=False)

        if self.vnf_entities:
            entities = entities[entities["firm_provided_key"].isin(self.vnf_entities)]

        return entities

    def get_calculation(self, calc_type: str, payload: dict):
        calc_call = self.api.calc(calc_type)
        response = calc_call.post(data=payload)
        if not response:
            raise NoResponseError("Request returned no result!")
        return response

    def get_entity_data(self, batch_size=1000):
        api_call = self.api.data
        api_call._store["base_url"] += f"{self.args.level}/"
        response = api_call.get(extra=f"limit={batch_size}")

        if response:
            response_list = []
            total_entries = response["count"]
            print(f"Total number of entries: {total_entries}")
            df = pd.DataFrame(response["results"])
            response_list.append(df)
            total_downloaded = min(batch_size, df.shape[0])
            print(f"Downloaded: {total_downloaded} entries")

            while total_downloaded < total_entries:
                extra = f"limit={batch_size}&offset={total_downloaded}"
                response = api_call.get(extra=extra)
                total_downloaded += batch_size
                df = pd.DataFrame(response["results"])
                response_list.append(df)
                print(f"Downloaded: {total_downloaded} entries")

            final_df = pd.concat(response_list).reset_index(drop=True)
            final_df = final_df[["firm_provided_key", "entity_id"]]
            return final_df

        else:
            raise NoResponseError

    def run_calc(self, firm_provided_key: str, entity_id: str):
        payload = self.payload
        if self.args.level == "accounts":
            payload["control"]["selected_entities"] = {"accounts_or_positions": [[entity_id]]}
        elif self.args.level == "clients":
            payload["control"]["selected_entities"] = {"clients": [entity_id]}
        elif self.args.level == "households":
            payload["control"]["selected_entities"] = {"households": [entity_id]}
        filename = f"{firm_provided_key}.csv"
        try:
            resp = self.get_calculation("net-asset-value-history", payload)
            parser = ChartTableFormatter(resp, payload)
            res = parser.parse_data()
            logger.info(f"Download OK for {self.args.level[:(-1)]} {firm_provided_key}")
            res.insert(1, f"{self.args.level.capitalize()[:(-1)]} ID", firm_provided_key)
            res.to_csv(os.path.join(self.output_folder, filename), index=False)
        except NoResponseError:
            logger.warning(f"No response for {self.args.level[:(-1)]} {firm_provided_key}")

    def run_parallel_calcs(self):
        downloaded_accounts = [
            acc.replace(".csv", "") for acc in os.listdir(self.output_folder)
        ]
        entity_ids = self.entity_ids[
            ~self.entity_ids["firm_provided_key"].isin(downloaded_accounts)
        ]
        account_entity_id_pairs = tuple(
            zip(
                entity_ids["firm_provided_key"].tolist(),
                entity_ids["entity_id"].tolist(),
            )
        )
        with Pool() as pool:
            pool.starmap(self.run_calc, account_entity_id_pairs)

    def concatenate_data(self):
        files = os.listdir(self.output_folder)
        if not files:
            logger.warning("No files to concatenate")
            return

        dataframes = []
        for file in files:
            file_path = os.path.join(self.output_folder, file)
            df = pd.read_csv(file_path)
            if not df.empty:
                dataframes.append(df)

        if dataframes:
            concatenated_df = pd.concat(dataframes, ignore_index=True).sort_values(
                by=[f"{self.args.level.capitalize()[:(-1)]} ID", "Date"]
            )
            concatenated_df.to_csv(
                os.path.join(self.output_folder, f"concatenated_{self.args.level}.csv"),
                index=False,
            )
            logger.info("Data concatenation completed")
        else:
            logger.warning("No valid dataframes to concatenate")

    def after_login(self):
        self.create_output_folder()
        self.run_parallel_calcs()
        self.concatenate_data()
        logger.info("Done!")


if __name__ == "__main__":
    work = OADataDownload()
    work.main()
