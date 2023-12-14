import os
from datetime import datetime
import requests
import logging
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bagel import Bagel, BagelIntegration, Bite, Table

logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Workday(BagelIntegration):
    source = "workday"

    ##############
    # Initialize #
    ##############

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self.workday_username = os.environ["WORKDAY_USERNAME"]
        self._workday_password = os.environ["WORKDAY_PASSWORD"]

    ###########
    # Get URL #
    ###########

    def workday_get_url(
        self, report, as_of_date, last_run_timestamp, current_timestamp
    ):
        query_url = f"https://services1.myworkday.com/ccx/service/{report}/trimedx/000003640/TMX_FIN_OUT_EmpByCostCenter?format=json"
        as_of_date = "As_of_Date=" + as_of_date
        updated_date_from = "Updated_Date_From=" + last_run_timestamp
        updated_date_to = "Updated_Date_To=" + current_timestamp

        query_url = f"{query_url}&{as_of_date}&{updated_date_from}&{updated_date_to}"

        return query_url

    ################################
    # Get Data (Make the API Call) #
    ################################

    def workday_api_call(self, url):
        # set up request features to try again in case of ConnectionError (Max retries exceeded with url)
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        response = session.get(
            url, auth=(self.workday_username, self._workday_password)
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"ERROR running {url}\n{response.status_code = }\n{response.text}"
            )

        data = response.json()
        data_list = [data]

        return data_list

    ########
    # MAIN #
    ########

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        table_name = table.name
        updated_date_from = last_run_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        updated_date_to = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        as_of_today = updated_date_to

        self.url = self.workday_get_url(
            table_name, as_of_today, updated_date_from, updated_date_to
        )

        data = self.workday_api_call(self.url)

        yield Bite(data)

        return None


######################
# Ready, Set, Action #
######################

if __name__ == "__main__":
    bagel = Bagel(Workday())

    bagel.run()
