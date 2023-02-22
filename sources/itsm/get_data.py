import os
from datetime import datetime
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bagel import Bagel, BagelIntegration, Bite, Table

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class ITSMData(BagelIntegration):

    source = "itsm"

    ##############
    # Initialize #
    ##############

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self._itsm_user = os.environ["ITSM_USER"]
        self._itsm_password = os.environ["ITSM_PASSWORD"]
        self.base_url = os.environ["ITSM_BASE_URL"]

    ###########
    # Get URL #
    ###########

    def itsm_get_url(self, table, page_size=50, offset=0):
        param_page_size = f"sysparm_limit={page_size}"
        param_offset = f"sysparm_offset={offset}"
        PARAM_EXCLUDE_LINK = "sysparm_exclude_reference_link=true"
        PARAM_INCLUDE_DV = "sysparm_display_value=all"
        PARAM_ORDER_BY = "sysparm_query=ORDERBYDESCsys_updated_on"
        PARAM_QA_TABLE_NAME = "table_name=x_ahho_compliance_request"

        table_url = f"{self.base_url}{table}?{param_page_size}&{param_offset}&{PARAM_EXCLUDE_LINK}&{PARAM_INCLUDE_DV}&{PARAM_ORDER_BY}"

        # add an extra parameter to this table call to limit large dataset to return only relevant records
        if table.upper() == "QUESTION_ANSWER":
            table_url += f"&{PARAM_QA_TABLE_NAME}"

        return table_url

    ################################
    # Get Data (Make the API Call) #
    ################################

    def itsm_api_call(self, url):

        # set up request features to try again in case of ConnectionError (Max retries exceeded with url)
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        # get the data for this page
        response = session.get(url, auth=(self._itsm_user, self._itsm_password))
        data = response.json()
        data_list = [data]
        total_row_count = int(response.headers["X-Total-Count"])

        return data_list, total_row_count

    ########
    # MAIN #
    ########

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):

        # initialize variables
        data = {}
        current_offset = 0
        per_page = 500
        out_of_bounds = False
        min_updated_datetime = datetime.now()

        while min_updated_datetime >= last_run_timestamp and not out_of_bounds:

            # set the url for the current page
            table_name = table.name
            self.next_url = self.itsm_get_url(
                table_name,
                page_size=per_page,
                offset=current_offset,
            )

            # get the data for this page and add it to the final list
            data, total_row_count = self.itsm_api_call(self.next_url)

            # check the number of actual rows returned in case it is under the set per_page number
            if current_offset + per_page > total_row_count:
                per_page = int(abs(total_row_count - current_offset))

            # update loop variables
            min_updated_datetime = datetime.strptime(
                data[0]["result"][per_page - 1]["sys_updated_on"]["value"],
                "%Y-%m-%d %H:%M:%S",
            )
            current_offset += per_page
            if total_row_count <= current_offset:
                out_of_bounds = True

            # before appending everything but after setting the variable, only add things from data to dict_list that are from the load datetime or more recent
            dict_list = []

            for row in range(per_page):
                updated_datetime = datetime.strptime(
                    data[0]["result"][row]["sys_updated_on"]["value"],
                    "%Y-%m-%d %H:%M:%S",
                )

                if updated_datetime >= last_run_timestamp:
                    dict_list.append(data[0]["result"][row])

            yield Bite(dict_list)

        return None


######################
# Ready, Set, Action #
######################

if __name__ == "__main__":
    bagel = Bagel(ITSMData())

    bagel.run()
