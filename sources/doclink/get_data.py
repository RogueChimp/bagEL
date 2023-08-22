import os
import requests
import logging
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bagel import Bagel, BagelIntegration, Bite, Table


logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class DocLink(BagelIntegration):
    source = "doclink"

    ##############
    # Initialize #
    ##############

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self.doclink_username = os.environ["DOCLINK_USERNAME"]
        self._doclink_password = os.environ["DOCLINK_PASSWORD"]
        self._doclink_site_code = os.environ["DOCLINK_SITE_CODE"]
        self.doclink_base_url = os.environ["DOCLINK_BASE_URL"]

    ##############
    # Get Header #
    ##############

    def doclink_get_header(self, doclink_authcode):
        header = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "AuthCode": doclink_authcode,
        }
        header = json.loads(str(header).replace("'", '"'))

        return header

    ##########
    # Log in #
    ##########

    def doclink_login(self, base_url, header):
        login_url = base_url + "LoginCloud"
        login = f'{{ "SiteCode": "{self._doclink_site_code}", "UserId": "{self.doclink_username}", "Password": "{self._doclink_password}", "MachineName": "Jacob" }}'

        response = requests.post(login_url, headers=header, data=login)
        doclink_authcode = str(response.json())

        return doclink_authcode

    ###########
    # Log out #
    ###########

    def doclink_logout(self, base_url, header):
        logout_url = base_url + "Logout"
        requests.post(logout_url, headers=header)

        return None

    ################################
    # Get Data (Make the API Call) #
    ################################

    def doclink_api_call(self, table, run_date, base_url, header):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        # initialize variables
        query_url = base_url + "ExecProcedure"
        procedure_name = '"Custom_' + str(table) + '"'
        body = f'{{ "ProcedureName": {procedure_name}, "Filters": [ {{"ArgumentName": "runDate", "ArgumentValue": "{run_date}"}} ] }}'
        data = {}
        dict_list = []

        # get the data for this page and add it to the final list
        response = session.post(query_url, headers=header, data=body)

        if response.status_code != 200:
            raise RuntimeError(
                f"ERROR running {query_url}\n{response.status_code = }\n{response.text}"
            )

        # the data returns with Columns separate from the Rows, so we need to rearrange to the standard key:value format
        try:
            data = response.json()
            cols = [x["Name"] for x in data[0]["Columns"]]
            rows = data[0]["Rows"]
            for row in rows:
                res = {}
                for i, key in enumerate(cols):
                    res[key] = row[i]
                dict_list.append(res)
        except ValueError:
            print("No Data Returned.")

        return dict_list

    ########
    # MAIN #
    ########

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        # initialize variables
        doclink_authcode = ""
        base_url = self.doclink_base_url
        last_run_date = last_run_timestamp.date()

        # initialize header
        header = self.doclink_get_header(doclink_authcode)

        # log in to get auth code
        doclink_authcode = self.doclink_login(base_url, header)

        # now that we have the auth code, plug it into the header for the main API call
        header = self.doclink_get_header(doclink_authcode)

        # get the data for this page and add it to the final list
        dict_list = self.doclink_api_call(table, last_run_date, base_url, header)

        # now that we have what we need, log out
        self.doclink_logout(base_url, header)

        yield Bite(dict_list)

        return None


######################
# Ready, Set, Action #
######################

if __name__ == "__main__":
    bagel = Bagel(DocLink())

    bagel.run()
