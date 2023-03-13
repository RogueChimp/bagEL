import os
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bagel import Bagel, BagelIntegration, Bite, Table


logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Aha(BagelIntegration):

    source = "aha"

    ##############
    # Initialize #
    ##############

    def __post_init__(self) -> None:
        self.base_url = "https://trimedx-solutions.aha.io/api/v1/"

    ###########################
    # Get URL (Updated_Since) #
    ###########################

    def aha_get_url(
        self, table, last_run_timestamp, idea_id=1, page=1, idea_list=False
    ):
        IDEAS = "ideas"
        last_run_timestamp = last_run_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fz")
        updated_since = f"updated_since={last_run_timestamp}"
        paging = f"page={page}"
        per_page = "per_page=200"

        if idea_list:
            table_url = f"{self.base_url}{IDEAS}?{updated_since}&{per_page}&{paging}"
        elif table == "endorsements":
            table_url = f"{self.base_url}{IDEAS}/{idea_id}/{table}?{updated_since}&{per_page}&{paging}"
        elif table == "ideas":
            table_url = (
                f"{self.base_url}{table}/{idea_id}?{updated_since}&{per_page}&{paging}"
            )

        return table_url

    ##############
    # Get Header #
    ##############

    def aha_get_header(self):
        self._auth_secret = os.getenv("AHA_TOKEN")
        header = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self._auth_secret}",
        }

        return header

    ################################
    # Get Data (Make the API Call) #
    ################################

    def aha_api_call(self, table, url, header, idea_list=False):

        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        # get the data for this page and add it to the final list
        response = session.get(url, headers=header)

        if response.status_code != 200:
            raise RuntimeError(
                f"ERROR running {url}\n{response.status_code = }\n{response.text}"
            )

        data = response.json()
        data_list = [data]

        # the ideas call will bring one idea at a time, so it will always be 1 page (it doesn't return "pagination")
        if table == "endorsements" or idea_list == True:
            total_pages = data["pagination"]["total_pages"]
        elif table == "ideas":
            total_pages = 1

        return data_list, total_pages

    ########
    # MAIN #
    ########

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):

        # initialize variables
        ZERO = 0
        idea_ids = []
        current_page = 1
        self.header = self.aha_get_header()

        #
        # first we want to get a list of ideas to get votes for
        #

        # get first URL
        table_name = table.name
        self.next_url = self.aha_get_url(
            table_name, last_run_timestamp, page=current_page, idea_list=True
        )

        # while there is another page
        while self.next_url:

            # set total pages expected
            # start building idea_ids to pass to "endorsements"
            data, total_pages = self.aha_api_call(
                table_name, self.next_url, self.header, idea_list=True
            )

            # add idea ids to list if applicable
            ideas_json = data[0]["ideas"]
            for row in ideas_json:
                idea_ids.append(row["id"])

            # if this isn't the final page, get the URL set for the next page and repeat the while loop
            if current_page < total_pages:
                current_page += 1
                self.next_url = self.aha_get_url(
                    table_name,
                    last_run_timestamp,
                    page=current_page,
                    idea_list=True,
                )
            else:
                self.next_url = None

        #
        # now that we have a list of ideas, get the data for each of them
        #

        # pseudo do-while loop
        while len(idea_ids) != 0:

            # reset the current_page variable
            current_page = 1

            # get first URL
            self.next_url = self.aha_get_url(
                table_name, last_run_timestamp, idea_ids[ZERO], current_page
            )

            # while there is another page
            while self.next_url:

                # return data to azure
                # set total pages expected for table
                data, total_pages = self.aha_api_call(
                    table_name, self.next_url, self.header
                )
                yield Bite(data)

                # if this isn't the final page, get the URL set for the next page and repeat the while loop
                if current_page < total_pages:
                    current_page += 1
                    self.next_url = self.aha_get_url(
                        table_name,
                        last_run_timestamp,
                        idea_ids[ZERO],
                        current_page,
                    )
                else:
                    self.next_url = None

            # once all the pages for the idea_id have been processed, pop the idea_id we just used so we can use the next one in line
            # the loop will stop once there are no more values in idea_ids[]
            idea_ids.pop(ZERO)

        return None


######################
# Ready, Set, Action #
######################

if __name__ == "__main__":

    bagel = Bagel(Aha())

    bagel.run()
