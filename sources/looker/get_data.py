import os
import requests
import json
import datetime
import logging

from bagel import Bagel, BagelIntegration, Bite, Table

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Looker(BagelIntegration):

    source = "looker"

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self.__base_url = (
            os.getenv("LOOKER_URL", "")
            + ":"
            + os.getenv("LOOKER_PORT", "")
            + os.getenv("LOOKER_API_ENDPOINT", "")
        )
        self.__client_id = os.getenv("LOOKER_CLIENT_ID")
        self.__client_secret = os.getenv("LOOKER_CLIENT_SECRET")

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):

        headers = self.looker_login()
        data = self.looker_get_data(
            headers, table, last_run_timestamp, current_timestamp
        )
        return Bite(data)

    def get_data_payload(self, table_name: str):
        """
        Don't know what os this can run on, but we assume the same file structure, e.g.
        this file sits in the same directory as the queries.
        We find the table under the "query" directory.
        The query directory is assume to be under the directory that this file is contained in.
        However, since we don't know the os, we separate out terms via os.sep (which is os dependent)
        """
        with open(
            os.sep.join(
                [
                    os.path.dirname(os.path.realpath(__file__)),
                    "queries",
                    table_name + ".json",
                ]
            ),
            "r",
        ) as fil:
            return json.load(fil)

    def looker_login(self):
        params = {"client_id": self.__client_id, "client_secret": self.__client_secret}
        login_url = f"{self.__base_url}/login"
        response = requests.post(url=login_url, params=params)
        data = response.json()
        headers = {
            "authorization": "Bearer " + data["access_token"],
            "cache-control": "no-cache",
        }
        logging.info(f"got auth headers...")
        return headers

    def looker_get_data(
        self,
        headers,
        table: Table,
        last_run_timestamp=None,
        current_timestamp=None,
    ):

        query_url = f"{self.__base_url}/queries/run/json"
        logging.info(f"table: {table}")

        data_payload = self.get_data_payload(table.name)
        elt_type = table.elt_type

        if elt_type == "full":
            data_payload["filters"] = None

        elif elt_type == "delta":
            # Subtract 15 minutes from last run time to account for API delay
            from_timestamp = self._set_last_run_time(last_run_timestamp)
            # Format has to match "YYYY-MM-DD HH:MM:SS"
            from_time = self._format_to_looker_time(from_timestamp)
            to_time = self._format_to_looker_time(current_timestamp)

            # Every single json query has a unique created_time field that matches its table
            # e.g. history_query (history dash) => query.created_time,
            # history_user (user dash) => user.created_time, etc.
            created_time_field = list(data_payload["filters"].keys())[0]
            data_payload["filters"][created_time_field] = f"{from_time} to {to_time}"

        else:
            raise Exception("Invalid elt_type in tables.yaml file")

        logging.info(f"data_payload: {data_payload}")

        response = requests.post(url=query_url, json=data_payload, headers=headers)
        data = response.json()
        return data

    def _format_to_looker_time(self, timestamp):
        if isinstance(timestamp, datetime.datetime):
            # Want to round to nearest second above, as microsecond will break things
            # This doesnt really matter as the entire operation for pulling data will still take longer, but it's here if we needed
            # ts = timestamp + datetime.timedelta(seconds=1)

            return timestamp.strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise TypeError(
                "Invalid timestamp type; should be <class 'datetime.datetime'>"
            )

    def _set_last_run_time(self, timestamp):
        if isinstance(timestamp, datetime.datetime):
            # Want to add 15 minutes to the last run timestamp in order to account for a 10 minute latency w/ Looker API

            return timestamp - datetime.timedelta(minutes=15)
        else:
            raise TypeError(
                "Invalid timestamp type; should be <class 'datetime.datetime'>"
            )


if __name__ == "__main__":

    bagel = Bagel(Looker())

    bagel.run()
