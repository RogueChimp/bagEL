import os
import requests
import json
import datetime

from bagel import Bagel, BagelIntegration


class Looker(BagelIntegration):

    name = "looker"

    def __init__(self) -> None:
        self._load_config()
        self.base_url = "https://lookerdev.trimedx.com:443/api/4.0"

    def _load_config(self):
        self.__client_id = os.getenv("LOOKER_CLIENT_ID")
        self.__client_secret = os.getenv("LOOKER_CLIENT_SECRET")

    def get_data(self, table: str, **kwargs):
        headers = self.looker_login()
        data = self.looker_get_data(
            headers,
            table,
            kwargs.get("elt_type", "full"),
            kwargs.get("last_run_timestamp", None),
            kwargs.get("current_timestamp", None),
        )
        return data

    def get_data_payload(self, table: str):
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
                    table + ".json",
                ]
            ),
            "r",
        ) as fil:
            return json.load(fil)

    def looker_login(self):
        params = {"client_id": self.__client_id, "client_secret": self.__client_secret}
        login_url = f"{self.base_url}/login"
        response = requests.post(url=login_url, params=params)
        data = response.json()
        headers = {
            "authorization": "Bearer " + data["access_token"],
            "cache-control": "no-cache",
        }
        return headers

    def looker_get_data(
        self,
        headers,
        table,
        elt_type="full",
        last_run_timestamp=None,
        current_timestamp=None,
    ):
        query_url = f"{self.base_url}/queries/run/json"
        data_payload = self.get_data_payload(table)

        if elt_type == "full":
            data_payload["filters"] = None

        elif elt_type == "delta":
            # Format has to match "YYYY-MM-DD HH:MM:SS"
            from_time = self._format_to_looker_time(last_run_timestamp)
            to_time = self._format_to_looker_time(current_timestamp)

            # Every single json query has a unique created_time field that matches its table
            # e.g. history_query (history dash) => query.created_time,
            # history_user (user dash) => user.created_time, etc.
            created_time_field = list(data_payload["filters"].keys())[0]
            data_payload["filters"][created_time_field] = f"{from_time} to {to_time}"

        else:
            raise Exception("Invalid elt_type in tables.yaml file")

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


if __name__ == "__main__":

    bagel = Bagel(Looker())

    bagel.run()
