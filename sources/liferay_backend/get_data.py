import calendar
import logging
import os
import requests

from bagel import Bagel, BagelIntegration, Bite, Table

from dotenv import load_dotenv

load_dotenv(override=True)


logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Liferay_backend(BagelIntegration):

    source = "liferay_backend"

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self._auth_user = os.getenv("LIFERAY_BACKEND_USER")
        self._auth_password = os.getenv("LIFERAY_BACKEND_SECRET")
        self._base_url = os.getenv("LIFERAY_BACKEND_BASE_URL")

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        table_name = table.name
        self.url = self.liferay_backend_get_url(
            table_name, last_run_timestamp, current_timestamp
        )
        data = self.liferay_backend_get_data(self.url)
        return Bite(data)

    def liferay_backend_get_url(
        self, table_name, last_run_timestamp, current_timestamp
    ):
        last_run_timestamp = (
            last_run_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        current_timestamp = (
            current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        since = f"startModifiedDate={last_run_timestamp}"
        until = f"endModifiedDate={current_timestamp}"
        table = str(table_name).replace("_", "-")
        url = f"{self._base_url}{table}?{since}&{until}"
        return url

    def liferay_backend_get_data(self, url):
        logging.info(f"url: {url}")
        response = requests.get(url, auth=(self._auth_user, self._auth_password))
        if response.status_code != 200:
            raise RuntimeError(
                f"ERROR running {url}\n{response.status_code = }\n{response.text}"
            )
        data = response.json()
        return data


if __name__ == "__main__":

    bagel = Bagel(Liferay_backend())

    bagel.run()
