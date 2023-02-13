import os
import requests
import logging
import time
import json
from bagel import Bagel, BagelIntegration, Bite, Table

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class LiferayAnalyticsCloud(BagelIntegration):

    source = "liferay_analytics_cloud"

    def __post_init__(self) -> None:
        self._load_config()
        self.base_url = "http://analytics.liferay.com/api/reports/export/"

    def _load_config(self):
        self.__auth_secret = os.getenv("LIFERAY_ANALYTICS_CLOUD_TOKEN")

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        table_name = table.name
        self.url = self.liferay_analytics_cloud_get_url(
            table_name, last_run_timestamp, current_timestamp
        )
        self.file_status = "PENDING"
        data = self.liferay_analytics_cloud_get_data(self.file_status, self.url)
        return Bite(data)

    def liferay_analytics_cloud_get_url(
        self, table_name, last_run_timestamp, current_timestamp
    ):
        last_run_timestamp = (
            last_run_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        current_timestamp = (
            current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        since = f"fromDate={last_run_timestamp}"
        until = f"toDate={current_timestamp}"
        url = f"{self.base_url}{table_name}?{since}&{until}"
        return url

    def liferay_analytics_cloud_get_data(self, file_status, url):
        logging.info(f"url: {url}")
        query = {"Authorization": f"Bearer {self.__auth_secret}", "User-Agent": "bagEL"}
        while file_status == "PENDING":
            data = []
            logging.info(f"...looking for file")
            response = requests.get(url, headers=query)
            data_text = response.text
            if "Content-Type" not in response.headers:
                logging.warning(f"...Blank response. Likely an auth problem... {url}")
            status = response.status_code
            if status != 200:
                logging.error(f"...API Error {status}")
                raise ValueError(f"API error.{status}...{url}")
            split_text = data_text.split("\n")
            logging.debug(f"...response.text: {data}")
            for item in split_text:
                if item:
                    j = json.loads(item)
                    data.append(j)
            logging.debug(f"...data: {data}")
            if any("status" in d for d in data):
                logging.info("..." + data[0]["status"])
                time.sleep(30)
            else:
                file_status = "COMPLETE"
        return data


if __name__ == "__main__":
    bagel = Bagel(LiferayAnalyticsCloud())

    bagel.run()
