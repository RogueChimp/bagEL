import os
import time
import logging
import pendulum
import requests

from bagel import Bagel, BagelIntegration, Bite, Table


logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Okta(BagelIntegration):

    source = "okta"

    def __post_init__(self) -> None:
        self._load_config()
        self.base_url = "https://trimedxext.okta.com/api/v1/"

    def _load_config(self):
        self._auth_secret = os.getenv("OKTA_SECRET")

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):

        table_name = table.name

        self.next_url = self.okta_get_url(
            table_name, last_run_timestamp, current_timestamp
        )
        rate_limit = -1
        while self.next_url:
            data_log_details = {}
            data_log_details["url"] = self.next_url
            if rate_limit == 0:
                wait_duration = self.okta_get_next_reset_time(next_reset_epoch)
                data_log_details["sleeping"] = wait_duration
                time.sleep(wait_duration)
            data, self.next_url, rate_limit, next_reset_epoch = self.okta_get_data(
                self.next_url
            )
            yield Bite(data)
        return None

    def okta_get_url(self, table_name, last_run_timestamp, current_timestamp):
        last_run_timestamp = last_run_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fz")
        current_timestamp = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fz")
        since = f"since={last_run_timestamp}"
        until = f"until={current_timestamp}"
        url = f"{self.base_url}{table_name}?{since}&{until}&limit=1000"
        return url

    def okta_get_data(self, url):

        logging.info(f"url: {url}")
        query = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"SSWS {self._auth_secret}",
        }
        response = requests.get(url, headers=query)
        data = response.json()
        # logging.warning(f'data: {str(data)}')
        status_code = response.status_code
        headers = response.headers
        logging.debug(f"headers: {headers}")
        if data == []:
            self.next_url = None
        else:
            self.next_url = self.okta_get_next_url(headers, status_code)
        try:
            rate_limit = int(headers["x-rate-limit-remaining"])
            next_reset_epoch = int(headers["x-rate-limit-reset"])
        except:
            print("not able to get headers")
        return data, self.next_url, rate_limit, next_reset_epoch

    def okta_get_next_url(self, headers, status_code):

        try:
            links = headers["link"].split(",")
            for link in links:
                if 'rel="next"' in link:
                    next_url_candidate = (
                        link.split(";")[0].replace("<", "").replace(">", "")
                    )
                    if self.next_url == next_url_candidate:
                        self.next_url = None
                    else:
                        self.next_url = next_url_candidate
                else:
                    self.next_url = None
        except:
            self.next_url = None
        if status_code != 200:
            self.next_url = None

        return self.next_url

    def okta_get_next_reset_time(self, next_reset_epoch):
        next_reset = pendulum.from_timestamp(next_reset_epoch)
        next_reset_duration = pendulum.now().diff(next_reset).in_seconds() + 30
        return next_reset_duration


if __name__ == "__main__":

    bagel = Bagel(Okta())

    bagel.run()
