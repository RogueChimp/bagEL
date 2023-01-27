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


def datetime_to_ms_epoch(dt):
    microseconds = round(
        (calendar.timegm(dt.timetuple()) * 1000) + (dt.microsecond / 10000)
    )
    return microseconds


class Liferay_backend(BagelIntegration):

    source = "liferay_backend"

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self._auth_user = os.getenv("LIFERAY_BACKEND_USER")
        self._auth_password = os.getenv("LIFERAY_BACKEND_SECRET")
        self._base_url = os.getenv("LIFERAY_BACKEND_BASE_URL")

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        """`get_data()` is how you extract data from the source system

        `PEP 484`_ type annotations are supported. If attribute, parameter, and
        return types are annotated according to `PEP 484`_, they do not need to be
        included in the docstring:
        """
        return getattr(self, table.name)(table, last_run_timestamp, current_timestamp)

    def customers(self, table: Table, last_run_timestamp, current_timestamp):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        logging.info(f"{cust_url = }")
        data = self.liferay_get_data(cust_url)
        to_send = []
        for record in data:
            if self.is_modified_record(record, last_run_timestamp, current_timestamp):
                to_send.append(record)
        yield Bite(to_send)

    def users(self, table: Table, last_run_timestamp, current_timestamp):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            user_url = (
                self._base_url
                + f"trimedxcore.customer/get-users?customerId={customerid}&start=-1&end=-1"
            )
            data = self.liferay_get_data(user_url)
            for record in data:
                if self.is_modified_record(
                    record, last_run_timestamp, current_timestamp
                ):
                    record["customerId"] = customerid
                    to_send.append(record)
        yield Bite(to_send)

    def user_affiliations(self, table: Table, last_run_timestamp, current_timestamp):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            user_url = (
                self._base_url
                + f"trimedxcore.customer/get-users?customerId={customerid}&start=-1&end=-1"
            )
            users = self.liferay_get_data(user_url)
            for user in users:
                userid = user["userId"]
                affil_url = (
                    self._base_url
                    + f"trimedxcore.customerlocationuser/get-user-affiliations?customerId={customerid}&userId={userid}"
                )
                logging.info(affil_url)
                data = self.liferay_get_data(affil_url)
                for record in data:
                    if self.is_modified_record(
                        record, last_run_timestamp, current_timestamp
                    ):
                        record["customerId"] = customerid
                        record["userId"] = userid
                        to_send.append(record)
        yield Bite(to_send)

    def customer_health_systems(
        self, table: Table, last_run_timestamp, current_timestamp
    ):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            health_system_url = (
                self._base_url
                + f"trimedxcore.customerlocation/get-customer-health-systems?customerId={customerid}&start=-1&end=-1"
            )
            data = self.liferay_get_data(health_system_url)
            for record in data:
                if self.is_modified_record(
                    record, last_run_timestamp, current_timestamp
                ):
                    record["customerId"] = customerid
                    to_send.append(record)
        yield Bite(to_send)

    def user_sites(self, table: Table, last_run_timestamp, current_timestamp):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            user_url = (
                self._base_url
                + f"trimedxcore.customer/get-users?customerId={customerid}&start=-1&end=-1"
            )
            users = self.liferay_get_data(user_url)
            for user in users:
                userid = user["userId"]
                site_url = (
                    self._base_url
                    + f"trimedxcore.customerlocationuser/get-user-sites?customerId={customerid}&userId={userid}"
                )
                logging.info(site_url)
                data = self.liferay_get_data(site_url)
                for record in data:
                    if self.is_modified_record(
                        record, last_run_timestamp, current_timestamp
                    ):
                        record["customerId"] = customerid
                        record["userId"] = userid
                        to_send.append(record)
        yield Bite(to_send)

    def user_health_systems(self, table: Table, last_run_timestamp, current_timestamp):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            user_url = (
                self._base_url
                + f"trimedxcore.customer/get-users?customerId={customerid}&start=-1&end=-1"
            )
            users = self.liferay_get_data(user_url)
            for user in users:
                userid = user["userId"]
                health_system_url = (
                    self._base_url
                    + f"trimedxcore.customerlocationuser/get-user-health-systems?customerId={customerid}&userId={userid}"
                )
                logging.info(health_system_url)
                data = self.liferay_get_data(health_system_url)
                for record in data:
                    if self.is_modified_record(
                        record, last_run_timestamp, current_timestamp
                    ):
                        record["customerId"] = customerid
                        record["userId"] = userid
                        to_send.append(record)
        yield Bite(to_send)

    def customer_affiliations(
        self, table: Table, last_run_timestamp, current_timestamp
    ):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            affiliations_url = (
                self._base_url
                + f"trimedxcore.customerlocation/get-customer-affiliations?customerId={customerid}&start=-1&end=-1"
            )
            data = self.liferay_get_data(affiliations_url)
            for record in data:
                if self.is_modified_record(
                    record, last_run_timestamp, current_timestamp
                ):
                    record["customerId"] = customerid
                    to_send.append(record)
        yield Bite(to_send)

    def customer_sites(self, table: Table, last_run_timestamp, current_timestamp):
        cust_url = (
            self._base_url + f"trimedxcore.customer/get-customers?start=-1&end=-1"
        )
        customers = self.liferay_get_data(cust_url)
        to_send = []
        for cust in customers:
            customerid = cust["customerId"]
            sites_url = (
                self._base_url
                + f"trimedxcore.customerlocation/get-customer-sites?customerId={customerid}&start=-1&end=-1"
            )
            data = self.liferay_get_data(sites_url)
            for record in data:
                if self.is_modified_record(
                    record, last_run_timestamp, current_timestamp
                ):
                    record["customerId"] = customerid
                    to_send.append(record)
        yield Bite(to_send)

    def liferay_get_data(self, url):
        response = requests.get(url, auth=(self._auth_user, self._auth_password))
        if response.status_code != 200:
            raise RuntimeError(
                f"ERROR running {url}\n{response.status_code = }\n{response.text}"
            )
        return response.json()

    def is_modified_record(self, record, last_run_timestamp, current_timestamp):
        last_run = datetime_to_ms_epoch(last_run_timestamp)
        current_time = datetime_to_ms_epoch(current_timestamp)
        modifiedDate = record["modifiedDate"]
        if isinstance(modifiedDate, int) == False:
            raise RuntimeError(
                f"ERROR converting modifiedDate to timestamp\n{modifiedDate}"
            )
        elif modifiedDate > last_run and modifiedDate <= current_time:
            return True
        else:
            return False


if __name__ == "__main__":

    bagel = Bagel(Liferay_backend())

    bagel.run()
