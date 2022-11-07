import datetime
import looker_sdk
import logging
from bagel import Bagel, BagelIntegration, Bite, Table

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Looker_SDK(BagelIntegration):

    source = "looker_sdk"

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self.sdk = looker_sdk.init40()
        return self.sdk

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        """`hasattr()` and `getattr()` are used here to dynamically call
        the corresponding function to the formatted table name."""

        return getattr(self, table.name)(table, last_run_timestamp, current_timestamp)

    def all_users(self, *args, **kwargs):
        """gets all user's data"""
        all_users = self.sdk.all_users()
        for user in all_users:
            yield Bite([user.__dict__])

    def user_attributes(self, *args, **kwargs):
        """gets all users, then loops through each user getting their attributes."""
        all_users = self.sdk.all_users()
        for user in all_users:
            user_attribtues_data = []
            user_attribtues = self.sdk.user_attribute_user_values(user_id=user.id)
            for attribute in user_attribtues:
                user_attribtues_data.append(attribute.__dict__)
            yield Bite(user_attribtues_data)

    def all_dashboards(self, table, last_run_timestamp, current_timestamp):
        """gets all dashboard data"""
        all_dashboards = self.sdk.all_dashboards()
        for dashboardBase in all_dashboards:
            dashboard = self.sdk.dashboard(dashboard_id=dashboardBase.id)
            updated_time = dashboard.updated_at or datetime.datetime(
                2022, 1, 1, tzinfo=datetime.timezone.utc
            )
            if (
                updated_time.timestamp() >= last_run_timestamp.timestamp()
                and updated_time.timestamp() <= current_timestamp.timestamp()
            ):
                yield Bite([dashboard.__dict__])

    def all_folders(self, *args, **kwargs):
        """gets all folder data"""
        all_folders = self.sdk.all_folders()
        for folder in all_folders:
            yield Bite([folder.__dict__])


if __name__ == "__main__":

    bagel = Bagel(Looker_SDK())

    bagel.run()
