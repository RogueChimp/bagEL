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


if __name__ == "__main__":

    bagel = Bagel(Looker_SDK())

    bagel.run()
