import looker_sdk
import logging
from bagel import Bagel, BagelIntegration, Bite

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Looker_SDK(BagelIntegration):

    name = "looker_sdk"

    def __init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self.sdk = looker_sdk.init40()
        return self.sdk

    def get_data(self, table: str, **kwargs):
        """`hasattr()` and `getattr()` are used here to dynamically call
        the corresponding function to the formatted table name."""

        return getattr(self, table)(table, **kwargs)

    def all_users(self, table, **kwargs):
        """gets all user's data"""
        all_users = self.sdk.all_users()
        for user in all_users:
            yield Bite([user.__dict__])

    def user_attributes(self, table, **kwargs):
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
