import requests
from bagel import Bagel, BagelIntegration, Bite, Table
import logging

logger = logging.getLogger(__name__)


class Schmear(BagelIntegration):

    source = "sample"

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):

        data = requests.get("http://127.0.0.1:5000").json()

        while data:
            yield Bite(data)
            data = requests.get("http://127.0.0.1:5000").json()
        return None


if __name__ == "__main__":

    bagel = Bagel(Schmear())
    bagel.run()
