from datetime import datetime
import json
import os
from typing import Optional
import requests
from bagel import Bagel, BagelIntegration
import logging

logger = logging.getLogger(__name__)


class Schmear(BagelIntegration):

    name = "sample"

    def get_data(
        self,
        table: str,
        last_run_timestamp: Optional[datetime] = None,
        current_timestamp: Optional[datetime] = None,
        elt_type: Optional[str] = None
    ) -> str:
        
        data = requests.get("http://127.0.0.1:5000").json()

        while data:
            yield data
            data = requests.get("http://127.0.0.1:5000").json()
        return None

if __name__ == "__main__":

    bagel = Bagel('bagel.cfg', Schmear())
    bagel.run()