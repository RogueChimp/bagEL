from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem
import os


class DataDogLogSubmitter:
    def __init__(self):
        self._api_key = os.getenv("DATADOG_API_KEY_BAGEL")

    def submit_log(self, log_data):
        body = HTTPLog([HTTPLogItem(**log_data)])
        configuration = Configuration()
        configuration.api_key["apiKeyAuth"] = self._api_key
        with ApiClient(configuration) as api_client:
            api_instance = LogsApi(api_client)
            response = api_instance.submit_log(body=body)
        return response
