from datetime import datetime
from typing import Optional
from src.bagel.base_clients import TimeboxClient, StorageClient


class MockTimeboxClient(TimeboxClient):
    def __init__(
        self, last_run_timestamp: datetime = None, new_timestamp: datetime = None
    ):
        self.lr_t = last_run_timestamp
        self.n_t = new_timestamp

    def get_last_run_timestamp(
        self, system: str, table: str, initial_timestamp: Optional[datetime] = None
    ) -> datetime:
        return self.lr_t

    def write_run_timestamp(
        self, system: str, table: str, timestamp: Optional[datetime] = None
    ) -> any:
        self.lr_t = self.n_t
        return f"Timestamp overwritten as {self.lr_t}"


class MockStorageClient(StorageClient):
    def upload_data(self, data: any, file_name: str):
        return f"Log {file_name} uploaded:\n{data}"

    def upload_log(self, log: any, file_name: str):
        return f"Log {file_name} uploaded:\n{log}"


class MockResponse:
    def __init__(self, **kwargs):
        self.json_data = kwargs.get("json_data")
        self.status_code = kwargs.get("status_code")
        self.text = kwargs.get("text", "fake text")
        self.content = kwargs.get("content")

    def json(self):
        return self.json_data


class MockDataDogResponse:
    def mock_get_request_200(**kwargs):
        url = kwargs.get("url", None)
        params = kwargs.get("params", None)
        headers = kwargs.get("headers", None)
        json = kwargs.get("json", None)
        return MockResponse(
            json_data=[
                {"col1": 1, "col2": 2},
                {"col1": 3, "col2": 4},
            ],
            status_code=200,
        )
