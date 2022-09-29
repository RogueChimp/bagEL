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
