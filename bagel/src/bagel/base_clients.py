import abc
from datetime import datetime
from typing import Optional, Tuple

from bagel.util import get_current_timestamp


class ClientInterface:  # pragma: nocover
    def connect(self):
        pass

    def close(self):
        pass


class TimeboxClient(ClientInterface, metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: nocover
        return (
            hasattr(subclass, "get_last_run_timestamp")
            and callable(subclass.get_last_run_timestamp)
            and hasattr(subclass, "write_run_timestamp")
            and callable(subclass.write_run_timestamp)
            and hasattr(subclass, "get_timebox")
            and callable(subclass.get_timebox)
        )

    @abc.abstractmethod
    def get_last_run_timestamp(
        self, system: str, table: str, initial_timestamp: Optional[datetime] = None
    ) -> datetime:  # pragma: nocover
        pass

    @abc.abstractmethod
    def write_run_timestamp(
        self, system: str, table: str, timestamp: datetime
    ):  # pragma: nocover
        pass

    def get_current_timestamp(self) -> datetime:
        return get_current_timestamp()

    def get_timebox(
        self, system: str, table: str, initial_timestamp: Optional[datetime] = None
    ) -> Tuple[datetime, datetime]:
        # get current timestamp
        current_timestamp = self.get_current_timestamp()

        # get last run timestamp
        last_run_timestamp = self.get_last_run_timestamp(
            system, table, initial_timestamp
        )

        return last_run_timestamp, current_timestamp


class StorageClient(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: nocover
        return (
            hasattr(subclass, "upload_log")
            and callable(subclass.upload_log)
            and hasattr(subclass, "upload_data")
            and callable(subclass.upload_data)
        )

    @abc.abstractmethod
    def upload_log(self, log: any, file_name: str):  # pragma: nocover
        pass

    @abc.abstractmethod
    def upload_data(self, data: any, file_name: str):  # pragma: nocover
        pass
