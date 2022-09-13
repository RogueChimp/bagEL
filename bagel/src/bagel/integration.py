import abc
from datetime import datetime
from typing import Optional


class BagelIntegration(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return hasattr(subclass, "get_data") and callable(subclass.get_data)

    @abc.abstractmethod
    def get_data(
        self,
        table: str,
        **kwargs,
    ):
        pass

    def execute(self, table: str, **kwargs):

        data = self.get_data(table, **kwargs)

        return data
