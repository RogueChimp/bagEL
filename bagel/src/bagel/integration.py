import abc
from typing import Generator, List, Union, final

from .data import Bite
from .table import Table


class BagelIntegration(metaclass=abc.ABCMeta):

    name: str

    @final
    def __init__(self, **kwargs):
        self.__post_init__(**kwargs)

    def __post_init__(self, **kwargs):
        """Any initialization by the user should be handled here."""
        pass

    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: nocover
        return hasattr(subclass, "get_data") and callable(subclass.get_data)

    @abc.abstractmethod
    def get_data(
        self, table: Table, **kwargs
    ) -> Union[Generator[Bite, None, None], List[Bite]]:  # pragma: nocover
        """This function contains all logic involved in extracting data
        from a source system. It returns data as a `Bite` object so
        Bagel can process it.
        """
        pass
