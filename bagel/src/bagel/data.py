from dataclasses import dataclass
from typing import Dict, List, Optional, Union


@dataclass(eq=True, frozen=True)
class Bite:

    data: Union[bytes, List[Dict]]
    file_name: Optional[str] = None

    def __post_init__(self):
        self._validate_content(self.data)

    @staticmethod
    def _validate_content(data):

        if not (isinstance(data, bytes) or isinstance(data, list)):
            raise TypeError(
                f"Datapoint needs to be of type bytes or list. Not {type(data)}"
            )

        if isinstance(data, list) and len(data) > 0 and not isinstance(data[0], dict):
            raise TypeError(
                "Bite data must be bytes or a list of dicts. If returning list of lists, use pagination/generator instead."
            )
