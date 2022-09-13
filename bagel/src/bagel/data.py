from dataclasses import dataclass
from typing import Dict, List, Optional, Union


@dataclass(eq=True, frozen=True)
class Bite:

    content: Union[bytes, List[Dict]]
    file_name: Optional[str] = None

    def __post_init__(self):
        self._validate_content(self.content)

    @staticmethod
    def _validate_content(content):

        if not (isinstance(content, bytes) or isinstance(content, list)):
            raise TypeError(
                f"Datapoint needs to be of type bytes or list. Not {type(content)}"
            )

        if (
            isinstance(content, list)
            and len(content) > 0
            and isinstance(content[0], list)
        ):
            raise TypeError("Cannot be list of lists. Use pagination/generator.")
