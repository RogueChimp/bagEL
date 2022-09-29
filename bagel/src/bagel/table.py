from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional
from typing_extensions import Self


def throw_error(msg: Optional[str] = None):
    # TODO: convert to error class
    raise RuntimeError(msg if msg else "TABLE ERROR")


@dataclass(eq=True)
class Table:
    """Use `Table.from_config(config)` to create an instance of Table."""

    name: str
    raw_config: Dict[str, any] = None
    elt_type: Optional[str] = None
    historical_batch: Optional[bool] = None
    historical_frequency: Optional[str] = None
    file_format: Optional[str] = None
    initial_timestamp: Optional[datetime] = None

    def __post_init__(self):
        self.name = self._format_table_name(self.name)

    def _format_table_name(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("-", "_")

    def __str__(self) -> str:
        return self.name

    @classmethod
    def from_config(cls, table_config: Dict[str, any]) -> Self:

        if "name" not in table_config:
            throw_error("Missing name in table config")

        return cls(
            name=table_config.get("name"),
            elt_type=table_config.get("elt_type"),
            historical_batch=table_config.get("historical_batch", False),
            historical_frequency=table_config.get("historical_frequency"),
            file_format=table_config.get("file_format"),
            initial_timestamp=table_config.get("initial_timestamp"),
            raw_config=table_config,
        )


if __name__ == "__main__":
    print(Table.load_table({"name": "test_table"}))
