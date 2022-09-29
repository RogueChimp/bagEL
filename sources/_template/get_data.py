import logging

from bagel import Bagel, BagelIntegration, Bite, Table

from dotenv import load_dotenv

load_dotenv(override=True)


logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


def utility_function():
    return "foo"


class Template(BagelIntegration):

    source = "template"  # name of source system

    def __post_init__(self) -> None:
        self.is_generator = True  # for example purposes only

    def _helper_function(self):
        return self.is_generator

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        """`get_data()` is how you extract data from the source system

        `PEP 484`_ type annotations are supported. If attribute, parameter, and
        return types are annotated according to `PEP 484`_, they do not need to be
        included in the docstring:

        Args:
            table (bagel.Table)
            last_run_timestamp (datetime)
            current_timestamp (datetime)

        Returns:
            generator or list: A generator yielding lists of data dictionaries, or a list of data dictionaries

        """
        if self.is_generator:

            data = [
                [{"a": 0}],
                [{"b": 1}],
                [{"c": 2}],
            ]

            for d in data:
                yield Bite(d)

            return None

        else:
            data = [
                {"a": 0},
                {"b": 1},
                {"c": 2},
            ]

            return Bite(data)


if __name__ == "__main__":

    bagel = Bagel(Template())

    bagel.run()
