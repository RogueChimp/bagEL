import logging

from bagel import Bagel, BagelIntegration


logging.basicConfig(
    level=logging.WARNING,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class Template(BagelIntegration):

    name = "template" # name of source system

    def __init__(self) -> None:
        self.is_generator = True # for example purposes only

    def get_data(
        self,
        table: str,
        **kwargs
    ):
        """`get_data()` is how you extract data from the source system

        `PEP 484`_ type annotations are supported. If attribute, parameter, and
        return types are annotated according to `PEP 484`_, they do not need to be
        included in the docstring:

        Args:
            table (str): The name of the "table" that is being uploaded.
            **kwargs:
                last_run_timestamp (datetime.datetime): Last time the table has been run. Optional.
                current_timestamp (datetime.datetime): Current timestamp for timeboxing. Optional.
                elt_type (str): Type of elt. Optional.

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
                yield d

            return None

        else:
            data = [
                {"a": 0},
                {"b": 1},
                {"c": 2},
            ]

            return data


if __name__ == "__main__":

    bagel = Bagel(Template())

    bagel.run()