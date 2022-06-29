from bagel.bagel import BagelError

import pytest
import unittest
from unittest import mock
from unittest.mock import call

from src.bagel.bagel import Bagel
from src.bagel.integration import BagelIntegration


class TestBagel(unittest.TestCase):
    def setUp(self):
        class TestIntegration(BagelIntegration):

            name = "test_integration"

            def get_data(self):
                return None

        self.test_integration = TestIntegration()

    @pytest.mark.unit_test
    # @mock.patch("src.bagel.bagel.Bagel.foo")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_environment_variables_arent_set_then_raise_value_error(
        self, mock_getenv
    ):
        mock_getenv.return_value = None

        with self.assertRaises(ValueError) as e:
            Bagel(self.test_integration)
        assert isinstance(e.exception, ValueError)
        assert e.exception.args[0] == "Environment variables must be properly loaded."

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel._run_table")
    @mock.patch("src.bagel.bagel.Bagel.get_table_list")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_error_happens_in_one_table_then_it_keeps_running_other_tables(
        self, mock_getenv, mock_get_table_list, mock__run_table
    ):
        mock_getenv.return_value = "asdf"
        mock_get_table_list.return_value = [
            {"name": "foo0", "elt_type": "bar0"},
            {"name": "foo1", "elt_type": "bar1"},
            {"name": "foo2", "elt_type": "bar2"},
        ]
        mock__run_table.side_effect = [
            Exception("BAD"),
            Exception("STILL BAD"),
            None,
        ]
        bagel = Bagel(self.test_integration)
        # TODO: figure out exception type
        with self.assertRaises(BagelError):
            bagel.run()
        self.assertEqual(mock__run_table.call_count, 3)
