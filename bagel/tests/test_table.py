import pytest
import unittest

from src.bagel.table import Table


class TestTable(unittest.TestCase):
    @pytest.mark.unit_test
    def test_when_table_made_without_table_name_then_raise(self):

        with self.assertRaises(RuntimeError):
            Table.from_config({})

    # @pytest.mark.unit_test
    # def test_when_table_made_without_system_name_then_raise(self):

    #     with self.assertRaises(RuntimeError):
    #         Table.from_config(None, {"name": "foo"})

    @pytest.mark.unit_test
    def test_when_table_created_then_format_name(self):

        t = Table("TABLE-asdf foo", "src")
        expected = "table_asdf_foo"
        result = t.name
        assert result == expected
