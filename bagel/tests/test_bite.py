import pytest
import unittest
from unittest import mock

from src.bagel.data import Bite


class TestBite(unittest.TestCase):
    @pytest.mark.unit_test
    def test_when_data_is_list_of_dicts_then_dont_raise(self):

        Bite([{}, {}, {}])

    @pytest.mark.unit_test
    def test_when_data_is_list_of_lists_then_raise(self):

        with self.assertRaises(TypeError):
            Bite([[], [], []])

    @pytest.mark.unit_test
    def test_when_data_is_empty_list_then_dont_raise(self):

        Bite([])

    @pytest.mark.unit_test
    def test_when_data_is_bytes_then_dont_raise(self):

        Bite(b"foo")

    @pytest.mark.unit_test
    def test_when_data_is_not_list_or_bytes_then_raise(self):

        with self.assertRaises(TypeError):
            Bite(42)

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.format_blob_name")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_integration_data_is_formatted_incorrectly_then_raise(
        self, mock_getenv, mock_format_blob_name
    ):

        mock_getenv.return_value = "asdf"
        mock_format_blob_name.return_value = "asdf"

        proper_list_format = [
            {"foo": "bar"},
            {"baz": "qux"},
        ]

        improper_list_format = [
            [
                {"foo": "bar"},
                {"baz": "qux"},
            ]
        ]

        Bite(data=proper_list_format)

        with self.assertRaises(TypeError):
            Bite(improper_list_format)

        def get_data_improper_generator():

            data = [improper_list_format]

            for d in data:
                yield Bite(d)

            return None

        with self.assertRaises(TypeError):
            get_data_improper_generator().__next__()
