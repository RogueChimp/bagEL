from datetime import datetime, timedelta, timezone
import json
import os

import pytest
import unittest
from unittest import mock

from src.bagel.bagel import (
    Bagel,
    BagelError,
    format_blob_name,
    format_dict_to_json_binary,
    extract_date_ranges,
    get_historical_batch_ranges,
)
from src.bagel.integration import BagelIntegration
from src.bagel.data import Bite


class TestBagel(unittest.TestCase):
    def setUp(self):
        class TestIntegration(BagelIntegration):

            name = "test_integration"

            def get_data(self):
                return None

        self.test_integration = TestIntegration()

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_environment_variables_arent_set_then_raise_value_error(
        self, mock_getenv
    ):
        mock_getenv.return_value = None

        with self.assertRaises(ValueError) as e:
            Bagel(self.test_integration)
        assert isinstance(e.exception, ValueError)
        assert (
            e.exception.args[0]
            == "Environment variables for Azure resources must be properly configured."
        )

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

    @pytest.mark.unit_test
    def test_when_formating_data_file_name_then_it_is_correct(self):

        system = "foo"
        table = "bar"
        dt = datetime(2022, 6, 24, 9, 26, 9, 548513)
        expected = "foo/data/bar/2022/06/24/bar_2022_06_24T09_26_09_548513Z.json"
        result = format_blob_name(system, table, dt)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formating_log_file_name_then_it_is_correct(self):

        system = "foo"
        table = "bar"
        dt = datetime(2022, 6, 24, 9, 26, 9, 548513)
        expected = "foo/log/bar/2022/06/24/bar_2022_06_24T09_26_09_548513Z.json"
        result = format_blob_name(system, table, dt, log=True)

        assert result == expected

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel._get_table_path")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_getting_table_list_then_return_expected_format(
        self, mock_getenv, mock__get_table_path
    ):
        mock__get_table_path.return_value = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "source_dir",
            "tables.yaml",
        )

        mock_getenv.return_value = "asdf"

        expected = [
            {"name": "My Table 0", "elt_type": "elt"},
            {"name": "my_table_1", "elt_type": "full"},
            {
                "name": "my_table_2",
                "elt_type": "delta",
                "historical_batch": True,
                "historical_frequency": "D",
            },
            {
                "name": "my_table_3",
                "initial_timestamp": datetime(
                    2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
                ),
            },
        ]

        bagel = Bagel(self.test_integration)
        results = bagel.get_table_list()

        assert len(results) == len(expected)
        self.assertListEqual(results, expected)

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel.write_to_blob")
    @mock.patch("src.bagel.bagel.format_blob_name")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_integration_data_is_formatted_incorrectly_then_raise(
        self, mock_getenv, mock_format_blob_name, mock_write_to_blob
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

        expected = [Bite(content=proper_list_format)]

        # test list format
        bagel = Bagel(self.test_integration)
        results = bagel._validate_data(Bite(content=proper_list_format))

        assert results == expected

        with self.assertRaises(TypeError):
            Bite(improper_list_format)

        # test generators
        def get_data_proper_generator():

            data = [proper_list_format]

            for d in data:
                yield Bite(d)

            return None

        data = bagel._validate_data(get_data_proper_generator())
        assert list(data) == expected

        def get_data_improper_generator():

            data = [improper_list_format]

            for d in data:
                yield Bite(d)

            return None

        with self.assertRaises(TypeError):
            data = bagel._validate_data(get_data_improper_generator())
            bagel._process_data(data)

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel.write_to_blob")
    @mock.patch("src.bagel.bagel.format_blob_name")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_data_is_empty_list_should_return_an_empty_array_and_pass_through(
        self, mock_getenv, mock_format_blob_name, mock_write_to_blob
    ):

        input_ = [
            {"foo": "bar"},
            {"baz": "qux"},
            {"None": None},
        ]

        expected = bytes(
            """[{"foo": "bar"}, {"baz": "qux"}, {"None": null}]""", "utf-8"
        )

        results = format_dict_to_json_binary(input_)

        assert results == expected

        assert json.loads(results) == input_

    @pytest.mark.unit_test
    def test_when_historical_batch_default_day_then_it_splits_the_ranges(self):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 7, 1, 1, 1, 548513)
        expected = 7
        date_ranges = get_historical_batch_ranges(start_time, end_time)
        result = len(date_ranges)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[-1] == end_time

    @pytest.mark.unit_test
    def test_when_historical_batch_by_hour_then_it_splits_the_ranges(self):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 1, 7, 1, 1, 548513)
        expected = 7
        date_ranges = get_historical_batch_ranges(start_time, end_time, "H")
        result = len(date_ranges)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[-1] == end_time

    @pytest.mark.unit_test
    def test_when_historical_batch_single_day_then_it_does_not_split_the_ranges(self):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 2, 1, 1, 1, 548513)
        expected = 2
        date_ranges = get_historical_batch_ranges(start_time, end_time)
        result = len(date_ranges)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[1] == end_time

    @pytest.mark.unit_test
    def test_when_not_historical_batch_single_day_then_it_does_not_split_the_ranges(
        self,
    ):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 7, 1, 1, 1, 548513)
        expected = 2
        date_ranges = extract_date_ranges(start_time, end_time, False, None)
        result = len(date_ranges)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[1] == end_time

    @pytest.mark.unit_test
    def test_when_delta_type_is_none_single_day_then_it_does_not_split_the_ranges(
        self,
    ):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 7, 1, 1, 1, 548513)
        expected = 2
        date_ranges = extract_date_ranges(start_time, end_time, None, None)
        result = len(date_ranges)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[1] == end_time

    @pytest.mark.unit_test
    def test_when_historical_load_D_true_then_it_does_split_the_ranges(
        self,
    ):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 7, 1, 1, 1, 548513)
        expected = 7
        date_ranges = extract_date_ranges(start_time, end_time, True, None)
        result = len(date_ranges)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[-1] == end_time

    @pytest.mark.unit_test
    def test_when_historical_load_H_true_then_it_does_split_the_ranges(
        self,
    ):
        start_time = datetime(2022, 1, 1, 1, 1, 1, 548513)
        end_time = datetime(2022, 1, 1, 7, 1, 1, 548513)
        expected = 7
        date_ranges = extract_date_ranges(start_time, end_time, True, "H")
        result = len(date_ranges)
        delta = timedelta(hours=1)
        assert result == expected
        assert date_ranges[0] == start_time
        assert date_ranges[-1] == end_time
        assert date_ranges[1] == start_time + delta

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel._get_table_path")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_when_initial_timestamp_in_config_then_load(
        self, mock_getenv, mock__get_table_path
    ):
        mock__get_table_path.return_value = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "source_dir",
            "tables.yaml",
        )

        mock_getenv.return_value = "asdf"

        bagel = Bagel(self.test_integration)
        tables = bagel.get_table_list()
        table_config = tables[3]
        loaded_config = bagel._load_table_config(table_config)
        result = loaded_config[-1]
        expected = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

        assert result == expected
