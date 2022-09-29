from datetime import datetime, timedelta
import json

import pytest
import unittest

from src.bagel.util import (
    format_blob_name,
    format_table_name,
    format_dict_to_json_binary,
    extract_date_ranges,
    get_historical_batch_ranges,
)


class TestUtil(unittest.TestCase):
    @pytest.mark.unit_test
    def test_when_formatting_data_file_name_then_it_is_correct(self):

        system = "foo"
        table = "bar"
        dt = datetime(2022, 6, 24, 9, 26, 9, 548513)
        expected = "foo/data/bar/2022/06/24/bar_2022_06_24T09_26_09_548513Z.json"
        result = format_blob_name(system, table, dt)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formatting_log_file_name_then_it_is_correct(self):

        system = "foo"
        table = "bar"
        dt = datetime(2022, 6, 24, 9, 26, 9, 548513)
        expected = "foo/log/bar/2022/06/24/bar_2022_06_24T09_26_09_548513Z.log"
        result = format_blob_name(system, table, dt, log=True)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formatting_file_name_with_custom_name_then_it_is_correct(self):

        system = "foo"
        table = "bar"
        dt = datetime(2022, 6, 24, 9, 26, 9, 548513)
        fname = "baz"
        expected = "foo/data/bar/2022/06/24/bar_2022_06_24T09_26_09_548513Z-baz.json"
        result = format_blob_name(system, table, dt, file_name=fname)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formatting_file_name_with_custom_ext_then_it_is_correct(self):

        system = "foo"
        table = "bar"
        dt = datetime(2022, 6, 24, 9, 26, 9, 548513)
        ext = "pdf"
        expected = "foo/data/bar/2022/06/24/bar_2022_06_24T09_26_09_548513Z.pdf"
        result = format_blob_name(system, table, dt, file_format=ext)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formatting_table_name_then_it_is_lower(self):

        table = "FOO"
        expected = "foo"
        result = format_table_name(table)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formatting_table_name_then_no_spaces(self):

        table = "foo bar"
        expected = "foo_bar"
        result = format_table_name(table)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_formatting_table_name_then_no_dashes(self):

        table = "foo-bar"
        expected = "foo_bar"
        result = format_table_name(table)

        assert result == expected

    @pytest.mark.unit_test
    def test_when_data_is_empty_list_should_return_an_empty_array_and_pass_through(
        self,
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
