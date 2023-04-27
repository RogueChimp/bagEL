from datetime import datetime, timezone
import os
from typing import Generator

import pytest
import unittest
from unittest import mock

from src.bagel.bagel import Bagel
from src.bagel.integration import BagelIntegration
from src.bagel.data import Bite
from src.bagel.errors import BagelError
from src.bagel.table import Table

from .fakes import MockStorageClient, MockTimeboxClient, MockDataDogResponse


class TestBagel(unittest.TestCase):
    def setUp(self):
        class TestIntegration(BagelIntegration):

            source = "test_integration"

            def get_data(self, table, last_run_timestamp, current_timestamp):
                return Bite([{"foo": "bar"}])

        self.test_integration = TestIntegration()

        tb_c = MockTimeboxClient()
        s_c = MockStorageClient()
        self.plain_bagel = Bagel(self.test_integration, tb_c, s_c)

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel.get_table_list")
    @mock.patch("src.bagel.bagel.Bagel._run_table")
    @mock.patch("src.bagel.bagel.os.getenv")
    @mock.patch("src.bagel.bagel.Bagel._log_datadog_error")
    def test_when_error_happens_in_one_table_then_it_keeps_running_other_tables(
        self, mock_log_datadog_error, mock_getenv, mock__run_table, mock_get_table_list
    ):
        mock_log_datadog_error.return_value = MockDataDogResponse.mock_get_request_200(
            json={"status": "success"}, status_code=200
        )
        mock_getenv.return_value = "asdf"
        mock_get_table_list.return_value = [Table("foo0"), Table("foo1"), Table("foo2")]
        mock__run_table.side_effect = [
            Exception("BAD"),
            Exception("STILL BAD"),
            None,
        ]
        bagel = Bagel(self.test_integration)
        with self.assertRaises(BagelError):
            bagel.run()
        self.assertEqual(mock__run_table.call_count, 3)

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
            Table.from_config(
                {"name": "My Table 0", "elt_type": "elt"},
            ),
            Table.from_config(
                {"name": "my_table_1", "elt_type": "full"},
            ),
            Table.from_config(
                {
                    "name": "my_table_2",
                    "elt_type": "delta",
                    "historical_batch": True,
                    "historical_frequency": "D",
                },
            ),
            Table.from_config(
                {
                    "name": "my_table_3",
                    "initial_timestamp": datetime(
                        2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
                    ),
                },
            ),
        ]

        bagel = Bagel(self.test_integration)
        results = bagel.get_table_list()

        assert len(results) == len(expected)
        self.assertListEqual(results, expected)

    @pytest.mark.unit_test
    def test_when_integration_data_is_proper_generator_then_dont_raise(self):
        proper_list_format = [
            {"foo": "bar"},
            {"baz": "qux"},
        ]

        def get_data_proper_generator():

            data = [proper_list_format]

            for d in data:
                yield Bite(d)

            return None

        self.plain_bagel._validate_data(get_data_proper_generator())

    @pytest.mark.unit_test
    def test_when_integration_data_is_formatted_incorrectly_then_raise(self):
        with self.assertRaises(TypeError):
            self.plain_bagel._validate_data([{"foo": "bar"}])

    @pytest.mark.unit_test
    def test_when_receive_bite_then_return_list(self):
        expected = [Bite([{"foo": "bar"}])]
        result = self.plain_bagel._bite_to_iterable(Bite([{"foo": "bar"}]))
        assert result == expected

    @pytest.mark.unit_test
    def test_when_receive_generator_then_return_generator(self):
        def gen():
            while True:
                yield 0

        result = self.plain_bagel._bite_to_iterable(gen())
        assert isinstance(result, Generator)

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
        table = tables[3]
        result = table.initial_timestamp
        expected = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

        assert result == expected

    @pytest.mark.unit_test
    def test_when__run_table_is_called_then_timestamp_is_updated(self):
        expected = datetime(2022, 1, 1, 0, 0, 0, 0)
        tb_c = MockTimeboxClient(datetime(2000, 1, 1, 0, 0, 0, 0), expected)
        s_c = MockStorageClient()

        bagel = Bagel(self.test_integration, timebox_client=tb_c, storage_client=s_c)
        bagel._run_table(Table.from_config({"name": "test"}))

        assert (
            bagel.timebox_client.get_last_run_timestamp(
                self.test_integration.source, "test"
            )
            == expected
        )

    # @pytest.mark.unit_test
    # @mock.patch("src.bagel.bagel.Bagel.get_table_list")
    # @mock.patch("src.bagel.bagel.Bagel._run_table")
    # @mock.patch("src.bagel.bagel.Bagel._log_datadog_error")
    # def test_when_error_happens_then_datadog_logs_it(
    #     self, mock_get_table_list, mock__run_table, mock_log_datadog_error
    # ):
    #     mock_get_table_list.return_value = [Table("foo1")]
    #     mock__run_table.return_value = [Exception("BAD")]
    #     bagel = Bagel(self.test_integration)
    #     bagel.run()
    #     assert mock_log_datadog_error.called

    @pytest.mark.unit_test
    @mock.patch("src.bagel.bagel.Bagel.get_table_list")
    @mock.patch("src.bagel.bagel.Bagel._run_table")
    @mock.patch("src.bagel.bagel.Bagel._log_datadog_error")
    def test_log_datadog_error(
        self, mock_log_datadog_error, mock__run_table, mock_get_table_list
    ):
        mock_get_table_list.return_value = [Table("foo1")]
        mock__run_table.return_value = Exception("BAD")
        bagel = Bagel(self.test_integration)
        bagel.run()
        assert mock_log_datadog_error.called_with(
            {
                "ddsource": "azurecontainer",
                "ddtags": f"env:asdf,integration:test_integration,table:foo2",
                "hostname": "azurecontainer",
                "message": "Successfully completed processing",
                "errormessage" "service": "bagEL",
                "status": "error",
            }
        )

    @pytest.mark.unit_test
    @mock.patch("src.bagel.DataDogLogSubmitter.submit_log")
    @mock.patch("src.bagel.bagel.Bagel.get_table_list")
    @mock.patch("src.bagel.bagel.Bagel._run_table")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_log_datadog_info(
        self,
        mock_getenv,
        mock__run_table,
        mock_get_table_list,
        mock_submit_log,
    ):
        mock_get_table_list.return_value = [Table("foo1")]
        mock__run_table.return_value = None
        mock_getenv.return_value = "asdf"
        bagel = Bagel(self.test_integration)
        bagel.run()
        mock_submit_log.return_value = None
        assert mock_submit_log.called_with(
            {
                "ddsource": "azurecontainer",
                "ddtags": f"env:asdf,integration:test_integration,table:foo1",
                "hostname": "azurecontainer",
                "message": "Successfully completed processing",
                "service": "bagEL",
                "status": "info",
            }
        )

    @pytest.mark.unit_test
    @mock.patch("src.bagel.DataDogLogSubmitter.submit_log")
    @mock.patch("src.bagel.bagel.Bagel.get_table_list")
    @mock.patch("src.bagel.bagel.Bagel._run_table")
    @mock.patch("src.bagel.bagel.Bagel._log_datadog_error")
    @mock.patch("src.bagel.bagel.os.getenv")
    def test_log_datadog_error(
        self,
        mock_getenv,
        mock_log_datadog_error,
        mock__run_table,
        mock_get_table_list,
        mock_submit_log,
    ):
        mock_get_table_list.return_value = [Table("foo1")]
        mock__run_table.return_value = None
        mock_getenv.return_value = Exception("BAD")
        bagel = Bagel(self.test_integration)
        bagel.run()
        mock_submit_log.return_value = None
        assert mock_submit_log.called_with(
            {
                "ddsource": "azurecontainer",
                "ddtags": f"env:asdf,integration:test_integration,table:foo1",
                "hostname": "azurecontainer",
                "message": "Some error message",
                "errorMessage": "BAD",
                "service": "bagEL",
                "status": "error",
            }
        )
