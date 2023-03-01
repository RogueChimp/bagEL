import datetime

from bagel.data import Bite
import pytest
import unittest
from unittest import mock
from bagel.table import Table
from ..get_data import Liferay_backend
from .fakes import mock_get_request_404, mock_get_request_200


class TestLiferayBackend(unittest.TestCase):
    @mock.patch("liferay_backend.get_data.os.getenv")
    def setUp(self, mock_os_getenv):
        self._auth_user, self._auth_password, self._base_url = [
            "FAKE_USER",
            "FAKE_AUTH_TOKEN",
            "https://example.com/",
        ]
        mock_os_getenv.side_effect = [
            self._auth_user,
            self._auth_password,
            self._base_url,
        ]
        self.current_timestamp = datetime.datetime(
            9999, 9, 9, 9, 9, 9, tzinfo=datetime.timezone.utc
        )
        self.last_run_timestamp = datetime.datetime(
            2022, 1, 1, 1, 1, 1, 24, tzinfo=datetime.timezone.utc
        )
        self.liferay_backend = Liferay_backend()

    @pytest.mark.unit_test
    def test_when_class_instantiated_then_sets_proper_secret_variables_in_load_config_and_base_url(
        self,
    ):
        expected = [self._auth_user, self._auth_password, self._base_url]
        result = [
            self.liferay_backend._auth_user,
            self.liferay_backend._auth_password,
            self.liferay_backend._base_url,
        ]
        self.assertListEqual(result, expected)

    @pytest.mark.unit_test
    def test_liferay_backend_get_url(
        self,
    ):
        expected_url = "https://example.com/my-table?startModifiedDate=2022-02-27T15:00:00.000Z&endModifiedDate=2023-02-27T15:00:00.000Z"
        backend = Liferay_backend()
        backend._base_url = "https://example.com/"
        last_run_timestamp = datetime.datetime(2022, 2, 27, 15, 0, 0)
        current_timestamp = datetime.datetime(2023, 2, 27, 15, 0, 0)
        table = Table(name="my_table")
        result = backend.liferay_backend_get_url(
            table, last_run_timestamp, current_timestamp
        )
        assert result == expected_url

    @pytest.mark.unit_test
    @mock.patch("liferay_backend.get_data.requests.get")
    def test_when_api_errors_then_raise_runtime_error(self, mock_requests_get):
        mock_requests_get.return_value = mock_get_request_404()
        backend = Liferay_backend()
        with self.assertRaises(RuntimeError):
            backend.liferay_backend_get_data("foo")

    @pytest.mark.unit_test
    @mock.patch("liferay_backend.get_data.Liferay_backend._load_config")
    def test_when_class_instantiated_then_calls_load_config(self, mock_load_config):
        Liferay_backend()
        assert mock_load_config.called

    @mock.patch("requests.get")
    def test_get_data(self, mock_requests_get):
        backend = Liferay_backend()
        mock_requests_get.return_value = mock_get_request_200()
        table = Table(name="my_table")
        current_timestamp = datetime.datetime(
            9999, 9, 9, 9, 9, 9, tzinfo=datetime.timezone.utc
        )
        last_run_timestamp = datetime.datetime(
            1111, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc
        )
        bite = backend.get_data(table, last_run_timestamp, current_timestamp)
        assert isinstance(bite, Bite)
        assert bite.data == [{"col1": 1, "col2": 2}, {"col1": 3, "col2": 4}]

    @mock.patch("requests.get")
    def test_get_data_error(self, mock_requests_get):
        backend = Liferay_backend()
        mock_requests_get.return_value = mock_get_request_404()
        with pytest.raises(RuntimeError):
            table = Table(name="my_table")
            current_timestamp = datetime.datetime(
                9999, 9, 9, 9, 9, 9, tzinfo=datetime.timezone.utc
            )
            last_run_timestamp = datetime.datetime(
                1111, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc
            )
            backend.get_data(table, last_run_timestamp, current_timestamp)
