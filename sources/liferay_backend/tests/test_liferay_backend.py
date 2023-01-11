import datetime
import pytest
import unittest
from unittest import mock
from bagel.table import Table
from ..get_data import Liferay_backend, datetime_to_ms_epoch
from .fakes import (
    mock_get_request_200,
    mock_get_request_200_delta,
    mock_get_request_404,
    mock_get_request,
)


class TestLiferayBackend(unittest.TestCase):
    @mock.patch("liferay_backend.get_data.os.getenv")
    def setUp(self, mock_os_getenv):
        self._auth_user, self._auth_password, self._base_url = [
            "FAKE_USER",
            "FAKE_AUTH_TOKEN",
            "https://clinicalassetinformaticsdev.trimedx.com/api/jsonws/",
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
    @mock.patch("liferay_backend.get_data.requests.get")
    def test_when_api_errors_then_raise_runtime_error(self, mock_requests_get):
        mock_requests_get.return_value = mock_get_request_404()

        with self.assertRaises(RuntimeError):
            self.liferay_backend.liferay_get_data("foo")

    @pytest.mark.unit_test
    def test_when_utility_function_called_then_return_correct_format(self):
        expected = 1640998861000
        result = datetime_to_ms_epoch(self.last_run_timestamp)
        assert result == expected

    @pytest.mark.unit_test
    @mock.patch("liferay_backend.get_data.Liferay_backend._load_config")
    def test_when_class_instantiated_then_calls_load_config(self, mock_load_config):
        Liferay_backend()

        assert mock_load_config.called

    @pytest.mark.unit_test
    @mock.patch("liferay_backend.get_data.requests.get")
    def test_when_get_data_has_no_results_then_exits_successfully(
        self, mock_requests_get
    ):
        mock_requests_get.return_value = mock_get_request()
        try:
            self.liferay_backend.get_data(
                Table(name="user_affiliations"),
                self.current_timestamp,
                self.last_run_timestamp,
            )
        except:
            pytest.fail("Unexpected error in get_data()")

    @pytest.mark.unit_test
    @mock.patch("liferay_backend.get_data.requests.get")
    def test_when_modified_date_is_missing_raise_key_error(self, mock_requests_get):
        mock_response = mock_get_request().json_data

        with self.assertRaises(KeyError):
            self.liferay_backend.is_modified_record(
                mock_response, self.last_run_timestamp, self.current_timestamp
            )

    @pytest.mark.unit_test
    def test_when_modified_date_before_last_run_then_true(self):
        mock_response = mock_get_request_200().json_data
        result = self.liferay_backend.is_modified_record(
            mock_response, self.last_run_timestamp, self.current_timestamp
        )
        assert result == True

    @pytest.mark.unit_test
    def test_when_modified_date_before_last_run_then_false(self):
        mock_response = mock_get_request_200_delta().json_data
        result = self.liferay_backend.is_modified_record(
            mock_response, self.last_run_timestamp, self.current_timestamp
        )
        assert result == False

    @pytest.mark.unit_test
    @mock.patch("liferay_backend.get_data.requests.get")
    def test_when_data_is_return_it_loops_through_results(self, mock_requests_get):
        mock_requests_get.return_value = mock_get_request_200_delta()

        try:
            self.liferay_backend.get_data(
                Table(name="customer_health_systems"),
                self.current_timestamp,
                self.last_run_timestamp,
            )
        except:
            pytest.fail("Unexpected error in get_data()")
