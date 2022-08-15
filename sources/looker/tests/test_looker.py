import datetime
import json
import os
import requests
import secrets

import pytest
import unittest
from unittest import mock

from looker.get_data import Looker
from looker.tests.fakes import mock_post_request


class TestLooker(unittest.TestCase):
    @mock.patch("looker.get_data.os.getenv")
    def setUp(self, mock_os_getenv):
        self.fake_client_id, self.fake_client_secret = [
            "FAKE_CLIENT_ID",
            "FAKE_CLIENT_SECRET",
        ]
        mock_os_getenv.side_effect = [self.fake_client_id, self.fake_client_secret]
        self.fake_table_name = f"THIS_TABLE_DOES_NOT_EXIST_{secrets.token_urlsafe(16)}"
        self.elt_type = {
            "full": "full",
            "delta": "delta",
            "DNE": f"ABSOLUTELY_INVALID_ELT_TYPE_{secrets.token_urlsafe(16)}",
        }
        self.fake_headers = {
            "authorization": "Bearer fake_access_token",
            "cache-control": "no-cache",
        }
        self.current_timestamp = datetime.datetime(
            9999, 9, 9, 9, 9, 9, tzinfo=datetime.timezone.utc
        )
        self.last_run_timestamp = datetime.datetime(
            1111, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc
        )
        self.lk = Looker()

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.Looker._load_config")
    def test_when_class_instantiated_then_calls_load_config(self, mock_load_config):
        Looker()

        assert mock_load_config.called

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.os.getenv")
    def test_when_class_instantiated_then_sets_proper_secret_variables_in_load_config_and_base_url(
        self, mock_os_getenv
    ):
        mock_os_getenv.side_effect = [self.fake_client_id, self.fake_client_secret]

        test_looker = Looker()

        assert test_looker._Looker__client_id == self.fake_client_id
        assert test_looker._Looker__client_secret == self.fake_client_secret
        assert test_looker.base_url == "https://lookerdev.trimedx.com:443/api/4.0"

    @pytest.mark.unit_test
    def test_when_format_to_looker_time_called_with_not_datetime_type_then_raises_exception(
        self,
    ):
        timestamp = str("Fail")

        with self.assertRaises(
            TypeError,
            msg="Invalid timestamp type; should be <class 'datetime.datetime'>",
        ):
            self.lk._format_to_looker_time(timestamp)

    @pytest.mark.unit_test
    def test_when_format_to_looker_time_called_with_datetime_type_then_should_return_formatted_string(
        self,
    ):
        assert (
            self.lk._format_to_looker_time(self.current_timestamp)
            == "9999-09-09 09:09:09"
        )
        assert (
            self.lk._format_to_looker_time(self.last_run_timestamp)
            == "1111-01-01 01:01:01"
        )

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.os.path.dirname")
    @mock.patch("looker.get_data.os.path.realpath")
    def test_when_get_data_payload_called_on_nonexistent_file_then_raises_error(
        self, mock_path_realpath, mock_path_dirname
    ):
        mock_path_dirname.return_value = (
            f"THIS/PATH/DOES/NOT/EXIST/{secrets.token_urlsafe(16)}"
        )

        with self.assertRaises(FileNotFoundError):
            self.lk.get_data_payload(self.fake_table_name)

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.__file__")
    @mock.patch("looker.get_data.open")
    @mock.patch("looker.get_data.json.load")
    def test_when_get_data_payload_called_then_creates_proper_path_directory(
        self, mock_json_load, mock_open, mock_filename
    ):
        mock_filename.return_value = "foo"
        mock_json_load.return_value = "foobar"

        val = self.lk.get_data_payload(self.fake_table_name)

        assert mock_open.call_args.args == (
            os.sep.join(
                [
                    os.path.dirname(os.path.realpath(mock_filename)),
                    "queries",
                    self.fake_table_name + ".json",
                ]
            ),
            "r",
        )
        assert val == mock_json_load.return_value

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.requests.post")
    def test_when_looker_login_called_then_requests_called_with_proper_variables(
        self, mock_requests_post
    ):
        mock_requests_post.side_effect = mock_post_request

        headers = self.lk.looker_login()

        mock_requests_post.assert_called_with(
            url=f"{self.lk.base_url}/login",
            params={
                "client_id": self.lk._Looker__client_id,
                "client_secret": self.lk._Looker__client_secret,
            },
        )
        self.assertDictEqual(headers, self.fake_headers)

    # technically not a unit_test, but including anyway
    @pytest.mark.unit_test
    def test_when_login_with_invalid_url_then_should_raise_ConnectionError(self):
        test_looker = Looker()
        test_looker.base_url = (
            f"http://this-url-definitely-does-not-exist{secrets.token_urlsafe(16)}"
        )

        with self.assertRaises(requests.exceptions.ConnectionError):
            test_looker.looker_login()

    # technically not a unit_test, but including anyway
    @pytest.mark.unit_test
    def test_when_login_with_valid_url_but_invalid_credentials_then_should_raise_JSONDecodeError(
        self,
    ):
        test_looker = Looker()
        test_looker._Looker__client_id = (
            self.lk._Looker__client_id + secrets.token_urlsafe(16)
        )
        test_looker._Looker__client_secret = (
            self.lk._Looker__client_secret + secrets.token_urlsafe(16)
        )

        with self.assertRaises(json.decoder.JSONDecodeError):
            test_looker.looker_login()

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.Looker.looker_login")
    @mock.patch("looker.get_data.Looker.looker_get_data")
    def test_when_get_data_called_then_looker_login_and_looker_get_data_called_within(
        self,
        mock_get_data,
        mock_login,
    ):
        mock_login.return_value = self.fake_headers

        self.lk.get_data(table=self.fake_table_name, elt_type=self.elt_type["full"])

        mock_login.assert_called()
        mock_get_data.assert_called_with(
            self.fake_headers, self.fake_table_name, self.elt_type["full"], None, None
        )

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.Looker.looker_login")
    @mock.patch("looker.get_data.Looker.looker_get_data")
    def test_when_get_data_has_certain_parameters_then_those_parameters_are_passed_along_to_looker_get_data(
        self,
        mock_get_data,
        mock_login,
    ):
        mock_login.return_value = self.fake_headers

        self.lk.get_data(
            table=self.fake_table_name,
            elt_type=self.elt_type["full"],
            last_run_timestamp=self.last_run_timestamp,
            current_timestamp=self.current_timestamp,
        )

        mock_login.assert_called()
        mock_get_data.assert_called_with(
            self.fake_headers,
            self.fake_table_name,
            self.elt_type["full"],
            self.last_run_timestamp,
            self.current_timestamp,
        )

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.Looker.get_data_payload")
    def test_when_invalid_elt_type_is_passed_in_then_get_data_throws_exception(
        self, mock_get_data_payload
    ):
        mock_get_data_payload.return_value = dict({"foo": "bar"})

        with self.assertRaises(Exception, msg="Invalid elt_type in tables.yaml file"):
            self.lk.looker_get_data(
                table=self.fake_table_name,
                elt_type=self.elt_type["DNE"],
            )

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.requests.post")
    @mock.patch("looker.get_data.Looker.get_data_payload")
    def test_when_passing_full_elt_type_then_set_payload_filters_value_to_None(
        self, mock_get_data_payload, mock_requests_post
    ):
        url = f"{self.lk.base_url}/queries/run/json"
        mock_requests_post.side_effect = mock_post_request
        fake_data_payload = dict({"foo": "bar", "filters": "bar"})
        mock_get_data_payload.return_value = fake_data_payload

        self.lk.looker_get_data(
            headers=self.fake_headers,
            table=self.fake_table_name,
            elt_type=self.elt_type["full"],
        )

        self.assertDictEqual(fake_data_payload, dict({"foo": "bar", "filters": None}))

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.requests.post")
    @mock.patch("looker.get_data.Looker.get_data_payload")
    def test_when_passing_delta_elt_type_then_call_format_looker_and_change_field_name(
        self, mock_get_data_payload, mock_requests_post
    ):
        url = f"{self.lk.base_url}/queries/run/json"
        mock_requests_post.side_effect = mock_post_request
        fake_data_payload = dict(
            {
                "foo": "bar",
                "filters": {f"{self.fake_table_name}.created_time": "fake_time"},
            }
        )

        mock_get_data_payload.return_value = fake_data_payload
        expected_created_time = f"{self.lk._format_to_looker_time(self.last_run_timestamp)} to {self.lk._format_to_looker_time(self.current_timestamp)}"

        self.lk.looker_get_data(
            headers=self.fake_headers,
            table=self.fake_table_name,
            elt_type=self.elt_type["delta"],
            last_run_timestamp=self.last_run_timestamp,
            current_timestamp=self.current_timestamp,
        )

        self.assertDictEqual(
            fake_data_payload,
            dict(
                {
                    "foo": "bar",
                    "filters": {
                        f"{self.fake_table_name}.created_time": expected_created_time
                    },
                }
            ),
        )

        mock_requests_post.assert_called_with(
            url=f"{self.lk.base_url}/queries/run/json",
            json=fake_data_payload,
            headers=self.fake_headers,
        )

    @pytest.mark.unit_test
    @mock.patch("looker.get_data.requests.post")
    @mock.patch("looker.get_data.Looker.get_data_payload")
    def test_when_calling_looker_get_data_then_expect_result_to_be_a_json_array(
        self, mock_get_data_payload, mock_requests_post
    ):
        url = f"{self.lk.base_url}/queries/run/json"
        mock_requests_post.side_effect = mock_post_request
        fake_data_payload = dict(
            {
                "foo": "bar",
                "filters": {f"{self.fake_table_name}.created_time": "fake_time"},
            }
        )

        mock_get_data_payload.return_value = fake_data_payload

        data = self.lk.looker_get_data(
            headers=self.fake_headers,
            table=self.fake_table_name,
            elt_type=self.elt_type["delta"],
            last_run_timestamp=self.last_run_timestamp,
            current_timestamp=self.current_timestamp,
        )

        assert type(data) == list
        for i in data:
            assert type(i) == dict
