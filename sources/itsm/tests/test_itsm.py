from bagel.data import Bite
from bagel.table import Table
import pytest
import unittest
from unittest import mock

from itsm.get_data import ITSMData
from .fakes import fake_datasource_data, mock_get_request, mock_get_request


class TestITSM(unittest.TestCase):
    @mock.patch("itsm.get_data.os.getenv")
    def setUp(self, mock_os_getenv) -> None:
        self.fake_user, self.fake_password, self.fake_base_url = [
            "FAKE_USER",
            "FAKE_PASSWORD",
            "https://trimedxllcdev.service-now.com/api/now/table/",
        ]

        mock_os_getenv.side_effect = [
            self.fake_user,
            self.fake_password,
            self.fake_base_url,
        ]

        self.itsm = ITSMData()

    @pytest.mark.unit_test
    def test_when_class_instantiated_then_sets_proper_secret_variables_in_load_config_and_base_url(
        self,
    ):
        expected = [self.fake_user, self.fake_password, self.fake_base_url]
        result = [
            self.itsm._itsm_user,
            self.itsm._itsm_password,
            self.itsm.base_url,
        ]
        self.assertListEqual(result, expected)
