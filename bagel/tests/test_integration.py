import pytest
import unittest
from unittest import mock

from src.bagel.integration import BagelIntegration

class TestBagelIntegration(unittest.TestCase):
    def setUp(self):
        pass

    @pytest.mark.unit_test
    def test_when_bagel_integration_instatiated_without_get_data_then_should_raise_type_error(
        self
    ):
        with pytest.raises(TypeError) as e:
            class TestIntegration(BagelIntegration):
                pass
            test_integration = TestIntegration()


    @pytest.mark.unit_test
    def test_when_bagel_integration_instantiated_with_get_data_call_then_should_pass(
        self
    ):
        class TestIntegration(BagelIntegration):
            def get_data(self):
                return None
        try:
            test_integration = TestIntegration()
        except:
            pytest.fail("Raised Exception to get data despite instantiation")
