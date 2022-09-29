import pytest
import unittest

from src.bagel.integration import BagelIntegration, Table


class TestBagelIntegration(unittest.TestCase):
    def setUp(self):
        pass

    @pytest.mark.unit_test
    def test_when_bagel_integration_instatiated_without_get_data_then_should_raise_type_error(
        self,
    ):
        with pytest.raises(TypeError):

            class TestIntegration(BagelIntegration):
                pass

            TestIntegration()

    @pytest.mark.unit_test
    def test_when_bagel_integration_instantiated_with_get_data_call_then_should_pass(
        self,
    ):
        class TestIntegration(BagelIntegration):
            def get_data(self):
                return None

        try:
            t = TestIntegration()
            t.get_data()
        except:
            pytest.fail("Raised Exception to get data despite instantiation")

    @pytest.mark.unit_test
    def test_when_bagel_integration_instantiated_with_get_data_call_then_should_pass(
        self,
    ):
        class TestIntegration(BagelIntegration):
            def get_data(self):
                return None

        try:
            t = TestIntegration()
            t.get_data()
        except:
            pytest.fail("Raised Exception to get data despite instantiation")
