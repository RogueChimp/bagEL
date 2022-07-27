import unittest

from ..get_data import Template, utility_function


class TestTemplate(unittest.TestCase):

    """Currently, we are using TestCase classes for pytest"""

    def setUp(self):
        self.template = Template()

    def test_when_utility_function_called_then_return_foo(self):
        expected = "foo"
        result = utility_function()
        assert result == expected

    def test_when__helper_function_called_then_return_type_boolean(self):
        expected = bool
        result = self.template._helper_function()
        assert isinstance(result, expected)
