from datetime import datetime
import pytest
import unittest
from unittest import mock

from azure.core.exceptions import ResourceNotFoundError

from src.bagel.clients import AzureTableClient, AzureBlobClient


class TestAzureTableClient(unittest.TestCase):
    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    def test_when_environment_variables_arent_set_then_raise_value_error(
        self, mock_getenv
    ):
        mock_getenv.return_value = None

        with self.assertRaises(ValueError) as e:
            AzureTableClient()
        assert isinstance(e.exception, ValueError)
        assert (
            e.exception.args[0]
            == "Environment variables for Azure resources must be properly configured."
        )

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.AzureNamedKeyCredential")
    @mock.patch("src.bagel.clients.TableServiceClient")
    def test_when_connecting_then_set_table_client(
        self, mock_TableServiceClient, mock_AzureNamedKeyCredential, mock_getenv
    ):
        expected = "table"

        class MockTableServiceClient:
            def get_table_client(self, table):
                return table

        mock_getenv.return_value = expected
        mock_TableServiceClient.return_value = MockTableServiceClient()

        t_c = AzureTableClient()
        t_c.connect()

        result = t_c.table_client
        assert result == expected

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    def test_when_no_table_client_then_get_last_run_timestamp_fails(self, mock_getenv):
        mock_getenv.return_value = "asdf"
        t_c = AzureTableClient()
        with self.assertRaises(RuntimeError):
            t_c.get_last_run_timestamp("test", "test")

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.AzureNamedKeyCredential")
    @mock.patch("src.bagel.clients.TableServiceClient")
    def test_when_entity_exists_then_return_its_timestamp(
        self, mock_TableServiceClient, mock_AzureNamedKeyCredential, mock_getenv
    ):
        class MockTableClient:
            def get_entity(self, **kwargs):
                return {"last_updated_timestamp": "2000-01-01T00:00:00.000000Z"}

            def close(self):
                pass

        class MockTableServiceClient:
            def get_table_client(self, table):
                return MockTableClient()

        mock_getenv.return_value = "asdf"
        mock_TableServiceClient.return_value = MockTableServiceClient()

        t_c = AzureTableClient()
        t_c.connect()

        expected = datetime(2000, 1, 1, 0, 0, 0, 0)

        result = t_c.get_last_run_timestamp("test", "test")

        assert result == expected

        t_c.close()

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.AzureNamedKeyCredential")
    @mock.patch("src.bagel.clients.TableServiceClient")
    @mock.patch("src.bagel.clients.AzureTableClient.get_current_timestamp")
    @mock.patch("src.bagel.clients.AzureTableClient.write_run_timestamp")
    def test_when_no_table_entity_exists_then_create_new_timestamp_default(
        self,
        mock_write_run_timestamp,
        mock_get_current_timestamp,
        mock_TableServiceClient,
        mock_AzureNamedKeyCredential,
        mock_getenv,
    ):
        class MockTableClient:
            def get_entity(self, **kwargs):
                raise ResourceNotFoundError()

            def close(self):
                pass

        class MockTableServiceClient:
            def get_table_client(self, table):
                return MockTableClient()

        mock_getenv.return_value = "asdf"
        mock_TableServiceClient.return_value = MockTableServiceClient()
        mock_get_current_timestamp.return_value = datetime(2000, 1, 4, 0, 0, 0, 0)

        t_c = AzureTableClient()
        t_c.connect()

        expected = datetime(2000, 1, 1, 0, 0, 0, 0)

        result = t_c.get_last_run_timestamp("test", "test")

        assert result == expected
        assert mock_write_run_timestamp.called

        t_c.close()

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.AzureNamedKeyCredential")
    @mock.patch("src.bagel.clients.TableServiceClient")
    @mock.patch("src.bagel.clients.AzureTableClient.get_current_timestamp")
    @mock.patch("src.bagel.clients.AzureTableClient.write_run_timestamp")
    def test_when_no_table_entity_exists_then_create_new_timestamp_with_initial_timestamp(
        self,
        mock_write_run_timestamp,
        mock_get_current_timestamp,
        mock_TableServiceClient,
        mock_AzureNamedKeyCredential,
        mock_getenv,
    ):
        class MockTableClient:
            def get_entity(self, **kwargs):
                raise ResourceNotFoundError()

            def close(self):
                pass

        class MockTableServiceClient:
            def get_table_client(self, table):
                return MockTableClient()

        mock_getenv.return_value = "asdf"
        mock_TableServiceClient.return_value = MockTableServiceClient()

        t_c = AzureTableClient()
        t_c.connect()

        expected = datetime(2000, 1, 1, 0, 0, 0, 0)

        result = t_c.get_last_run_timestamp("test", "test", expected)

        assert result == expected
        assert mock_write_run_timestamp.called

        t_c.close()

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.AzureNamedKeyCredential")
    @mock.patch("src.bagel.clients.TableServiceClient")
    def test_when_writing_timestamp_then_call_upsert_entity(
        self, mock_TableServiceClient, mock_AzureNamedKeyCredential, mock_getenv
    ):
        """TODO: This is mostly for code coverage at the moment.
        Whoever has a better test, please replace this.
        """

        class MockTableClient:
            def __init__(self):
                self.upsert_called = False

            def get_entity(self, **kwargs):
                return None

            def upsert_entity(self, **kwargs):
                self.upsert_called = True

            def close(self):
                pass

        class MockTableServiceClient:
            def get_table_client(self, table):
                return MockTableClient()

        mock_getenv.return_value = "asdf"
        mock_TableServiceClient.return_value = MockTableServiceClient()

        t_c = AzureTableClient()
        t_c.connect()

        dummy_sys = "foo"
        dummy_table = "bar"

        expected = {
            "PartitionKey": dummy_sys,
            "RowKey": dummy_table,
            "last_updated_timestamp": "2000-01-01T00:00:00.000000Z",
        }

        result = t_c.write_run_timestamp(
            dummy_sys, dummy_table, datetime(2000, 1, 1, 0, 0, 0, 0)
        )

        assert t_c.table_client.upsert_called
        assert result == expected

        t_c.close()


class TestAzureBlobClient(unittest.TestCase):
    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    def test_when_environment_variables_arent_set_then_raise_value_error(
        self, mock_getenv
    ):
        mock_getenv.return_value = None

        with self.assertRaises(ValueError) as e:
            AzureBlobClient()
        assert isinstance(e.exception, ValueError)
        assert (
            e.exception.args[0]
            == "Environment variables for Azure resources must be properly configured."
        )

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.BlobServiceClient.from_connection_string")
    def test_when_upload_data_called_then_upload_to_blob_is_called(
        self,
        mock_from_connection_string,
        mock_getenv,
    ):
        class MockContainerClient:
            def __init__(self):
                self.upload_blob_called = False

            def upload_blob(self, *args, **kwargs):
                self.upload_blob_called = True

            def connect(self):
                pass

            def close(self):
                pass

        class MockBlobServiceClient:
            def get_container_client(self, _):
                return MockContainerClient()

        mock_getenv.return_value = "asdf"
        mock_from_connection_string.return_value = MockBlobServiceClient()

        b_c = AzureBlobClient()
        assert not b_c.container_client

        b_c.connect()
        b_c.upload_data("foo", "bar")

        assert b_c.container_client.upload_blob_called
        b_c.close()

    @pytest.mark.unit_test
    @mock.patch("src.bagel.clients.os.getenv")
    @mock.patch("src.bagel.clients.BlobServiceClient.from_connection_string")
    def test_when_upload_log_called_then_upload_to_blob_is_called(
        self,
        mock_from_connection_string,
        mock_getenv,
    ):
        class MockContainerClient:
            def __init__(self):
                self.upload_blob_called = False

            def upload_blob(self, *args, **kwargs):
                self.upload_blob_called = True

            def connect(self):
                pass

            def close(self):
                pass

        class MockBlobServiceClient:
            def get_container_client(self, _):
                return MockContainerClient()

        mock_getenv.return_value = "asdf"
        mock_from_connection_string.return_value = MockBlobServiceClient()

        b_c = AzureBlobClient()
        assert not b_c.container_client

        b_c.connect()
        b_c.upload_log("foo", "bar")

        assert b_c.container_client.upload_blob_called
        b_c.close()
