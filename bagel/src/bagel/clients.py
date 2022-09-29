from datetime import datetime, timedelta
import os
from typing import Optional, Union
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

from .base_clients import StorageClient, TimeboxClient


class AzureTableClient(TimeboxClient):
    def __init__(self):
        self._load_config()
        self.table_client = None

    def _load_config(self):
        self.azure_storage_account = os.getenv("STORAGE_ACCOUNT")
        self.azure_storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        self.azure_storage_account_endpoint = os.getenv("STORAGE_ACCOUNT_ENDPOINT")
        self.azure_table = os.getenv("AZURE_TABLE")

        if None in [
            self.azure_storage_account,
            self.azure_storage_account_key,
            self.azure_storage_account_endpoint,
            self.azure_table,
        ]:
            raise ValueError(
                "Environment variables for Azure resources must be properly configured."
            )

    def connect(self):
        self._connect_azure_table()

    def close(self):
        self.table_client.close()

    def _connect_azure_table(self):
        credential = AzureNamedKeyCredential(
            self.azure_storage_account, self.azure_storage_account_key
        )
        table_service_client = TableServiceClient(
            endpoint=self.azure_storage_account_endpoint, credential=credential
        )
        self.table_client = table_service_client.get_table_client(self.azure_table)

    def get_last_run_timestamp(
        self, system: str, table: str, initial_timestamp: Optional[datetime] = None
    ):
        """
        queries the Azure table to get the last time the system/table was run.
        """
        if not self.table_client:
            raise RuntimeError("Table client is not connected.")

        try:
            entity = self.table_client.get_entity(partition_key=system, row_key=table)
        except ResourceNotFoundError:
            entity = None

        if entity:
            timestamp = str(entity["last_updated_timestamp"])
            final_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        elif not entity:
            timestamp = (
                initial_timestamp
                if initial_timestamp
                else self.get_current_timestamp() - timedelta(days=3)
            )
            self.write_run_timestamp(system, table, timestamp)
            final_timestamp = timestamp

        return final_timestamp

    def write_run_timestamp(self, system: str, table: str, timestamp: datetime) -> any:
        """
        upserts the timestamp of the current run into the Azure table.
        """
        timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        new_entity = {
            "PartitionKey": system,
            "RowKey": table,
            "last_updated_timestamp": timestamp,
        }
        self.table_client.upsert_entity(mode=UpdateMode.MERGE, entity=new_entity)
        return new_entity


class AzureBlobClient(StorageClient):
    def __init__(self):
        self._load_config()
        self.container_client: Union[ContainerClient, None] = None

    def _load_config(self):
        self.azure_storage_account_connnection_string = os.getenv(
            "STORAGE_ACCOUNT_CONNECTION_STRING"
        )
        self.azure_container = os.getenv("AZURE_CONTAINER")

        if None in [
            self.azure_storage_account_connnection_string,
            self.azure_container,
        ]:
            raise ValueError(
                "Environment variables for Azure resources must be properly configured."
            )

    def connect(self):
        self._connect_azure_blob()

    def close(self):
        self.container_client.close()

    def _connect_azure_blob(self):
        blob_service_client = BlobServiceClient.from_connection_string(
            self.azure_storage_account_connnection_string
        )
        self.container_client = blob_service_client.get_container_client(
            self.azure_container
        )

    def _upload_data(self, file_name: str, data: any):
        self.connect()
        self.container_client.upload_blob(file_name, data, overwrite=True)
        self.close()

    def upload_log(self, file_name: str, data: any):
        self._upload_data(file_name, data)

    def upload_data(self, file_name: str, data: any):
        """
        takes json from API call and creates a blob in the correct folders.
        TODO: calls split_file if neccesary for large files.
        """
        self._upload_data(file_name, data)
