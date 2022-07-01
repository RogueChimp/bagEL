from datetime import datetime, timedelta
import json
import logging
import os
import traceback
import types
from typing import List
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient
import yaml

from .integration import BagelIntegration

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def format_table_name(name: str) -> str:
    return name.lower().replace(" ", "_")


def format_json_blob_name(system, table, timestamp, log=False):
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    full_date = format_time(timestamp)
    if log:
        file_type = "log"
    else:
        file_type = "data"
    file_name = (
        f"{system}/{file_type}/{table}/{year}/{month}/{day}/{table}_{full_date}.json"
    )
    return file_name


def format_time(timestamp):
    full_date = timestamp.strftime("%Y_%m_%dT%H_%M_%S_%fZ")
    return full_date


def format_dict_to_json_binary(d):
    return bytes(json.dumps(d, default=str), "utf-8")


# TODO loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]


class BagelError(Exception):
    def __init__(self, errors):
        spacer = "---------------------------"
        self.message = f"Errors occured in Bagel:\n{spacer}\n" + f"\n{spacer}\n".join(
            errors
        )
        super().__init__(self.message)


class Bagel:
    def __init__(self, integration: BagelIntegration) -> None:
        # self.logger = logging.getLogger(__name__)
        self._add_stream_handler()
        self._load_config()
        self.integration = integration

    def _refresh_file_handler(self, log_name) -> None:
        if len(logger.handlers) == 2:
            handler = logger.handlers[1]
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
            else:
                logger.removeHandler(logger.handlers[0])

        fh_formatter = '{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}'

        log_dir = os.path.dirname(log_name)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        fh = logging.FileHandler(log_name)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(fmt=fh_formatter))

        logger.addHandler(fh)

    def _add_stream_handler(self) -> None:
        ch_formatter = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter(fmt=ch_formatter))

        logger.addHandler(ch)

    def _load_config(self) -> None:

        logger.info("Loading environment variables...")

        self.azure_storage_account = os.getenv("STORAGE_ACCOUNT")
        self.azure_storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        self.azure_storage_account_connnection_string = os.getenv(
            "STORAGE_ACCOUNT_CONNECTION_STRING"
        )
        self.azure_storage_account_endpoint = os.getenv("STORAGE_ACCOUNT_ENDPOINT")
        self.azure_container = os.getenv("AZURE_CONTAINER")
        self.azure_table = os.getenv("AZURE_TABLE")

        if None in [
            self.azure_storage_account,
            self.azure_storage_account_key,
            self.azure_storage_account_connnection_string,
            self.azure_storage_account_endpoint,
            self.azure_container,
            self.azure_table,
        ]:
            logger.error("Environment variables must be properly loaded.")
            raise ValueError(
                "Environment variables for Azure resources must be properly configured."
            )

    def get_table_list(self) -> List[dict]:
        path = self._get_table_path()
        with open(path, "r") as f:
            y = yaml.safe_load(f)
        tables = dict(y.items())["tables"]
        return tables

    def _get_table_path(self):
        dir_path = os.path.join("source_dir")

        return (
            os.path.join(dir_path, "tables.yaml")
            if os.path.exists(os.path.join(dir_path, "tables.yaml"))
            else os.path.join(dir_path, "tables.yml")
        )

    def run(self) -> None:

        errors = []

        tables = self.get_table_list()

        for t in tables:

            table_name = format_table_name(t["name"])
            elt_type = t["elt_type"]

            try:
                self._run_table(table_name, elt_type)

            except Exception as e:
                errors.append(traceback.format_exc())
                logger.error(e)
                logger.error(traceback.format_exc())

        if errors:
            raise BagelError(errors)

    def _run_table(self, table_name, elt_type):

        log_file_name = os.path.join(
            "logs",
            f"{self.integration.name}_{table_name}_{format_time(self.get_current_timestamp())}.log",
        )

        table_client = self.connect_azure_table()

        self._refresh_file_handler(log_file_name)
        logger.info(f"table_name: {table_name}")
        logger.info(f"elt_type: {elt_type}")

        last_run_timestamp, current_timestamp = self.get_timebox(
            table_client, table_name
        )

        logger.info(f"Current Timestamp: {current_timestamp}")
        logger.info(f"Last Run Timestamp: {last_run_timestamp}")

        integration_data = self.integration.get_data(
            table_name,
            last_run_timestamp=last_run_timestamp,
            current_timestamp=current_timestamp,
            elt_type=elt_type,
        )

        data = self._validate_data(integration_data)

        # get data
        counter, data_log = self._process_data(table_name, data)

        logger.info(f"Files: {data_log}")

        # overwrite last run timestamp
        entity = self.write_run_timestamp(
            table_client, self.integration.name, table_name, current_timestamp
        )
        table_client.close()

        logger.info(f"Azure Table New Entity: {entity}")
        logger.info(f"Files Created: {counter}")

        # upload log
        with open(log_file_name, "rb") as logfile:
            self.write_json_to_blob(
                format_json_blob_name(
                    self.integration.name, table_name, datetime.utcnow(), True
                ),
                logfile,
            )
        logger.info("Job Complete")

    def _process_data(self, table_name, data):
        counter = 0
        data_log = []
        for d in data:
            if not isinstance(d, list):
                raise TypeError("Datapoint needs to be of type list")

            # generate file_name
            file_name = format_json_blob_name(
                self.integration.name, table_name, datetime.utcnow()
            )

            self.write_json_to_blob(file_name, format_dict_to_json_binary(d))
            data_log.append(file_name)

            counter += 1
        return counter, data_log

    def _validate_data(self, data):

        if not (isinstance(data, types.GeneratorType) or isinstance(data, list)):
            raise TypeError("get_data must provide list or generator object")

        if isinstance(data, list):
            if isinstance(data[0], list):
                raise TypeError("Cannot be list of lists. Use pagination/generator.")
            data = [data]

        return data

    def get_timebox(self, table_client, table_name):

        # get current timestamp
        current_timestamp = self.get_current_timestamp()

        # get last run timestamp
        last_run_timestamp = self.get_last_run_timestamp(
            table_client, self.integration.name, table_name
        )

        return last_run_timestamp, current_timestamp

    def get_current_timestamp(self):
        """
        common function to get the current time.
        """
        return datetime.utcnow()

    def connect_azure_table(self):
        credential = AzureNamedKeyCredential(
            self.azure_storage_account, self.azure_storage_account_key
        )
        table_service_client = TableServiceClient(
            endpoint=self.azure_storage_account_endpoint, credential=credential
        )
        table_client = table_service_client.get_table_client(self.azure_table)
        return table_client

    def connect_azure_blob(self):
        blob_service_client = BlobServiceClient.from_connection_string(
            self.azure_storage_account_connnection_string
        )
        container_client = blob_service_client.get_container_client(
            self.azure_container
        )
        return container_client

    def get_last_run_timestamp(self, table_client, system, table):
        """
        queries the Azure table to get the last time the system/table was run.
        """
        try:
            entity = table_client.get_entity(partition_key=system, row_key=table)
            timestamp = str(entity["last_updated_timestamp"])
            final_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            timestamp = datetime.now() - timedelta(days=3)
            self.write_run_timestamp(table_client, system, table, timestamp)
            final_timestamp = timestamp
        return final_timestamp

    def write_json_to_blob(self, file_name, data):
        """
        takes json from API call and creates a blob in the correct folders.
        TODO: calls split_file if neccesary for large files.
        """
        container_client = self.connect_azure_blob()
        container_client.upload_blob(file_name, data, overwrite=True)
        container_client.close()

    def write_run_timestamp(self, table_client, system, table, timestamp):
        """
        upserts the timestamp of the current run into the Azure table.
        """
        timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        new_entity = {
            "PartitionKey": system,
            "RowKey": table,
            "last_updated_timestamp": timestamp,
        }
        table_client.upsert_entity(mode=UpdateMode.MERGE, entity=new_entity)
        return new_entity
