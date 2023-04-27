import os
import traceback
from typing import Generator, List, Optional, Union

import yaml

from .base_clients import StorageClient, TimeboxClient
from .clients import AzureBlobClient, AzureTableClient
from .data import Bite
from .errors import BagelError
from .integration import BagelIntegration
from .logger import BagelLogger
from .table import Table
from .util import (
    extract_date_ranges,
    format_blob_name,
    format_dict_to_json_binary,
    format_timestamp_to_str,
    get_current_timestamp,
)
from .datadog_logs import DataDogLogSubmitter


class Bagel:
    def __init__(
        self,
        integration: BagelIntegration,
        timebox_client: TimeboxClient = None,
        storage_client: StorageClient = None,
    ):
        """🥯🥯🥯"""

        self.logger = BagelLogger()

        self.integration = integration

        self.timebox_client = timebox_client if timebox_client else AzureTableClient()
        self.storage_client = storage_client if storage_client else AzureBlobClient()

    def run(self):

        errors: List[BagelError] = []

        tables = self.get_table_list()

        for t in tables:

            try:
                self._run_table(t)
                self._log_datadog_info(self.integration.source, t.name)

            except Exception as e:
                errors.append(traceback.format_exc())
                self.logger.error(e)
                self._log_datadog_error(e, self.integration.source, t.name)
                self.logger.error(traceback.format_exc())

        if errors:
            raise BagelError(errors)

    def _run_table(self, table: Table):
        formatted_ts = format_timestamp_to_str(get_current_timestamp())
        log_file_name = os.path.join(
            "logs",
            f"{self.integration.source}_{table.name}_{formatted_ts}.log",
        )

        self.logger.new_log_file(log_file_name)
        self.logger.info(f"Now running {table}")

        self.timebox_client.connect()

        last_run_timestamp, current_timestamp = self.timebox_client.get_timebox(
            self.integration.source, table.name
        )

        self.logger.info(f"Current Timestamp: {current_timestamp}")
        self.logger.info(f"Last Run Timestamp: {last_run_timestamp}")

        date_ranges = extract_date_ranges(
            last_run_timestamp,
            current_timestamp,
            table.historical_batch,
            table.historical_frequency,
        )

        for i in range(len(date_ranges) - 1):
            lr_t = date_ranges[i]
            c_t = date_ranges[i + 1]

            integration_data = self.integration.get_data(
                table, last_run_timestamp=lr_t, current_timestamp=c_t
            )

            # Validate Data
            self._validate_data(integration_data)
            data = self._bite_to_iterable(integration_data)

            counter = 0
            data_log = []
            for bite in data:

                file_name = self._upload_bite(
                    self.integration.source, table.name, bite, table.file_format
                )
                data_log.append(file_name)

                counter += 1

            self.logger.info(f"Uploaded {counter} Rows / Files: {data_log}")

            # overwrite last run timestamp
            write_result = self.timebox_client.write_run_timestamp(
                self.integration.source, table.name, c_t
            )

            self.logger.info(f"Timebox write result: {write_result}")

            # upload log
            with open(log_file_name, "rb") as logfile:
                self.storage_client.upload_log(
                    format_blob_name(
                        self.integration.source,
                        table.name,
                        get_current_timestamp(),
                        log=True,
                    ),
                    logfile.read(),
                )

        self.timebox_client.close()
        self.logger.info("Job Complete")

    def get_table_list(self) -> List[Table]:
        path = self._get_table_path()
        with open(path, "r") as f:
            y = yaml.safe_load(f)
        tables_raw = dict(y.items())["tables"]
        tables = [Table.from_config(t) for t in tables_raw]
        return tables

    def _get_table_path(self):  # pragma: nocover
        dir_path = os.path.split(traceback.extract_stack()[0].filename)[0]

        return (
            os.path.join(dir_path, "tables.yaml")
            if os.path.exists(os.path.join(dir_path, "tables.yaml"))
            else os.path.join(dir_path, "tables.yml")
        )

    def _validate_data(self, data: Union[Bite, Generator]):

        if not (isinstance(data, Generator) or data.__class__.__name__ == "Bite"):
            raise TypeError("get_data must provide Bite or generator object")

    def _bite_to_iterable(self, data: Union[Bite, Generator]):
        if data.__class__.__name__ == "Bite":
            data = [data]

        return data

    def _upload_bite(
        self, src_system, table_name: str, bite: Bite, file_format: Optional[str] = None
    ):
        # generate file_name
        file_name = format_blob_name(
            src_system,
            table_name,
            get_current_timestamp(),
            file_format=file_format,
            file_name=bite.file_name,
        )

        formatted_data = (
            format_dict_to_json_binary(bite.data)
            if file_format in ["json", None]
            else bite.data
        )

        self.storage_client.upload_data(file_name, formatted_data)
        return file_name

    def _log_datadog_error(self, error_message, integration, table_name):
        log_submitter = DataDogLogSubmitter()
        env = os.getenv("ENV")
        log_data = {
            "ddsource": "azurecontainer",
            "ddtags": f"env:{env},integration:{integration},table:{table_name}",
            "hostname": "azurecontainer",
            "message": str(error_message),
            "errormessage": str(traceback.format_exc()),
            "service": "bagEL",
            "status": "error",
        }
        log_submitter.submit_log(log_data)

    def _log_datadog_info(self, integration, table_name):
        log_submitter = DataDogLogSubmitter()
        env = os.getenv("ENV")
        log_data = {
            "ddsource": "azurecontainer",
            "ddtags": f"env:{env},integration:{integration},table:{table_name}",
            "hostname": "azurecontainer",
            "message": "Successfully completed processing",
            "service": "bagEL",
            "status": "info",
        }
        log_submitter.submit_log(log_data)
