from datetime import datetime
import os
import requests
import logging

from bagel import Bagel, BagelIntegration, Bite, Table

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class ETQDocuments(BagelIntegration):

    source = "etq"
    page_size = 2000

    def __post_init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self._etq_user = os.getenv("ETQ_USER")
        self._etq_password = os.getenv("ETQ_PASSWORD")
        self.base_url = os.getenv("ETQ_BASE_URL")

    def get_data(self, table: Table, last_run_timestamp, current_timestamp):
        """`hasattr()` and `getattr()` are used here to dynamically call
        the corresponding function to the formatted table name if there is one.
        Otherwise `_get_datasource()` is called."""

        if hasattr(self, table.name):
            return getattr(self, table.name)(
                table, last_run_timestamp, current_timestamp
            )

        else:
            return self._get_datasource(table, last_run_timestamp, current_timestamp)

    def _docwork_closed_bynumber(self, table, last_run_timestamp, current_timestamp):
        docword_records_raw = self._get_datasource(
            table, last_run_timestamp, current_timestamp
        )
        for records in docword_records_raw:

            to_send = []
            for record in records.data:

                elt_type = table.elt_type
                if elt_type and elt_type == "delta":
                    modified_date = datetime.strptime(
                        record["DOCWOR_DOCUMEN_ETQ$MODIFIE_DAT"], "%Y-%m-%d %H:%M:%S.%f"
                    )
                    lr_t: datetime = last_run_timestamp
                    if modified_date.timestamp() <= lr_t.timestamp():
                        continue

                to_send.append(record)

            yield to_send

    def docwork_closed_bynumber(self, table, last_run_timestamp, current_timestamp):
        for r in self._docwork_closed_bynumber(
            table, last_run_timestamp, current_timestamp
        ):
            yield Bite(r)

    def _docwork_document(self, table: Table, last_run_timestamp, current_timestamp):

        docwork_records = []

        t = Table(name="docwork_closed_bynumber", elt_type=table.elt_type)

        for d in self._docwork_closed_bynumber(
            t, last_run_timestamp, current_timestamp
        ):
            docwork_records += d

        docs = []
        for record in docwork_records:

            doc_url = (
                self.base_url
                + f"documents/DOCWORK/DOCWORK_DOCUMENT/{record['DOCWORK_ID']}"
            )
            logging.info(f"{doc_url = }")

            response = requests.get(doc_url, auth=(self._etq_user, self._etq_password))

            data = response.json()
            docs.append(data)

        return docs

    def docwork_document(self, table, last_run_timestamp, current_timestamp):
        return Bite(
            self._docwork_document(table, last_run_timestamp, current_timestamp)
        )

    def docwork_attachment(self, table, last_run_timestamp, current_timestamp):

        for data in self._docwork_document(
            table, last_run_timestamp, current_timestamp
        ):
            document = data["Document"][0]
            fields = document["Fields"]
            attachment = [f for f in fields if f["fieldName"] == "DOCWORK_ATTACHMENTS"][
                0
            ]

            attachment_path = attachment.get("attachmentPath")
            values = attachment.get("Values")

            if not attachment_path or not values:
                continue

            document_id = document["documentId"]

            attachment_name_list = [v for v in values if v.lower().endswith(".pdf")]
            if not attachment_name_list:
                continue
            attachment_name = attachment_name_list[0]

            attachment_url = (
                self.base_url
                + f"attachments?path={attachment_path}&name={attachment_name}"
            )
            logging.info(f"{attachment_url = }")

            file_content = requests.get(
                attachment_url, auth=(self._etq_user, self._etq_password)
            ).content

            yield Bite(file_content, file_name=document_id)

    def _get_datasource(self, table: Table, last_run_timestamp, current_timestamp):
        table_name = table.name

        page = 1
        while True:
            datasource_url = (
                self.base_url
                + f"datasources/{table_name}/execute?pagesize={self.page_size}&pagenumber={page}"
            )
            logging.info(f"{datasource_url = }")
            response = requests.get(
                datasource_url, auth=(self._etq_user, self._etq_password)
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"ERROR running {datasource_url}\n{response.status_code = }\n{response.text}"
                )

            d = response.json()

            if d["count"] == 0:
                return None

            data = self._format_datasource_data(d)

            yield Bite(data)

            page += 1

    def _format_datasource_data(self, d):
        data = []
        for record in d["Records"]:
            columns = record["Columns"]
            record_dict = {k: v for k, v in [(c["name"], c["value"]) for c in columns]}
            data.append(record_dict)
        return data


if __name__ == "__main__":
    bagel = Bagel(ETQDocuments())

    bagel.run()
