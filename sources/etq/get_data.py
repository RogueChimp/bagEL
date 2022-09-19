from datetime import datetime
import os
import requests
import logging
from bagel import Bagel, BagelIntegration, Bite

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}',
)


class ETQDocuments(BagelIntegration):

    name = "etq"
    page_size = 2000

    def __init__(self) -> None:
        self._load_config()

    def _load_config(self):
        self._etq_user = os.getenv("ETQ_USER")
        self._etq_password = os.getenv("ETQ_PASSWORD")
        self._env = os.getenv("ETQ_ENV", "dev")
        self.base_url = f"https://trimedx.etq.com:8443/{self._env}/rest/v1/"

    def get_data(self, table: str, **kwargs):
        """`hasattr()` and `getattr()` are used here to dynamically call
        the corresponding function to the formatted table name if there is one.
        Otherwise `_get_datasource()` is called."""

        if hasattr(self, table):
            return getattr(self, table)(table, **kwargs)

        else:
            return self._get_datasource(table, **kwargs)

    def _docwork_closed_bynumber(self, table, **kwargs):
        docword_records_raw = self._get_datasource(table)
        for records in docword_records_raw:

            to_send = []
            for record in records.content:

                elt_type = kwargs.get("elt_type")
                if elt_type and elt_type == "delta":
                    modified_date = datetime.strptime(
                        record["DOCWOR_DOCUMEN_ETQ$MODIFIE_DAT"], "%Y-%m-%d %H:%M:%S.%f"
                    )
                    if modified_date <= kwargs["last_run_timestamp"]:
                        continue

                to_send.append(record)

            yield to_send

    def docwork_closed_bynumber(self, table, **kwargs):
        for r in self._docwork_closed_bynumber(table, **kwargs):
            yield Bite(r)

    def _docwork_document(self):

        docwork_records = []

        for d in self._docwork_closed_bynumber("docwork_closed_bynumber"):
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

    def docwork_document(self, table, **kwargs):
        return Bite(self._docwork_document())

    def docwork_attachment(self, table, **kwargs):

        for data in self._docwork_document():
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

    def _get_datasource(self, table: str, **kwargs):

        page = 1
        while True:
            datasource_url = (
                self.base_url
                + f"datasources/{table}/execute?pagesize={self.page_size}&pagenumber={page}"
            )
            logging.info(f"{datasource_url = }")
            response = requests.get(
                datasource_url, auth=(self._etq_user, self._etq_password)
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"{self._etq_user = }, {'.'.join([*self._etq_password]) = }, {response.status_code = }\n{response.text}"
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
