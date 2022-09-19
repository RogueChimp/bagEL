from bagel.data import Bite
import pytest
import unittest
from unittest import mock

from etq.get_data import ETQDocuments
from .fakes import fake_datasource_data, mock_get_request, mock_get_request


class TestETQ(unittest.TestCase):
    @mock.patch("etq.get_data.os.getenv")
    def setUp(self, mock_os_getenv) -> None:
        self.fake_user, self.fake_password, self.env, self.base_url = [
            "FAKE_USER",
            "FAKE_PASSWORD",
            "dev",
            "https://trimedx.etq.com:8443/dev/rest/v1/",
        ]

        mock_os_getenv.side_effect = [self.fake_user, self.fake_password, self.env]

        self.etq = ETQDocuments()

    @pytest.mark.unit_test
    def test_when_class_instantiated_then_sets_proper_secret_variables_in_load_config_and_base_url(
        self,
    ):
        expected = [self.fake_user, self.fake_password, self.env, self.base_url]
        result = [
            self.etq._etq_user,
            self.etq._etq_password,
            self.etq._env,
            self.etq.base_url,
        ]
        self.assertListEqual(result, expected)

    @pytest.mark.unit_test
    @mock.patch("etq.get_data.ETQDocuments.docwork_closed_bynumber")
    def test_when_table_name_matches_function_then_function_is_called(
        self, mock_docwork_closed_bynumber
    ):
        expected = "foo"

        mock_docwork_closed_bynumber.return_value = expected

        result = self.etq.get_data("docwork_closed_bynumber")

        assert result == expected

    @pytest.mark.unit_test
    @mock.patch("etq.get_data.ETQDocuments._get_datasource")
    def test_when_table_name_doesnt_match_function_then__get_datasource_is_called(
        self, mock__get_datasource
    ):
        expected = "foo"

        mock__get_datasource.return_value = expected

        result = self.etq.get_data("bar")

        assert result == expected

    @pytest.mark.unit_test
    def test_when_datasource_data_is_formatted_then_it_is_formatted_correctly(self):
        input_ = fake_datasource_data

        expected = [
            {"foo": "bar", "baz": "spam", "ham": "eggs"},
            {"foo": "rab", "baz": "maps", "ham": "sgge"},
        ]

        result = self.etq._format_datasource_data(input_)

        assert result == expected

    @pytest.mark.unit_test
    @mock.patch("etq.get_data.requests.get")
    def test_when_datasource_is_fetched_then_json_is_reformatted_correctly(
        self, mock_requests_get
    ):

        expected = Bite(
            [
                {"foo": "bar", "baz": "spam", "ham": "eggs"},
                {"foo": "rab", "baz": "maps", "ham": "sgge"},
            ]
        )
        mock_requests_get.side_effect = [
            mock_get_request(json_data=fake_datasource_data),
            mock_get_request(json_data={"count": 0}),
        ]

        result = self.etq.get_data("bar")

        assert result.__next__() == expected
        with self.assertRaises(StopIteration):
            result.__next__()

    @pytest.mark.unit_test
    @mock.patch("etq.get_data.requests.get")
    def test_when_datasource_errors_then_raise_runtime_error(self, mock_requests_get):

        mock_requests_get.return_value = mock_get_request(status_code=404)

        with self.assertRaises(RuntimeError):
            self.etq.get_data("foo").__next__()

    @pytest.mark.unit_test
    @mock.patch("etq.get_data.requests.get")
    @mock.patch("etq.get_data.ETQDocuments._docwork_document")
    def test_when_fetching_attachments_then_no_attachments_are_skipped(
        self, mock__docwork_document, mock_requests_get
    ):
        mock__docwork_document.return_value = [
            {
                "Document": [
                    {
                        "applicationName": "DOCWORK",
                        "formName": "DOCWORK_DOCUMENT",
                        "documentId": "1",
                        "phase": "DOCWORK_DRAWING_COMPLETED",
                        "Fields": [
                            {
                                "fieldName": "DOCWORK_REFERENCE_LINKS",
                                "Values": ["DOCWORK/DOCWORK_DOCUMENT/338"],
                            },
                            {
                                "fieldName": "DOCWORK_TITLE",
                                "Values": ["Test for Mail 1"],
                            },
                            {"fieldName": "DOCWORK_ISO_ELEMENTS"},
                            {"fieldName": "DOCWORK_CATEGORIES"},
                            {"fieldName": "DOCWORK_REASON"},
                            {"fieldName": "ETQ_TEMP"},
                            {
                                "fieldName": "DOCWORK_WORKFLOW_STATUS",
                                "Values": ["APPROVED"],
                            },
                            {"fieldName": "ETQ_DOCWORK_TRAINING_WHY_NOT_REQUIRED"},
                            {"fieldName": "ETQ_DOCWORK_TRAINING_COURSE_PROFILE_LINK"},
                            {"fieldName": "ETQ_DOCWORK_TRAINING_IS_REQUIRED"},
                            {
                                "fieldName": "DOCWORK_RELATED_CHANGE_MANAGEMENT_DOCUMENTS"
                            },
                            {"fieldName": "TMX_DC_DC_LEGACY_DOC_NUMBER_P"},
                            {"fieldName": "TMX_DC_DC_DOCUMENT_TYPE_P"},
                            {"fieldName": "DOCWORK_DOCUMENT_ACTIVE_DEVIATIONS"},
                            {"fieldName": "ETQ_DOCWORK_DOCUMENT_CHANGE_REQUEST_STATUS"},
                            {
                                "fieldName": "DOCWORK_ORIGINATOR",
                                "Values": ["EtQAdministrator"],
                            },
                            {"fieldName": "DOCWORK_ORIGINATION_DATE"},
                            {"fieldName": "ETQ_DOCWORK_DEPARTMENT"},
                            {"fieldName": "DOCWORK_ATTACHMENTS"},
                            {
                                "fieldName": "ETQ_DOCWORK_DOCUMENT_OBSOLETE_REQUEST_STATUS"
                            },
                            {"fieldName": "DOCWORK_DOCUMENT_NEXT_REVISION"},
                        ],
                    }
                ]
            },
            {
                "Document": [
                    {
                        "applicationName": "DOCWORK",
                        "formName": "DOCWORK_DOCUMENT",
                        "documentId": "78",
                        "phase": "DOCWORK_EXTERNAL_COMPLETED",
                        "Fields": [
                            {"fieldName": "DOCWORK_REFERENCE_LINKS"},
                            {
                                "fieldName": "DOCWORK_TITLE",
                                "Values": ["Post Market Surveillance Procedure"],
                            },
                            {
                                "fieldName": "DOCWORK_ISO_ELEMENTS",
                                "Values": ["ISO 13485 : 2016"],
                            },
                            {"fieldName": "DOCWORK_CATEGORIES"},
                            {"fieldName": "DOCWORK_REASON"},
                            {"fieldName": "ETQ_TEMP"},
                            {
                                "fieldName": "DOCWORK_WORKFLOW_STATUS",
                                "Values": ["APPROVED"],
                            },
                            {"fieldName": "ETQ_DOCWORK_TRAINING_WHY_NOT_REQUIRED"},
                            {
                                "fieldName": "ETQ_DOCWORK_TRAINING_COURSE_PROFILE_LINK",
                                "Values": [
                                    "ETQ_TRAINING/ETQ_TRAINING_COURSE_PROFILE/31"
                                ],
                            },
                            {
                                "fieldName": "ETQ_DOCWORK_TRAINING_IS_REQUIRED",
                                "Values": ["Yes"],
                            },
                            {
                                "fieldName": "DOCWORK_RELATED_CHANGE_MANAGEMENT_DOCUMENTS"
                            },
                            {
                                "fieldName": "TMX_DC_DC_LEGACY_DOC_NUMBER_P",
                                "Values": ["7684314"],
                            },
                            {
                                "fieldName": "TMX_DC_DC_DOCUMENT_TYPE_P",
                                "Values": ["QMS"],
                            },
                            {"fieldName": "DOCWORK_DOCUMENT_ACTIVE_DEVIATIONS"},
                            {"fieldName": "ETQ_DOCWORK_DOCUMENT_CHANGE_REQUEST_STATUS"},
                            {
                                "fieldName": "DOCWORK_ORIGINATOR",
                                "Values": ["Scott E Trevino"],
                            },
                            {
                                "fieldName": "DOCWORK_ORIGINATION_DATE",
                                "Values": ["Feb 1, 2020"],
                            },
                            {
                                "fieldName": "ETQ_DOCWORK_DEPARTMENT",
                                "Values": ["Quality Management System (QMS)"],
                            },
                            {
                                "fieldName": "DOCWORK_ATTACHMENTS",
                                "attachmentPath": "/7/37/0078/78/1233",
                                "Values": [
                                    "Post Market Surveillance.docx",
                                    "Post Market Surveillance.PDF",
                                ],
                            },
                            {
                                "fieldName": "ETQ_DOCWORK_DOCUMENT_OBSOLETE_REQUEST_STATUS"
                            },
                            {
                                "fieldName": "DOCWORK_DOCUMENT_NEXT_REVISION",
                                "Values": ["DOCWORK/DOCWORK_DOCUMENT/410"],
                            },
                        ],
                    }
                ]
            },
        ]

        mock_requests_get.return_value = mock_get_request(content=bytes("foo", "utf-8"))

        expected = 1
        result = len(list(self.etq.docwork_attachment("asdf")))

        assert result == expected
