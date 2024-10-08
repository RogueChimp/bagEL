# test_etq.py

import pytest
from unittest import mock
from bagel.data import Bite
from bagel.table import Table

from etq.get_data import ETQDocuments
from .fakes import fake_datasource_data, mock_get_request  # Removed duplicate import


@pytest.fixture
def etq_documents():
    """
    Fixture to initialize ETQDocuments with mocked environment variables.
    """
    with mock.patch("etq.get_data.os.getenv") as mock_os_getenv:
        fake_user, fake_password, fake_base_url = [
            "FAKE_USER",
            "FAKE_PASSWORD",
            "https://trimedx.etq.com:8443/dev/rest/v1/",
        ]

        # Mock environment variables
        mock_os_getenv.side_effect = [
            fake_user,
            fake_password,
            fake_base_url,
        ]

        # Initialize ETQDocuments instance with mocked environment variables
        etq = ETQDocuments()

    return etq


@pytest.mark.unit_test
def test_when_class_instantiated_then_sets_proper_secret_variables_in_load_config_and_base_url(
    etq_documents,
):
    """
    Test that ETQDocuments initializes with correct environment variables.
    """
    expected = [
        "FAKE_USER",
        "FAKE_PASSWORD",
        "https://trimedx.etq.com:8443/dev/rest/v1/",
    ]
    result = [
        etq_documents._etq_user,
        etq_documents._etq_password,
        etq_documents.base_url,
    ]
    assert result == expected, f"Expected {expected}, got {result}"


@pytest.mark.unit_test
@mock.patch("etq.get_data.ETQDocuments.docwork_closed_bynumber")
def test_when_table_name_matches_function_then_function_is_called(
    mock_docwork_closed_bynumber, etq_documents
):
    """
    Test that the correct function is called when the table name matches.
    """
    expected = "foo"
    mock_docwork_closed_bynumber.return_value = expected

    result = etq_documents.get_data(Table("docwork_closed_bynumber", "etq"), None, None)

    assert result == expected, f"Expected {expected}, got {result}"


@pytest.mark.unit_test
@mock.patch("etq.get_data.ETQDocuments._get_datasource")
def test_when_table_name_doesnt_match_function_then__get_datasource_is_called(
    mock__get_datasource, etq_documents
):
    """
    Test that _get_datasource is called when the table name doesn't match any function.
    """
    expected = "foo"
    mock__get_datasource.return_value = expected

    result = etq_documents.get_data(Table("bar", "baz"), None, None)

    assert result == expected, f"Expected {expected}, got {result}"


@pytest.mark.unit_test
def test_when_datasource_data_is_formatted_then_it_is_formatted_correctly(
    etq_documents,
):
    """
    Test that the datasource data is formatted correctly.
    """
    input_ = fake_datasource_data

    expected = [
        {"foo": "bar", "baz": "spam", "ham": "eggs"},
        {"foo": "rab", "baz": "maps", "ham": "sgge"},
    ]

    result = etq_documents._format_datasource_data(input_)

    assert result == expected, f"Expected {expected}, got {result}"


@pytest.mark.unit_test
@mock.patch("etq.get_data.requests.get")
def test_when_datasource_is_fetched_then_json_is_reformatted_correctly(
    mock_requests_get, etq_documents
):
    """
    Test that fetching the datasource reformats JSON correctly.
    """
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

    result = etq_documents.get_data(Table("bar", "baz"), None, None)

    assert (
        next(result) == expected
    ), "The first item does not match the expected Bite instance."
    with pytest.raises(StopIteration):
        next(result)


@pytest.mark.unit_test
@mock.patch("etq.get_data.requests.get")
def test_when_datasource_errors_then_raise_runtime_error(
    mock_requests_get, etq_documents
):
    """
    Test that a RuntimeError is raised when fetching the datasource results in an error.
    """
    mock_requests_get.return_value = mock_get_request(status_code=404)

    with pytest.raises(RuntimeError):
        next(etq_documents.get_data(Table("bar", "baz"), None, None))


@pytest.mark.unit_test
@mock.patch("etq.get_data.requests.get")  # Ensure this path is correct
@mock.patch("etq.get_data.ETQDocuments._docwork_document")
def test_when_fetching_attachments_then_no_attachments_are_skipped(
    mock__docwork_document, mock_requests_get, etq_documents  # Inject the fixture
):
    """
    Test that no attachments are skipped when fetching attachments.
    """
    # Mock the return value of _docwork_document
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
                        {"fieldName": "DOCWORK_TITLE", "Values": ["Test for Mail 1"]},
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
                        {"fieldName": "DOCWORK_RELATED_CHANGE_MANAGEMENT_DOCUMENTS"},
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
                        {"fieldName": "ETQ_DOCWORK_DOCUMENT_OBSOLETE_REQUEST_STATUS"},
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
                            "Values": ["ETQ_TRAINING/ETQ_TRAINING_COURSE_PROFILE/31"],
                        },
                        {
                            "fieldName": "ETQ_DOCWORK_TRAINING_IS_REQUIRED",
                            "Values": ["Yes"],
                        },
                        {"fieldName": "DOCWORK_RELATED_CHANGE_MANAGEMENT_DOCUMENTS"},
                        {
                            "fieldName": "TMX_DC_DC_LEGACY_DOC_NUMBER_P",
                            "Values": ["7684314"],
                        },
                        {"fieldName": "TMX_DC_DC_DOCUMENT_TYPE_P", "Values": ["QMS"]},
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
                        {"fieldName": "ETQ_DOCWORK_DOCUMENT_OBSOLETE_REQUEST_STATUS"},
                        {
                            "fieldName": "DOCWORK_DOCUMENT_NEXT_REVISION",
                            "Values": ["DOCWORK/DOCWORK_DOCUMENT/410"],
                        },
                    ],
                }
            ]
        },
    ]

    # Mock `requests.get` to handle multiple calls using `side_effect`
    mock_requests_get.side_effect = [
        mock_get_request(content=b"foo"),  # First attachment
        mock_get_request(content=b"bar"),  # Second attachment
    ]

    # Expected number of attachments is 2
    expected = 2

    # Call the method under test
    result = list(etq_documents.docwork_attachment(Table("asdf"), None, None))

    # Assert the number of attachments processed
    assert (
        len(result) == expected
    ), f"Expected {expected} attachments, got {len(result)}"

    # Optionally, verify the contents of the attachments
    assert result[0].data == b"foo", "First attachment content mismatch."
    assert (
        result[0].file_name == "78-Post Market Surveillance.docx"
    ), "First attachment filename mismatch."
    assert result[1].data == b"bar", "Second attachment content mismatch."
    assert (
        result[1].file_name == "78-Post Market Surveillance.PDF"
    ), "Second attachment filename mismatch."
