fake_text = "response text"


class MockResponse:
    def __init__(self, **kwargs):
        self.json_data = kwargs.get("json_data")
        self.status_code = kwargs.get("status_code")
        self.text = kwargs.get("text", fake_text)
        self.content = kwargs.get("content")

    def json(self):
        return self.json_data


def mock_get_request(**kwargs):
    url = kwargs.get("url", None)
    params = kwargs.get("params", None)
    headers = kwargs.get("headers", None)
    json = kwargs.get("json", None)
    return MockResponse(
        json_data={
            "totalResults": "0",
            "resultsPerPage": "0",
            "array": [{"val1": "b"}, {"val1": "c"}],
        },
        status_code=200,
    )


def mock_get_request_200_delta(**kwargs):
    url = kwargs.get("url", None)
    params = kwargs.get("params", None)
    headers = kwargs.get("headers", None)
    json = kwargs.get("json", None)
    return MockResponse(
        json_data={
            "val1": "a",
            "val2": "a",
            "modifiedDate": 100000000,
            "array": [{"val1": "b"}, {"val1": "c"}],
        },
        status_code=200,
    )


def mock_get_request_200(**kwargs):
    url = kwargs.get("url", None)
    params = kwargs.get("params", None)
    headers = kwargs.get("headers", None)
    json = kwargs.get("json", None)
    return MockResponse(
        json_data={
            "val1": "a",
            "val2": "a",
            "modifiedDate": 2070600121000,
            "array": [{"val1": "b"}, {"val1": "c"}],
        },
        status_code=200,
    )


def mock_get_request_404(**kwargs):
    url = kwargs.get("url", None)
    params = kwargs.get("params", None)
    headers = kwargs.get("headers", None)
    json = kwargs.get("json", None)
    return MockResponse(
        json_data={"val1": "a", "val2": "a", "array": [{"val1": "b"}, {"val1": "c"}]},
        status_code=404,
    )
