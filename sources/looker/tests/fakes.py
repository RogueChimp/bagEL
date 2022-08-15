def mock_post_request(**kwargs):
    url = kwargs.get("url", None)
    params = kwargs.get("params", None)
    headers = kwargs.get("headers", None)
    json = kwargs.get("json", None)
    if not json and params:  # login case
        return MockResponse(
            json_data={"access_token": "fake_access_token"}, status_code=200
        )
    elif json is not None and headers:  # get_data case
        return MockResponse(
            json_data=[{"val1": "a", "val2": "a"}, {"val1": "b", "val2": "b"}],
            status_code=200,
        )


def mock_get_request(self, **kwargs):
    pass


class MockResponse:
    def __init__(self, **kwargs):
        self.json_data = kwargs.get("json_data", None)
        self.status_code = kwargs.get("status_code", None)

    def json(self):
        return self.json_data
