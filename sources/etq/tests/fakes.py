fake_datasource_data = {
    "count": 2,
    "Records": [
        {
            "Columns": [
                {"name": "foo", "value": "bar"},
                {"name": "baz", "value": "spam"},
                {"name": "ham", "value": "eggs"},
            ]
        },
        {
            "Columns": [
                {"name": "foo", "value": "rab"},
                {"name": "baz", "value": "maps"},
                {"name": "ham", "value": "sgge"},
            ]
        },
    ],
}

fake_text = "response text"


class MockResponse:
    def __init__(self, **kwargs):
        self.json_data = kwargs.get("json_data")
        self.status_code = kwargs.get("status_code")
        self.text = kwargs.get("text", fake_text)
        self.content = kwargs.get("content")

    def json(self):
        return self.json_data


def mock_get_request(**kwargs) -> MockResponse:
    json_data = kwargs.get("json_data")
    status_code = kwargs.get("status_code", 200)
    content = kwargs.get("content")
    return MockResponse(json_data=json_data, status_code=status_code, content=content)
