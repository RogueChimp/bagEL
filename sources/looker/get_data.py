import os
import requests

from bagel import Bagel, BagelIntegration


class Looker(BagelIntegration):

    name = "looker"

    def __init__(self) -> None:
        self._load_config()
        self.base_url = "https://lookerdev.trimedx.com:443/api/3.1/"

    def _load_config(self):
        self._client_id = os.getenv("LOOKER_CLIENT_ID")
        self._client_secret = os.getenv("LOOKER_CLIENT_SECRET")

    def get_data(
        self,
        table: str,
        **kwargs
    ):
        headers = self.looker_login(self._client_id, self._client_secret)
        look_id = self.looker_get_look_id(headers, table)
        data = self.looker_get_data(headers, look_id, kwargs["elt_type"])
        return data


    def looker_login(self, client_id, client_secret):
        params = {"client_id": client_id, "client_secret": client_secret}
        login_url = f"{self.base_url}login"
        response = requests.post(login_url, params=params)
        data = response.json()
        bearer = data["access_token"]
        headers = {
            "authorization": "Bearer " + bearer,
            "cache-control": "no-cache",
        }
        return headers


    def looker_get_look_id(self, headers, title):
        look_url = f"{self.base_url}looks/search"
        params = {"title": title}
        response = requests.get(look_url, headers=headers, params=params)
        data = response.json()
        for item in data:
            look_id = item["id"]
        return look_id


    def looker_get_data(self, headers, look_id, elt_type, timestamp=None):
        if elt_type == "full":
            look_url = f"{self.base_url}looks/{look_id}/run/json"
            response = requests.get(look_url, headers=headers)
            data = response.json()
        elif elt_type == "delta":
            raise Exception("delta not configured yet")
            # TODO: get query from look, add filter, run query
        else:
            raise Exception("Invalid elt_type in tables.yaml file")
        return data


if __name__ == "__main__":
    
    bagel = Bagel(Looker())

    bagel.run()