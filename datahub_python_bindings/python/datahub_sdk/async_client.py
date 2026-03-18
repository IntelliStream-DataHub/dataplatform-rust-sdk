import os

import dotenv
import load_dotenv

from ._rust_interface import PyAsyncClient
class AsyncClient:
    _client: PyAsyncClient
    def __init__(
            self,
            base_url: str,
            token: str | None= None,
            auth_url: str | None = None,
            token_url: str | None = None,
            redirect_url: str | None= None,
            client_id: str | None= None,
            client_secret: str | None= None,
            project_name: str | None= None,

    ):
        if base_url is None:
            raise ValueError("base_url cannot be None")
        if token is None and any((
                auth_url is None,
                token_url is None,
                redirect_url is None,
                client_id is None,
                client_secret is None)
        ):
            raise ValueError("Either provide a token or authentication details")
        self._client = PyAsyncClient(
            base_url=base_url,
            token=token,
            auth_url=auth_url,
            token_url=token_url,
            redirect_url=redirect_url,
            client_id=client_id,
            client_secret=client_secret,
            project_name=project_name
        )

    @classmethod
    def from_env(cls,path):
        load_dotenv.load_dotenv(path)
        base_url = os.getenv("BASE_URL")
        token = os.getenv("TOKEN")
        auth_url = os.getenv("AUTH_URL") or os.getenv("AUTH_URI")
        token_url = os.getenv("TOKEN_URL") or os.getenv("TOKEN_URI")
        redirect_url = os.getenv("REDIRECT_URL") or os.getenv("REDIRECT_URI")
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        project_name = os.getenv("PROJECT_NAME")
        return cls(base_url, token, auth_url, token_url, redirect_url, client_id, client_secret, project_name)

    @classmethod
    def from_dict(cls, map: dict):
        return cls(**map)

    @classmethod
    def from_token(cls,base_url, token: str):
        return cls(base_url,token=token)

    @property
    def client(self):
        return self._client

    @property
    def base_url(self):
        return self._client.base_url

    @property
    def timeseries(self):
        return self._client.timeseries


    @property
    def units(self):
        return self._client.units

    @property
    def events(self):
        pass
        return self._client.events