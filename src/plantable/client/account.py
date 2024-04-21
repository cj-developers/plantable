import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
import requests
from pydantic import BaseModel
from tabulate import tabulate

from ..model import (
    DTABLE_ICON_COLORS,
    DTABLE_ICON_LIST,
    Admin,
    ApiToken,
    Base,
    BaseToken,
    Column,
    Table,
    Team,
    User,
    Webhook,
)
from .base import BaseClient
from .conf import SEATABLE_ACCOUNT_TOKEN, SEATABLE_PASSWORD, SEATABLE_URL, SEATABLE_USERNAME
from .core import TABULATE_CONF, HttpClient

logger = logging.getLogger()


################################################################
# AccountClient
################################################################
class AccountClient(HttpClient):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        seatable_username: str = SEATABLE_USERNAME,
        seatable_password: str = SEATABLE_PASSWORD,
    ):
        super().__init__(seatable_url=seatable_url)
        self.username = seatable_username
        self.password = seatable_password
        self.account_token = None

        self.is_admin = False

        # do login
        self.login()

    def login(self):
        auth_url = self.seatable_url + "/api2/auth-token/"
        response = requests.post(auth_url, json={"username": self.username, "password": self.password})
        response.raise_for_status()
        results = response.json()
        self.account_token = results["token"]

    ################################################################
    # AUTHENTICATION - API TOKEN
    ################################################################
    # [API TOKEN] list api tokens
    async def list_api_tokens(self, workspace_id: str, base_name: str, model: BaseModel = ApiToken):
        """
        [NOTE]
         workspace id : group = 1 : 1
        """
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        ITEM = "api_tokens"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # [API TOKEN] create api token
    async def get_or_create_api_token(
        self,
        workspace_id: str,
        base_name: str,
        app_name: str,
        permission: str = "rw",
        model: BaseModel = ApiToken,
    ):
        """
        [NOTE]
         "bad request" returns if app_name is already exists.
        """
        api_tokens = await self.list_api_tokens(workspace_id=workspace_id, base_name=base_name)
        for api_token in api_tokens:
            if api_token.app_name == app_name:
                return api_token

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        JSON = {"app_name": app_name, "permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = model(**response) if model else response

        return results

    # [API TOKEN] create temporary api token
    async def create_temp_api_token(self, workspace_id: str, base_name: str, model: BaseModel = ApiToken):
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/temp-api-token/"
        ITEM = "api_token"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            now = datetime.now()
            results = model(
                app_name="__temp_token",
                api_token=results,
                generated_by="__temp_token",
                generated_at=now,
                last_access=now,
                permission="r",
            )

        return results

    # [API TOKEN] update api token
    async def update_api_token(
        self,
        workspace_id: str,
        base_name: str,
        app_name: str,
        permission: str = "rw",
        model: BaseModel = ApiToken,
    ):
        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        JSON = {"permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = model(**response) if model else response

        return results

    # [API TOKEN] delete api token
    async def delete_api_token(self, workspace_id: str, base_name: str, app_name: str):
        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    ################################################################
    # AUTHENTICATION - BASE TOKEN
    ################################################################
    # [BASE TOKEN] get base token with account token
    async def get_base_token_with_account_token(
        self,
        workspace_id: str = None,
        base_name: str = None,
        model: BaseModel = BaseToken,
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/access-token/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        if model:
            results = model(**results)

        return results

    # [BASE TOKEN] get base token with api token
    async def get_base_token_with_api_token(self, api_token: str, model: BaseModel = BaseToken):
        METHOD = "GET"
        URL = "/api/v2.1/dtable/app-access-token/"

        async with self.session_maker(token=api_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        if model:
            results = model(**results)

        return results

    # [BASE TOKEN] get base token with invite link
    async def get_base_token_with_invite_link(self, link: str, model: BaseModel = BaseToken):
        link = link.rsplit("/links/", 1)[-1].strip("/")
        METHOD = "GET"
        URL = "/api/v2.1/dtable/share-link-access-token/"

        async with self.session_maker(token=link) as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        if model:
            results = model(**results)

        return results

    # [BASE TOKEN] get base token with external link
    async def get_base_token_with_external_link(self, link: str, model: BaseModel = BaseToken):
        link = link.rsplit("/external-links/", 1)[-1].strip("/")
        METHOD = "GET"
        URL = f"/api/v2.1/external-link-tokens/{link}/access-token/"

        async with self.session_maker(token=link) as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        if model:
            results = model(**results)

        return results

    ################################################################
    # (CUSTOM) GET BASE CLIENT
    ################################################################
    # [BASE CLIENT] (custom) get base client with account token
    async def get_base_client_with_account_token(self, workspace_id: str, base_name: str):
        base_token = await self.get_base_token_with_account_token(workspace_id=workspace_id, base_name=base_name)
        return BaseClient(base_token=base_token)
