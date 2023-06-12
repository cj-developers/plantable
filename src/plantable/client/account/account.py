import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from ...conf import (
    SEATABLE_ACCOUNT_TOKEN,
    SEATABLE_API_TOKEN,
    SEATABLE_BASE_TOKEN,
    SEATABLE_URL,
)
from ...model import (
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
from ..base import BaseClient
from ..core import TABULATE_CONF, HttpClient

logger = logging.getLogger()


################################################################
# AccountClient
################################################################
class AccountClient(HttpClient):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        account_token: str = SEATABLE_ACCOUNT_TOKEN,
        api_token: str = None,
    ):
        super().__init__(seatable_url=seatable_url)
        self.account_token = account_token
        self.api_token = api_token
        self.base_tokens = dict()

    async def login(self, username: str, password: str):
        async with self.session_maker() as session:
            response = await self.request(
                session=session,
                method="POST",
                url="/api2/auth-token/",
                json={"username": username, "password": password},
            )
        self.account_token = response["token"]

    ################################################################
    # AUTHENTICATION - API TOKEN
    ################################################################
    # [API TOKEN] list api tokens
    async def list_api_tokens(
        self,
        *,
        workspace_id: str,
        base_name: str,
        model: BaseModel = ApiToken,
    ):
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
    async def create_api_token(
        self,
        app_name: str,
        permission: str = "r",
        *,
        workspace_id: str,
        base_name: str,
        model: BaseModel = ApiToken,
    ):
        """
        [NOTE]
         "bad request" returns if app_name is already exists.
        """

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        JSON = {"app_name": app_name, "permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(
                session=session, method=METHOD, url=URL, json=JSON
            )
            results = model(**response) if model else response

        return results

    # [API TOKEN] create temporary api token
    async def create_temp_api_token(
        self, *, workspace_id: str, base_name: str, model: BaseModel = ApiToken
    ):
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
        permission: str = "r",
        model: BaseModel = ApiToken,
    ):
        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        JSON = {"permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(
                session=session, method=METHOD, url=URL, json=JSON
            )
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
    # [BASE TOKEN] (custom) update base token
    async def update_base_token(
        self, key: str, method: str, url: str, model: BaseModel = BaseToken, **params
    ):
        if key not in self.base_tokens:
            async with self.session_maker(token=self.account_token) as session:
                results = await self.request(
                    session=session, method=method, url=url, **params
                )
            if model:
                results = model(**results)
            self.base_tokens.update({key: results})

        return self.base_tokens[key]

    # [BASE TOKEN] get base token with api token
    # NOT WORKING - 403 Forbidden
    async def get_base_token_with_api_token(self, *, model: BaseModel = BaseToken):
        METHOD = "GET"
        URL = "/api/v2.1/dtable/app-access-token/"

        return await self.update_base_token(
            key=self.api_token, method=METHOD, url=URL, model=model
        )

    # [BASE TOKEN] get base token with account token
    async def get_base_token_with_account_token(
        self,
        *,
        workspace_id: str = None,
        base_name: str = None,
        model: BaseModel = BaseToken,
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/access-token/"

        return await self.update_base_token(
            key=f"{workspace_id}/{base_name}", method=METHOD, url=URL, model=model
        )

    # [BASE TOKEN] get base token with invite link
    async def get_base_token_with_invite_link(
        self, link: str, model: BaseModel = BaseToken
    ):
        link = link.rsplit("/links/", 1)[-1].strip("/")
        METHOD = "GET"
        URL = "/api/v2.1/dtable/share-link-access-token/"

        return await self.update_base_token(
            key=link, method=METHOD, url=URL, model=model, token=link
        )

    # [BASE TOKEN] get base token with external link
    async def get_base_token_with_external_link(
        self, link: str, model: BaseModel = BaseToken
    ):
        link = link.rsplit("/external-links/", 1)[-1].strip("/")
        METHOD = "GET"
        URL = f"/api/v2.1/external-link-tokens/{link}/access-token/"

        return await self.update_base_token(
            key=link, method=METHOD, url=URL, model=model
        )

    ################################################################
    # (CUSTOM) GET BASE CLIENT
    ################################################################
    # [BASE CLIENT] (custom) get base client with account token
    async def get_base_client_with_account_token(
        self, workspace_id: str, base_name: str
    ):
        base_token = await self.get_base_token_with_account_token(
            workspace_id=workspace_id, base_name=base_name
        )
        client = BaseClient(base_token=base_token)

        return client
