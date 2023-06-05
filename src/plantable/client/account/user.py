import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from ...conf import SEATABLE_ACCOUNT_TOKEN, SEATABLE_API_TOKEN, SEATABLE_BASE_TOKEN, SEATABLE_URL
from ...model import (
    DTABLE_ICON_COLORS,
    DTABLE_ICON_LIST,
    AccountInfo,
    Activity,
    Admin,
    ApiToken,
    Base,
    BaseToken,
    Column,
    Table,
    Team,
    User,
    UserInfo,
    Webhook,
    Favorite,
)
from ..base import BaseClient
from ..core import TABULATE_CONF, HttpClient, parse_base

logger = logging.getLogger()


################################################################
# UserClient
################################################################
class UserClient(HttpClient):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        account_token: str = SEATABLE_ACCOUNT_TOKEN,
    ):
        super().__init__(seatable_url=seatable_url)
        self.account_token = account_token

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
    # USER ACCOUNT OPERATIONS - USER
    ################################################################
    # [USER] get account info
    async def get_account_info(self, model: BaseModel = AccountInfo):
        METHOD = "GET"
        URL = "/api2/account/info/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        if model:
            results = model(**results)

        return results

    # [USER] update email address
    async def update_email_address(self):
        raise NotImplementedError

    # [USER] upload/update user avatar
    async def update_user_avartar(self):
        raise NotImplementedError

    # [USER] get public user info
    async def get_public_user_info(self, user_id: str, model: BaseModel = UserInfo):
        METHOD = "GET"
        URL = f"/api/v2.1/user-common-info/{user_id}/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        if model:
            results = model(**results)

        return results

    # [USER] list public user info
    async def list_public_user_info(self, user_id_list: List[str], model: BaseModel = UserInfo):
        METHOD = "POST"
        URL = "/api/v2.1/user-list/"
        JSON = {"user_id_list": user_id_list}
        ITEM = "user_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    ################################################################
    # USER ACCOUNT OPERATION - USER
    ################################################################

    # [BASES] Create Base
    async def create_base(
        self,
        workspace_id: str,
        base_name: str,
        icon: str = None,
        color: str = None,
        model: BaseModel = Base,
    ):
        METHOD = "POST"
        URL = f"/api/v2.1/dtables/"
        ITEM = "table"
        DATA = aiohttp.FormData([("workspace_id", workspace_id), ("name", base_name)])
        _ = DATA.add_field("icon", icon) if icon else None
        _ = DATA.add_field("color", color) if color else None

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, data=DATA)
            results = response[ITEM]

        if model:
            results = model(**results)

        return results

    # [BASES] update base
    async def update_base(
        self,
        workspace_id: str,
        base_name: str,
        new_base_name: str = None,
        new_icon: str = None,
        new_color: str = None,
        model: BaseModel = Base,
    ):
        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/"
        ITEM = "table"
        DATA = aiohttp.FormData([("name", base_name)])
        _ = DATA.add_field("new_name", new_base_name) if new_base_name else None
        _ = DATA.add_field("icon", new_icon) if new_icon else None
        _ = DATA.add_field("color", new_color) if new_color else None

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, data=DATA)
            results = response[ITEM]

        if model:
            results = model(**results)

        return results

    # [BASES] delete base
    # NOT WORKING
    async def delete_base(self, workspace_id: int):
        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # [BASES] get base activities
    async def get_base_activities(self, model: BaseModel = Activity):
        METHOD = "GET"
        URL = "/api/v2.1/dtable-activities/"
        ITEM = "table_activities"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # [BASES] list_favorites
    async def list_favorites(self, model: BaseModel = Favorite):
        METHOD = "GET"
        URL = "/api/v2.1/starred-dtables/"
        ITEM = "user_starred_dtable_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # [BASES] favorite base
    async def favorite_base(self, base: Base):
        METHOD = "POST"
        URL = "/api/v2.1/starred-dtables/"
        ITEM = "success"
        JSON = {"dtable_uuid": base.uuid}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    # [BASES] unfavorite base
    async def unfavorite_base(self, base: Base):
        METHOD = "DELETE"
        URL = "/api/v2.1/starred-dtables/"
        ITEM = "success"
        PARAMS = {"dtable_uuid": base.uuid}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

        return results

    # [BASES] list bases user can admin
    async def list_bases_user_can_admin(self, model: BaseModel = Base):
        METHOD = "GET"
        URL = "/api/v2.1/user-admin-dtables/"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response

        if model:
            if results["personal"]:
                results["personal"] = [model(**x) for x in results["personal"]]
            for group in results["groups"]:
                group["dtables"] = [model(**x) for x in group["dtables"]]

        return results

    # [BASES] (custom) get base by name
    async def get_base(self, group_name: str, base_name: str):
        results = await self.list_bases_user_can_admin()

        # personal base
        if group_name in ["personal"]:
            for base in results["personal"]:
                if base.name == base_name:
                    return base
            else:
                raise KeyError()

        # group base
        for group in results["groups"]:
            if group["group_name"] != group_name:
                continue
            for base in group["dtables"]:
                if base.name == base_name:
                    return base
        else:
            raise KeyError()

    ################################################################
    # USER ACCOUNT OPERATION - GROUPS & WORKSPACES
    ################################################################
    # [GROUPS & WORKSPACES] list workspaces
    async def list_workspaces(self, detail: bool = True):
        METHOD = "GET"
        URL = "/api/v2.1/workspaces/"
        ITEM = "workspace_list"
        PARAMS = {"detail": str(detail).lower()}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

        return results

    ################################################################
    # USER ACCOUNT OPERATION - WEBHOOKS
    ################################################################
    # [WEBHOOKS] list webhooks
    async def list_webhooks(self, workspace_id: str, base_name: str, model: BaseModel = Webhook):
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/webhooks/"
        ITEM = "webhook_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # [WEBHOOKS] create webhook
    async def create_webhook(
        self,
        workspace_id: str,
        base_name: str,
        url: str,
        secret: int = 0,
        model: BaseModel = Webhook,
    ):
        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/webhooks/"
        JSON = {"url": url, "secret": str(secret)}
        ITEM = "webhook"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        if model:
            results = model(**results)

        return results

    # [WEBHOOKS] update webhook
    async def update_webhook(
        self,
        workspace_id: str,
        base_name: str,
        webhook_id: str,
        url: str,
        secret: int = None,
    ):
        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/webhooks/{webhook_id}/"
        JSON = {"url": url, "secret": str(secret)}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            print(response)

    # [WEBHOOKS] delete webhook
    async def delete_webhook(self, workspace_id: str, base_name: str, webhook_id: str):
        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/webhooks/{webhook_id}/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results
