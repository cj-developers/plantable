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
from ..core import TABULATE_CONF, HttpClient, parse_base

logger = logging.getLogger()


################################################################
# AdminClient
################################################################
class AdminClient(HttpClient):
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
    # AUTHENTICATION
    ################################################################
    # [API-TOKEN] list api tokens
    async def list_api_tokens(
        self,
        *,
        base: Base = None,
        workspace_id: str = None,
        base_name: str = None,
        model: BaseModel = ApiToken,
        **params,
    ):
        """
        [NOTE]
         workspace id : group = 1 : 1
        """
        if base:
            workspace_id, base_name = parse_base(base)
        if not workspace_id or not base_name:
            raise KeyError()

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        ITEM = "api_tokens"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # [API-TOKEN] create api token
    async def create_api_token(
        self,
        app_name: str,
        permission: str = "r",
        *,
        workspace_id: str = None,
        base_name: str = None,
        base: Base = None,
        model: BaseModel = ApiToken,
        **params,
    ):
        """
        [NOTE]
         "bad request" returns if app_name is already exists.
        """
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        JSON = {"app_name": app_name, "permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON, **params)
            results = model(**response) if model else response

        return results

    # [API-TOKEN] create temporary api token
    async def create_temp_api_token(
        self,
        *,
        base: Base = None,
        workspace_id: str = None,
        base_name: str = None,
        model: BaseModel = ApiToken,
        **params,
    ):
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/temp-api-token/"
        ITEM = "api_token"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
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

    # [API-TOKEN] update api token
    async def update_api_token(
        self,
        app_name: str,
        permission: str = "r",
        *,
        base: Base = None,
        workspace_id: str = None,
        base_name: str = None,
        model: BaseModel = ApiToken,
        **params,
    ):
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        JSON = {"permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON, **params)
            results = model(**response) if model else response

        return results

    # [API-TOKEN] delete api token
    async def delete_api_token(
        self, app_name: str, *, base: Base = None, workspace_id: str = None, base_name: str = None
    ):
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # [BASE-TOKEN] get base token with api token
    # NOT WORKING YET
    async def get_base_token_with_api_token(self, *, api_token: Union[ApiToken, str], model: BaseModel = BaseToken):
        api_token = api_token.api_token if isinstance(api_token, ApiToken) else api_token

        METHOD = "GET"
        URL = "/api/v2.1/dtable/app-access-token/"

        async with self.session_maker(token=api_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)
            if model:
                results = model(**results)

        return results

    # [BASE-TOKEN] get base token with account token
    async def get_base_token_with_account_token(
        self,
        *,
        base: Base = None,
        workspace_id: str = None,
        base_name: str = None,
        model: BaseModel = BaseToken,
        **params,
    ):
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/access-token/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
            if model:
                results = model(**results, workspace_id=workspace_id, base_name=base_name)

        return results

    # [BASE-TOKEN] get base token with invite link
    async def get_base_token_with_invite_link(self, token: str, model: BaseModel = BaseToken, **params):
        METHOD = "GET"
        URL = "/api/v2.1/dtable/share-link-access-token/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, token=token, **params)
            if model:
                results = model(**results)

        return results

    # [BASE-TOKEN] get base token with external link
    async def get_base_token_with_external_link(
        self, external_link_token: str, model: BaseModel = BaseToken, **params
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/external-link-tokens/{external_link_token}/access-token/"

        async with self.session_maker(token=None) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
            if model:
                results = model(**results)

        return results

    ################################################################
    # SYSTEM ADMIN - USERS
    ################################################################
    # [USERS] list users
    async def list_users(self, per_page: int = 25, model: BaseModel = User, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/users"
        ITEM = "data"
        PARAMS = {"per_page": per_page, **params}

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

            # all pages
            pages = range(2, response["total_count"] + 1, 25)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **PARAMS) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [user for response in responses for user in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # [USERS] list admins
    async def list_admins(self, model: BaseModel = Admin, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/admin-users"
        ITEM = "admin_user_list"

        # admins endpoint has no pagenation
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # [USERS] search users
    async def search_users(self, query: str, model: BaseModel = User, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/search-user"
        ITEM = "user_list"

        # admins endpoint has no pagenation
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, query=query, **params)
        results = response[ITEM]

        # model
        if model:
            results = [model(**x) for x in results]

        return results

    ################################################################
    # SYSTEM ADMIN - BASES
    ################################################################
    # [BASES] list bases
    async def list_bases(self, model: BaseModel = Base, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/dtables"
        ITEM = "dtables"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

            # all pages
            while response["page_info"]["has_next_page"]:
                page = response["page_info"]["current_page"] + 1
                params.update({"page": page})
                response = await self.request(session=session, method="GET", url=URL, **params)
                results += [response[ITEM]]

        # model
        if model:
            results = [model(**x) for x in results]

        return results

    # [BASES] list user's bases
    async def list_user_bases(self, user_id: str, model: BaseModel = Base, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = f"/api/v2.1/admin/users/{user_id}/dtables/"
        ITEM = "dtable_list"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, 25)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **params) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [user for response in responses for user in response[ITEM]]

        # model
        if model:
            results = [model(**x) for x in results]

        return results

    # [BASES] delete base
    async def delete_base(self, base_uuid):
        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/dtable/{base_uuid}"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # [BASES] list trashed bases
    async def list_trashed_bases(self, per_page: int = 25, model: BaseModel = Base, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/trash-dtables"
        ITEM = "trash_dtable_list"
        PARAMS = {"per_page": per_page, **params}

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, per_page)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **PARAMS) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [user for response in responses for user in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # [BASES] (custom) ls
    async def ls(self):
        bases = await self.list_bases()
        bases = [b.to_record() for b in bases]
        print(tabulate(sorted(bases, key=lambda x: int(x["workspace_id"])), **TABULATE_CONF))

    # [BASES] (custom) get base by group name
    async def get_base_by_name(self, group_name: int, base_name: str):
        bases = await self.list_bases()
        for base in bases:
            _base = base.dict() if isinstance(base, BaseModel) else base
            _group_name = _base["owner"].replace(" (group)", "")
            _base_name = _base["name"]
            if _group_name == group_name and _base_name == base_name:
                return base
        else:
            msg = f"group_name '{group_name}', base_name '{base_name}' is not exists!"
            raise KeyError(msg)

    # [BASES] (custom) get base client
    async def get_base_client(self, *, base: Base = None, workspace_id: str = None, base_name: str = None):
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        base_token = await self.get_base_token_with_account_token(
            base=base, workspace_id=workspace_id, base_name=base_name
        )
        client = BaseClient(base_token=base_token)

        return client

    ################################################################
    # SYSTEM ADMIN - TEAMS
    # [NOTE] TEAMS는 Server Version에서 안 되는 것 같다.
    ################################################################
    # [TEAMS] list teams
    async def list_teams(self, role: str = None, per_page: int = 25, model: BaseModel = Team, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/organizations/"
        ITEM = "organizations"
        PARAMS = {"per_page": per_page, "role": role, **params}

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, per_page)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **PARAMS) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [x for response in responses for x in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # [TEAMS] get_organization_names
    # NOT WORKING YET - Server 버전에서는 안 되는 것 같다. ORG가 없어서.
    async def get_organization_names(self, org_ids: List[str]):
        METHOD = "GET"
        URL = "/api/v2.1/admin/organizations-basic-info"
        PARAMS = {"org_ids": org_ids if isinstance(org_ids, list) else [org_ids]}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return response

    # [TEAMS] list team groups
    # NOT WORKING YET - Server 버전에서는 안 되는 것 같다. ORG가 없어서.
    async def list_team_groups(self, org_id: int = -1, model: BaseModel = Team, **params):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/organizations/{org_id}/groups"
        ITEM = "group_list"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

            print(results)

            # all pages
            pages = range(2, response["total_count"] + 1, 25)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **params) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [x for response in responses for x in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    ################################################################
    # SYSTEM ADMIN - DEPARTMENTS
    # [NOTE] Server 버전에서 안 되는 것 같다. ORG가 없어서.
    ################################################################
    # [DEPARTMENTS] list departments
    async def list_departments(self, parent_department_id: int = -1, model: BaseModel = None, **params):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/address-book/groups/{parent_department_id}/"
        ITEM = "group_list"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response

            print(results)

            # all pages
            pages = range(2, response["total_count"] + 1, 25)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **params) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [x for response in responses for x in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # [SYSTEM INFO & CUSTOMIZING] get system information
    async def get_system_info(self):
        METHOD = "GET"
        URL = "/api/v2.1/admin/sysinfo/"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)

        return response
