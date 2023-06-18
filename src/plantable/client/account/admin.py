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
from .account import AccountClient

logger = logging.getLogger()


################################################################
# AdminClient
################################################################
class AdminClient(AccountClient):
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
            response = await self.request(
                session=session, method=METHOD, url=URL, **PARAMS
            )
            results = response[ITEM]

            # all pages
            pages = range(2, response["total_count"] + 1, 25)
            coros = [
                self.request(
                    session=session, method=METHOD, url=URL, page=page, **PARAMS
                )
                for page in pages
            ]
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
            response = await self.request(
                session=session, method=METHOD, url=URL, **params
            )
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
            response = await self.request(
                session=session, method=METHOD, url=URL, query=query, **params
            )
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
            response = await self.request(
                session=session, method=METHOD, url=URL, **params
            )
            results = response[ITEM]

            # all pages
            while response["page_info"]["has_next_page"]:
                page = response["page_info"]["current_page"] + 1
                params.update({"page": page})
                response = await self.request(
                    session=session, method="GET", url=URL, **params
                )
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
            response = await self.request(
                session=session, method=METHOD, url=URL, **params
            )
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, 25)
            coros = [
                self.request(
                    session=session, method=METHOD, url=URL, page=page, **params
                )
                for page in pages
            ]
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
    async def list_trashed_bases(
        self, per_page: int = 25, model: BaseModel = Base, **params
    ):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/trash-dtables"
        ITEM = "trash_dtable_list"
        PARAMS = {"per_page": per_page, **params}

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(
                session=session, method=METHOD, url=URL, **PARAMS
            )
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, per_page)
            coros = [
                self.request(
                    session=session, method=METHOD, url=URL, page=page, **PARAMS
                )
                for page in pages
            ]
            responses = await asyncio.gather(*coros)
            results += [user for response in responses for user in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # [BASES] (custom) ls
    async def ls(self):
        bases = await self.list_bases()
        records = [b.to_record() for b in bases]
        self.print(records=records)

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

    ################################################################
    # SYSTEM ADMIN - TEAMS
    # [NOTE] TEAMS는 Server Version에서 안 되는 것 같다.
    ################################################################
    # [TEAMS] list teams
    async def list_teams(
        self, role: str = None, per_page: int = 25, model: BaseModel = Team, **params
    ):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/organizations/"
        ITEM = "organizations"
        PARAMS = {"per_page": per_page, "role": role, **params}

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(
                session=session, method=METHOD, url=URL, **PARAMS
            )
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, per_page)
            coros = [
                self.request(
                    session=session, method=METHOD, url=URL, page=page, **PARAMS
                )
                for page in pages
            ]
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
            response = await self.request(
                session=session, method=METHOD, url=URL, **PARAMS
            )

        return response

    # [TEAMS] list team groups
    # NOT WORKING YET - Server 버전에서는 안 되는 것 같다. ORG가 없어서.
    async def list_team_groups(
        self, org_id: int = -1, model: BaseModel = Team, **params
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/organizations/{org_id}/groups"
        ITEM = "group_list"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(
                session=session, method=METHOD, url=URL, **params
            )
            results = response[ITEM]

            print(results)

            # all pages
            pages = range(2, response["total_count"] + 1, 25)
            coros = [
                self.request(
                    session=session, method=METHOD, url=URL, page=page, **params
                )
                for page in pages
            ]
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
    async def list_departments(
        self, parent_department_id: int = -1, model: BaseModel = None, **params
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/address-book/groups/{parent_department_id}/"
        ITEM = "group_list"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(
                session=session, method=METHOD, url=URL, **params
            )
            results = response

            print(results)

            # all pages
            pages = range(2, response["total_count"] + 1, 25)
            coros = [
                self.request(
                    session=session, method=METHOD, url=URL, page=page, **params
                )
                for page in pages
            ]
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
