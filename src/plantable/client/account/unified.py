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
    BaseInfo,
    BaseToken,
    Column,
    File,
    Table,
    Team,
    User,
    Group,
    UserInfo,
    Webhook,
    Workspace,
)
from ..base import BaseClient
from ..core import TABULATE_CONF, HttpClient
from .account import AccountClient

logger = logging.getLogger()


################################################################
# UnifiedClient = AdminClient + UserClient
################################################################
class UnifiedClient(AccountClient):
    ################################################################
    # Account (Me who logged in)
    ################################################################
    # (User) get account info
    async def get_account_info(self, model: BaseModel = AccountInfo):
        METHOD = "GET"
        URL = "/api2/account/info/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        if model:
            results = model(**results)

        return results

    # (User) update email address
    async def update_email_address(self):
        raise NotImplementedError

    # (User) upload/update user avatar
    async def update_user_avartar(self):
        raise NotImplementedError

    # (User) get public user info
    async def get_public_user_info(self, user_id: str, model: BaseModel = UserInfo):
        METHOD = "GET"
        URL = f"/api/v2.1/user-common-info/{user_id}/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        if model:
            results = model(**results)

        return results

    # (User) list public user info
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
    # USERS (Admin Only)
    ################################################################
    # (Admin) List Users
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

    # (Admin) Add New Users
    async def add_user(self):
        raise NotImplementedError

    # (Admin) Get User
    async def get_user(self, user_email: str, model: BaseModel = User):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/users/{user_email}/"

        # admins endpoint has no pagenation
        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        if model:
            results = model(**results)

        return results

    # (Admin) Update User
    async def update_user(self):
        raise NotImplementedError

    # (Admin) Delete User
    async def delete_user(self):
        raise NotImplementedError

    # (Admin) List Admin Users
    async def list_admin_users(self, model: BaseModel = Admin, **params):
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

    # (Admin) Update Admin's Role
    async def update_admin_role(self):
        raise NotImplementedError

    # (Admin) Reset User's Password
    async def reset_user_password(self):
        raise NotImplementedError

    # (Admin) Enforce 2FA
    async def enforce_2fa(self):
        raise NotImplementedError

    # (Admin) Disable 2FA
    async def disable_2fa(self):
        raise NotImplementedError

    # (Admin) Search User / Users
    async def search_user(self, query: str, model: BaseModel = User):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/search-user"
        ITEM = "user_list"

        # admins endpoint has no pagenation
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, query=query)
        results = response[ITEM]

        # model
        if model:
            results = [model(**x) for x in results]

        return results

    # (Admin) Import Users
    async def import_users(self):
        raise NotImplementedError

    # (Admin) List Bases Shared to User
    async def list_bases_shared_to_user(self):
        raise NotImplementedError

    # (Admin) List User Storage Object
    async def list_user_storage_object(self):
        raise NotImplementedError

    # (Custom) Decode User Emails
    async def decode_user(self, user_email):
        if user_email.endswith("@auth.local"):
            return user_email
        user = await self.search_user(query=user_email)
        if len(user) < 1:
            raise KeyError("{} is not found!".format(user_email))
        if len(user) > 1:
            raise KeyError("Multiple users are found!")
        return user[0].email

    # (Custom) Encode User Emails
    async def encode_user(self, user_email):
        if not user_email.endswith("@auth.local"):
            return user_email
        user = await self.get_user(user_email=user_email)
        return user.email

    ################################################################
    # Bases (Admin, User)
    ################################################################
    # (Admin) List User's Bases
    async def list_user_bases(self, user_id: str, model: BaseModel = Base):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = f"/api/v2.1/admin/users/{user_id}/dtables/"
        ITEM = "dtable_list"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

            # all pages
            pages = range(2, response["count"] + 1, 25)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [user for response in responses for user in response[ITEM]]

        # model
        if model:
            results = [model(**x) for x in results]

        return results

    # (Admin) List All Bases
    async def list_all_bases(self, model: BaseModel = Base):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/dtables"
        ITEM = "dtables"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

            # all pages
            while response["page_info"]["has_next_page"]:
                page = response["page_info"]["current_page"] + 1
                response = await self.request(session=session, method="GET", url=URL, page=page)
                results += [response[ITEM]]

        # model
        if model:
            results = [model(**x) for x in results]

        return results

    # (User) Create Base
    async def create_base(
        self, workspace_name: str, base_name: str, icon: str = None, color: str = None, model: BaseModel = Base
    ):
        workspace = await self.get_workspace(workspace_name=workspace_name)

        METHOD = "POST"
        URL = f"/api/v2.1/dtables/"
        ITEM = "table"
        DATA = aiohttp.FormData([("workspace_id", workspace.id), ("name", base_name)])
        _ = DATA.add_field("icon", icon) if icon else None
        _ = DATA.add_field("color", color) if color else None

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, data=DATA)
            results = response[ITEM]

        if model:
            results = model(**results)

        return results

    # (User) Update Base
    async def update_base(
        self,
        workspace_name: str,
        base_name: str,
        new_base_name: str = None,
        new_icon: str = None,
        new_color: str = None,
        model: BaseModel = Base,
    ):
        workspace = await self.get_workspace(workspace_name=workspace_name)

        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/"
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

    # (Admin) Delete Base
    async def delete_base(self, base_uuid):
        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/dtable/{base_uuid}"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # NOT WORKING
    async def delete_base_as_user(self, workspace_name: str):
        workspace = await self.get_workspace(workspace_name)

        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # (admin) list trashed bases
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

    ################################################################
    # CUSTOM
    ################################################################
    # [BASES] (custom) ls
    async def ls(self):
        bases = await self.list_bases()
        records = [b.to_record() for b in bases]
        self.print(records=records)

    # [BASES] (custom) get base by group name
    async def get_base(self, workspace_name_or_id: Union[str, int], base_name: str):
        bases = await self.list_all_bases()
        for base in bases:
            _base = base.dict() if isinstance(base, BaseModel) else base
            _workspace_name_or_id = _base["owner"].replace(" (group)", "")
            _base_name = _base["name"]
            if _workspace_name_or_id == workspace_name_or_id and _base_name == base_name:
                return base
        else:
            msg = f"workspace_name_or_id '{workspace_name_or_id}', base_name '{base_name}' is not exists!"
            raise KeyError(msg)

    ################################################################
    # GROUPS (Admin)
    ################################################################
    # (Admin) List Groups
    async def list_groups(self, model: BaseModel = Group):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/groups/"
        ITEM = "groups"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, page=1)
            results = response[ITEM]

            # all pages
            while response["page_info"]["has_next_page"]:
                page = response["page_info"]["current_page"] + 1
                response = await self.request(session=session, method="GET", url=URL, page=page)
                results += [response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # (Admin) Add Group
    async def add_group(self, group_name: str, group_owner: str = None, model: BaseModel = None):
        METHOD = "POST"
        URL = "/api/v2.1/admin/groups/"
        DATA = aiohttp.FormData()
        DATA.add_field("group_name", group_name)
        if not group_owner:
            me = await self.get_account_info()
            group_owner = me.email
        DATA.add_field("group_owner", group_owner)

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, data=DATA)

        if model:
            results = model(**results)

        return results

    # (CUSTOM) (Admin) Get Group
    async def get_group(self, name_or_id: Union[str, int]):
        groups = await self.list_groups()
        for group in groups:
            if isinstance(name_or_id, int) and group.id == name_or_id:
                return group
            if group.name == name_or_id:
                return group
        else:
            raise KeyError

    # (Admin) Update Group
    # [NOTE] new_group_name으로 안 바뀌어서 Forum에 문의 중
    async def update_group(self, name_or_id: Union[str, int], new_owner: str = None, new_group_name: str = None):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id

        METHOD = "PUT"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/"
        DATA = aiohttp.FormData()
        _ = DATA.add_field("new_owner", new_owner) if new_owner else None
        _ = DATA.add_field("new_group_name", new_group_name) if new_group_name else None

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, data=DATA)

        return results

    # (Admin) Delete Group
    async def delete_group(self, name_or_id: Union[str, int]):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id

        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # (Admin) List Group Bases
    async def list_group_bases(self, name_or_id: Union[str, int], model: BaseModel = Base):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id

        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/dtables/"
        ITEM = "tables"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # (Admin) Reorder Your Groups
    async def reorder_group(
        self, name_or_id: Union[str, int], anchor_group_name_or_id: Union[str, int] = None, to_last: bool = False
    ):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id
        if isinstance(anchor_group_name_or_id, str):
            anchor_group = await self.get_group(name_or_id=anchor_group_name_or_id)
            anchor_group_name_or_id = anchor_group.id

        METHOD = "PUT"
        URL = "/api/v2.1/groups/move-group/"
        DATA = aiohttp.FormData()
        _ = DATA.add_field("group_id", name_or_id)
        _ = DATA.add_field("anchor_group_id", anchor_group_name_or_id) if anchor_group_name_or_id else None
        _ = DATA.add_field("to_last", to_last)

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, data=DATA)

        return results

    # (Admin) List Group Members
    async def list_group_members(self, name_or_id: Union[str, int], model: BaseModel = UserInfo):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id

        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/members/"
        ITEM = "members"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # (Admin) Add Group Members
    async def add_group_member(self, name_or_id: Union[str, int], user_emails: List[str], model: BaseModel = None):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id

        user_emails = user_emails if isinstance(user_emails, list) else [user_emails]
        user_emails = await asyncio.gather(*[self.decode_user(user_email=user_email) for user_email in user_emails])

        METHOD = "POST"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/members/"
        DATA = aiohttp.FormData()
        for user_email in user_emails:
            DATA.add_field("email", user_email)

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, data=DATA)

        if model:
            results = model(**results)

        return results

    ################################################################
    # DEPARTMENTS (Admin)
    ################################################################
    # List Department
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
