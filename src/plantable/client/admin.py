import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from ..model import (
    DTABLE_ICON_COLORS,
    DTABLE_ICON_LIST,
    AccountInfo,
    Activity,
    Admin,
    ApiToken,
    Base,
    BaseExternalLink,
    BaseInfo,
    BaseToken,
    Column,
    File,
    Group,
    Table,
    Team,
    User,
    UserInfo,
    ViewExternalLink,
    Webhook,
    Workspace,
)
from .account import AccountClient
from .base import BaseClient

logger = logging.getLogger()


class AdminClient(AccountClient):
    ################################################################
    # USERS (Admin Only)
    ################################################################
    # List Users
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

    # Add New Users
    async def add_user(self):
        raise NotImplementedError

    # Get User
    async def get_user(self, user_email: str, model: BaseModel = User):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/users/{user_email}/"

        # admins endpoint has no pagenation
        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        if model:
            results = model(**results)

        return results

    # Update User
    async def update_user(self):
        raise NotImplementedError

    # Delete User
    async def delete_user(self):
        raise NotImplementedError

    # List Admin Users
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

    # Update Admin's Role
    async def update_admin_role(self):
        raise NotImplementedError

    # Reset User's Password
    async def reset_user_password(self):
        raise NotImplementedError

    # Enforce 2FA
    async def enforce_2fa(self):
        raise NotImplementedError

    # Disable 2FA
    async def disable_2fa(self):
        raise NotImplementedError

    # Search User / Users
    async def search_users(self, query: str, model: BaseModel = User):
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

    # Import Users
    async def import_users(self):
        raise NotImplementedError

    # List User Storage Object
    async def list_user_storage_object(self):
        raise NotImplementedError

    # (CUSTOM) Encode User Emails
    async def encode_user(self, contact_email: str):
        if contact_email.endswith("@auth.local"):
            return contact_email
        users = await self.search_users(query=contact_email)
        if len(users) < 1:
            raise KeyError("{} is not found!".format(contact_email))
        for user in users:
            if user.contact_email == contact_email:
                return user.email
        raise KeyError("{} is not found!".format(contact_email))

    # (CUSTOM) Decode User Emails
    async def decode_user(self, user_email: str):
        if not user_email.endswith("@auth.local"):
            return user_email
        user = await self.get_user(user_email=user_email)
        return user.contact_email

    ################################################################
    # Bases (Admin)
    ################################################################
    # List User's Bases
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

    # List Bases (List All Bases)
    async def list_bases(self, model: BaseModel = Base) -> List[Base]:
        METHOD = "GET"
        URL = "/api/v2.1/admin/dtables/"
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

    # List Bases Shared to User
    async def list_bases_shared_to_user(self, contact_email: str, per_page: int = 25, model: BaseModel = Base):
        # correct args
        contact_email = await self.encode_user(contact_email=contact_email)

        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = f"/api/v2.1/admin/users/{contact_email}/shared-dtables"
        ITEM = "dtable_list"
        PARAMS = {"per_page": per_page}

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

    # (CUSTOM) get base
    async def get_base(self, group_name_or_id: Union[str, int], base_name_or_id: Union[str, int]) -> Base:
        bases = await self.list_group_bases(name_or_id=group_name_or_id)
        for base in bases:
            if base.id == base_name_or_id or base.name == base_name_or_id:
                return base
        else:
            raise KeyError("{}/{} is not exist".format(group_name_or_id, base_name_or_id))

    # (CUSTOM) get base by uuid
    async def get_base_by_uuid(self, base_uuid: str) -> Base:
        bases = await self.list_bases()
        for base in bases:
            if base.uuid == base_uuid:
                return base
        else:
            raise KeyError("base with uuid '{}' is not exist!".format(base_uuid))

    # Delete Base
    async def delete_base(self, base_uuid):
        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/dtable/{base_uuid}"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # list trashed bases
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
    # [BASES] (CUSTOM) ls
    async def ls(self):
        bases = await self.list_bases()
        records = [b.to_record() for b in bases]
        self.print(records=records)

    ################################################################
    # GROUPS (Admin)
    ################################################################
    # List Groups
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

    # Add Group
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

    # (CUSTOM) Get Group
    async def get_group(self, name_or_id: Union[str, int]):
        groups = await self.list_groups()
        for group in groups:
            if isinstance(name_or_id, int) and group.id == name_or_id:
                return group
            if group.name == name_or_id:
                return group
        else:
            raise KeyError

    # (CUSTOM) get base by workspace_id
    async def get_group_by_workspace_id(self, workspace_id: int):
        bases = await self.list_bases()
        for base in bases:
            if base.workspace_id == workspace_id:
                return await self.get_group(name_or_id=base.group_id)
        else:
            raise KeyError("workspace id {} is not exist".format(workspace_id))

    # Transfer Group
    # [NOTE] new_group_name으로 안 바뀌어서 Forum에 문의 중
    async def transfer_group(self, name_or_id: Union[str, int], owner: str = None, name: str = None):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id

        METHOD = "PUT"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/"
        DATA = aiohttp.FormData()
        _ = DATA.add_field("owner", owner) if owner else None
        _ = DATA.add_field("name", name) if name else None

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, data=DATA)

        return results

    # Delete Group
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

    # List Group Bases
    async def list_group_bases(self, name_or_id: Union[str, int], model: BaseModel = Base):
        """
        [NOTE] id input is cheaper. use id when you know it.
        """
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

    # Reorder Your Groups
    async def reorder_group(
        self,
        name_or_id: Union[str, int],
        anchor_group_name_or_id: Union[str, int] = None,
        to_last: bool = False,
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

    # List Group Members
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

    # Add Group Members
    async def add_group_members(
        self,
        name_or_id: Union[str, int],
        user_emails: List[str],
        model: BaseModel = None,
    ):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id
        user_emails = user_emails if isinstance(user_emails, list) else [user_emails]
        user_emails = await asyncio.gather(*[self.encode_user(contact_email=user_email) for user_email in user_emails])

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

    # Remove Group Member
    async def remove_group_member(self, name_or_id: Union[str, int], user_email: List[str]):
        if isinstance(name_or_id, str):
            group = await self.get_group(name_or_id=name_or_id)
            name_or_id = group.id
        user_email = await self.encode_user(contact_email=user_email)

        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/groups/{name_or_id}/members/{user_email}/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    ################################################################
    # SHARING LINKS (Admin)
    ################################################################
    # List External Links (Base Links Only)
    async def list_external_links(
        self,
        per_page: int = 25,
        model: BaseModel = BaseExternalLink,
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/external-links/"
        ITEM = "external_link_list"

        async with self.session_maker(token=self.account_token) as session:
            page = 1
            response = await self.request(session=session, method=METHOD, url=URL, page=page, per_page=per_page)
            results = response[ITEM]

            # all pages
            while response["has_next_page"]:
                page = page + 1
                response = await self.request(session=session, method="GET", url=URL, page=page, per_page=per_page)
                results += [response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # Delete Base External Link
    async def delete_base_external_link(self, external_link_token):
        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/external-links/{external_link_token}/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # List External Links (Base Links Only)
    async def list_view_external_links(
        self,
        per_page: int = 25,
        model: BaseModel = ViewExternalLink,
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/admin/view-external-links/"
        ITEM = "external_link_list"

        async with self.session_maker(token=self.account_token) as session:
            page = 1
            response = await self.request(session=session, method=METHOD, url=URL, page=page, per_page=per_page)
            results = response[ITEM]

            # all pages
            while response["has_next_page"]:
                page = page + 1
                response = await self.request(session=session, method="GET", url=URL, page=page, per_page=per_page)
                results += [response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # Delete View External Link
    async def delete_view_external_link(self, view_external_link_token):
        METHOD = "DELETE"
        URL = f"/api/v2.1/admin/view-external-links/{view_external_link_token}/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # List Base External Links (Base Links and View Links)
    async def list_base_external_links(
        self,
        group_name_or_id: Union[str, int],
        base_name_or_id: Union[str, int],
        base_model: BaseModel = BaseExternalLink,
        view_model: BaseModel = ViewExternalLink,
    ):
        base = await self.get_base(group_name_or_id=group_name_or_id, base_name_or_id=base_name_or_id)

        METHOD = "GET"
        URL = f"/api/v2.1/admin/dtable/{base.id}/external-links/"
        ITEM = "dtable_external_link_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if base_model:
            results["base_external_links"] = [base_model(**x) for x in results["base_external_links"]]
        if view_model:
            results["view_external_links"] = [view_model(**x) for x in results["view_external_links"]]

        return results

    # List Invite Links
    # NOT WORKING (404)
    async def list_invite_links(self, per_page: int = 25, model: BaseModel = None):
        METHOD = "GET"
        URL = "/api/v2.1/admin/invite-links/"
        ITEM = "invite_link_list"

        async with self.session_maker(token=self.account_token) as session:
            page = 1
            response = await self.request(session=session, method=METHOD, url=URL, page=page, per_page=per_page)
            results = response[ITEM]

            # all pages
            while response["has_next_page"]:
                page = page + 1
                response = await self.request(session=session, method="GET", url=URL, page=page, per_page=per_page)
                results += [response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

    # Update Invite Link
    async def update_invite_links(self):
        raise NotImplementedError

    # Delete Invite Link
    async def delete_invite_links(self):
        raise NotImplementedError

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

    ################################################################
    # OVERRIDE ACCOUNT CLIENT
    ################################################################
    async def infer_workspace_id(self, group_name_or_id: Union[str, int]):
        bases = await self.list_group_bases(group_name_or_id)
        workspace_id = None
        for base in bases:
            if workspace_id is None:
                workspace_id = base.workspace_id
            if workspace_id != base.workspace_id:
                raise KeyError("workspace id is not unique!")
        return workspace_id

    async def get_or_create_api_token(
        self,
        group_name_or_id: str,
        base_name: str,
        app_name: str,
        permission: str = "rw",
    ):
        workspace_id = await self.infer_workspace_id(group_name_or_id=group_name_or_id)
        return await super().get_or_create_api_token(
            workspace_id=workspace_id,
            base_name=base_name,
            app_name=app_name,
            permission=permission,
        )

    async def update_api_token(
        self,
        group_name_or_id: str,
        base_name: str,
        app_name: str,
        permission: str = "rw",
    ):
        workspace_id = await self.infer_workspace_id(group_name_or_id=group_name_or_id)
        return await super().update_api_token(
            workspace_id=workspace_id,
            base_name=base_name,
            app_name=app_name,
            permission=permission,
        )

    async def delete_api_token(self, group_name_or_id: str, base_name: str, app_name: str):
        workspace_id = await self.infer_workspace_id(group_name_or_id=group_name_or_id)
        return await super().delete_api_token(workspace_id=workspace_id, base_name=base_name, app_name=app_name)

    async def create_temp_api_token(self, group_name_or_id: str, base_name: str):
        workspace_id = await self.infer_workspace_id(group_name_or_id=group_name_or_id)
        return await super().create_temp_api_token(workspace_id=workspace_id, base_name=base_name)

    async def get_base_token_with_account_token(self, group_name_or_id: Union[str, int], base_name: str):
        workspace_id = await self.infer_workspace_id(group_name_or_id=group_name_or_id)
        base_token = await super().get_base_token_with_account_token(workspace_id=workspace_id, base_name=base_name)

        # add info
        group, base = await asyncio.gather(
            self.get_group(name_or_id=group_name_or_id),
            self.get_base_by_uuid(base_token.dtable_uuid),
        )
        base_token.group_id = group.id
        base_token.group_name = group.name
        if not base_token.app_name:
            base_token.app_name = f"{self.username} (user)"
        if not base_token.workspace_id:
            base_token.workspace_id = base.workspace_id
        base_token.base_name = base.name

        return base_token

    # (CUSTOM)
    async def get_base_client_with_account_token(self, group_name_or_id: Union[str, int], base_name: str):
        base_token = await self.get_base_token_with_account_token(
            group_name_or_id=group_name_or_id, base_name=base_name
        )
        return BaseClient(seatable_url=self.seatable_url, base_token=base_token)

    # (CUSTOM)
    async def get_base_client_with_account_token_by_base_uuid(self, base_uuid: str):
        bases = await self.list_bases()
        for base in bases:
            if base.uuid == base_uuid:
                break
        else:
            _msg = f"base_uuid '{base_uuid}' not found!"
            raise KeyError(_msg)
        base_token = await super().get_base_token_with_account_token(base.workspace_id, base.name)
        return BaseClient(seatable_url=self.seatable_url, base_token=base_token)

    # ensure group member - add me to group member if not
    async def ensure_group_member(self, group_name_or_id: Union[str, int]):
        members = await self.list_group_members(name_or_id=group_name_or_id)
        for member in members:
            if member.contact_email == self.username:
                return
        user_email = await self.encode_user(contact_email=self.username)
        _ = await self.add_group_members(name_or_id=group_name_or_id, user_emails=[user_email])

    ################################################################
    # LOGS
    ################################################################
    # List Email Logs
    async def list_email_logs(self):
        raise NotImplementedError

    # List Registration Logs
    async def list_registration_logs(self):
        METHOD = "GET"
        URL = "/api/v2.1/admin/registration-logs"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response

        return results
