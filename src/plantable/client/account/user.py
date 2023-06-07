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
    BaseInfo,
    Workspace,
    File,
)
from .account import AccountClient
from ..base import BaseClient
from ..core import TABULATE_CONF, HttpClient, parse_base

logger = logging.getLogger()


################################################################
# UserClient
################################################################
class UserClient(AccountClient):
    ################################################################
    # USER
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
    # BASE
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
    async def list_favorites(self, model: BaseModel = BaseInfo):
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
            results["personal"] = [model(**x) for x in results["personal"]]
            for group in results["groups"]:
                group["dtables"] = [model(**x) for x in group["dtables"]]

        return results

    # [BASES] (custom) get base by name
    async def get_base(self, workspace_name: str, base_name: str):
        results = await self.list_bases_user_can_admin()

        # personal bases
        if workspace_name == "personal":
            for base in results["personal"]:
                if base.name == base_name:
                    return base

        # group bases
        for group in results["groups"]:
            if group["group_name"] != workspace_name:
                continue
            for base in group["dtables"]:
                if base.name == base_name:
                    return base
        else:
            raise KeyError()

    ################################################################
    # GROUPS & WORKSPACES
    ################################################################
    # [GROUPS & WORKSPACES] list workspaces
    async def list_workspaces(self, detail: bool = True, incl: List[str] = None, model: BaseModel = Workspace) -> dict:
        METHOD = "GET"
        URL = "/api/v2.1/workspaces/"
        ITEM = "workspace_list"
        PARAMS = {"detail": str(detail).lower()}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

        if incl:
            incl = incl if isinstance(incl, list) else [incl]
            results = [x for x in results if x["type"] in incl]

        if model:
            results = [model(**x) for x in results]

        return results

    # [GROUPS & WORKSPACES] get workspace
    async def get_workspace(self, workspace_name: str, workspace_type: str = "group"):
        """
        workspace_type: "group", "personal", "starred", or "shared"
        """
        workspaces = await self.list_workspaces(detail=True, model=Workspace)
        for workspace in workspaces:
            if workspace_type and workspace.type != workspace_type:
                continue
            if workspace.name == workspace_name:
                return workspace
        else:
            raise KeyError()

    ################################################################
    # (CUSTOM) LS
    ################################################################
    # (custom) ls
    async def ls(self, workspace_name: str = None, base_name: str = None):
        # helper
        async def _get_records(workspace_name, base):
            bc = await self.get_base_client_with_account_token(base=base)
            tables = await bc.list_tables()
            return {
                "base_uuid": base.uuid,
                "base": base.name,
                "tables": [x.name for x in tables],
            }

        # ls workspaces
        if not workspace_name:
            workspaces = await self.list_workspaces(detail=True, model=Workspace)
            records = [x.to_record() for x in workspaces]
            self.print(records=records)
            return

        # ls bases
        workspace = await self.get_workspace(workspace_name=workspace_name)
        if not base_name:
            bases = list()
            if workspace.shared_bases:
                for base in workspace.shared_bases:
                    bases.append((workspace.name, base))
            if workspace.bases:
                for base in workspace.bases:
                    bases.append((workspace.name, base))

            records = await asyncio.gather(*[_get_records(w, b) for w, b in bases])
            self.print(records=records)
            return

        # ls tables
        base = await self.get_base(workspace_name=workspace.name, base_name=base_name)
        bc = await self.get_base_client_with_account_token(base=base)
        tables = await bc.list_tables()

        records = [{"table_id": x.id, "table": x.name, "columns": [c.name for c in x.columns]} for x in tables]
        self.print(records=records)

    # [GROUPS & WORKSPACES] search group members
    # NOT WORKING
    async def search_group_members(
        self, group_name: str, query: str = None, model: BaseModel = UserInfo
    ) -> List[UserInfo]:
        group = await self.get_workspace(group_name=group_name)

        METHOD = "GET"
        URL = f"/api/v2.1/groups/{group.id}/search-member/"
        PARAMS = {"q": query}

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        if model:
            results = [model(**x) for x in results]

        return results

    # [GROUPS & WORKSPACES] copy base from workspace
    async def copy_base_from_workspace(
        self,
        src_workspace_id: int,
        src_base_name: str,
        dst_workspace_id: int,
    ) -> dict:
        METHOD = "POST"
        URL = f"/api/v2.1/dtable-copy/"
        JSON = {"src_workspace_id": src_workspace_id, "name": src_base_name, "dst_workspace_id": dst_workspace_id}
        ITEM = "dtable"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    # [GROUPS & WORKSPACES] copy base from external link
    async def copy_base_from_external_link(
        self,
        link: str,
        dst_workspace_id: int,
    ) -> dict:
        METHOD = "POST"
        URL = f"/api/v2.1/dtable-external-link/dtable-copy/"
        JSON = {"link": link, "dst_workspace_id": dst_workspace_id}
        ITEM = "dtable"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    # [GROUPS & WORKSPACES] (custom) copy base between groups
    async def copy_base_between_groups(
        self,
        src_group_name: int,
        src_base_name: str,
        dst_group_name: int,
        model: BaseModel = Base,
    ) -> dict:
        src_group, dst_group = await asyncio.gather(
            self.get_workspace(group_name=src_group_name), self.get_workspace(group_name=dst_group_name)
        )
        results = await self.copy_base_from_workspace(
            src_workspace_id=src_group.id, src_base_name=src_base_name, dst_workspace_id=dst_group.id
        )
        if model:
            results = model(**results)

        return results

    # [GROUPS & WORKSPACES] (custom) copy base between groups
    async def copy_base_from_external_link_to_group(
        self,
        link: str,
        dst_group_name: int,
        model: BaseModel = Base,
    ) -> dict:
        dst_group = await self.get_workspace(group_name=dst_group_name)
        results = await self.copy_base_from_external_link(link=link, dst_workspace_id=dst_group.id)
        if model:
            results = model(**results)

        return results

    ################################################################
    # ATTACHMENT
    ################################################################
    # TBD

    ################################################################
    # IMPORT & EXPORT
    ################################################################
    # [IMPORT & EXPORT] import base from xlsx or csv
    async def import_base_from_xlsx_or_csv(self, workspace_id: int, file: bytes, folder_id: int = None):
        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/synchronous-import/import-excel-csv-to-base/"
        JSON = {"dtable": file, "folder": folder_id}

        raise NotImplementedError

    # [IMPORT & EXPORT] import table from xlsx or csv
    async def import_table_from_xlsx_or_csv(self, workspace_id: int, file: bytes, base_uuid: str, table_name: str):
        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/synchronous-import/import-excel-csv-to-table/"
        JSON = {
            "workspace_id": workspace_id,
            "file": file,
            "dtable_uuid": base_uuid,
            "table_name": table_name,
        }
        raise NotImplementedError

    # [IMPORT & EXPORT] update table from xlsx or csv
    async def update_base_from_xlsx_or_csv(
        self, workspace_id: int, file: bytes, base_uuid: str, table_name: str, selected_columns: List[str]
    ):
        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/synchronous-import/update-table-via-excel-csv/"
        JSON = {
            "workspace_id": workspace_id,
            "file": file,
            "dtable_uuid": base_uuid,
            "table_name": table_name,
            "selected_columns": ",".join(selected_columns) if isinstance(selected_columns, list) else selected_columns,
        }
        raise NotImplementedError

    # [IMPORT & EXPORT] export base
    # NOT WORKING
    async def export_base(self, workspace_name: str, base_name: str):
        workspace = await self.get_workspace(workspace_name=workspace_name)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-export/export-dtable/"
        PARAMS = {"dtable_name": base_name}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return response

    # [IMPORT & EXPORT] export table
    async def export_table(
        self,
        workspace_name: str,
        base_name: str,
        table_id: int,
        table_name: str,
    ):
        workspace = await self.get_workspace(workspace_name=workspace_name)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-export/export-table-to-excel/"
        PARAMS = {"dtable_name": base_name, "table_id": table_id, "table_name": table_name}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return response

    # [IMPORT & EXPORT] (custom) export_table
    async def export_table_by_name(self, group_name: str, base_name: str, table_name: str):
        pass

    ################################################################
    # SHARING
    ################################################################
    # TBD

    ################################################################
    # SHARING LINKS
    ################################################################
    # TBD

    ################################################################
    # COMMON DATASET
    ################################################################
    # TBD

    ################################################################
    # DEPARTMENTS
    ################################################################
    # TBD

    ################################################################
    # FORMS
    ################################################################
    # TBD

    ################################################################
    # AUTOMATIONS
    ################################################################
    # TBD

    ################################################################
    # NOTIFICATIONS
    ################################################################
    # TBD

    ################################################################
    # SYSTEM NOTIFICATIONS
    ################################################################
    # TBD

    ################################################################
    # E-MAIL ACCOUNTS
    ################################################################
    # TBD

    ################################################################
    # WEBHOOKS
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

    ################################################################
    # SNAPSHOTS
    ################################################################
    # TBD
