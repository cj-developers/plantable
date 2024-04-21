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
    BaseInfo,
    BaseToken,
    Column,
    File,
    SharedView,
    Table,
    Team,
    User,
    UserInfo,
    Webhook,
    Workspace,
)
from .account import AccountClient
from .core import parse_name

logger = logging.getLogger()


################################################################
# UserClient
################################################################
class UserClient(AccountClient):
    ################################################################
    # Helper
    ################################################################
    async def get_base_client_with_account_token(
        self, workspace_name_or_id: Union[str, int] = None, base_name: str = None
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)
        return await super().get_base_client_with_account_token(workspace_id=workspace.id, base_name=base_name)

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
    # [BASES] list bases user can admin
    async def list_bases(self, model: BaseModel = Base):
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

    # [BASES] Create Base
    async def create_base(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        icon: str = None,
        color: str = None,
        model: BaseModel = Base,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

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

    # [BASES] update base
    async def update_base(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        new_base_name: str = None,
        new_icon: str = None,
        new_color: str = None,
        model: BaseModel = Base,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

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

    # [BASES] delete base
    # NOT WORKING
    async def delete_base(self, workspace_name_or_id: Union[str, int]):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/"
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

    # [BASES] (custom) get base by name
    async def get_base(self, workspace_name: str, base_name: str):
        results = await self.list_bases()

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
        """
        incl: "personal" or "group"
        """
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
            # [NOTE]
            # shared는 데이터 형식이 다르기 때문에 별도 method 사용, 여기서는 ignore
            results = [x for x in results if x["type"] != "shared"]
            results = [model(**x) for x in results]

        return results

    # [GROUPS & WORKSPACES] get workspace
    async def get_workspace(self, name_or_id: Union[str, int], workspace_type: str = "group"):
        """
        workspace_type: "group", "personal", "starred", or "shared"
        """
        workspaces = await self.list_workspaces(detail=True, model=Workspace)
        for workspace in workspaces:
            if workspace_type and workspace.type != workspace_type:
                continue
            if isinstance(name_or_id, str) and workspace.name == name_or_id:
                return workspace
            if isinstance(name_or_id, int) and workspace.id == name_or_id:
                return workspace
        else:
            raise KeyError()

    ################################################################
    # (CUSTOM) LS & GET
    ################################################################
    # (custom) get
    async def get(self, workspace_name_or_id: str, base_name: str = None, table_name: str = None):
        workspace_name_or_id, base_name, table_name = parse_name(workspace_name_or_id, base_name, table_name)

        if table_name:
            bc = await self.get_base_client_with_account_token(
                workspace_name_or_id=workspace_name_or_id, base_name=base_name
            )
            return await bc.get_table(table_name=table_name)
        if base_name:
            return await self.get_base(workspace_name_or_id=workspace_name_or_id, base_name=base_name)
        return await self.get_workspace(name_or_id=workspace_name_or_id)

    # (custom) ls
    async def ls(
        self,
        workspace_name_or_id: str = None,
        base_name: str = None,
        table_name: str = None,
    ):
        # coros
        async def _get_records(workspace_name_or_id, base_name):
            bc = await self.get_base_client_with_account_token(
                workspace_name_or_id=workspace_name_or_id, base_name=base_name
            )
            tables = await bc.list_tables()
            return {
                "base_uuid": base.uuid,
                "base": base_name,
                "tables": [x.name for x in tables],
            }

        # ls workspaces
        if not workspace_name_or_id:
            workspaces = await self.list_workspaces(detail=True, model=Workspace)
            records = [x.to_record() for x in workspaces]
            self.print(records=records)
            return

        # ls bases
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)
        if not base_name:
            list_bases = list()
            if workspace.shared_bases:
                for base in workspace.shared_bases:
                    list_bases.append((workspace.name, base.name))
            if workspace.bases:
                for base in workspace.bases:
                    list_bases.append((workspace.name, base.name))

            records = await asyncio.gather(*[_get_records(w, b) for w, b in list_bases])
            self.print(records=records)
            return

        # ls tables
        bc = await self.get_base_client_with_account_token(
            workspace_name_or_id=workspace_name_or_id, base_name=base_name
        )
        await bc.ls(table_name=table_name)

    # [GROUPS & WORKSPACES] copy base from workspace
    async def copy_base_from_workspace(
        self,
        src_workspace_name_or_id: str,
        src_base_name: str,
        dst_workspace_name_or_id: str,
    ) -> dict:
        src_workspace = await self.get_workspace(workspace_name_or_id=src_workspace_name_or_id)
        dst_workspace = await self.get_workspace(workspace_name_or_id=dst_workspace_name_or_id)

        METHOD = "POST"
        URL = f"/api/v2.1/dtable-copy/"
        JSON = {
            "src_workspace_id": src_workspace.id,
            "name": src_base_name,
            "dst_workspace_id": dst_workspace.id,
        }
        ITEM = "dtable"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    # [GROUPS & WORKSPACES] copy base from external link
    async def copy_base_from_external_link(
        self,
        link: str,
        dst_workspace_name: str,
    ) -> dict:
        dst_workspace = await self.get_workspace(workspace_name=dst_workspace_name)

        METHOD = "POST"
        URL = f"/api/v2.1/dtable-external-link/dtable-copy/"
        JSON = {"link": link, "dst_workspace_id": dst_workspace.id}
        ITEM = "dtable"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    ################################################################
    # ATTACHMENT
    ################################################################
    # TBD

    ################################################################
    # IMPORT & EXPORT
    ################################################################
    # (CUSTOM) Get Table Name and View Name from Table ID and View ID - View ID is Optional
    async def get_ids_by_names(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        table_name: str,
        view_name: str = None,
    ):
        bc = await self.get_base_client_with_account_token(
            workspace_name_or_id=workspace_name_or_id, base_name=base_name
        )
        tables = await bc.list_tables()

        for table in tables:
            if table.name == table_name:
                break
        else:
            KeyError
        if view_name is None:
            return table.id

        for view in table.views:
            if view.name == view_name:
                return table.id, view.id
        else:
            KeyError

    # Import Base from xlsx or csv
    async def import_base_from_xlsx_or_csv(
        self, workspace_name_or_id: Union[str, int], file: bytes, folder_id: int = None
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-import/import-excel-csv-to-base/"
        JSON = {"dtable": file, "folder": folder_id}

        raise NotImplementedError

    # Import Table from xlsx or csv
    async def import_table_from_xlsx_or_csv(
        self,
        workspace_name_or_id: Union[str, int],
        file: bytes,
        base_uuid: str,
        table_name: str,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-import/import-excel-csv-to-table/"
        JSON = {
            "workspace_id": workspace.id,
            "file": file,
            "dtable_uuid": base_uuid,
            "table_name": table_name,
        }
        raise NotImplementedError

    # Update Table from xlsx or csv
    async def update_base_from_xlsx_or_csv(
        self,
        workspace_name_or_id: Union[str, int],
        file: bytes,
        base_uuid: str,
        table_name: str,
        selected_columns: List[str],
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-import/update-table-via-excel-csv/"
        JSON = {
            "workspace_id": workspace.id,
            "file": file,
            "dtable_uuid": base_uuid,
            "table_name": table_name,
            "selected_columns": ",".join(selected_columns) if isinstance(selected_columns, list) else selected_columns,
        }
        raise NotImplementedError

    # Export Base
    # NOT WORKING
    async def export_base(self, workspace_name_or_id: Union[str, int], base_name: str):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-export/export-dtable/"
        PARAMS = {"dtable_name": base_name}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return response

    # Export Table
    async def export_table(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        table_id: int = None,
        table_name: str = None,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-export/export-table-to-excel/"
        PARAMS = {
            "dtable_name": base_name,
            "table_id": table_id,
            "table_name": table_name,
        }

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return response

    # (CUSTOM) Export Table by Name
    async def export_table_by_name(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        table_name: str = None,
    ):
        table_id = await self.get_ids_by_names(
            workspace_name_or_id=workspace_name_or_id,
            base_name=base_name,
            table_name=table_name,
        )
        return await self.export_table(
            workspace_name_or_id=workspace_name_or_id,
            base_name=base_name,
            table_id=table_id,
            table_name=table_name,
        )

    # Export View
    async def export_view(
        self,
        workspace_name_or_id: int,
        base_name: str,
        table_id: str,
        table_name: str,
        view_id: str,
        view_name: str,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/synchronous-export/export-view-to-excel/"
        PARAMS = {
            "dtable_name": base_name,
            "table_id": table_id,
            "table_name": table_name,
            "view_id": view_id,
            "view_name": view_name,
        }

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return response

    # (CUSTOM) Export View by Name
    async def export_view_by_name(self, workspace_name_or_id: int, base_name: str, table_name: str, view_name: str):
        table_id, view_id = await self.get_ids_by_names(
            workspace_name_or_id=workspace_name_or_id,
            base_name=base_name,
            table_name=table_name,
            view_name=view_name,
        )

        return await self.export_view(
            workspace_name_or_id=workspace_name_or_id,
            base_name=base_name,
            table_id=table_id,
            table_name=table_name,
            view_id=view_id,
            view_name=view_name,
        )

    ################################################################
    # SHARING
    ################################################################
    # My User View Shares
    async def list_shared_views(self, model: BaseModel = SharedView):
        METHOD = "GET"
        URL = "/api/v2.1/dtables/view-shares-user-shared/"
        ITEM = "view_share_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # List User Shares
    async def list_users_share_base(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        model: BaseModel = UserInfo,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/{base_name}/share/"
        ITEM = "user_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # Create User Share
    async def create_user_share(self):
        raise NotImplementedError

    # Update User Share
    async def update_user_share(self):
        raise NotImplementedError

    # Delete User Share
    async def delete_user_share(self):
        raise NotImplementedError

    # My Group Shares
    async def list_tables_shared_to_my_groups(self) -> List[dict]:
        METHOD = "GET"
        URL = "/api/v2.1/dtables/group-shared/"
        ITEM = "group_shared_dtables"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # List Group Shares
    async def list_tables_shared_to_group(self, workspace_name_or_id: Union[str, int], base_name: str):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/{base_name}/group-shares/"
        ITEM = "dtable_group_share_list"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # Create Group Share
    async def create_group_share(self):
        raise NotImplementedError

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
    async def list_webhooks(
        self,
        workspace_name_or_id: Union[str, int],
        base_name: str,
        model: BaseModel = Webhook,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/{base_name}/webhooks/"
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
        workspace_name_or_id: Union[str, int],
        base_name: str,
        url: str,
        secret: int = 0,
        model: BaseModel = Webhook,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/{base_name}/webhooks/"
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
        workspace_name_or_id: Union[str, int],
        base_name: str,
        webhook_id: str,
        url: str,
        secret: int = None,
    ):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/{base_name}/webhooks/{webhook_id}/"
        JSON = {"url": url, "secret": str(secret)}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            print(response)

    # [WEBHOOKS] delete webhook
    async def delete_webhook(self, workspace_name_or_id: Union[str, int], base_name: str, webhook_id: str):
        workspace = await self.get_workspace(name_or_id=workspace_name_or_id)

        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace.id}/dtable/{base_name}/webhooks/{webhook_id}/"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    ################################################################
    # SNAPSHOTS
    ################################################################
    # TBD
