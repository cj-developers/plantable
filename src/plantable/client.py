import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from .conf import SEATABLE_ACCOUNT_TOKEN, SEATABLE_API_TOKEN, SEATABLE_BASE_TOKEN, SEATABLE_URL
from .model.account import Admin, ApiToken, Base, BaseToken, Team, User, Webhook
from .model.base import Table, Column

logger = logging.getLogger()
TABULATE_CONF = {"tablefmt": "psql", "headers": "keys"}


def parse_base(base: Base = None):
    return base.workspace_id, base.name


################################################################
# HttpClient
################################################################
class HttpClient:
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
    ):
        self.seatable_url = seatable_url
        self.headers = {"accept": "application/json"}

    async def info(self):
        return await self.request(method="GET", url="/server-info/")

    async def ping(self):
        return await self.request(method="GET", url="/api2/ping/")

    def session_maker(self, token: str = None):
        headers = self.headers.copy()
        if token:
            headers.update({"authorization": "Token {}".format(token)})
        return aiohttp.ClientSession(base_url=self.seatable_url, headers=headers)

    async def request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        json: str = None,
        data: bytes = None,
        **params,
    ):
        # Wrong content-type from
        print("HI")
        async with session.request(method=method, url=url, json=json, data=data, params=params) as response:
            response.raise_for_status()
            try:
                if response.content_type in ["application/json"]:
                    return await response.json()
                if response.content_type in ["text/html"]:
                    print(f"! content-type: {response.content_type}")
                    body = await response.text()
                    return orjson.loads(body)
            except Exception as ex:
                raise ex


################################################################
# AccountClient
################################################################
class AccountClient(HttpClient):
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
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

        return results

    # [BASE-TOKEN] get base token with api token
    # NOT WORKING YET
    async def get_base_token_with_api_token(self, *, api_token: Union[ApiToken, str], model: BaseModel = BaseToken):
        api_token = api_token.api_token if isinstance(api_token, ApiToken) else api_token

        METHOD = "GET"
        URL = "/api/v2.1/dtable/app-access-token/"

        async with self.session_maker(token=api_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
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
    # ACCOUNT OPERATIONS: SYSTEM ADMIN
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

    # [BASES] (custom) ls
    async def ls(self):
        bases = await self.list_bases()
        bases = [b.view() for b in bases]
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

    # [BASES] (custom) generate base client
    async def generate_base_client(self, *, base: Base = None, workspace_id: str = None, base_name: str = None):
        if base:
            workspace_id, base_name = parse_base(base=base)
        if not workspace_id or not base_name:
            raise KeyError()

        base_token = await self.get_base_token_with_account_token(
            base=base, workspace_id=workspace_id, base_name=base_name
        )
        client = BaseClient(base_token=base_token)

        return client

    # [TEAMS] list teams
    async def list_teams(self, role: str = None, per_page: int = 25, model: BaseModel = Team, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/organizations/"
        ITEM = "organizations"
        PARAMS = {k: v for k, v in {"per_page": per_page, "role": role, **params}.items() if v}

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

    # [DEPARTMENTS] list departments
    # NOT WORKING YET - Server 버전에서는 안 되는 것 같다. ORG가 없어서.
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
    # ACCOUNT OPERATIONS: TEAMS
    ################################################################
    # [GROUPS] list groups
    async def list_groups(self, org_id: int = 1, per_page: int = 25):
        METHOD = "GET"
        URL = f"/api/v2.1/org/{org_id}/admin/groups/"
        ITEM = "groups"
        PARAMS = {"per_page": per_page}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

        return results

    ################################################################
    # ACCOUNT OPERATIONS: USERS
    ################################################################

    # [GROUPS & WORKSPACES] list workspaces
    async def list_workspaces(self, detail: bool = True):
        METHOD = "GET"
        URL = "/api/v2.1/workspaces/"
        ITEM = "workspace_list"
        PARAMS = {k: v for k, v in {"detail": str(detail).lower()}.items() if v}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

        return results

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
# BaseClient
################################################################
class BaseClient(HttpClient):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        base_token: str = Union[BaseToken, str],
        base_uuid: str = None,
    ):
        super().__init__(seatable_url=seatable_url)
        if not isinstance(base_token, BaseToken):
            self.base_token = base_token
            self.base_uuid = base_uuid
        else:
            self.base_token = base_token.access_token
            self.base_uuid = base_token.dtable_uuid

    ################################################################
    # BASE OPERATIONS
    ################################################################
    # [BASE INFO] get base info
    async def get_base_info(self):
        METHOD = "GET"
        URL = f"/dtable-server/dtables/{self.base_uuid}"

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

    # [BASE INFO] get metadata
    async def get_metadata(self):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/metadata/"
        ITEM = "metadata"

        async with self.session_maker(token=self.base_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        return results

    # [BASE INFO] list tables
    async def list_tables(self, model: BaseModel = Table):
        metadata = await self.get_metadata()
        tables = metadata["tables"]

        if model:
            tables = [model(**x) for x in tables]

        return tables

    # [BASE INFO] ls
    async def ls(self, table_name: str = None):
        metadata = await self.get_metadata()
        tables = metadata["tables"]
        if table_name:
            for table in tables:
                if table["name"] == table_name:
                    break
            else:
                raise KeyError()
            columns = [{"key": c["key"], "name": c["name"], "type": c["type"]} for c in table["columns"]]
            print(tabulate(columns, **TABULATE_CONF))
            return
        _tables = list()
        for table in tables:
            _n = len(table["columns"])
            _columns = ", ".join(c["name"] for c in table["columns"])
            if len(_columns) > 50:
                _columns = _columns[:50] + "..."
            _columns += f" ({_n})"
            _tables += [
                {
                    "id": table["_id"],
                    "name": table["name"],
                    "views": ", ".join([v["name"] for v in table["views"]]),
                    "columns": _columns,
                },
            ]
        print(tabulate(_tables, **TABULATE_CONF))

    async def get_bigdata_status(self):
        raise NotImplementedError

    async def list_collaborators(self):
        # base_uuid = dtable_uuid
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/related-users/"

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

    # ROW
    async def query(self, sql: str, convert_keys: bool = True):
        # MAX 10,000 Rows - [TODO] Check length
        METHOD = "POST"
        URL = f"/dtable-db/api/v1/query/{self.base_uuid}/"
        JSON = {"sql": sql, "convert_keys": convert_keys}
        SUCCESS = "success"
        ITEM = "results"

        async with self.session_maker(token=self.base_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            if not response[SUCCESS]:
                raise Exception(response)
            results = response[ITEM]

        if len(results) == 10000:
            logger.warning("Only 10,000 rows are returned!")

        return results

    async def list_rows(
        self,
        table_name: str,
        view_name: str,
        convert_link_id: bool = False,
        order_by: str = None,
        direction: str = "asc",
        start: int = 0,
        limit: int = None,
    ):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        ITEM = "rows"

        get_all_rows = False
        if not limit:
            get_all_rows = True
            limit = limit or 1000

        params = {
            "table_name": table_name,
            "view_name": view_name,
            "convert_link_id": str(convert_link_id).lower(),
            "order_by": order_by,
            "direction": direction,
            "start": start,
            "limit": limit,
        }
        params = {k: v for k, v in params.items() if v is not None}

        async with self.session_maker(token=self.base_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            response = response[ITEM]
            results = response

            # pagination
            if get_all_rows:
                while len(response) == limit:
                    params.update({"start": params["start"] + 1})
                    response = await self.request(session=session, method=METHOD, url=URL, **params)
                    response = response[ITEM]
                    results += response

        return results

    async def add_row(
        self,
        table_name: str,
        row: dict,
        anchor_row_id: str = None,
        row_insert_position: str = "insert_below",
    ):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        JSON = {"table_name": table_name, "row": row}
        if anchor_row_id:
            JSON.update(
                {
                    "ahchor_row_id": anchor_row_id,
                    "row_insert_position": row_insert_position,
                }
            )

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def update_row(self, table_name: str, row_id: str, row: dict):
        # NOT WORKING
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        JSON = {"table_name": table_name, "row_id": row_id, "row": row}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def delete_row(self, table_name: str, row_id: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        JSON = {"table_name": table_name, "row_id": row_id}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def get_row(self, table_name: str, row_id: str, convert: bool = False):
        # NOT WORKING
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/{row_id}/"

        params = {"table_name": table_name, "convert": str(convert).lower()}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)

        return results

    async def append_rows(self, table_name: str, rows: List[dict]):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/batch-append-rows/"
        JSON = {"table_name": table_name, "rows": rows}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def update_rows(self, table_name: str, updates: List[dict]):
        # updates = [{"row_id": xxx, "row": {"key": "value"}}, ...]
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/batch-update-rows/"
        JSON = {"table_name": table_name, "updates": updates}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def delete_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/batch-delete-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def lock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/lock-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def unlock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/unlock-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # LINKS

    # FILES & IMAGES

    # TABLES
    async def create_new_table(self, table_name: str, columns: List[dict]):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/"
        JSON = {"table_name": table_name, "columns": columns}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def rename_table(self, table_name: str, new_table_name: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/"
        JSON = {"table_name": table_name, "new_table_name": new_table_name}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def delete_table(self, table_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/"
        JSON = {"table_name": table_name}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    async def duplicate_table(self, table_name: str, is_duplicate_records: bool = True):
        # rename table in a second step
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/duplicate-table/"
        JSON = {"table_name": table_name, "is_duplicate_records": is_duplicate_records}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Views

    # Columns

    # Big Data

    # Row Comments

    # Notifications

    # Activities & Logs

    # Snapshots
