import asyncio
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from .conf import SEATABLE_ACCOUNT_TOKEN, SEATABLE_API_TOKEN, SEATABLE_BASE_TOKEN, SEATABLE_URL
from .model.account import Admin, ApiToken, BaseToken, Dtable, User

PAGE_INFO = "page_info"


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
    async def list_api_tokens(self, workspace_id: str, base_name: str, model: BaseModel = ApiToken, **params):
        # workpace id = group id
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        ITEM = "api_tokens"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    async def create_api_token(
        self,
        workspace_id: str,
        base_name: str,
        app_name: str,
        permission: str = "r",
        model: BaseModel = ApiToken,
        **params,
    ):
        # bad request return if app_name is already exists
        METHOD = "POST"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/"
        JSON = {"app_name": app_name, "permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON, **params)
            results = model(**response) if model else response

        return results

    async def create_temp_api_token(
        self,
        workspace_id: str,
        base_name: str,
        **params,
    ):
        # bad request return if app_name is already exists
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/temp-api-token/"
        ITEM = "api_token"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

        return results

    async def update_api_token(
        self,
        workspace_id: str,
        base_name: str,
        app_name: str,
        permission: str = "r",
        model: BaseModel = ApiToken,
        **params,
    ):
        # bad request return if app_name is already exists
        METHOD = "PUT"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        JSON = {"permission": permission}

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON, **params)
            results = model(**response) if model else response

        return results

    async def delete_api_token(self, workspace_id: str, base_name: str, app_name: str, **params):
        # return example {"success": True}
        METHOD = "DELETE"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/api-tokens/{app_name}"
        ITEM = "success"

        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

        return results

    async def get_base_token_with_account_token(
        self, workspace_id: str, base_name: str, model: BaseModel = BaseToken, **params
    ):
        METHOD = "GET"
        URL = f"/api/v2.1/workspace/{workspace_id}/dtable/{base_name}/access-token/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
            if model:
                results = model(**results, workspace_id=workspace_id, base_name=base_name)

        return results

    async def get_base_token_with_api_token(self, api_token: str, model: BaseModel = BaseToken, **params):
        METHOD = "GET"
        URL = "/api/v2.1/dtable/app-access-token/"

        async with self.session_maker(token=api_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
            if model:
                results = model(**results)

        return results

    async def get_base_token_with_invite_link(self, token: str, model: BaseModel = BaseToken, **params):
        METHOD = "GET"
        URL = "/api/v2.1/dtable/share-link-access-token/"

        async with self.session_maker(token=self.account_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, token=token, **params)
            if model:
                results = model(**results)

        return results

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
    async def list_users(self, model: BaseModel = User, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/users"
        ITEM = "data"

        # 1st page
        async with self.session_maker(token=self.account_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]

            # all pages
            pages = range(2, response["total_count"] + 1, 1)
            coros = [self.request(session=session, method=METHOD, url=URL, page=page, **params) for page in pages]
            responses = await asyncio.gather(*coros)
            results += [user for response in responses for user in response[ITEM]]

        if model:
            results = [model(**x) for x in results]

        return results

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

    async def list_bases(self, model=Dtable, **params):
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
    # BASE INFO
    async def get_base_info(self):
        # base_uuid = dtable_uuid
        METHOD = "GET"
        URL = f"/dtable-server/dtables/{self.base_uuid}"

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

    async def get_metadata(self):
        # base_uuid = dtable_uuid
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/metadata/"

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

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
    async def list_rows_with_sql(self, sql: str, convert_keys: bool = True):
        # MAX 10,000 Rows - [TODO] Check length
        METHOD = "POST"
        URL = f"/dtable-db/api/v1/query/{self.base_uuid}/"
        JSON = {"sql": sql, "convert_keys": convert_keys}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

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
        self, table_name: str, row: dict, anchor_row_id: str = None, row_insert_position: str = "insert_below"
    ):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        JSON = {"table_name": table_name, "row": row}
        if anchor_row_id:
            JSON.update({"ahchor_row_id": anchor_row_id, "row_insert_position": row_insert_position})

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
