import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, List, Tuple, Union
import aiohttp

import pyarrow as pa
import requests
from fastapi import HTTPException, status
from pydantic import BaseModel
from pypika import MySQLQuery as PikaQuery
from pypika import Order
from pypika import Table as PikaTable
from pypika.dialects import QueryBuilder
from tabulate import tabulate

from ...const import DT_FMT, TZ
from ...model import BaseActivity, BaseToken, Column, Metadata, SelectOption, Table, UserInfo, View
from ...model.column import COLUMN_DATA
from ...serde import Deserializer, FromPython, ToPython
from ...utils import divide_chunks, parse_str_datetime
from ..conf import SEATABLE_URL
from ..core import TABULATE_CONF, HttpClient

logger = logging.getLogger()

FIRST_COLUMN_TYPES = ["text", "number", "date", "single-select", "formular", "autonumber"]


################################################################
# BuiltInBaseClient
################################################################
class BuiltInBaseClient(HttpClient):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        api_token: str = None,
        base_token: BaseToken = None,
        access_token_refresh_sec: int = 86400,
    ):
        if not seatable_url:
            raise KeyError("seatable_url is required!")

        super().__init__(seatable_url=seatable_url.rstrip("/"))

        self.api_token = api_token
        self.base_token = base_token
        self.access_token_refresh_sec = access_token_refresh_sec

        if api_token:
            self.update_base_token()

        # self info
        self.dtable_uuid = self.base_token.dtable_uuid
        self.workspace_id = self.base_token.workspace_id
        self.group_id = self.base_token.group_id
        self.group_name = self.base_token.group_name
        self.base_name = self.base_token.base_name

        # store
        self.metadata = None
        self.collaborators = None
        self.views = dict()
        self.row_id_map = dict()

    # update base_token
    def update_base_token(self):
        auth_url = self.seatable_url + "/api/v2.1/dtable/app-access-token/"
        response = requests.get(auth_url, headers={"Authorization": f"Token {self.api_token}"})
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            error_msg = response.json()["error_msg"]
            if error_msg in ["Permission denied."]:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Wrong base token!")
            raise ex
        results = response.json()
        self.base_token = BaseToken(**results)

    # override
    def session_maker(self):
        token_uptime = (datetime.now() - self.base_token.generated_at).seconds
        if token_uptime > self.access_token_refresh_sec:
            self.update_base_token()
            _msg = f"access token for workspace '{self.workspace_id}' is updated after {token_uptime} seconds uptime."
            logger.warning(_msg)
        headers = self.headers.copy()
        headers.update({"authorization": "Bearer {}".format(self.base_token.access_token)})
        return aiohttp.ClientSession(base_url=self.seatable_url, headers=headers)

    ################################################################
    # BASE INFO
    ################################################################
    # Get Base Info
    async def get_base_info(self):
        METHOD = "GET"
        URL = f"/dtable-server/dtables/{self.base_token.dtable_uuid}"

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        return results

    # Get Metadata
    async def get_metadata(self, model: BaseModel = Metadata, refresh: bool = True):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/metadata/"
        ITEM = "metadata"

        if not refresh and self.metadata is not None:
            return self.metadata

        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]
        if model:
            results = model(**results)

        self.metadata = results
        return self.metadata

    # Get Big Data Status
    async def get_bigdata_status(self):
        METHOD = "GET"
        URL = f"/dtable-db/api/v1/base-info/{self.base_token.dtable_uuid}/"

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        return results

    # List Collaborators
    async def list_collaborators(self, model: BaseModel = UserInfo, refresh: bool = True):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/related-users/"
        ITEM = "user_list"

        if not refresh and self.collaborators is not None:
            return self.collaborators

        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]
        if model:
            results = [model(**x) for x in results]

        self.collaborators = results
        return self.collaborators

    ################################################################
    # ROWS
    ################################################################
    # List Rows (Table, with SQL)
    async def list_rows_with_sql(self, sql: Union[str, QueryBuilder], convert_keys: bool = True):
        """
        [NOTE]
         default LIMIT 100 when not LIMIT is given!
         max LIMIT 10000!
        """
        METHOD = "POST"
        URL = f"/dtable-db/api/v1/query/{self.base_token.dtable_uuid}/"
        SUCCESS = "success"
        ITEM = "results"

        json = {"sql": sql.get_sql() if isinstance(sql, QueryBuilder) else sql, "convert_keys": convert_keys}
        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=json)
            if not response[SUCCESS]:
                raise Exception(response)
            results = response[ITEM]
        return results

    # List Rows (View)
    # [NOTE] Seatable 4.1에서 첫 Row에 없는 값은 안 읽어오는 이슈
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
        MAX_LIMIT = 1000

        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"
        ITEM = "rows"

        get_all_rows = False
        if not limit:
            get_all_rows = True
            limit = limit or MAX_LIMIT

        params = {
            "table_name": table_name,
            "view_name": view_name,
            "convert_link_id": str(convert_link_id).lower(),
            "order_by": order_by,
            "direction": direction,
            "start": start,
            "limit": limit,
        }

        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            response = response[ITEM]
            results = response

            # pagination
            if get_all_rows:
                while len(response) == limit:
                    params.update({"start": params["start"] + limit})
                    response = await self.request(session=session, method=METHOD, url=URL, **params)
                    response = response[ITEM]
                    results += response

        return results

    # Add Row
    async def add_row(
        self, table_name: str, row: dict = {}, anchor_row_id: str = None, row_insert_position: str = "insert_below"
    ):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"

        table = await self.get_table(table_name=table_name)
        serializer = FromPython(table)

        json = {"table_name": table_name, "row": serializer(row)}
        if anchor_row_id:
            json.update(
                {
                    "ahchor_row_id": anchor_row_id,
                    "row_insert_position": row_insert_position,
                }
            )

        # add select options if not exists
        _ = await self.add_select_options_if_not_exists(table_name=table_name, rows=[row])

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # Update Row
    async def update_row(self, table_name: str, row_id: str, row: dict):
        # NOT WORKING
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"
        ITEM = "success"

        table = await self.get_table(table_name=table_name)
        serializer = FromPython(table=table)
        json = {"table_name": table_name, "row_id": row_id, "row": serializer(row)}

        # add select options if not exists
        _ = await self.add_select_options_if_not_exists(table_name=table_name, rows=[row])

        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=json)
            results = response[ITEM]

        return results

    # Delete Row
    async def delete_row(self, table_name: str, row_id: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"

        json = {"table_name": table_name, "row_id": row_id}

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # Get Row
    async def get_row(self, table_name: str, row_id: str, convert: bool = False):
        # NOT WORKING
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/{row_id}/"

        params = {"table_name": table_name, "convert": str(convert).lower()}

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)

        return results

    # Append Rows
    async def append_rows(self, table_name: str, rows: List[dict]):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-append-rows/"

        # get pk and serializer
        table = await self.get_table(table_name=table_name)
        serializer = FromPython(table=table)

        # add select options if not exists
        _ = await self.add_select_options_if_not_exists(table_name=table_name, rows=rows)

        # divide chunk - [NOTE] 1000 rows까지만 됨
        UPDATE_LIMIT = 1000
        chunks = divide_chunks(rows, UPDATE_LIMIT)
        list_json = [{"table_name": table_name, "rows": [serializer(r) for r in chunk]} for chunk in chunks]

        async with self.session_maker() as session:
            coros = [self.request(session=session, method=METHOD, url=URL, json=json) for json in list_json]
            list_results = await asyncio.gather(*coros)

        results = {"inserted_row_count": 0}
        for r in list_results:
            results["inserted_row_count"] += r["inserted_row_count"]

        return results

    # Update Rows
    async def update_rows(self, table_name: str, updates: List[dict]):
        # updates = [{"row_id": xxx, "row": {"key": "value"}}, ...]
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-update-rows/"

        # get serializer
        table = await self.get_table(table_name=table_name)
        serializer = FromPython(table=table)

        # add select options if not exists
        _ = await self.add_select_options_if_not_exists(
            table_name=table_name, rows=[update["row"] for update in updates]
        )

        # divide chunk - [NOTE] 1000 rows까지만 됨
        UPDATE_LIMIT = 1000
        chunks = divide_chunks(updates, UPDATE_LIMIT)
        list_json = [
            {
                "table_name": table_name,
                "updates": [{"row_id": r["row_id"], "row": serializer(r["row"])} for r in chunk],
            }
            for chunk in chunks
        ]

        async with self.session_maker() as session:
            coros = [self.request(session=session, method=METHOD, url=URL, json=json) for json in list_json]
            list_results = await asyncio.gather(*coros)

        for results in list_results:
            if isinstance(results, Exception):
                raise results

        return {"updated_row_count": len(updates)}

    # Delete Rows
    async def delete_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-delete-rows/"

        json = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # Rock Rows
    async def lock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/lock-rows/"

        json = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # Unrock Rows
    async def unlock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/unlock-rows/"

        json = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    ################################################################
    # LINKS
    ################################################################
    # Create Row Link
    async def create_row_link(
        self, table_name: str, other_table_name: str, link_id: str, row_id: str, other_row_id: str
    ):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/links/"

        json = {
            "table_name": table_name,
            "other_table_name": other_table_name,
            "link_id": link_id,
            "table_row_id": row_id,
            "other_table_row_id": other_row_id,
        }

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # Create Row Links (1:n)
    async def create_row_links(
        self, table_name: str, other_table_name: str, link_id: str, row_id: str, other_rows_ids: List[str]
    ):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/links/"

        other_rows_ids = other_rows_ids if isinstance(other_rows_ids, list) else [other_rows_ids]

        json = {
            "table_name": table_name,
            "other_table_name": other_table_name,
            "link_id": link_id,
            "row_id": row_id,
            "other_rows_ids": other_rows_ids,
        }

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # Create Row Links Batch (m:n)
    async def create_row_links_batch(
        self,
        table_id: str,
        other_table_id: str,
        link_id: str,
        row_id_list: List[str],
        other_rows_ids_map: List[str],
    ):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-update-links/"

        json = {
            "table_id": table_id,
            "other_table_id": other_table_id,
            "link_id": link_id,
            "row_id_list": row_id_list,
            "other_rows_ids_map": other_rows_ids_map,
        }

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    # List Row Links
    # [NOTE] Offset 처리해야 함 - limit 어디까지 가능한지 모름, default가 무한인지도 모름
    async def list_row_links(self, table_id: str, link_column: str, row_ids: List[str]):
        METHOD = "POST"
        URL = f"/dtable-db/api/v1/linked-records/{self.base_token.dtable_uuid}"

        json = {"table_id": table_id, "link_column": link_column, "rows": [{"row_id": row_id} for row_id in row_ids]}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)

        return results

    ################################################################
    # FILES & IMAGES
    ################################################################

    ################################################################
    # TABLES
    ################################################################
    # Add Table
    async def add_table(self, table_name: str):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"

        json = {"table_name": table_name}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Create New Table
    # [NOTE] 이 endpoint는 Link 컬럼을 처리하지 못 함. (2023.9.9 현재)
    async def _create_new_table(self, table_name: str, columns: List[dict] = None):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"

        json = {"table_name": table_name}
        if columns:
            json.update({"columns": columns})
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Rename Table
    async def rename_table(self, table_name: str, new_table_name: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"

        json = {"table_name": table_name, "new_table_name": new_table_name}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Delete Table
    async def delete_table(self, table_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"
        ITEM = "success"

        json = {"table_name": table_name}
        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=json)
            results = response[ITEM]
        return results

    # Duplicate Table
    async def duplicate_table(self, table_name: str, is_duplicate_records: bool = True):
        # rename table in a second step
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/duplicate-table/"

        json = {"table_name": table_name, "is_duplicate_records": is_duplicate_records}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    ################################################################
    # IMPORT
    ################################################################
    # (DEPRECATED) Create Table from CSV

    # (DEPRECATED) Append Rows from CSV

    ################################################################
    # VIEWS
    ################################################################
    # List Views
    async def list_views(self, table_name: str, model: BaseModel = View, refresh: bool = True):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/"
        ITEM = "views"

        if not refresh and table_name in self.views:
            return self.views[table_name]

        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, table_name=table_name)
            results = response[ITEM]
        if model:
            results = [model(**x) for x in results]

        self.views.update({table_name: results})
        return self.views[table_name]

    # Create View
    async def create_view(
        self,
        table_name: str,
        name: str,
        type: str = "table",
        is_locked: bool = False,
        model: BaseModel = View,
    ):
        """
        type: "table" or "archive" (bigdata)
        """
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/"

        json = {"name": name, "type": type, "is_locked": str(is_locked).lower()}
        async with self.session_maker() as session:
            results = await self.request(
                session=session,
                method=METHOD,
                url=URL,
                json=json,
                table_name=table_name,
            )
        if model:
            results = model(**results)
        return results

    # Get View
    async def get_view(self, table_name: str, view_name: str, model: BaseModel = View):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/{view_name}/"

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, table_name=table_name)
        if model:
            results = model(**results)
        return results

    # Update View
    # NOT TESTED!
    async def update_view(self, table_name: str, view_name: str, conf: Union[dict, BaseModel] = None):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/{view_name}/"

        if isinstance(conf, BaseModel):
            conf = conf.dict()
        async with self.session_maker() as session:
            results = await self.request(
                session=session,
                method=METHOD,
                url=URL,
                json=conf,
                table_name=table_name,
            )
        return results

    # Delete View
    async def delete_view(self, table_name: str, view_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/{view_name}/"
        ITEM = "success"

        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, table_name=table_name)
            results = response[ITEM]
        return results

    ################################################################
    # COLUMNS
    ################################################################
    # Insert Column
    async def insert_column(
        self, table_name: str, column_name: str, column_type: str, column_data: dict = None, anchor_column: str = None
    ):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"

        json = await self.ensure_column(
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
            column_data=column_data,
            anchor_column=anchor_column,
        )
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Append Columns
    # [NOTE] 이 endpoint는 Link 컬럼을 처리하지 못 함. (2023.9.9 현재)
    async def append_columns(self, table_name: str, columns: List[dict]):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-append-columns/"

        columns = await asyncio.gather(*[self.ensure_column(table_name=table_name, **column) for column in columns])
        json = {"table_name": table_name, "columns": list()}
        for column in columns:
            column.pop("table_name")
            json["columns"].append(column)
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Update Column
    async def modify_column_type(self, table_name: str, column_name: str, new_column_type: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"

        json = {
            "op_type": "modify_column_type",
            "table_name": table_name,
            "column": column_name,
            "new_column_type": new_column_type,
        }
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Rename Column
    async def rename_column(self, table_name: str, column_name: str, new_column_name: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"

        json = {
            "op_type": "rename_column",
            "table_name": table_name,
            "column": column_name,
            "new_column_name": new_column_name,
        }
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Resize Column
    async def resize_column(self, table_name: str, column_name: str, new_column_width: int):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"

        json = {
            "op_type": "resize_column",
            "table_name": table_name,
            "column": column_name,
            "new_column_width": new_column_width,
        }
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Move Column
    async def move_column(self, table_name: str, column_name: str, target_column: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"

        json = {
            "op_type": "move_column",
            "table_name": table_name,
            "column": column_name,
            "target_column": target_column,
        }
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Freeze Column
    async def freeze_column(self, table_name: str, column_name: str, frozen: bool = True):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"

        json = {"op_type": "freeze_column", "table_name": table_name, "column": column_name, "frozen": frozen}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        return results

    # Delete Column
    async def delete_column(self, table_name: str, column_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/columns/"
        ITEM = "success"

        json = {"table_name": table_name, "column": column_name}
        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=json)
            results = response[ITEM]
        return results

    # Add Select Options
    async def add_select_options(
        self, table_name: str, column_name: str, options: List[SelectOption], model: BaseModel = None
    ):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/column-options/"

        json = {
            "table_name": table_name,
            "column": column_name,
            "options": [opt.dict(exclude_none=True) for opt in options],
        }
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=json)
        if model:
            results = model(**results)
        return results

    ################################################################
    # BIG DATA
    ################################################################

    ################################################################
    # ROW COMMENTS
    ################################################################
    async def list_row_comments(self, row_id: str):
        # NOT WORKING
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/comments/"

        params = {"row_id": row_id}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
        return results

    ################################################################
    # NOTIFICATION
    ################################################################

    ################################################################
    # ACTIVITIES & LOGS
    ################################################################
    # Get Base Activity Logs
    async def get_base_activity_log(self, page: int = 1, per_page: int = 25, model: BaseModel = BaseActivity):
        # rename table in a second step
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/operations/"
        ITEM = "operations"

        params = {"page": page, "per_page": per_page}
        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]
        if model:
            results = [model(**x) for x in results]
        return results

    # List Row Activities
    async def list_row_activities(self, row_id: str, page: int = 1, per_page: int = 25):
        # rename table in a second step
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/activities/"

        params = {"row_id": row_id, "page": page, "per_page": per_page}
        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)
        return results

    # List Delete Operation Logs
    async def list_delete_operation_logs(self, op_type: str, page: int = 1, per_page: int = 25):
        """
        op_type: delete_row, delete_rows, delete_table, delete_column
        """
        # rename table in a second step
        METHOD = "GET"
        URL = f"/api/v2.1/dtables/{self.base_token.dtable_uuid}/delete-operation-logs/"
        ITEM = "delete_operation_logs"

        params = {"op_type": op_type, "page": page, "per_page": per_page}
        async with self.session_maker() as session:
            response = await self.request(session=session, method=METHOD, url=URL, **params)
            results = response[ITEM]
        return results

    # List Delete Rows
    async def list_delete_rows(self):
        # rename table in a second step
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/deleted-rows/"

        async with self.session_maker() as session:
            results = await self.request(session=session, method=METHOD, url=URL)
        return results

    ################################################################
    # SNAPSHOTS
    ################################################################
    # TBD

    ################################################################
    # Helper
    ################################################################
    # Column Validator
    async def ensure_column(
        self, table_name: str, column_name: str, column_type: str, column_data: dict = None, anchor_column: str = None
    ):
        column = {"table_name": table_name, "column_name": column_name, "column_type": column_type}
        column_data = {} if not column_data else column_data

        # for single-select, mulitple-select - auto index number
        if column_type in ["single-select", "multiple-select"] and "options" in column_data:
            options = column_data["options"]
            if all([option.get("id") is None for option in options]):
                for id, option in enumerate(options, start=1):
                    option.update({"id": id})
                column_data.update({"options": options})

        # for link - auto add table
        if column_type in ["link"] and "table" not in column_data:
            column_data.update({"table": table_name})

        # for button
        if column_type in ["button"]:
            table = await self.get_table(table_name=table_name)
            table_id = table.id

            for button_action in column_data["button_action_list"]:
                # update current_table_id
                button_action.update({"current_table_id": table_id})

                # [NOTE] contact_email or name to email
                #  - input: {to_users: ["some_user@mail.com", "some_user_name"]}
                #  - output: {to_users: [{"value": "jlkajdfald@auth.local"}, {"value": "kajsldfasdf@auth.local"}]}
                if "to_users" in button_action:
                    collaborators = await self.list_collaborators()
                    to_users = button_action.pop("to_users")
                    to_user_ids = list()
                    for user in to_users:
                        for collaborator in collaborators:
                            if collaborator.name == user or collaborator.contact_email == user:
                                to_user_ids.append({"value": collaborator.email})
                    button_action.update({"to_users": to_user_ids})
                else:
                    button_action.update({"to_users": []})

                # [NOTE] user_column to user_col_key
                #  - input: {to_users: ["some_user@mail.com", "some_user_name"]}
                #  - output: {to_users: [{"value": "jlkajdfald@auth.local"}, {"value": "kajsldfasdf@auth.local"}]}
                if "user_column" in button_action:
                    user_column = button_action.pop("user_column")
                    for c in table.columns:
                        if c.name == user_column:
                            user_col_key = c.key
                            break
                    else:
                        _msg = f"user_column '{user_column}' is not in table '{table_name}'!"
                        raise KeyError(_msg)
                    button_action.update({"user_col_key": user_col_key})
            print(column_data)

        corrected_column_data = COLUMN_DATA[column_type](**column_data)
        column.update({"column_data": corrected_column_data.dict()})

        if anchor_column:
            column.update({"anchor_column": anchor_column})

        return column
