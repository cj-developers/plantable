import asyncio
import logging
from datetime import datetime
from typing import List, Union
from fastapi import HTTPException, status

import requests
import pandas as pd
from pydantic import BaseModel
from pypika import MySQLQuery as PikaQuery
from pypika import Table as PikaTable
from pypika.dialects import QueryBuilder
from tabulate import tabulate

from ..model import (
    DTABLE_ICON_COLORS,
    DTABLE_ICON_LIST,
    Admin,
    ApiToken,
    Base,
    BaseActivity,
    BaseToken,
    Column,
    Table,
    Metadata,
    Team,
    User,
    UserInfo,
    View,
    Webhook,
)
from ..schema.serde import DT_FMT, Sea2Py, to_str_datetime
from .conf import SEATABLE_URL
from .core import TABULATE_CONF, HttpClient
from .exception import MoreRows

logger = logging.getLogger()


################################################################
# BaseClient
################################################################
class BaseClient(HttpClient):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        base_token: Union[BaseToken, str] = None,
        api_token: str = None,
    ):
        super().__init__(seatable_url=seatable_url)

        self.base_token = base_token
        self.api_token = api_token

        if api_token:
            auth_url = self.seatable_url.rstrip("/") + "/api/v2.1/dtable/app-access-token/"
            response = requests.get(auth_url, headers={"Authorization": f"Token {self.api_token}"})
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                error_msg = response.json()["error_msg"]
                if error_msg in ["Permission denied."]:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Wrong base token!",
                    )
                raise ex
            results = response.json()
            self.base_token = BaseToken(**results)

    ################################################################
    # BASE INFO
    ################################################################
    # Get Base Info
    async def get_base_info(self):
        METHOD = "GET"
        URL = f"/dtable-server/dtables/{self.base_token.dtable_uuid}"

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

    # Get Metadata
    async def get_metadata(self, model: BaseModel = Metadata):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/metadata/"
        ITEM = "metadata"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = model(**results)

        return results

    # (CUSTOM) List Tables
    async def list_tables(self):
        metadata = await self.get_metadata()
        tables = metadata.tables

        return tables

    # (CUSTOM) Get Table
    async def get_table(self, table_name: str):
        tables = await self.list_tables()
        for table in tables:
            if table.name == table_name:
                return table
        else:
            raise KeyError()

    # (CUSTOM) Get Table by ID
    async def get_table_by_id(self, table_id: str):
        tables = await self.list_tables()
        for table in tables:
            if table.id == table_id:
                return table
        else:
            raise KeyError()

    # (CUSTOM) Get Names by ID
    async def get_names_by_ids(self, table_id: str, view_id: str):
        table = await self.get_table_by_id(table_id=table_id)
        views = await self.list_views(table_name=table.name)
        for view in views:
            if view.id == view_id:
                break
        else:
            raise KeyError

        return table.name, view.name

    # Get Big Data Status
    async def get_bigdata_status(self):
        METHOD = "GET"
        URL = f"/dtable-db/api/v1/base-info/{self.base_token.dtable_uuid}/"

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

    # List Collaborators
    async def list_collaborators(self, model: BaseModel = UserInfo):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/related-users/"
        ITEM = "user_list"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # (CUSTOM) ls
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

    ################################################################
    # ROWS
    ################################################################
    # List Rows (Table, with SQL)
    async def list_rows_with_sql(self, sql: Union[str, QueryBuilder], convert_keys: bool = True):
        """
        [NOTE]
         default LIMIT 100 when not LIMIT is given!
         max LIMIT 10000
        """
        METHOD = "POST"
        URL = f"/dtable-db/api/v1/query/{self.base_token.dtable_uuid}/"
        JSON = {"sql": sql.get_sql() if isinstance(sql, QueryBuilder) else sql, "convert_keys": convert_keys}
        SUCCESS = "success"
        ITEM = "results"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            if not response[SUCCESS]:
                raise Exception(response)
            results = response[ITEM]

        return results

    # List Rows (View)
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
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"
        ITEM = "rows"

        get_all_rows = False
        if not limit:
            get_all_rows = True
            limit = limit or 6

        params = {
            "table_name": table_name,
            "view_name": view_name,
            "convert_link_id": str(convert_link_id).lower(),
            "order_by": order_by,
            "direction": direction,
            "start": start,
            "limit": limit,
        }

        async with self.session_maker(token=self.base_token.access_token) as session:
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

    # (CUSTOM) List Rows by ID
    async def list_rows_by_id(
        self,
        table_id: str,
        view_id: str,
        convert_link_id: bool = False,
        order_by: str = None,
        direction: str = "asc",
        start: int = 0,
        limit: int = None,
    ):
        table_name, view_name = self.get_names_by_ids(table_id=table_id, view_id=view_id)
        return await self.list_rows(
            table_name=table_name,
            view_name=view_name,
            convert_link_id=convert_link_id,
            order_by=order_by,
            direction=direction,
            start=start,
            limit=limit,
        )

    # Add Row
    async def add_row(
        self, table_name: str, row: dict, anchor_row_id: str = None, row_insert_position: str = "insert_below"
    ):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"
        JSON = {"table_name": table_name, "row": row}
        if anchor_row_id:
            JSON.update(
                {
                    "ahchor_row_id": anchor_row_id,
                    "row_insert_position": row_insert_position,
                }
            )

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Update Row
    async def update_row(self, table_name: str, row_id: str, row: dict):
        # NOT WORKING
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"
        JSON = {"table_name": table_name, "row_id": row_id, "row": row}
        ITEM = "success"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    # Delete Row
    async def delete_row(self, table_name: str, row_id: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/"
        JSON = {"table_name": table_name, "row_id": row_id}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Get Row
    async def get_row(self, table_name: str, row_id: str, convert: bool = False):
        # NOT WORKING
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/rows/{row_id}/"

        params = {"table_name": table_name, "convert": str(convert).lower()}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)

        return results

    # Append Rows
    async def append_rows(self, table_name: str, rows: List[dict]):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-append-rows/"
        JSON = {"table_name": table_name, "rows": rows}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Update Rows
    async def update_rows(self, table_name: str, updates: List[dict]):
        # updates = [{"row_id": xxx, "row": {"key": "value"}}, ...]
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-update-rows/"
        JSON = {"table_name": table_name, "updates": updates}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # (CUSTOM) Upsert Rows
    async def upsert_rows(self, table_name: str, rows: List[dict], key_column: str):
        key_map = await self.query_key_map(table_name=table_name, key_column=key_column)
        # [TODO!]

    # Delete Rows
    async def delete_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/batch-delete-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Rock Rows
    async def lock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/lock-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Unrock Rows
    async def unlock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/unlock-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    ################################################################
    # (CUSTOM) QUERY
    ################################################################
    # (CUSTOM) Query Key Map
    async def query_key_map(self, table_name: str, key_column: str):
        results = await self.query(table_name=table_name, columns=[key_column])
        return {v[key_column]: k for k, v in results.items()}

    # (CUSTOM) read_table
    async def read_table(
        self,
        table_name: str,
        columns: List[str] = None,
        modified_before: str = None,
        modified_after: str = None,
        offset: int = 0,
        limit: int = None,
        mtime: str = "_mtime",
        deserialize: bool = True,
        incl_sys_cols: bool = True,
    ) -> List[dict]:
        LIMIT = 100
        OFFSET = 0

        # correct args
        table = PikaTable(table_name)
        if columns:
            if "_id" not in columns:
                columns.append("_id")
        else:
            columns = ["*"]
        _limit = min(LIMIT, limit) if limit else limit
        _offset = offset if offset else OFFSET

        q = PikaQuery.from_(table).select(*columns)
        if modified_before:
            if isinstance(modified_before, datetime):
                modified_before = to_str_datetime(modified_before)
            q = q.where(table[mtime] < modified_before)
        if modified_after:
            if isinstance(modified_after, datetime):
                modified_after = to_str_datetime(modified_after)
            q = q.where(table[mtime] > modified_after)
        q = q.limit(_limit or LIMIT)

        # 1st hit
        rows = await self.list_rows_with_sql(sql=q.offset(_offset))

        # get all records
        if not limit or len(rows) < limit:
            while True:
                _offset += LIMIT
                _rows = await self.list_rows_with_sql(sql=q.offset(_offset))
                rows += _rows
                if len(_rows) < LIMIT:
                    break

        # to python data type
        if deserialize:
            table_info = await self.get_table(table_name)
            deserializer = Sea2Py(table_info=table_info, incl_sys_cols=incl_sys_cols)
            rows = [deserializer(r) for r in rows]

        return rows

    async def read_view(
        self,
        table_name: str,
        view_name: str,
        convert_link_id: bool = False,
        order_by: str = None,
        direction: str = "asc",
        start: int = 0,
        limit: int = None,
        deserialize: bool = True,
        incl_sys_cols: bool = True,
    ):
        rows = await self.list_rows(
            table_name=table_name,
            view_name=view_name,
            convert_link_id=convert_link_id,
            order_by=order_by,
            direction=direction,
            start=start,
            limit=limit,
        )

        # to python data type
        table_info = await self.get_table(table_name)
        if deserialize:
            deserializer = Sea2Py(table_info=table_info, incl_sys_cols=incl_sys_cols)
            rows = [deserializer(r) for r in rows]

        return rows

    ################################################################
    # LINKS
    ################################################################

    ################################################################
    # FILES & IMAGES
    ################################################################

    ################################################################
    # TABLES
    ################################################################
    # Create New Table
    async def create_new_table(self, table_name: str, columns: List[dict]):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"
        JSON = {"table_name": table_name, "columns": columns}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Rename Table
    async def rename_table(self, table_name: str, new_table_name: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"
        JSON = {"table_name": table_name, "new_table_name": new_table_name}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Delete Table
    async def delete_table(self, table_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/"
        JSON = {"table_name": table_name}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # Duplicate Table
    async def duplicate_table(self, table_name: str, is_duplicate_records: bool = True):
        # rename table in a second step
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/tables/duplicate-table/"
        JSON = {"table_name": table_name, "is_duplicate_records": is_duplicate_records}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

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
    async def list_views(self, table_name: str, model: BaseModel = View):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/"
        ITEM = "views"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, table_name=table_name)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # Create View
    async def create_view(
        self, table_name: str, name: str, type: str = "table", is_locked: bool = False, model: BaseModel = View
    ):
        """
        type: "table" or "archive" (bigdata)
        """
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/"
        JSON = {
            "name": name,
            "type": type,
            "is_locked": str(is_locked).lower(),
        }

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON, table_name=table_name)

        if model:
            results = model(**results)

        return results

    # Get View
    async def get_view(self, table_name: str, view_name: str, model: BaseModel = View):
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/{view_name}/"

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, table_name=table_name)

        if model:
            results = model(**results)

        return results

    # (CUSTOM) Get View by ID
    async def get_view_by_id(self, table_id: str, view_id: str, model: BaseModel = View):
        table_name, view_name = await self.get_names_by_ids(table_id=table_id, view_id=view_id)

        return await self.get_view(table_name=table_name, view_name=view_name)

    # Update View
    # NOT TESTED!
    async def update_view(self, table_name: str, view_name: str, conf: Union[dict, BaseModel] = None):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/{view_name}/"

        if isinstance(conf, BaseModel):
            conf = conf.dict()

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=conf, table_name=table_name)

        return results

    # Delete View
    async def delete_view(self, table_name: str, view_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/views/{view_name}/"
        ITEM = "success"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, table_name=table_name)
            results = response[ITEM]

        return results

    ################################################################
    # COLUMNS
    ################################################################

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
        PARAMS = {"row_id": row_id}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

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
        PARAMS = {"page": page, "per_page": per_page}
        ITEM = "operations"

        async with self.session_maker(token=self.base_token.access_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, **PARAMS)
            results = response[ITEM]

        if model:
            results = [model(**x) for x in results]

        return results

    # List Row Activities
    async def list_row_activities(self, row_id: str, page: int = 1, per_page: int = 25):
        # rename table in a second step
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/activities/"
        PARAMS = {"row_id": row_id, "page": page, "per_page": per_page}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return results

    # List Delete Operation Logs
    async def list_delete_operation_logs(self, op_type: str, page: int = 1, per_page: int = 25):
        """
        op_type
         delete_row
         delete_rows
         delete_table
         delete_column
        """
        # rename table in a second step
        METHOD = "GET"
        URL = f"/api/v2.1/dtables/{self.base_token.dtable_uuid}/delete-operation-logs/"
        PARAMS = {"op_type": op_type, "page": page, "per_page": per_page}

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **PARAMS)

        return results

    # List Delete Rows
    async def list_delete_rows(self):
        # rename table in a second step
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_token.dtable_uuid}/deleted-rows/"

        async with self.session_maker(token=self.base_token.access_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL)

        return results

    ################################################################
    # SNAPSHOTS
    ################################################################
