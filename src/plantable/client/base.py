import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from ..conf import SEATABLE_ACCOUNT_TOKEN, SEATABLE_API_TOKEN, SEATABLE_BASE_TOKEN, SEATABLE_URL
from ..model import (
    Admin,
    ApiToken,
    Base,
    BaseToken,
    Column,
    Table,
    Team,
    User,
    Webhook,
    DTABLE_ICON_LIST,
    DTABLE_ICON_COLORS,
)
from .core import TABULATE_CONF, HttpClient, parse_base

logger = logging.getLogger()


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
    # BASE INFO
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

    # [BASE INFO] (custom) list tables
    async def list_tables(self, model: BaseModel = Table):
        metadata = await self.get_metadata()
        tables = metadata["tables"]

        if model:
            tables = [model(**x) for x in tables]

        return tables

    # [BASE INFO] (custom) get_table
    async def get_table(self, table_name: str):
        tables = await self.list_tables()
        for table in tables:
            if table.name == table_name:
                return table
        else:
            raise KeyError()

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

    ################################################################
    # ROWS
    ################################################################
    # [ROWS] list rows (SQL)
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

    # [ROWS] list rows
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

    # [ROWS] add row
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

    # [ROWS] update row
    async def update_row(self, table_name: str, row_id: str, row: dict):
        # NOT WORKING
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        JSON = {"table_name": table_name, "row_id": row_id, "row": row}
        ITEM = "success"

        async with self.session_maker(token=self.base_token) as session:
            response = await self.request(session=session, method=METHOD, url=URL, json=JSON)
            results = response[ITEM]

        return results

    # [ROWS] delete row
    async def delete_row(self, table_name: str, row_id: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/"
        JSON = {"table_name": table_name, "row_id": row_id}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [ROWS] get row
    async def get_row(self, table_name: str, row_id: str, convert: bool = False):
        # NOT WORKING
        METHOD = "GET"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/rows/{row_id}/"

        params = {"table_name": table_name, "convert": str(convert).lower()}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, **params)

        return results

    # [ROWS] append rows
    async def append_rows(self, table_name: str, rows: List[dict]):
        # insert_below or insert_above
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/batch-append-rows/"
        JSON = {"table_name": table_name, "rows": rows}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [ROWS] update rows
    async def update_rows(self, table_name: str, updates: List[dict]):
        # updates = [{"row_id": xxx, "row": {"key": "value"}}, ...]
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/batch-update-rows/"
        JSON = {"table_name": table_name, "updates": updates}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [ROWS] delete rows
    async def delete_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/batch-delete-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [ROWS] lock rows
    async def lock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/lock-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [ROWS] unlock rows
    async def unlock_rows(self, table_name: str, row_ids: List[str]):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/unlock-rows/"
        JSON = {"table_name": table_name, "row_ids": row_ids}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    ################################################################
    # ROWS
    ################################################################

    ################################################################
    # FILES & IMAGES
    ################################################################

    ################################################################
    # TABLES
    ################################################################
    # [TABLES] create new table
    async def create_new_table(self, table_name: str, columns: List[dict]):
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/"
        JSON = {"table_name": table_name, "columns": columns}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [TABLES] rename table
    async def rename_table(self, table_name: str, new_table_name: str):
        METHOD = "PUT"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/"
        JSON = {"table_name": table_name, "new_table_name": new_table_name}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [TABLES] delete table
    async def delete_table(self, table_name: str):
        METHOD = "DELETE"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/"
        JSON = {"table_name": table_name}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    # [TABLES] duplicate table
    async def duplicate_table(self, table_name: str, is_duplicate_records: bool = True):
        # rename table in a second step
        METHOD = "POST"
        URL = f"/dtable-server/api/v1/dtables/{self.base_uuid}/tables/duplicate-table/"
        JSON = {"table_name": table_name, "is_duplicate_records": is_duplicate_records}

        async with self.session_maker(token=self.base_token) as session:
            results = await self.request(session=session, method=METHOD, url=URL, json=JSON)

        return results

    ################################################################
    # VIEWS
    ################################################################

    ################################################################
    # COLUMNS
    ################################################################

    ################################################################
    # BIG DATA
    ################################################################

    ################################################################
    # ROW COMMENTS
    ################################################################

    ################################################################
    # NOTIFICATION
    ################################################################

    ################################################################
    # ACTIVITIES & LOGS
    ################################################################

    ################################################################
    # SNAPSHOTS
    ################################################################
