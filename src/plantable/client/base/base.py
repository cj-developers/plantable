import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, List, Tuple, Union

import pyarrow as pa
from pydantic import BaseModel
from pypika import MySQLQuery as PikaQuery
from pypika import Order
from pypika import Table as PikaTable
from tabulate import tabulate

from ...const import DT_FMT, TZ
from ...model import BaseActivity, BaseToken, Column, Metadata, SelectOption, Table, UserInfo, View
from ...model.column import COLUMN_DATA
from ...serde import Deserializer, FromPython, ToPython
from ...utils import divide_chunks, parse_str_datetime
from ..conf import SEATABLE_URL
from ..core import TABULATE_CONF
from .builtin import BuiltInBaseClient

logger = logging.getLogger()

FIRST_COLUMN_TYPES = ["text", "number", "date", "single-select", "formular", "autonumber"]


################################################################
# BaseClient
################################################################
class BaseClient(BuiltInBaseClient):
    ################################################################
    # BASE INFO
    ################################################################
    # List Tables
    async def list_tables(self, refresh: bool = True):
        metadata = await self.get_metadata(refresh=refresh)
        tables = metadata.tables
        return tables

    # Get Table
    async def get_table(self, table_name: str, refresh: bool = True):
        tables = await self.list_tables(refresh=refresh)
        for table in tables:
            if table.name == table_name:
                return table
        else:
            raise KeyError()

    # Get Table by ID
    async def get_table_by_id(self, table_id: str, refresh: bool = True):
        tables = await self.list_tables(refresh=refresh)
        for table in tables:
            if table.id == table_id:
                return table
        else:
            raise KeyError()

    # Get Names by ID
    async def get_names_by_ids(self, table_id: str, view_id: str, refresh: bool = True):
        table = await self.get_table_by_id(table_id=table_id, refresh=refresh)
        views = await self.list_views(table_name=table.name)
        for view in views:
            if view.id == view_id:
                break
        else:
            raise KeyError

        return table.name, view.name

    # ls
    async def ls(self, table_name: str = None):
        metadata = await self.get_metadata()
        tables = metadata.tables
        if table_name:
            for table in tables:
                if table.name == table_name:
                    break
            else:
                raise KeyError()
            columns = [{"key": c.key, "name": c.name, "type": c.type} for c in table.columns]
            print(tabulate(columns, **TABULATE_CONF))
            return
        _tables = list()
        for table in tables:
            _n = len(table.columns)
            _columns = ", ".join(c.name for c in table.columns)
            if len(_columns) > 50:
                _columns = _columns[:50] + "..."
            _columns += f" ({_n})"
            _tables += [
                {
                    "id": table.id,
                    "name": table.name,
                    "views": ", ".join([v.name for v in table.views]),
                    "columns": _columns,
                },
            ]
        print(tabulate(_tables, **TABULATE_CONF))

    ################################################################
    # ROWS
    ################################################################
    # List Rows by ID
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

    # prep link columns
    @staticmethod
    def prep_link_columns(table: Table):
        return [c for c in table.columns if c.type == "link"]

    # Upsert Rows
    async def upsert_rows(self, table_name: str, rows: List[dict], key_column: str = None):
        if not key_column:
            table = await self.get_table(table_name=table_name)
            key_column = table.columns[0].name

        row_id_map = await self.get_row_id_map(table_name=table_name, key_column=key_column)

        rows_to_update = list()
        rows_to_append = list()
        for row in rows:
            if row[key_column] in row_id_map:
                rows_to_update.append({"row_id": row_id_map[row[key_column]], "row": row})
            else:
                rows_to_append.append(row)

        results = dict()
        if rows_to_update:
            results_update = await self.update_rows(table_name=table_name, updates=rows_to_update)
            results.update({"update_rows": results_update})
        if rows_to_append:
            results_append = await self.append_rows(table_name=table_name, rows=rows_to_append)
            results.update({"append_rows": results_append})

        return results

    ################################################################
    # QUERY
    ################################################################
    # Query Key Map
    async def get_row_id_map(self, table_name: str, key_column: str = None, refresh: bool = True):
        if not refresh and table_name in self.row_id_map and key_column in self.row_id_map[table_name]:
            return self.row_id_map[table_name][key_column]

        if key_column is None:
            table = await self.get_table(table_name=table_name)
            key_column = table.columns[0].name
        results = await self.read_table(table_name=table_name, select=["_id", key_column])

        if table_name not in self.row_id_map:
            self.row_id_map[table_name] = dict()
        self.row_id_map[table_name].update({key_column: {r[key_column]: r["_id"] for r in results}})
        return self.row_id_map[table_name][key_column]

    async def _read_table(
        self,
        table_name: str,
        select: List[str] = None,
        modified_before: str = None,
        modified_after: str = None,
        order_by: str = None,
        desc: bool = False,
        offset: int = 0,
        limit: int = None,
    ):
        MAX_LIMIT = 10000
        OFFSET = 0

        # correct args
        table = PikaTable(table_name)
        if not select:
            select = ["*"]
        if not isinstance(select, list):
            select = [x.strip() for x in select.split(",")]
        _limit = min(MAX_LIMIT, limit) if limit else limit
        _offset = offset if offset else OFFSET

        # generate query
        q = PikaQuery.from_(table).select(*select)

        if modified_before or modified_after:
            last_modified = "_mtime"
            tbl = await self.get_table(table_name=table_name)
            for c in tbl.columns:
                if c.key == "_mtime":
                    last_modified = c.name
                    break
            if modified_after:
                if isinstance(modified_after, datetime):
                    modified_after = modified_after.isoformat(timespec="milliseconds")
                q = q.where(table[last_modified] > modified_after)
            if modified_before:
                if isinstance(modified_before, datetime):
                    modified_before = modified_before.isoformat(timespec="milliseconds")
                q = q.where(table[last_modified] < modified_before)

        if order_by:
            q = q.orderby(order_by, order=Order.desc if desc else Order.asc)

        q = q.limit(_limit or MAX_LIMIT)

        # 1st hit
        rows = await self.list_rows_with_sql(sql=q.offset(_offset))

        # get all records
        if not limit or len(rows) < limit:
            while True:
                _offset += MAX_LIMIT
                _rows = await self.list_rows_with_sql(sql=q.offset(_offset))
                rows += _rows
                if len(_rows) < MAX_LIMIT:
                    break

        return rows

    # read table with schema
    async def read_table_with_schema(
        self,
        table_name: str,
        select: List[str] = None,
        modified_before: str = None,
        modified_after: str = None,
        order_by: str = None,
        desc: bool = False,
        offset: int = 0,
        limit: int = None,
        Deserializer: Deserializer = ToPython,
    ) -> dict:
        # list rows
        rows = await self._read_table(
            table_name=table_name,
            select=select,
            modified_before=modified_before,
            modified_after=modified_after,
            order_by=order_by,
            desc=desc,
            offset=offset,
            limit=limit,
        )

        if not Deserializer:
            _msg = "Deserializer required!"
            raise KeyError(_msg)

        # to python data type
        metadata = await self.get_metadata()
        collaborators = await self.list_collaborators()
        deserializer = Deserializer(
            metadata=metadata,
            table_name=table_name,
            base_name=self.base_name,
            group_name=self.group_name,
            collaborators=collaborators,
        )
        try:
            rows = deserializer(*rows, select=select)
        except Exception as ex:
            _msg = f"deserializer failed - group '{self.group_name}', base '{self.base_name}', table '{table_name}'"
            logger.error(_msg)
            raise ex

        return {
            "table": deserializer.schema(),
            "rows": rows,
            "last_mtime": deserializer.last_modified,
        }

    # read table
    async def read_table(
        self,
        table_name: str,
        select: List[str] = None,
        modified_before: str = None,
        modified_after: str = None,
        order_by: str = None,
        desc: bool = False,
        offset: int = 0,
        limit: int = None,
        Deserializer: Deserializer = ToPython,
    ) -> List[dict]:
        # list rows
        rows = await self._read_table(
            table_name=table_name,
            select=select,
            modified_before=modified_before,
            modified_after=modified_after,
            order_by=order_by,
            desc=desc,
            offset=offset,
            limit=limit,
        )

        # deserializer
        if Deserializer:
            metadata = await self.get_metadata()
            collaborators = await self.list_collaborators()
            deserializer = Deserializer(
                metadata=metadata,
                table_name=table_name,
                base_name=self.base_name,
                group_name=self.group_name,
                collaborators=collaborators,
            )
            try:
                rows = deserializer(*rows, select=select)
            except Exception as ex:
                _msg = (
                    f"deserializer failed - group '{self.group_name}', base '{self.base_name}', table '{table_name}'"
                )
                logger.error(_msg)
                raise ex

        return rows

    # read table as DataFrame
    async def read_table_as_df(
        self,
        table_name: str,
        select: List[str] = None,
        modified_before: str = None,
        modified_after: str = None,
        offset: int = 0,
        limit: int = None,
    ):
        rows = await self.read_table(
            table_name=table_name,
            select=select,
            modified_before=modified_before,
            modified_after=modified_after,
            offset=offset,
            limit=limit,
            Deserializer=ToPython,
        )

        if not rows:
            return None
        tbl = pa.Table.from_pylist(rows).to_pandas()
        return tbl.set_index("_id", drop=True).rename_axis("row_id")

    # read view
    async def read_view(
        self,
        table_name: str,
        view_name: str,
        convert_link_id: bool = False,
        order_by: str = None,
        direction: str = "asc",
        start: int = 0,
        limit: int = None,
        Deserializer: Deserializer = ToPython,
        return_schema: bool = False,
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
        if Deserializer:
            metadata = await self.get_metadata()
            collaborators = await self.list_collaborators()
            deserializer = Deserializer(
                metadata=metadata,
                table_name=table_name,
                base_name=self.base_name,
                group_name=self.group_name,
                collaborators=collaborators,
            )
            try:
                rows = deserializer(*rows)
            except Exception as ex:
                _msg = f"deserializer failed - group '{self.group_name}', base '{self.base_name}', table '{table_name}', view '{view_name}'"
                logger.error(_msg)
                raise ex
            if return_schema:
                return rows, deserializer.schema()

        return rows

    # read view as DataFrame
    async def read_view_as_df(
        self,
        table_name: str,
        view_name: str,
        convert_link_id: bool = False,
        order_by: str = None,
        direction: str = "asc",
        start: int = 0,
        limit: int = None,
    ):
        rows = await self.read_view(
            table_name=table_name,
            view_name=view_name,
            convert_link_id=convert_link_id,
            order_by=order_by,
            direction=direction,
            start=start,
            limit=limit,
            Deserializer=ToPython,
            return_schema=False,
        )

        if not rows:
            return None
        tbl = pa.Table.from_pylist(rows).to_pandas()
        return tbl.set_index("_id", drop=True).rename_axis("row_id")

    # Generate Deserializer
    async def generate_deserializer(self, table_name: str):
        table = await self.get_table(table_name)
        users = await self.list_collaborators() if "collaborator" in [c.type for c in table.columns] else None
        return ToPython(table=table, users=users)

    # get last modified at
    async def get_last_mtime(self, table_name: str):
        table = await self.get_table(table_name=table_name)
        for column in table.columns:
            if column.type == "mtime":
                c = column.name
                q = f"SELECT {c} FROM {table_name} ORDER BY {c} DESC LIMIT 1;"
                r = await self.list_rows_with_sql(q)
                last_mtime = parse_str_datetime(r[0][c])
                return last_mtime
        else:
            raise KeyError

    ################################################################
    # LINKS
    ################################################################

    # Get Link
    async def get_link(self, table_name: str, column_name: str):
        column = await self.get_column(table_name=table_name, column_name=column_name)
        if column.type != "link":
            _msg = f"type of column '{column_name}' is not link type."
            raise KeyError(_msg)
        return column.data

    # Get Ohter Rows Ids
    async def get_other_rows_ids(self, table_name, column_name):
        link = await self.get_link(table_name=table_name, column_name=column_name)
        other_table = await self.get_table_by_id(table_id=link["other_table_id"])
        for column in other_table.columns:
            if column.key == link["display_column_key"]:
                break
        else:
            raise KeyError
        return await self.get_row_id_map(table_name=other_table.name, key_column=column.name)

    # (custom)
    async def upsert_link_rows(self, table: Table, column_name: str, rows: List[dict], key_column: str = None):
        map_pk_to_row_id = await self.get_row_id_map(table.name, key_column=key_column)
        pass
        # [HERE!!!]

    ################################################################
    # FILES & IMAGES
    ################################################################

    ################################################################
    # TABLES
    ################################################################

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

    # Create Table
    # [NOTE] 현재 Create New Table API 문제 때문에 사용 - 2번째 Colmnn부터는 insert_column으로 추가.
    async def create_table(self, table_name: str, columns: List[dict], overwrite: bool = False):
        tables = await self.list_tables()
        if table_name in [t.name for t in tables]:
            if not overwrite:
                _msg = f"table '{table_name}' already exists!"
                raise KeyError(_msg)
            r = await self.delete_table(table_name=table_name)
            if not r["success"]:
                _msg = f"delete table '{table_name}' failed!"
                raise KeyError(_msg)

        # seprate key column
        key_column, columns = columns[0], columns[1:]
        if key_column["column_type"] not in FIRST_COLUMN_TYPES:
            _msg = f"""only '{", ".join(FIRST_COLUMN_TYPES)}' can be a first column"""
            raise KeyError(_msg)

        # create table
        _ = await self._create_new_table(table_name=table_name, columns=[key_column])

        # insert columns
        for column in columns:
            _ = await self.insert_column(table_name=table_name, **column)

        return True

    ################################################################
    # IMPORT
    ################################################################
    # (DEPRECATED) Create Table from CSV

    # (DEPRECATED) Append Rows from CSV

    ################################################################
    # VIEWS
    ################################################################

    # Get View by ID
    async def get_view_by_id(self, table_id: str, view_id: str, model: BaseModel = View):
        table_name, view_name = await self.get_names_by_ids(table_id=table_id, view_id=view_id)

        return await self.get_view(table_name=table_name, view_name=view_name)

    ################################################################
    # COLUMNS
    ################################################################

    # Get Column
    async def get_column(self, table_name: str, column_name: str):
        table = await self.get_table(table_name=table_name)
        for column in table.columns:
            if column.name == column_name:
                return column
        else:
            _msg = f"no column (name: {column_name}) in table (name: {table_name})."
            raise KeyError(_msg)

    # Get Column by ID
    async def get_column_by_id(self, table_id: str, column_id: str):
        table = await self.get_table_by_id(table_id=table_id)
        for column in table.columns:
            if column.key == column_id:
                return column
        else:
            _msg = f"no column (id: {column_id}) in table (id: {table_id})."
            raise KeyError(_msg)

    # add select options if not exists
    async def add_select_options_if_not_exists(self, table_name: str, rows: List[dict]):
        table = await self.get_table(table_name=table_name)
        columns_and_options = {
            c.name: [o["name"] for o in c.data["options"]] if c.data else []
            for c in table.columns
            if c.type in ["single-select", "multiple-select"]
        }

        if not columns_and_options:
            return

        options = {c: set([r.get(c) for r in rows if r.get(c)]) for c in columns_and_options}
        options_to_add = dict()
        for column_name, column_options in options.items():
            for column_opt in column_options:
                if column_opt not in columns_and_options[column_name]:
                    if column_name not in options_to_add:
                        options_to_add[column_name] = list()
                    options_to_add[column_name].append(SelectOption(name=column_opt))

        coros = [
            self.add_select_options(table_name=table_name, column_name=column_name, options=options)
            for column_name, options in options_to_add.items()
        ]

        return await asyncio.gather(*coros)

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
    # List Delete Operation Logs After
    async def list_delete_operation_logs_since(self, op_type: str, op_time: Union[datetime, str], per_page: int = 100):
        # correct op_time
        op_time = datetime.fromisoformat(op_time) if isinstance(op_time, str) else op_time

        delete_logs = list()
        page = 1
        while True:
            logs = await self.list_delete_operation_logs(op_type=op_type, page=page, per_page=per_page)
            if not logs:
                break
            for log in logs:
                if datetime.fromisoformat(log["op_time"]) < op_time:
                    break
                delete_logs.append(log)
            page += 1

        return delete_logs

    ################################################################
    # SNAPSHOTS
    ################################################################
    # TBD
