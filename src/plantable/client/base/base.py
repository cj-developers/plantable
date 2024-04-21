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


class LinkValueNotExists(Exception):
    pass


class ColumnNotExists(Exception):
    pass


class KeyNotUnique(Exception):
    pass


################################################################
# BaseClient
################################################################
class BaseClient(BuiltInBaseClient):
    ################################################################
    # List/Get Table, View, Column
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

    # List Views
    # [NOTE] BuiltIn에 있음

    # Get View
    # [NOTE] BuiltIn에 있음

    # Get View by ID
    async def get_view_by_id(self, table_id: str, view_id: str, refresh: bool = True):
        table = await self.get_table_by_id(table_id=table_id, refresh=refresh)
        for view in table.views:
            if view.id == view_id:
                break
        else:
            _msg = f"no view id '{view_id}' in table '{table.name}'!"
            raise KeyError(_msg)

        view = await self.get_view(table_name=table.name, view_name=view.name)

        return view

    # List Columns
    async def list_columns(self, table_name: str, refresh: bool = True):
        table = await self.get_table(table_name=table_name, refresh=refresh)
        return table.columns

    # Get Column
    async def get_column(self, table_name: str, column_name: str, refresh: bool = True):
        table = await self.get_table(table_name=table_name, refresh=refresh)
        for column in table.columns:
            if column.name == column_name:
                return column
        else:
            _msg = f"no column (name: {column_name}) in table (name: {table_name})."
            raise KeyError(_msg)

    # Get Column by ID
    async def get_column_by_id(self, table_id: str, column_id: str, refresh: bool = True):
        table = await self.get_table_by_id(table_id=table_id, refresh=refresh)
        for column in table.columns:
            if column.key == column_id:
                return column
        else:
            _msg = f"no column (id: {column_id}) in table (id: {table_id})."
            raise KeyError(_msg)

    # Get 1st Column
    async def get_first_column(self, table_name: str, refresh: bool = True):
        table = await self.get_table(table_name=table_name, refresh=refresh)
        for column in table.columns:
            if column.key == "0000":
                return column
        else:
            _msg = f"column key '0000' not found!"
            raise KeyError(_msg)

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
    # (OVERRIDE) Append Rows
    # [NOTE] Rate Limit 조정하는 로직 필요, 지금은 금방 Rate Limit 걸릴 것 같음.
    async def append_rows(
        self, table_name: str, rows: List[dict], add_link_if_not_exists: bool = False, refresh: bool = True
    ):
        link_columns = await self.list_link_columns(table_name=table_name, refresh=refresh)
        link_column_names = [x.name for x in link_columns]

        list_links = list()
        for row in rows:
            links = dict()
            for lc in link_column_names:
                if lc in row:
                    value = row.pop(lc)
                    links.update({lc: value})
            list_links.append(links)

        # link 없으면 그냥 append_rows 수행 (성능 개선 및 Rate Limit 절약)
        if not any(list_links):
            return await super().append_rows(table_name=table_name, rows=rows)

        # 순서보장 위해서 gather 사용하지 않음
        add_rows_results = list()
        for row in rows:
            add_row_result = await self.add_row(table_name=table_name, row=row)
            add_rows_results.append(add_row_result)

        # gather 사용하여도 순서보장
        # [NOTE] create_row_links_batch로 수정하면 Rate Limit 조금 더 아낄 수 있을 것
        coros_create_row_links = list()
        for result, links in zip(add_rows_results, list_links):
            if not links:
                continue
            for column_name, display_values in links.items():
                kwargs = await self._prep_create_row_links(
                    table_name=table_name,
                    column_name=column_name,
                    display_values=display_values,
                    add_link_if_not_exists=add_link_if_not_exists,
                )
                coros_create_row_links.append(self.create_row_links(row_id=result["_id"], **kwargs))
        create_links_results = await asyncio.gather(*coros_create_row_links)
        for result in create_links_results:
            if isinstance(result, Exception):
                raise result

        return {"inserted_rows": len(add_rows_results)}

    # (OVERRIDE) Update Rows
    async def update_rows(
        self, table_name: str, updates: List[dict], add_link_if_not_exists: bool = False, refresh: bool = True
    ):
        link_columns = await self.list_link_columns(table_name=table_name, refresh=refresh)
        link_column_names = [x.name for x in link_columns]

        # 순서에 따라 동작! asyncio.gather 금지!
        coros_create_row_links = list()
        for up in updates:
            for column_name in link_column_names:
                if column_name in up["row"]:
                    display_values = up["row"].pop(column_name)
                    kwargs = await self._prep_create_row_links(
                        table_name=table_name,
                        column_name=column_name,
                        display_values=display_values,
                        add_link_if_not_exists=add_link_if_not_exists,
                    )
                    coros_create_row_links.append(self.create_row_links(row_id=up["row_id"], **kwargs))

        # update rows
        update_rows_results = await super().update_rows(table_name=table_name, updates=updates)

        # create row links
        create_links_results = await asyncio.gather(*coros_create_row_links)
        for result in create_links_results:
            if isinstance(result, Exception):
                raise result
            if not result["success"]:
                _msg = "create_row_links failed!"
                logger.error(_msg)

        return update_rows_results

    # Validate Input Columns
    async def _validate_input_columns(self, table_name: str, rows: List[dict], refresh: bool = True):
        columns = await self.list_columns(table_name=table_name, refresh=refresh)
        column_names = [c.name for c in columns]

        input_columns = set()
        _ = [input_columns.add(k) for row in rows for k in row]

        for input_column in input_columns:
            if input_column not in column_names:
                _msg = f"table {table_name} does not have column '{input_column}'!"
                raise ColumnNotExists(_msg)

    # Upsert Rows - 궁극의 메쏘드!
    # [NOTE] 첫 Column을 Unique하게 사용하기만 한다면, 이 메쏘드 하나만 써서 Append, Update 해결 가능!
    async def upsert_rows(
        self,
        table_name: str,
        rows: List[dict],
        key_column: str = None,
        add_link_if_not_exists: bool = False,
        raise_key_not_unique_error: bool = True,
    ):
        # correct input
        rows = rows if isinstance(rows, list) else [rows]

        # validate
        await self._validate_input_columns(table_name=table_name, rows=rows, refresh=True)

        # default key column is first column
        if not key_column:
            first_column = await self.get_first_column(table_name=table_name, refresh=False)
            key_column = first_column.name

        # get row id map
        id_map = await self.get_row_id_map(
            table_name=table_name, key_column=key_column, raise_key_not_unique_error=raise_key_not_unique_error
        )

        # split updates & appends
        updates, appends = list(), list()
        for row in rows:
            key = row.get(key_column)
            if key is None or key not in id_map:
                appends.append(row)
            else:
                updates.append({"row_id": id_map[key], "row": row})

        # 1. Key Column 값 존재하면 Update
        update_coro = self.update_rows(
            table_name=table_name, updates=updates, add_link_if_not_exists=add_link_if_not_exists
        )

        # 2. Key Column 값 존재하지 않으면 Append
        append_coro = self.append_rows(
            table_name=table_name, rows=appends, add_link_if_not_exists=add_link_if_not_exists
        )

        update_results, append_results = await asyncio.gather(update_coro, append_coro)

        return {**update_results, **append_results}

    ################################################################
    # QUERY
    ################################################################
    # Get Row ID Map
    async def get_row_id_map(
        self, table_name: str, key_column: str = None, raise_key_not_unique_error: bool = True, refresh: bool = True
    ):
        if not refresh and table_name in self.row_id_map and key_column in self.row_id_map[table_name]:
            return self.row_id_map[table_name][key_column]

        if key_column is None:
            first_column = await self.get_first_column(table_name=table_name)
            key_column = first_column.name
        rows = await self.read_table(table_name=table_name, select=["_id", key_column])

        # check key column is unique
        l = [r[key_column] for r in rows]
        s = set(l)
        if len(l) != len(s):
            _dups = list()
            for e in s:
                cnt = l.count(e)
                if cnt > 1:
                    _dups.append(f"{e} * {cnt}")
            _dups = ", ".join(_dups)
            _msg = f"key column is not unique. do not use upsert method! (dups: {_dups})"
            if raise_key_not_unique_error:
                raise KeyNotUnique(_msg)
            logger.warning(_msg)

        if table_name not in self.row_id_map:
            self.row_id_map[table_name] = dict()
        self.row_id_map[table_name].update({key_column: {r[key_column]: r["_id"] for r in rows}})

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
    # List Link Columns
    async def list_link_columns(self, table_name: str, refresh: bool = True):
        table = await self.get_table(table_name=table_name, refresh=refresh)

        link_columns = list()
        for c in table.columns:
            if c.type != "link":
                continue
            link_columns.append(c)

        return link_columns

    # Prep - Create Row Link
    async def _prep_create_row_link(
        self,
        table_name: str,
        column_name: str,
        display_value: Union[str, int, float, datetime],
        add_link_if_not_exists: bool = False,
    ):
        prep = await self._prep_create_row_links(
            table_name=table_name,
            column_name=column_name,
            display_values=[display_value],
            add_link_if_not_exists=add_link_if_not_exists,
        )

        other_rows_ids = prep.pop("other_rows_ids")
        prep.update({"other_table_row_id": other_rows_ids[0]})
        return prep

    # Prep - Create Row Links
    async def _prep_create_row_links(
        self,
        table_name: str,
        column_name: str,
        display_values: list,
        add_link_if_not_exists: bool = False,
        raise_key_not_unique_error: bool = True,
    ):
        # correct display values
        display_values = display_values if isinstance(display_values, list) else [display_values]

        table = await self.get_table(table_name=table_name, refresh=True)
        column = await self.get_column(table_name=table_name, column_name=column_name, refresh=False)
        if column.type != "link":
            _msg = f"type of column '{column_name}' is not link type."
            raise KeyError(_msg)

        is_host = True if table.id == column.data["table_id"] else None

        if is_host:
            _table = table
            _other_table = await self.get_table_by_id(table_id=column.data["other_table_id"], refresh=False)
        else:
            _table = await self.get_table_by_id(table_id=column.data["table_id"], refresh=False)
            _other_table = table
        display_column_key = column.data["display_column_key"]

        display_column = await self.get_column_by_id(
            table_id=_other_table.id, column_id=display_column_key, refresh=False
        )
        row_id_map = await self.get_row_id_map(
            table_name=_other_table.name,
            key_column=display_column.name,
            raise_key_not_unique_error=raise_key_not_unique_error,
        )

        other_rows_ids = list()
        for display_value in display_values:
            if display_value not in row_id_map:
                if not add_link_if_not_exists:
                    _msg = f"display value '{display_value}' not exists. please add this value into table '{_other_table.name}' first."
                    raise LinkValueNotExists(_msg)
                # add link
                await self.add_row(table_name=_other_table.name, row={display_column.name: display_value})
                row_id_map = await self.get_row_id_map(
                    table_name=_other_table.name,
                    key_column=display_column.name,
                    raise_key_not_unique_error=raise_key_not_unique_error,
                )
            other_rows_ids.append(row_id_map[display_value])

        return {
            "table_name": _table.name,
            "other_table_name": _other_table.name,
            "link_id": column.data["link_id"],
            "other_rows_ids": other_rows_ids,
        }

    # Get Ohter Rows Ids
    async def get_other_rows_ids(self, table_name, column_name, raise_key_not_unique_error: bool = True):
        link = await self.get_link(table_name=table_name, column_name=column_name)
        other_table = await self.get_table_by_id(table_id=link["other_table_id"])
        for column in other_table.columns:
            if column.key == link["display_column_key"]:
                break
        else:
            raise KeyError
        return await self.get_row_id_map(
            table_name=other_table.name, key_column=column.name, raise_key_not_unique_error=raise_key_not_unique_error
        )

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

    ################################################################
    # COLUMNS
    ################################################################

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
