import logging
from abc import abstractmethod
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Union

from ...model import Column, Table, User, Metadata
from ...utils import parse_str_datetime

logger = logging.getLogger(__name__)


SYSTEM_COLUMNS = [
    {"name": "_id", "key": "_id", "type": "row-id", "data": None},
    {"name": "_locked", "key": "_locked", "type": "checkbox", "data": None},
    {"name": "_locked_by", "key": "_locked_by", "type": "text", "data": None},
    {"name": "_archived", "key": "_archived", "type": "checkbox", "data": None},
    {"name": "_creator", "key": "_creator", "type": "creator", "data": None},
    {"name": "_ctime", "key": "_ctime", "type": "ctime", "data": None},
    {"name": "_mtime", "key": "_mtime", "type": "mtime", "data": None},
    {"name": "_last_modifier", "key": "_last_modifier", "type": "last-modifier", "data": None},
]

USER_FIELDS = ["user", "collaborator", "creator", "last-midifier"]


class CreateDeserializerFailed(Exception):
    pass


class DeserializeError(Exception):
    pass


class ColumnDeserializer:
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        metadata: Column = None,
        collaborator_map: dict = None,
    ):
        self.name = name
        self.seatable_type = seatable_type
        self.data = data
        self.metadata = metadata
        self.collaborator_map = collaborator_map

    def __call__(self, x):
        if not x:
            return None
        try:
            return self.convert(x)
        except:
            _msg = f"deserialize failed: {self.seatable_type}({x})."
            raise DeserializeError(_msg)

    def schema(self):
        raise NotImplementedError

    def convert(self, x):
        raise NotImplementedError

    def get_table(self, table_name: str):
        for table in self.metadata.tables:
            if table.name == table_name:
                return table
        else:
            _msg = f"table '{table_name}' not exists!"
            raise KeyError(_msg)

    def get_table_by_id(self, table_id: str):
        for table in self.metadata.tables:
            if table.id == table_id:
                return table
        else:
            _msg = f"table id '{table_id}' not exists!"
            raise KeyError(_msg)

    def get_column_by_id(self, table_id: str, column_id: str):
        table = self.get_table_by_id(table_id=table_id)
        for column in table.columns:
            if column.key == column_id:
                return column
        else:
            _msg = f"no column (id: {column_id}) in table (id: {table_id})."
            raise KeyError(_msg)


class Deserializer:
    def __init__(
        self,
        metadata: Metadata,
        table_name: str,
        base_name: str = None,
        group_name: str = None,
        table_name_sep: str = "__",
        collaborators: List[User] = None,
    ):
        self.metadata = metadata
        self.table_name = table_name
        self.base_name = base_name
        self.group_name = group_name
        self.table_name_sep = table_name_sep
        self.collaborators = collaborators

        # get table
        for table in self.metadata.tables:
            if table.name == table_name:
                break
        else:
            _msg = f"table '{table_name}' not exists!"
            raise KeyError(_msg)
        self.table = table

        # get collaborator_map
        if self.collaborators:
            self.collaborator_map = {
                collaborator.email: f"{collaborator.name}({collaborator.contact_email})"
                for collaborator in self.collaborators
            }
        else:
            self.collaborator_map = None

        # prefix
        prefix = []
        if self.group_name:
            prefix.append(self.group_name)
        if self.base_name:
            prefix.append(self.base_name)
        self.table_name_prefix = self.table_name_sep.join(prefix)

        # helper
        self.mtime_column = None
        self.last_modified = None

        self.init_columns()

    @property
    @abstractmethod
    def Deserializer(self): ...

    @abstractmethod
    def schema(self): ...

    def generate_table_name(self):
        if self.table_name_prefix:
            return self.table_name_sep.join([self.table_name_prefix, self.table.name])
        return self.table.name

    def init_columns(self):
        LINK_REQUIRED = ["link", "link-formula"]
        COLLABORATOR_REQUIRED = ["user", "collaborator", "creator", "last-modifier"]

        column_keys = [c.key for c in self.table.columns]
        columns = [
            *[c.dict() for c in self.table.columns],
            *[c for c in SYSTEM_COLUMNS if c["key"] not in column_keys],
        ]

        self.columns = dict()
        for c in columns:
            # update column deserializer
            try:
                metadata = self.metadata if c["type"] in LINK_REQUIRED else None
                collaborator_map = self.collaborator_map if c["type"] in COLLABORATOR_REQUIRED else None
                deseriailizer = self.Deserializer[c["type"]](
                    name=c["name"],
                    seatable_type=c["type"],
                    data=c["data"],
                    metadata=metadata,
                    collaborator_map=collaborator_map,
                )
            except Exception:
                _msg = "create column deserializer failed - name: '{name}', seatable_type: '{type}', data: '{data}'.".format(
                    **c
                )
                raise CreateDeserializerFailed(_msg)
            self.columns.update({c["name"]: deseriailizer})

            # we need '_mtime' always!
            if c["type"] == "mtime" and c["name"] != "_mtime":
                self.mtime_column = c["name"]

        # we need '_mtime' always!
        if self.mtime_column:
            for c in SYSTEM_COLUMNS:
                if c["name"] == "_mtime":
                    self.columns.update(
                        {
                            "_mtime": self.Deserializer[c["type"]](
                                name=c["name"],
                                seatable_type=c["type"],
                                data=c["data"],
                            )
                        }
                    )
                    break

    def __call__(self, *row, select: list = None):
        if row is None:
            return

        if select == "*":
            select = None
        if select and not isinstance(select, list):
            select = [select]

        self.last_modified = None
        deserialized_rows = list()
        for r in row:
            deserialized_row = dict()
            for name in self.columns:
                if select and name not in select:
                    continue
                if name not in r:
                    continue
                value = self.columns[name](r[name])
                deserialized_row.update({name: value})
                if self.mtime_column and name == self.mtime_column:
                    self.last_modified = parse_str_datetime(r[name])
            if not select and self.mtime_column:
                deserialized_row.update({"_mtime": deserialized_row[self.mtime_column]})
            deserialized_rows.append(deserialized_row)

        return deserialized_rows
