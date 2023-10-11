import logging
from abc import abstractmethod
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Union

from ...model import Column, Table, User
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
        users: List[dict] = None,
        link_column: Column = None,
    ):
        self.name = name
        self.seatable_type = seatable_type
        self.data = data
        self.users = users
        self.link_column = link_column

    def __call__(self, x):
        if not x:
            return None
        try:
            return self.convert(x)
        except Exception as ex:
            _msg = f"deserialize failed: {self.seatable_type}({x})."
            raise DeserializeError(_msg)

    def schema(self):
        raise NotImplementedError

    def convert(self, x):
        raise NotImplementedError


class Deserializer:
    def __init__(
        self,
        table: Table,
        group_name: str = None,
        base_name: str = None,
        table_name_sep: str = "__",
        users: List[User] = None,
    ):
        self.table = table
        self.group_name = group_name
        self.base_name = base_name
        self.table_name_sep = table_name_sep
        self.users = {user.email: f"{user.name} ({user.contact_email})" for user in users} if users else None

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
    def Deserializer(self):
        ...

    @abstractmethod
    def schema(self):
        ...

    def generate_table_name(self):
        if self.table_name_prefix:
            return self.table_name_sep.join([self.table_name_prefix, self.table.name])
        return self.table.name

    def init_columns(self):
        column_keys = [c.key for c in self.table.columns]
        columns = [
            *[c.dict() for c in self.table.columns],
            *[c for c in SYSTEM_COLUMNS if c["key"] not in column_keys],
        ]

        self.columns = dict()
        for c in columns:
            # update column deserializer
            try:
                deseriailizer = self.Deserializer[c["type"]](
                    name=c["name"], seatable_type=c["type"], data=c["data"], users=self.users
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
                                name=c["name"], seatable_type=c["type"], data=c["data"], users=self.users
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
