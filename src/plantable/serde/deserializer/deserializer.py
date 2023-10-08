import logging

from functools import partial
from typing import Any, Callable, Dict, List, Union
from abc import abstractmethod

from ...model import Column, Table, User


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
        base_uuid: str = None,
        users: List[User] = None,
    ):
        self.table = table
        self.base_uuid = base_uuid or "unknown-base-uuid"
        self.users = {user.email: f"{user.name} ({user.contact_email})" for user in users} if users else None

        self.init_columns()

    @property
    @abstractmethod
    def Deserializer(self):
        ...

    @abstractmethod
    def schema(self):
        ...

    def generate_unique_table_name(self):
        return f"{self.base_uuid if self.base_uuid else 'unknown-base-uuid'}_{self.table.id}"

    def init_columns(self):
        column_keys = [c.key for c in self.table.columns]
        columns = [
            *[c.dict() for c in self.table.columns],
            *[c for c in SYSTEM_COLUMNS if c["key"] not in column_keys],
        ]

        self.columns = dict()
        for c in columns:
            name, _type, data = c["name"], c["type"], c["data"]
            try:
                deseriailizer = self.Deserializer[_type](name=name, seatable_type=_type, data=data, users=self.users)
            except Exception as ex:
                _msg = f"create column deserializer failed - name: '{name}', seatable_type: '{_type}', data: '{data}'."
                raise CreateDeserializerFailed(_msg)
            self.columns.update({c["name"]: deseriailizer})

    def __call__(self, *row):
        if row is None:
            return
        return [{name: self.columns[name](r[name]) for name in self.columns if name in r} for r in row]
