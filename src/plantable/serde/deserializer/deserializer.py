import logging
from datetime import date, datetime
from functools import partial
from typing import Any, List, Union

from ...model import Column, Table, User
from ..const import DT_FMT, TZ

logger = logging.getLogger(__name__)


SYSTEM_SCHEMA = {
    "_id": {"key": "_id", "type": "text", "data": None},
    "_locked": {"key": "_locked", "type": "checkbox", "data": None},
    "_locked_by": {"key": "_locked_by", "type": "text", "data": None},
    "_archived": {"key": "_archived", "type": "checkbox", "data": None},
    "_creator": {"key": "_creator", "type": "creator", "data": None},
    "_ctime": {"key": "_ctime", "type": "ctime", "data": None},
    "_mtime": {"key": "_mtime", "type": "mtime", "data": None},
    "_last_modifier": {"key": "_last_modifier", "type": "last-modifier", "data": None},
}


class DeserializeError(Exception):
    pass


class ColumnDeserializer:
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        nullable: bool = True,
        users: List[dict] = None,
        link_column: Column = None,
    ):
        self.name = name
        self.seatable_type = seatable_type
        self.data = data
        self.nullable = nullable
        self.users = users
        self.link_column = link_column

    def __call__(self, x):
        if not x:
            return None
        try:
            return self.converter(x)
        except Exception as ex:
            _msg = f"deserialize failed: {self.seatable_type}({x})."
            raise DeserializeError(_msg)

    def schema(self):
        raise NotImplementedError

    def converter(self, x):
        raise NotImplementedError


class PythonCheckbox(ColumnDeserializer):
    def schema(self):
        return bool

    def converter(self, x):
        return bool(x)


class PythonText(ColumnDeserializer):
    def schema(self):
        return str

    def converter(self, x):
        return str(x)


class PythonRate(ColumnDeserializer):
    def schema(self):
        return int

    def converter(self, x):
        return int(x)


class _PythonInteger(ColumnDeserializer):
    def schema(self):
        return int

    def converter(self, x):
        return int(x)


class _PythonFloat(ColumnDeserializer):
    def schema(self):
        return float

    def converter(self, x):
        return float(x)


class PythonNumber(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        nullable: bool = True,
        users: List[dict] = None,
        link_column: Column = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, nullable=nullable, users=users, link_column=link_column
        )
        if self.data.get("enable_precision"):
            self.sub_deserializer = _PythonInteger(
                name=self.name, seatable_type=self.seatable_type, data=self.data, nullable=self.nullable
            )
        else:
            self.sub_deserializer = _PythonFloat(
                name=self.name, seatable_type=self.seatable_type, data=self.data, nullable=self.nullable
            )

    def schema(self):
        return self.sub_deserializer.schema()

    def converter(self, x):
        return self.sub_deserializer(x)


class _PythonDate(ColumnDeserializer):
    def schema(self):
        return date

    def converter(self, x):
        return date.fromisoformat(x[:10])


class _PythonDatetime(ColumnDeserializer):
    def schema(self):
        return datetime

    def converter(self, x):
        if x.endswith("Z"):
            x = x.replace("Z", "+00:00", 1)
        try:
            x = datetime.strptime(x, DT_FMT)
        except Exception as ex:
            x = datetime.fromisoformat(x)
        return x.astimezone(TZ)


class PythonDate(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        nullable: bool = True,
        users: List[dict] = None,
        link_column: Column = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, nullable=nullable, users=users, link_column=link_column
        )
        if self.data and self.data["format"] == "YYYY-MM-DD":
            self.sub_deserializer = _PythonDate(
                name=self.name, seatable_type=self.seatable_type, data=self.data, nullable=self.nullable
            )
        else:
            self.sub_deserializer = _PythonDatetime(
                name=self.name, seatable_type=self.seatable_type, data=self.data, nullable=self.nullable
            )

    def schema(self):
        return self.sub_deserializer.schema()

    def converter(self, x):
        return self.sub_deserializer(x)


class PythonDuration(ColumnDeserializer):
    def schema(self):
        return int

    def converter(self, x):
        return x


class PythonSingleSelect(ColumnDeserializer):
    def schema(self):
        return str

    def converter(self, x):
        return str(x)


class PythonMulitpleSelect(ColumnDeserializer):
    def schema(self):
        return List[str]

    def converter(self, x):
        return [str(_x) for _x in x]


class PythonUser(ColumnDeserializer):
    def schema(self):
        return str

    def converter(self, x):
        return str(x)


class PythonListUsers(ColumnDeserializer):
    def schema(self):
        return List[str]

    def converter(self, x):
        return [str(_x) for _x in x]


class PythonFile(ColumnDeserializer):
    def schema(self):
        return List[str]

    def converter(self, x):
        return [_x["url"] for _x in x]


class PythonImage(ColumnDeserializer):
    def schema(self):
        return List[str]

    def converter(self, x):
        return [_x["url"] for _x in x]


class PythonAutoNumber(ColumnDeserializer):
    def schema(self):
        return str

    def converter(self, x):
        return str(x)


COLUMN_DESERIALIZER = {
    "checkbox": PythonCheckbox,
    "text": PythonText,
    "long-text": PythonText,
    "string": PythonText,  # [NOTE] formula column의 result_type이 'text'가 아닌 'string'을 반환.
    "button": PythonText,
    "email": PythonText,
    "url": PythonText,
    "rate": PythonRate,
    "number": PythonNumber,
    "date": PythonDate,
    "duration": PythonDuration,
    "ctime": PythonDate,
    "mtime": PythonDate,
    "single-select": PythonSingleSelect,
    "multiple-select": PythonMulitpleSelect,
    "user": PythonUser,
    "collaborator": PythonListUsers,
    "creator": PythonUser,
    "last-modifier": PythonUser,
    "file": PythonFile,
    "image": PythonImage,
    "auto-number": PythonAutoNumber,
}


class PythonFormula(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        nullable: bool = True,
        users: List[dict] = None,
        link_column: Column = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, nullable=nullable, users=users, link_column=link_column
        )
        self.sub_deserializer = COLUMN_DESERIALIZER[self.data["result_type"]](
            name=self.name, seatable_type=self.data["result_type"], data=dict()
        )

    def schema(self):
        return self.sub_deserializer.schema()

    def converter(self, x):
        if x == "#VALUE!":
            return None
        return self.sub_deserializer(x)


class PythonLink(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        nullable: bool = True,
        users: List[dict] = None,
        link_column: Column = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, nullable=nullable, users=users, link_column=link_column
        )

        self.sub_deserializer = COLUMN_DESERIALIZER[self.data["array_type"]](
            name=self.name, seatable_type=self.data["array_type"], data=self.data["array_data"]
        )
        self.is_multiple = self.data["is_multiple"]

    def schema(self):
        return self.sub_deserializer.schema()

    def converter(self, x):
        x = [self.sub_deserializer(_x.get("display_value")) for _x in x]
        if not x:
            return None
        if self.is_multiple:
            return x
        return x[0]


class PythonLinkFormula(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        nullable: bool = True,
        users: List[dict] = None,
        link_column: Column = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, nullable=nullable, users=users, link_column=link_column
        )

        self.sub_deserializer = COLUMN_DESERIALIZER[self.data["array_type"]](
            name=self.name, seatable_type=self.data["array_type"], data=self.data["array_data"]
        )

    def schema(self):
        return self.sub_deserializer.schema()

    def converter(self, x):
        return [self.sub_deserializer(_x) for _x in x]


COLUMN_DESERIALIZER.update({"formula": PythonFormula, "link": PythonLink, "link-formula": PythonLinkFormula})
USER_FIELDS = ["user", "collaborator", "creator", "last-midifier"]


class TableDeserializer:
    def __init__(self, table: Table, users: List[User] = None):
        self.table = table
        self.users = users

        hidden_columns = SYSTEM_SCHEMA.copy()
        self.column_key_map = dict()
        self.columns = dict()

        for c in self.table.columns:
            self.columns.update({c.name: COLUMN_DESERIALIZER[c.type](name=c.name, seatable_type=c.type, data=c.data)})
            self.column_key_map.update({c.key: c.name})
            if c.key in hidden_columns:
                hidden_columns.pop(c.key)
        for name in hidden_columns:
            hc = hidden_columns[name]
            self.columns.update(
                {name: COLUMN_DESERIALIZER[hc["type"]](name=name, seatable_type=hc["type"], data=hc["data"])}
            )
            self.column_key_map.update({hc["key"]: name})

        # user map
        if users:
            self.user_map = {user.email: f"{user.name} ({user.contact_email})" for user in self.users}
        else:
            self.user_map = None

    def __call__(self, *row):
        if row is None:
            return
        return [{name: self.columns[name](r[name]) for name in self.columns if name in r} for r in row]
