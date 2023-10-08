import logging
from datetime import date, datetime
from typing import Any, List, Union

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIGINT,
    BIT,
    BOOLEAN,
    BYTEA,
    CHAR,
    CIDR,
    CITEXT,
    DATE,
    DATEMULTIRANGE,
    DATERANGE,
    DOMAIN,
    DOUBLE_PRECISION,
    ENUM,
    FLOAT,
    HSTORE,
    INET,
    INT4MULTIRANGE,
    INT4RANGE,
    INT8MULTIRANGE,
    INT8RANGE,
    INTEGER,
    INTERVAL,
    JSON,
    JSONB,
    JSONPATH,
    MACADDR,
    MACADDR8,
    MONEY,
    NUMERIC,
    NUMMULTIRANGE,
    NUMRANGE,
    OID,
    REAL,
    REGCLASS,
    REGCONFIG,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    TSMULTIRANGE,
    TSQUERY,
    TSRANGE,
    TSTZMULTIRANGE,
    TSTZRANGE,
    TSVECTOR,
    UUID,
    VARCHAR,
)

from plantable import model as pm
from plantable.serde.const import DT_FMT, SYSTEM_FIELDS, TZ

from .deserializer import Deserializer

logger = logging.getLogger(__name__)


SYSTEM_COLUMNS = {
    "_id": {"key": "_id", "type": "text", "data": None},
    "_locked": {"key": "_locked", "type": "checkbox", "data": None},
    "_locked_by": {"key": "_locked_by", "type": "text", "data": None},
    "_archived": {"key": "_archived", "type": "checkbox", "data": None},
    "_creator": {"key": "_creator", "type": "creator", "data": None},
    "_ctime": {"key": "_ctime", "type": "ctime", "data": None},
    "_mtime": {"key": "_mtime", "type": "mtime", "data": None},
    "_last_modifier": {"key": "_last_modifier", "type": "last-modifier", "data": None},
}


class PostgresType:
    def __init__(self, name: str, data: dict = None, nullable: bool = True):
        self.name = name
        self.data = data
        self.nullable = nullable
        self.subtype = None

    def __call__(self, x):
        return {self.name: x if x is None else self.converter(x)}

    def schema(self):
        raise NotImplementedError

    def converter(self, x):
        return x


class PostgresCheckbox(PostgresType):
    def schema(self):
        return Column(self.name, BOOLEAN, nullable=self.nullable)


class PostgresText(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresNumber(PostgresType):
    def __init__(self, name: str, data: dict = None, nullable: bool = True):
        super().__init__(name=name, data=data, nullable=nullable)
        if self.data and self.data.get("enable_precision") and self.data["precision"] == 0:
            self.subType = "INTEGER"

    def schema(self):
        if self.subType == "INTEGER":
            return Column(self.name, INTEGER, nullable=self.nullable)
        return Column(self.name, FLOAT, nullable=self.nullable)

    def converter(self, x):
        if self.subType == "INTEGER":
            return int(x)
        return float(x)


class PostgresDate(PostgresType):
    def __init__(self, name: str, data: dict = None, nullable: bool = True):
        super().__init__(name=name, data=data, nullable=nullable)
        if self.data and self.data["format"] == "YYYY-MM-DD":
            self.subType = "TIMESTAMP"

    def schema(self):
        if self.subType == "TIMESTAMP":
            return Column(self.name, TIMESTAMP, nullable=self.nullable)
        return Column(self.name, DATE, nullable=self.nullable)

    def converter(self, x):
        if self.subType == "TIMESTAMP":
            return x
        return x


class PostgresDuration(PostgresType):
    def schema(self):
        return Column(self.name, INTERVAL("seconds"), nullable=self.nullable)


class PostgresTimestamp(PostgresType):
    def schema(self):
        return Column(self.name, TIMESTAMP, self.nullable)

    def converter(self, x):
        return x


class PostgresSingleSelect(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresMultipleSelect(PostgresType):
    def schema(self):
        return Column(self.name, ARRAY(TEXT), nullable=self.nullable)


class PostgresLink(PostgresType):
    def schema(self):
        return Column(self.name, ARRAY(TEXT), nullable=self.nullable)


class PostgresLinkFormula(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresUser(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresCollaborator(PostgresType):
    def schema(self):
        return Column(self.name, ARRAY(TEXT), nullable=self.nullable)


class PostgresCreator(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresLastModifier(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresFile(PostgresType):
    def schema(self):
        return Column(self.name, ARRAY(TEXT), nullable=self.nullable)


class PostgresImage(PostgresType):
    def schema(self):
        return Column(self.name, ARRAY(TEXT), nullable=self.nullable)


class PostgresFormula(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


class PostgresAutoNumber(PostgresType):
    def schema(self):
        return Column(self.name, TEXT, nullable=self.nullable)


SCHEMA_MAP = {
    "checkbox": PostgresCheckbox,
    "text": PostgresText,
    "string": PostgresText,
    "button": PostgresText,
    "long-text": PostgresText,
    "email": PostgresText,
    "url": PostgresText,
    "rate": PostgresText,
    "number": PostgresNumber,
    "date": PostgresDate,
    "duration": PostgresDuration,
    "ctime": PostgresTimestamp,
    "mtime": PostgresTimestamp,
    "single-select": PostgresSingleSelect,
    "multiple-select": PostgresText,
    "link": PostgresLink,
    "link-formula": PostgresText,
    "user": PostgresUser,
    "collaborator": PostgresCollaborator,
    "creator": PostgresCreator,
    "last-modifier": PostgresLastModifier,
    "file": PostgresFile,
    "image": PostgresImage,
    "formula": PostgresFormula,
    "auto-number": PostgresAutoNumber,
}


def seatable_to_mysql_table(table: pm.Table, table_name_prefix: str = None) -> List[Column]:
    # copy system columns
    sys_columns = SYSTEM_COLUMNS.copy()

    # add columns
    columns = [Column("_id", VARCHAR(32), primary_key=True)]
    for c in table.columns:
        columns.append(SCHEMA_MAP[c.type](c))
        if c.key in sys_columns:
            sys_columns.pop(c.key)

    # add system columns if is not added
    for c in sys_columns:
        columns.append(sys_columns[c])

    table_name = table.name if not table_name_prefix else "__".join([table_name_prefix, table.name])
    return Table(table_name, MetaData(), *columns)


################################################################
# Converter
################################################################
class ToPostgres(Deserializer):
    def __init__(self, table: Table, users: dict = None):
        self.table = table
        self.users = users

        self.columns = {
            **{column.name: {"column_type": column.type, "column_data": column.data} for column in table.columns},
            **SYSTEM_FIELDS,
        }

        self.schema = self.init_schema()

        self.user_map = (
            {user.email: f"{user.name} ({user.contact_email})" for user in self.users} if self.users else None
        )
        self.row_id_map = {column.key: column.name for column in table.columns}

    def init_schema(self):
        # copy system columns
        hidden_fields = SYSTEM_COLUMNS.copy()

        # add fields
        fields = [Column("_locked", VARCHAR(255), nullable=True)]
        for c in self.table.columns:
            fields.append(SCHEMA_MAP[c.type](c))
            if c.key in hidden_fields:
                hidden_fields.pop(c.key)

        # ramained hidden fields
        for c in hidden_fields:
            fields.append(hidden_fields[c])

        return Table(self.table.name, MetaData(), *fields)
