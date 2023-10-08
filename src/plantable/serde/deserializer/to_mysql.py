import logging
from datetime import date, datetime
from typing import Any, List, Union

from sqlalchemy import DATE, Boolean, Column, MetaData, Table
from sqlalchemy.dialects.mysql.types import DATETIME, FLOAT, INTEGER, TEXT, TIME, TINYINT, VARCHAR

from plantable import model as pm
from plantable.serde.const import DT_FMT, SYSTEM_FIELDS, TZ

from .deserializer import TableDeserializer

logger = logging.getLogger(__name__)


def _number(column: pm.Column):
    if column.data and column.data.get("enable_precision") and column.data["precision"] == 0:
        return Column(column.name, INTEGER, nullable=True)
    return Column(column.name, FLOAT, nullable=True)


def _date(column):
    if column.data and column.data["format"] == "YYYY-MM-DD":
        return Column(column.name, DATE, nullable=True)
    return Column(column.name, DATETIME, nullable=True)


SCHEMA_MAP = {
    "checkbox": lambda column: Column(column.name, TINYINT(1), nullable=True),
    "text": lambda column: Column(column.name, TEXT(), nullable=True),
    "string": lambda column: Column(column.name, TEXT(), nullable=True),
    "button": lambda column: Column(column.name, VARCHAR(255), nullable=True),
    "long-text": lambda column: Column(column.name, TEXT(), nullable=True),
    "email": lambda column: Column(column.name, VARCHAR(253), nullable=True),
    "url": lambda column: Column(column.name, VARCHAR(2083), nullable=True),
    "rate": lambda column: Column(column.name, INTEGER, nullable=True),
    "number": _number,
    "date": _date,
    "duration": lambda column: Column(column.name, TIME, nullable=True),
    "ctime": lambda column: Column(column.name, DATETIME, nullable=False),
    "mtime": lambda column: Column(column.name, DATETIME, nullable=False),
    "single-select": lambda column: Column(column.name, VARCHAR(512), nullable=True),
    "multiple-select": lambda column: Column(column.name, TEXT(), nullable=True),
    "link": lambda column: Column(column.name, VARCHAR(512), nullable=True),
    "link-formula": lambda column: Column(column.name, TEXT(), nullable=True),
    "user": lambda column: Column(column.name, VARCHAR(255), nullable=True),
    "collaborator": lambda column: Column(column.name, TEXT(), nullable=True),
    "creator": lambda column: Column(column.name, VARCHAR(255), nullable=True),
    "last-modifier": lambda column: Column(column.name, VARCHAR(255), nullable=True),
    "file": lambda column: Column(column.name, VARCHAR(2083), nullable=True),
    "image": lambda column: Column(column.name, VARCHAR(2083), nullable=True),
    "formula": lambda column: Column(column.name, TEXT(), nullable=True),
    "auto-number": lambda column: Column(column.name, VARCHAR(255), nullable=True),
}

SYSTEM_COLUMNS = {
    "_locked": Column("_locked", TINYINT(1), nullable=True),
    "_locked_by": Column("_locked_by", VARCHAR(255), nullable=True),
    "_archived": Column("_archived", TINYINT(1), nullable=True),
    "_ctime": Column("_ctime", DATETIME, nullable=False),
    "_mtime": Column("_mtime", DATETIME, nullable=False),
    "_creator": Column("_creator", VARCHAR(255), nullable=False),
    "_last_modifier": Column("_last_modifier", VARCHAR(255), nullable=False),
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
class ToMysql(TableDeserializer):
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
