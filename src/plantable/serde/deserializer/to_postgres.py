import logging
from datetime import date, datetime
from typing import Any, List, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, BOOLEAN, DATE, FLOAT, INTEGER, SMALLINT, TEXT, TIMESTAMP, VARCHAR

from plantable.const import DT_FMT, TZ

from ...model import Column, Table, User, Metadata
from .deserializer import ColumnDeserializer, Deserializer

logger = logging.getLogger(__name__)


################################################################
# Postgres Types for SeaTable
################################################################
class PostgresId(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, VARCHAR(255), nullable=False, primary_key=True)

    def convert(self, x):
        return str(x)


class PostgresBool(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, BOOLEAN, nullable=True)

    def convert(self, x):
        return bool(x)


class PostgresText(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, TEXT, nullable=True)

    def convert(self, x):
        return str(x)


class PostgresEmail(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, VARCHAR(2083), nullable=True)

    def convert(self, x):
        return str(x)


class PostgresUrl(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, VARCHAR(2083), nullable=True)

    def convert(self, x):
        return str(x)


class PostgresRate(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, SMALLINT, nullable=True)

    def convert(self, x):
        return int(x)


class _PostgresInteger(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, INTEGER, nullable=True)

    def convert(self, x):
        return int(x)


class _PostgresFloat(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, FLOAT, nullable=True)

    def convert(self, x):
        return float(x)


class PostgresNumber(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        metadata: Metadata = None,
        collaborator_map: dict = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, metadata=metadata, collaborator_map=collaborator_map
        )
        if self.data.get("enable_precision"):
            self.sub_deserializer = _PostgresInteger(name=self.name, seatable_type=self.seatable_type, data=self.data)
        else:
            self.sub_deserializer = _PostgresFloat(name=self.name, seatable_type=self.seatable_type, data=self.data)

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        return self.sub_deserializer(x)


class _PostgresDate(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, DATE, nullable=True)

    def convert(self, x):
        return date.fromisoformat(x[:10])


class _PostgresDatetime(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, TIMESTAMP(timezone=True), nullable=True)

    def convert(self, x):
        if x.endswith("Z"):
            x = x.replace("Z", "+00:00", 1)
        try:
            x = datetime.strptime(x, DT_FMT)
        except Exception as ex:
            x = datetime.fromisoformat(x)
        return x.astimezone(TZ)


class PostgresDate(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        metadata: Metadata = None,
        collaborator_map: dict = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, metadata=metadata, collaborator_map=collaborator_map
        )
        if self.data and self.data["format"] == "YYYY-MM-DD":
            self.sub_deserializer = _PostgresDate(name=self.name, seatable_type=self.seatable_type, data=self.data)
        else:
            self.sub_deserializer = _PostgresDatetime(name=self.name, seatable_type=self.seatable_type, data=self.data)

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        return self.sub_deserializer(x)


class PostgresDuration(ColumnDeserializer):
    def schema(self):
        # [TODO] 지금은 "초"만 사용하는 정수 - 나중에 바꿀 것
        return sa.Column(self.name, INTEGER, nullable=True)

    def convert(self, x):
        return int(x)


class PostgresSingleSelect(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, VARCHAR(255), nullable=True)

    def convert(self, x):
        return str(x)


class PostgresMultipleSelect(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, ARRAY(VARCHAR(255)), nullable=True)

    def convert(self, x):
        return [str(_x) for _x in x]


class PostgresUser(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, VARCHAR(255), nullable=True)

    def convert(self, x):
        if not self.collaborator_map:
            return x
        return self.collaborator_map[x] if x in self.collaborator_map else x


class PostgresListcollaborator_map(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, ARRAY(VARCHAR(255)), nullable=True)

    def convert(self, x):
        if not self.collaborator_map:
            return x
        return [self.collaborator_map[_x] if _x in self.collaborator_map else _x for _x in x]


class PostgresFile(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, ARRAY(VARCHAR(2083)), nullable=True)

    def convert(self, x):
        return [_x["url"] for _x in x]


class PostgresImage(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, ARRAY(VARCHAR(2083)), nullable=True)

    def convert(self, x):
        return x


class PostgresAutoNumber(ColumnDeserializer):
    def schema(self):
        return sa.Column(self.name, VARCHAR(255), nullable=True)

    def convert(self, x):
        return str(x)


DESERIALIZER = {
    "row-id": PostgresId,
    "checkbox": PostgresBool,
    "bool": PostgresBool,  # formula가 result_type으로 bool을 사용
    "text": PostgresText,
    "string": PostgresText,  # formula가 result_type으로 string을 사용
    "button": PostgresText,
    "long-text": PostgresText,
    "email": PostgresEmail,
    "url": PostgresUrl,
    "rate": PostgresRate,
    "number": PostgresNumber,
    "date": PostgresDate,
    "duration": PostgresDuration,
    "ctime": PostgresDate,
    "mtime": PostgresDate,
    "single-select": PostgresSingleSelect,
    "multiple-select": PostgresText,
    "user": PostgresUser,
    "collaborator": PostgresListcollaborator_map,
    "creator": PostgresUser,
    "last-modifier": PostgresUser,
    "file": PostgresFile,
    "image": PostgresImage,
    "auto-number": PostgresAutoNumber,
}


class PostgresFormula(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        metadata: Metadata = None,
        collaborator_map: dict = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, metadata=metadata, collaborator_map=collaborator_map
        )
        self.sub_deserializer = DESERIALIZER[self.data["result_type"]](
            name=self.name, seatable_type=self.data["result_type"], data=dict()
        )

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        if x == "#VALUE!":
            return None
        return self.sub_deserializer(x)


class PostgresLink(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        metadata: Metadata = None,
        collaborator_map: dict = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, metadata=metadata, collaborator_map=collaborator_map
        )

        self.sub_deserializer = DESERIALIZER[self.data["array_type"]](
            name=self.name, seatable_type=self.data["array_type"], data=self.data["array_data"]
        )
        self.is_multiple = self.data["is_multiple"]

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        x = [self.sub_deserializer(_x.get("display_value")) for _x in x]
        if not x:
            return None
        if self.is_multiple:
            return x
        return x[0]


class PostgresLinkFormula(ColumnDeserializer):
    def __init__(
        self,
        name: str,
        seatable_type: str,
        data: dict = None,
        metadata: Metadata = None,
        collaborator_map: dict = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, metadata=metadata, collaborator_map=collaborator_map
        )

        self.sub_deserializer = DESERIALIZER[self.data["array_type"]](
            name=self.name, seatable_type=self.data["array_type"], data=self.data["array_data"]
        )

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        return [self.sub_deserializer(_x) for _x in x]


DESERIALIZER.update({"formula": PostgresFormula, "link": PostgresLink, "link-formula": PostgresText})


################################################################
# Seatable to Postgres
################################################################
class ToPostgres(Deserializer):
    Deserializer = DESERIALIZER

    def schema(self):
        name = self.generate_table_name()
        columns = [column.schema() for _, column in self.columns.items()]
        return sa.Table(name, sa.MetaData(), *columns)
