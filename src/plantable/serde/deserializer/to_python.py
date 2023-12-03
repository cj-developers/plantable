import logging
from datetime import date, datetime
from typing import List

from ...const import DT_FMT, TZ
from ...model import Column, Table, User, Metadata
from .deserializer import ColumnDeserializer, Deserializer

logger = logging.getLogger(__name__)


################################################################
# Python Types for SeaTable
################################################################
class PythonCheckbox(ColumnDeserializer):
    def schema(self):
        return bool

    def convert(self, x):
        return bool(x)


class PythonText(ColumnDeserializer):
    def schema(self):
        return str

    def convert(self, x):
        return str(x)


class PythonRate(ColumnDeserializer):
    def schema(self):
        return int

    def convert(self, x):
        return int(x)


class _PythonInteger(ColumnDeserializer):
    def schema(self):
        return int

    def convert(self, x):
        return int(x)


class _PythonFloat(ColumnDeserializer):
    def schema(self):
        return float

    def convert(self, x):
        return float(x)


class PythonNumber(ColumnDeserializer):
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
            self.sub_deserializer = _PythonInteger(name=self.name, seatable_type=self.seatable_type, data=self.data)
        else:
            self.sub_deserializer = _PythonFloat(name=self.name, seatable_type=self.seatable_type, data=self.data)

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        return self.sub_deserializer(x)


class _PythonDate(ColumnDeserializer):
    def schema(self):
        return date

    def convert(self, x):
        return date.fromisoformat(x[:10])


class _PythonDatetime(ColumnDeserializer):
    def schema(self):
        return datetime

    def convert(self, x):
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
        metadata: Metadata = None,
        collaborator_map: dict = None,
    ):
        super().__init__(
            name=name, seatable_type=seatable_type, data=data, metadata=metadata, collaborator_map=collaborator_map
        )
        if self.data and self.data["format"] == "YYYY-MM-DD":
            self.sub_deserializer = _PythonDate(name=self.name, seatable_type=self.seatable_type, data=self.data)
        else:
            self.sub_deserializer = _PythonDatetime(name=self.name, seatable_type=self.seatable_type, data=self.data)

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        return self.sub_deserializer(x)


class PythonDuration(ColumnDeserializer):
    def schema(self):
        return int

    def convert(self, x):
        return x


class PythonSingleSelect(ColumnDeserializer):
    def schema(self):
        return str

    def convert(self, x):
        return str(x)


class PythonMulitpleSelect(ColumnDeserializer):
    def schema(self):
        return List[str]

    def convert(self, x):
        return [str(_x) for _x in x]


class PythonUser(ColumnDeserializer):
    def schema(self):
        return str

    def convert(self, x):
        if not self.collaborator_map:
            return x
        return self.collaborator_map[x] if x in self.collaborator_map else x


class PythonListUsers(ColumnDeserializer):
    def schema(self):
        return List[str]

    def convert(self, x):
        if not self.collaborator_map:
            return x
        return [self.collaborator_map[_x] if _x in self.collaborator_map else _x for _x in x]


class PythonFile(ColumnDeserializer):
    def schema(self):
        return List[str]

    def convert(self, x):
        return [_x["url"] for _x in x]


class PythonImage(ColumnDeserializer):
    def schema(self):
        return List[str]

    def convert(self, x):
        return x


class PythonAutoNumber(ColumnDeserializer):
    def schema(self):
        return str

    def convert(self, x):
        return str(x)


DESERIALIZER = {
    "row-id": PythonText,
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


class PythonLink(ColumnDeserializer):
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

        if "array_type" in data:
            array_type = self.data["array_type"]
            array_data = self.data["array_data"]
        else:
            column = self.get_column_by_id(table_id=data["table_id"], column_id=data["display_column_key"])
            array_type = column.type
            array_data = column.data

        self.sub_deserializer = DESERIALIZER[array_type](name=self.name, seatable_type=array_type, data=array_data)
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


class PythonLinkFormula(ColumnDeserializer):
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

        if "array_type" in data:
            array_type = self.data["array_type"]
            array_data = self.data["array_data"]
        else:
            column = self.get_column_by_id(table_id=data["table_id"], column_id=data["display_column_key"])
            array_type = column.type
            array_data = column.data

        self.sub_deserializer = DESERIALIZER[array_type](name=self.name, seatable_type=array_type, data=array_data)

    def schema(self):
        return self.sub_deserializer.schema()

    def convert(self, x):
        return [self.sub_deserializer(_x) for _x in x]


DESERIALIZER.update(
    {
        "formula": PythonFormula,
        "link": PythonLink,
        "link-formula": PythonLinkFormula,
    }
)


################################################################
# Seatable To Python Deserializer
################################################################
class ToPython(Deserializer):
    Deserializer = DESERIALIZER

    def schema(self):
        return {
            "name": self.generate_table_name(),
            "columns": [{name: column.schema()} for name, column in self.columns.items()],
        }
