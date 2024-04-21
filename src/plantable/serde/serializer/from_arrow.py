import parse
import pyarrow as pa

from ...const import SYSTEM_FIELDS
from ...model.column import (
    AutoNumber,
    Button,
    Checkbox,
    Collaborator,
    CreationTime,
    Creator,
    Date,
    Datetime,
    Duration,
    Email,
    File,
    Formula,
    Image,
    Integer,
    LastModificationTime,
    LastModifier,
    Link,
    LinkFomula,
    LongText,
    MultipleSelect,
    Number,
    Rate,
    SingleSelect,
    Text,
    Url,
)

# Arrow to Seatable Schema
SCHEMA_MAP = {
    "bool": Checkbox,
    "int8": Integer,
    "int16": Integer,
    "int32": Integer,
    "int64": Integer,
    "uint8": Integer,
    "uint16": Integer,
    "uint32": Integer,
    "uint64": Integer,
    "halffloat": Number,  # float16
    "float": Number,  # float32
    "double": Number,  # float64
    "time32": Duration,
    "time64": Duration,
    "timestamp": Datetime,
    "date32": Date,
    "date64": Date,
    "duration": Duration,
    "string": Text,
    "utf8": Text,
    "large_string": LongText,
    "large_utf8": LongText,
    "decimal128": Number,
    "list": MultipleSelect,
    "large_list": MultipleSelect,
}


ARROW_STR_DTYPE_PATTERNS = [
    parse.compile("{dtype}[{unit}, tz={tz}]"),
    parse.compile("{dtype}[{unit}]"),
    parse.compile("{dtype}({precision}, {scale})"),
    parse.compile("{dtype}<item: {item}>"),
    parse.compile("{dtype}<item: {item}>[{list_size}]"),
    parse.compile("{dtype}"),
]


class FromArrowTable:
    def __init__(self, tbl: pa.Table):
        self.tbl = tbl
        self._schema = [(c, str(tbl.schema.field(c).type)) for c in tbl.schema.names]

        # get deserializer opts
        self.opts = {column: self.dtype_parser(dtype) for column, dtype in self._schema}

        # seatable schema
        self.columns = [
            SCHEMA_MAP[opt["dtype"]](name=name) for name, opt in self.opts.items() if name not in SYSTEM_FIELDS
        ]

    @staticmethod
    def dtype_parser(x):
        for pattern in ARROW_STR_DTYPE_PATTERNS:
            r = pattern.parse(x)
            if r:
                return r.named

    def get_rows_for_append(self):
        return [{k: v for k, v in r.items() if k not in SYSTEM_FIELDS} for r in self.tbl.to_pylist()]

    def get_rows_for_update(self, row_id_field: str = "_id"):
        updates = list()
        for row in self.tbl.to_pylist():
            updates.append(
                {
                    "row_id": row[row_id_field],
                    "row": {k: v for k, v in row.items() if k not in SYSTEM_FIELDS},
                }
            )
        return updates

    def null(self, value):
        pass

    def bool(self, value):
        pass

    def int8(self, value):
        pass

    def int16(self, value):
        pass

    def int32(self, value):
        pass

    def int64(self, value):
        pass

    def uint8(self, value):
        pass

    def uint16(self, value):
        pass

    def uint32(self, value):
        pass

    def uint64(self, value):
        pass

    def halffloat(self, value):
        pass

    def float(self, value):
        pass

    def double(self, value):
        pass

    def time32(self, value, unit):
        pass

    def time64(self, value, unit):
        pass

    def timestamp(self, value, unit, tz: str = None):
        pass

    def date32(self, value, unit: str):
        pass

    def date64(self, value, unit: str):
        pass

    def duration(self, value, unit: str):
        pass

    def string(self, value):
        pass

    def large_string(self, value):
        pass

    def decimal128(self, value, precision: int = 0, scale: int = 0):
        pass

    def list(self, value, item, list_size: int = -1):
        pass

    def large_list(self, value, item, list_size: int = -1):
        pass
