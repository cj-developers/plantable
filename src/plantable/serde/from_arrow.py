import parse
import pyarrow as pa

from .const import SYSTEM_FIELDS

# SeaTable dtypes
CHECKBOX = {"column_type": "checkbox"}
TEXT = {"column_type": "text"}
LONG_TEXT = {"column_type": "long-text"}
INTEGER = {
    "column_type": "number",
    "column_data": {"format": "number", "decimal": "dot", "thousands": "comma"},
}
NUMBER = {
    "column_type": "number",
    "column_data": {"format": "number", "decimal": "dot", "thousands": "comma"},
}
DURATION = {"column_type": "duration", "column_data": {"format": "h:mm:ss"}}
DATE = {"column_type": "date", "column_data": {"format": "YYYY-MM-DD"}}
DATETIME = {"column_type": "date", "column_data": {"format": "YYYY-MM-DD HH:mm"}}
SINGLE_SELECT = {"column_type": "single-select"}
MULTIPLE_SELECT = {"column_type": "multiple-select"}

# Arrow to Seatable Schema
SCHEMA_MAP = {
    "null": TEXT,
    "bool": CHECKBOX,
    "int8": INTEGER,
    "int16": INTEGER,
    "int32": INTEGER,
    "int64": INTEGER,
    "uint8": INTEGER,
    "uint16": INTEGER,
    "uint32": INTEGER,
    "uint64": INTEGER,
    "halffloat": NUMBER,  # float16
    "float": NUMBER,  # float32
    "double": NUMBER,  # float64
    "time32": DURATION,
    "time64": DURATION,
    "timestamp": DATETIME,
    "date32": DATE,
    "date64": DATE,
    "duration": DURATION,
    "string": TEXT,
    "utf8": TEXT,
    "large_string": LONG_TEXT,
    "large_utf8": LONG_TEXT,
    "decimal128": NUMBER,
    "list": MULTIPLE_SELECT,
    "large_list": MULTIPLE_SELECT,
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
            {"column_name": name, **SCHEMA_MAP[opt["dtype"]]}
            for name, opt in self.opts.items()
            if name not in SYSTEM_FIELDS
        ]

    @staticmethod
    def dtype_parser(x):
        for pattern in ARROW_STR_DTYPE_PATTERNS:
            r = pattern.parse(x)
            if r:
                return r.named

    def get_rows_for_append(self):
        return [
            {k: v for k, v in r.items() if k not in SYSTEM_FIELDS}
            for r in self.tbl.to_pylist()
        ]

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
