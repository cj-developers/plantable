import pyarrow as pa
import parse

CHECKBOX = {"column_type": "checkbox"}
TEXT = {"column_type": "text"}
LONG_TEXT = {"column_type": "long-text"}
INTEGER = {"column_type": "number", "column_data": {"format": "number", "decimal": "dot", "thousands": "comma"}}
NUMBER = {"column_type": "number", "column_data": {"format": "number", "decimal": "dot", "thousands": "comma"}}
DURATION = {"column_type": "duration", "column_data": {"format": "h:mm:ss"}}
DATE = {"column_type": "date", "column_data": {"format": "YYYY-MM-DD"}}
DATETIME = {"column_type": "date", "column_data": {"format": "YYYY-MM-DD HH:mm"}}
SINGLE_SELECT = {"column_type": "single-select"}
MULTIPLE_SELECT = {"column_type": "multiple-select"}

SCHMEA_MAP = {
    "bool": CHECKBOX,
    "int8": INTEGER,
    "int16": INTEGER,
    "int32": INTEGER,
    "int64": INTEGER,
    "uint8": INTEGER,
    "uint16": INTEGER,
    "uint32": INTEGER,
    "uint64": INTEGER,
    "float16": NUMBER,
    "float32": NUMBER,
    "float64": NUMBER,
    "time32(unit)": DURATION,
    "time64(unit)": DURATION,
    "timestamp(unit[, tz])": DATETIME,
    "date32": DATE,
    "date64": DATE,
    "duration(unit)": DURATION,
    "string": TEXT,
    "utf8": TEXT,
    "large_string": LONG_TEXT,
    "large_utf8": LONG_TEXT,
    "decimal128(int precision, int scale=0)": NUMBER,
    "list_(value_type, int list_size=-1)": MULTIPLE_SELECT,
}

ARROW_STR_DTYPE_PATTERNS = [
    parse.compile("{dtype}[{unit}, tz={tz}]"),
    parse.compile("{dtype}[{unit}]"),
    parse.compile("{dtype}({m}, {d})"),
    parse.compile("{dtype}<item: {item_dtype}>"),
]


class RowsFromArrowTable:
    def __init__(self, tbl: pa.Table):
        self.tbl = tbl
        self.schema = [(c, str(tbl.schema.field(c).type)) for c in tbl.schema.names]

    def seatable_schema(self):
        schema = dict()
        for column, dtype in self.schema:
            for pattern in ARROW_STR_DTYPE_PATTERNS:
                r = pattern.parse(dtype)
                if r:
                    break

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

    def float16(self, value):
        pass

    def float32(self, value):
        pass

    def float64(self, value):
        pass

    def time32(self, value, unit):
        pass

    def time64(self, value, unit):
        pass

    def timestamp(self, value, unit, tz: str = None):
        pass

    def date32(self, value):
        pass

    def date64(self, value):
        pass

    def duration(self, value, unit):
        pass

    def string(self, value):
        pass

    def utf8(self, value):
        pass

    def large_string(self, value):
        pass

    def large_utf8(self, value):
        pass

    def decimal128(self, value, precision, scale: int = 0):
        pass

    def list_(self, value, value_type, list_size: int = 0):
        pass
