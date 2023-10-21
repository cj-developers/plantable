################################
# UNDER DEV. - DO NOT USE
# ToPostgres에 맞추어서 다시 작성할 것!
################################

import logging

from plantable import model as pm
from plantable.const import DT_FMT, TZ

from ...model import Table
from .deserializer import Deserializer

logger = logging.getLogger(__name__)


AVRO_BOOLEAN_NULLABLE = ["null", "int"]
AVRO_STRING = "string"
AVRO_STRING_NULLABLE = ["null", "string"]
AVRO_INT_NULLABLE = ["null", "int"]
AVRO_LONG_NULLABLE = ["null", "long"]
AVRO_DOUBLE_NULLABLE = ["null", "double"]
AVRO_DATE_NULLABLE = ["nulll", {"type": "int", "logicalType": "date"}]
AVRO_TIMESTAMP = {"type": "int", "logicalType": "timestamp-millis"}
AVRO_TIMESTAMP_NULLABLE = ["nulll", {"type": "int", "logicalType": "timestamp-millis"}]
AVRO_DURATION_NULLABLE = ["nulll", {"type": "int", "logicalType": "duration"}]
AVRO_STRING_ARRAY_NULLABLE = ["null", {"type": "array", "items": "string"}]


def convert_number(column: pm.Column):
    if column.data and column.data.get("enable_precision") and column.data["precision"] == 0:
        return {"name": column.name, "type": AVRO_LONG_NULLABLE}
    return {"name": column.name, "type": AVRO_DOUBLE_NULLABLE}


def convert_date(column):
    if column.data and column.data["format"] == "YYYY-MM-DD":
        return {"name": column.name, "type": AVRO_DATE_NULLABLE}
    return {"name": column.name, "type": AVRO_TIMESTAMP_NULLABLE}


SCHEMA_MAP = {
    "checkbox": lambda column: {"name": column.name, "type": AVRO_BOOLEAN_NULLABLE},
    "text": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "string": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "button": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "long-text": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "email": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "url": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "rate": lambda column: {"name": column.name, "type": AVRO_INT_NULLABLE},
    "number": convert_number,
    "date": convert_date,
    "duration": lambda column: {"name": column.name, "type": AVRO_DURATION_NULLABLE},
    "ctime": lambda column: {"name": column.name, "type": AVRO_TIMESTAMP},
    "mtime": lambda column: {"name": column.name, "type": AVRO_TIMESTAMP},
    "single-select": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "multiple-select": lambda column: {"name": column.name, "type": AVRO_STRING_ARRAY_NULLABLE},
    "link": lambda column: {"name": column.name, "type": AVRO_STRING_ARRAY_NULLABLE},  # CHECK
    "link-formula": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "user": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "collaborator": lambda column: {"name": column.name, "type": AVRO_STRING_ARRAY_NULLABLE},  # CHECK
    "creator": lambda column: {"name": column.name, "type": AVRO_STRING},
    "last-modifier": lambda column: {"name": column.name, "type": AVRO_STRING},
    "file": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "image": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "formula": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
    "auto-number": lambda column: {"name": column.name, "type": AVRO_STRING_NULLABLE},
}

SYSTEM_COLUMNS = {
    "_locked": {"name": "_locked", "type": AVRO_BOOLEAN_NULLABLE},
    "_locked_by": {"name": "_locked_by", "type": AVRO_STRING_NULLABLE},
    "_archived": {"name": "_archived", "type": AVRO_BOOLEAN_NULLABLE},
    "_ctime": {"name": "_ctime", "type": AVRO_TIMESTAMP},
    "_mtime": {"name": "_mtime", "type": AVRO_TIMESTAMP},
    "_creator": {"name": "_creator", "type": AVRO_STRING},
    "_last_modifier": {"name": "_last_modifier", "type": AVRO_STRING},
}


################################################################
# Converter
################################################################
class ToAvro(Deserializer):
    def init_schema(self):
        # copy system columns
        hidden_fields = SYSTEM_COLUMNS.copy()

        # add fields
        fields = [{"name": "_id", "type": AVRO_STRING}]
        for c in self.table.columns:
            fields.append(SCHEMA_MAP[c.type](c))
            if c.key in hidden_fields:
                hidden_fields.pop(c.key)

        # ramained hidden fields
        for c in hidden_fields:
            fields.append(hidden_fields[c])

        return {"namespace": "", "type": "record", "name": self.table.name, "fields": fields}
