from .model import Table
from datetime import datetime

RESERVED_COLUMNS = ["_id", "_locked", "_locked_by", "_archived", "_creator", "_ctime", "_mtime", "_last_modifier"]

SCHEMA_MAP = {
    "text": "text",
    "long_text": "long-text",
    "number": "number",
    "collaborator": "collaborator",
    "date": "date",
    "duration": "duration",
    "single_select": "single-select",
    "multiple_select": "multiple-select",
    "image": "image",
    "file": "file",
    "email": "email",
    "url": "url",
    "checkbox": "checkbox",
    "rating": "rating",
    "formula": "formula",
    "link": "link",
    "link_formula": "link-formula",
    "creator": "creator",
    "ctime": "ctime",
    "last_modifier": "last-modifier",
    "mtime": "mtime",
    "auto_number": "auto-number",
}


class Sea2Py:
    def __init__(self, table_info: Table):
        self.table_info = table_info
        self.columns = {x.name: {"type": x.type, "data": x.data} for x in self.table_info.columns}

    def __call__(self, row):
        return {c: self._deserialize(c, row[c]) for c in self.columns.keys()}

    def _deserialize(self, key, value):
        if key in self.columns:
            if self.columns[key]["type"] in ["link"]:
                return [x["display_value"] for x in value]
            if self.columns[key]["type"] in ["date"]:
                return datetime.fromisoformat(value)
            return value
