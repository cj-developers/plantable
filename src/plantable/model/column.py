from enum import Enum
from typing import List

from pydantic import Extra, Field, BaseModel

__all__ = [
    "Text",
    "LongText",
    "Integer",
    "Number",
    "Collaborator",
    "Date",
    "Datetime",
    "Duration",
    "SingleSelect",
    "MultipleSelect",
    "Image",
    "File",
    "Email",
    "Url",
    "Checkbox",
    "Rate",
    "Formula",
    "Link",
    "LinkFomula",
    "Creator",
    "CreationTime",
    "LastModifier",
    "LastModificationTime",
    "AutoNumber",
    "Button",
    # Options
    "SelectOption",
    "OpenUrl",
]


# SeaTableBaseModel
class SeaTableType(BaseModel):
    name: str
    anchor: str = None
    _type: str = "text"

    def seatable_schema(self):
        DEFAULT_FIELDS = ["name", "anchor"]
        schema = {
            "column_name": self.name,
            "column_type": self._type,
            "colum_data": dict(),
            "anchor_column": self.anchor,
        }

        for k in self.schema()["properties"]:
            if k not in DEFAULT_FIELDS:
                value = getattr(self, k)
                if value is None:
                    continue
                if "column_data" not in schema:
                    schema["column_data"] = dict()
                if isinstance(value, list):
                    value = [x.to_dict() if isinstance(x, BaseModel) else x for x in value]
                else:
                    value = value.to_dict() if isinstance(value, BaseModel) else value
                schema["column_data"].update({k: value})

        return {k: v for k, v in schema.items() if v}

    class Config:
        extra = Extra.forbid


# [TEXT]
class Text(SeaTableType):
    _type: str = "text"


# [LONG TEXT]
class LongText(SeaTableType):
    _type: str = "long-text"


# [NUMBER]
# number format
class NumberFormat(str, Enum):
    number: str = "number"
    percent: str = "percent"
    dollar: str = "dollar"
    euro: str = "euro"
    yen: str = "yen"


# number decimal
class NumberDecimal(str, Enum):
    dot: str = "dot"
    # comma: str = "comma"


# number thousands
class NumberThousands(str, Enum):
    no: str = "no"
    space: str = "space"
    comma: str = "comma"


# INTEGER
class Integer(SeaTableType):
    _type: str = "number"
    format: NumberFormat = "number"
    decimal: NumberDecimal = "dot"
    thousands: NumberThousands = "no"
    enable_precision: bool = True
    precision: int = 0


# NUMBER
class Number(SeaTableType):
    _type: str = "number"
    format: NumberFormat = "number"
    decimal: NumberDecimal = "dot"
    thousands: NumberThousands = "no"


# [COLLABORATOR]
class Collaborator(SeaTableType):
    _type: str = "collaborator"


# [DATE]
# DATE
class Date(SeaTableType):
    _type: str = "date"
    format: str = Field("YYYY-MM-DD", const=True)


# DATETIME
class Datetime(SeaTableType):
    _type: str = "date"
    format: str = Field("YYYY-MM-DD HH:mm", const=True)


# [DURATION]
class Duration(SeaTableType):
    _type: str = "duration"
    format: str = "duration"
    duration_format: str = "h:mm:ss"


# [SINGLE-SELECT, MULTIPLE-SELECT]
# select option
class SelectOption(SeaTableType):
    id: str
    color: str = None
    text_color: str = Field(None, alias="text-color")


# SINGLE SELECT
class SingleSelect(SeaTableType):
    _type = "single-select"
    options: List[SelectOption] = None


# MULTIPLE SELECT
class MultipleSelect(SeaTableType):
    _type = "multiple-select"
    options: List[SelectOption] = None


# [IMAGE]
class Image(SeaTableType):
    _type = "image"


# [FILE]
class File(SeaTableType):
    _type = "file"


# [EMAIL]
class Email(SeaTableType):
    _type = "email"


# [URL]
class Url(SeaTableType):
    _type = "url"


# [CHECKBOX]
class Checkbox(SeaTableType):
    _type = "checkbox"


# [Rate]
class Rate(SeaTableType):
    _type = "rate"
    rate_max_number: str = 5
    rate_style_color: str = "#FF8000"
    rate_style_type: str = "dtable-icon-rate"


# [FORMULA]
class Formula(SeaTableType):
    _type = "formula"
    formula: str = None


# [LINK COLUMN] - NOT WORKING!
class Link(SeaTableType):
    _type = "link"
    table: str
    other_table: str


# [LINK FORMULA COLUMN] - NOT WORKING!
class LinkFomula(SeaTableType):
    _type = "link-fomula"
    # [TBD]


# [CRAETOR]
class Creator(SeaTableType):
    _type = "creator"


# [CREATION TIME]
class CreationTime(SeaTableType):
    _type = "ctime"


# [LAST MODIFIER]
class LastModifier(SeaTableType):
    _type = "last-modifier"


# [LAST MODIFICATION TIME]
class LastModificationTime(SeaTableType):
    _type = "mtime"


# [AUTO NUMBER]
class AutoNumber(SeaTableType):
    _type = "auto-number"
    format: str = "ID-000000"


# [BUTTON]
# button type
class ButtonActionType(str, Enum):
    run_script: str = "run_script"
    send_email: str = "send_email"
    copy_row_to_another_table: str = "copy_row_to_another_table"
    modify_row: str = "modify_row"
    open_url: str = "open_url"


class ButtonAction(BaseModel):
    action_type: ButtonActionType
    enable_condition_execution: bool = False


class Filter(BaseModel):
    column_key: str
    filter_predicate: str = "is_not_empty"
    filter_term: str = ""


class OpenUrl(ButtonAction):
    action_type: ButtonActionType = Field("open_url", const=True)
    url_address: str
    enable_condition_execution: bool = False
    filters: List[Filter] = None


# Button
class Button(SeaTableType):
    _type = "button"
    button_name: str
    button_color: str
    button_action_list: List[ButtonAction] = None
