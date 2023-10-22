from enum import Enum
from typing import List

from pydantic import BaseModel, Extra, Field


# ColumnData
class ColumnData(BaseModel):
    class Config:
        extra = Extra.forbid


################################################################
# text
################################################################
class TextData(ColumnData):
    pass


################################################################
# long-text
################################################################
class LongTextData(ColumnData):
    pass


################################################################
# number
################################################################
# format
class NumberFormat(str, Enum):
    number: str = "number"
    percent: str = "percent"
    dollar: str = "dollar"
    euro: str = "euro"
    yen: str = "yen"


# decimal
class NumberDecimal(str, Enum):
    dot: str = "dot"


# thousands
class NumberThousands(str, Enum):
    no: str = "no"
    dot: str = "dot"
    comma: str = "comma"


# number column data
class NumberData(ColumnData):
    format: NumberFormat = "number"
    decimal: NumberDecimal = "dot"
    thousands: NumberThousands = "comma"


################################################################
# collaborator
################################################################
class CollaboratorData(ColumnData):
    pass


################################################################
# date
################################################################
# format
class DateFormat(str, Enum):
    iso_date: str = "YYYY-MM-DD"
    iso_datetime: str = "YYYY-MM-DD HH:mm"
    us_date: str = "M/D/YYYY"
    us_datetime: str = "M/D/YYYY HH:mm"
    european_date: str = "DD/MM/YYYY"
    european_datetime: str = "DD/MM/YYYY HH:mm"
    german_date: str = "DD.MM.YYYY"
    german_datetime: str = "DD.MM.YYYY HH:mm"


# date column data
class DateData(ColumnData):
    format: DateFormat


################################################################
# duration
################################################################
# format
class DurationFormat(str, Enum):
    minutes: str = "h:mm"
    seconds: str = "h:mm:ss"


class DurationData(ColumnData):
    format: str = "duration"
    duration_format: DurationFormat = "h:mm:ss"


################################################################
# single-select, multiple-select
################################################################
# option
class SelectOption(BaseModel):
    id: int = None
    name: str
    color: str = "lightgray"
    text_color: str = Field("black", alias="text-color")

    class Config:
        extra = Extra.forbid


# single select
class SingleSelectData(ColumnData):
    options: List[SelectOption] = None


# multiple select
class MultipleSelectData(ColumnData):
    options: List[SelectOption] = None


################################################################
# image
################################################################
class ImageData(ColumnData):
    pass


################################################################
# file
################################################################
class FileData(ColumnData):
    pass


################################################################
# email
################################################################
class EmailData(ColumnData):
    pass


################################################################
# url
################################################################
class UrlData(ColumnData):
    pass


################################################################
# checkbox
################################################################
class CheckboxData(ColumnData):
    pass


################################################################
# rate
################################################################
class RateStyleType(str, Enum):
    dtable_icon_rate: str = "dtable-icon-rate"
    dtable_icon_like: str = "dtable-icon-like"
    dtable_icon_praise: str = "dtable-icon-praise"
    dtable_icon_flag: str = "dtable-icon-flag"


class RateData(ColumnData):
    rate_max_number: int = 5
    rate_style_color: str = "#FF8000"
    rate_style_type: RateStyleType = "dtable-icon-rate"


################################################################
# formula
################################################################
class FormulaData(ColumnData):
    formula: str


################################################################
# link-column
################################################################
class LinkData(ColumnData):
    table: str
    other_table: str


################################################################
# link-formula
################################################################
# formula
class Formula(str, Enum):
    count_links: str = "count_links"
    lookup: str = "lookup"
    rollup: str = "rollup"
    findmax: str = "findmax"
    findmin: str = "findmin"


class LinkFomulaData(ColumnData):
    formula: Formula
    link_column: str
    level1_linked_column: str = None
    summary_column: str = None
    summary_method: str = None
    searched_column: str = None
    comparison_column: str = None


################################################################
# creator
################################################################
class CreatorData(ColumnData):
    pass


################################################################
# ctime
################################################################
class CtimeData(ColumnData):
    pass


################################################################
# last-modifier
################################################################
class LastModifierData(ColumnData):
    pass


################################################################
# mtime
################################################################
class MtimeData(ColumnData):
    pass


################################################################
# auto-number
################################################################
class AutoNumberData(ColumnData):
    format: str = "0000"


################################################################
# button
################################################################
class ButtonActionType(str, Enum):
    send_notification: str = "send_notification"
    modify_row: str = "modify_row"
    copy_row_to_another_table: str = "copy_row_to_another_table"
    open_url: str = "open_url"
    send_email: str = "send_email"
    run_script: str = "run_script"


class NotificationUser(BaseModel):
    value: str

    class Config:
        extra = Extra.forbid


class SelectedColumn(BaseModel):
    key: str
    value: str

    class Config:
        extra = Extra.forbid


class ButtonAction(BaseModel):
    action_type: ButtonActionType
    filters: List[dict] = None
    filter_conjunction: str = "And"
    current_table_id: str
    # notification
    msg: str = None
    to_users: List[NotificationUser] = None
    user_col_key: str = None
    # modify row
    selected_columns: List[SelectedColumn] = None
    # copy_row_to_another_table
    table_id: str = None
    # open_url
    url_address: str = None


# button column data
class ButtonData(ColumnData):
    button_name: str = "noname"
    button_color: str = "lightgray"
    button_action_list: List[ButtonAction]


################################################################
# validator
################################################################
COLUMN_DATA = {
    "text": TextData,
    "long-text": LongTextData,
    "number": NumberData,
    "collaborator": CollaboratorData,
    "date": DateData,
    "duration": DurationData,
    "single-select": SingleSelectData,
    "multiple-select": MultipleSelectData,
    "image": ImageData,
    "file": FileData,
    "email": EmailData,
    "url": UrlData,
    "checkbox": CheckboxData,
    "rate": RateData,
    "formula": FormulaData,
    "link": LinkData,
    "link-formula": LinkFomulaData,
    "creator": CreatorData,
    "ctime": CtimeData,
    "last-modifier": LastModifierData,
    "mtime": MtimeData,
    "auto-number": AutoNumberData,
    "button": ButtonData,  # button -> file
}
