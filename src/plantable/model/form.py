from enum import Enum

from pydantic import BaseModel, Field

from .core import ColumnType, _Model

__all__ = ["NewColumn", "SelectOption"]


class NumberFormat(Enum):
    number = "number"
    percent = "percent"
    dollar = "dollar"
    euro = "euro"
    yuan = "yuan"


class DateFormat(Enum):
    date = "YYYY-MM-DD"
    datetime = "YYYY-MM-DD HH:mm"


class DurationFormat(Enum):
    hm: str = "H:mm"
    hms: str = "H:mm:ss"


class RateType(Enum):
    rate = "dtable-icon-rate"
    like = "dtable-icon-like"
    praise = "dtable-icon-praise"
    flag = "dtable-icon-flag"


################################################################
# Data
################################################################
#
class ColumnData(_Model):
    pass


# Number Data
class NumberData(ColumnData):
    format: NumberFormat
    decimal: str = "dot"
    thousands: str = "comma"


# Date Data
class DateData(ColumnData):
    format: DateFormat = "datetime"


# Duration Data
class DurationData(ColumnData):
    format: str = "duration"
    duration_format: DurationFormat = "hms"


# Rating Data
class RatingData(ColumnData):
    rate_max_number: int = 5
    rate_style_color: str = "#EB00B1"
    rate_style_type: RateType = "dtable-icon-rate"


# Select Data (Single, Multi)
class SelectData(ColumnData):
    id: int
    name: str
    color: str
    text_color: str = Field(..., alias="text-color")


################################################################
# Data
################################################################
# Column
class NewColumn(_Model):
    column_name: str = Field(..., alias="name")
    column_type: ColumnType = Field(..., alias="type")
    column_data: ColumnData = Field(None, alias="data")
    anchor_column: str = None


# SelectOption
class SelectOption(_Model):
    name: str = None
    color: str = "gray"
    textColor: str = "white"
