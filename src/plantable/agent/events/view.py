from typing import List
from pydantic import BaseModel

from .const import (
    Event,
    Option,
    OP_INSERT_VIEW,
    OP_DELETE_VIEW,
    OP_RENAME_VIEW,
    OP_MODIFY_VIEW_LOCK,
    OP_MODIFY_FILTERS,
    OP_MODIFY_SORTS,
    OP_MODIFY_GROUPBYS,
    OP_MODIFY_HIDDEN_COLUMNS,
    OP_MODIFY_ROW_COLOR,
    OP_MODIFY_ROW_HEIGHT,
)


class View(Option):
    _id: str  # "ht2Q"
    name: str  # "test"
    type: str  # "table"
    private_for: str = None
    is_locked: bool = None
    row_height: str = None
    filter_conjunction: str  # "And"
    filters: list  # []
    sorts: list  # []
    groupbys: list  # []
    colorbys: list  # {}
    hidden_columns: list  # []
    rows: list  # []
    formula_rows: dict  # {}
    link_rows: dict  # {}
    summaries: dict  # {}
    colors: dict  # {}
    column_colors: dict  # {}
    groups: list  # []


################################################################
# INSERT VIEW
################################################################
# View Event
class ViewEvent(Event):
    op_type: str
    table_id: str
    view_id: str = None


# Insert View
class InsertView(ViewEvent):
    view_data: dict
    view_folder_id: str


# Helper
class ViewSorts(Option):
    column_key: str
    sort_type: str


# ModifySorts
class ModifySorts(ViewEvent):
    sorts: List[ViewSorts]


# ModifyRowColor
class ModifyRowColor(ViewEvent):
    colorbys: dict


################################################################
# View Event Parser
################################################################
def view_event_parser(data):
    op_type = data["op_type"]

    if op_type == OP_INSERT_VIEW:
        return [InsertView(**data)]

    if op_type == OP_MODIFY_SORTS:
        return [ModifySorts(**data)]

    if op_type == OP_MODIFY_ROW_COLOR:
        return [ModifyRowColor(**data)]

    _msg = f"view_parser - unknown op_type '{op_type}'."
    raise KeyError(_msg)
