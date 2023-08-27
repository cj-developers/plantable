# ROWS
OP_INSERT_ROW = "insert_row"
OP_INSERT_ROWS = "insert_rows"
OP_APPEND_ROW = "append_row"
OP_APPEND_ROWS = "append_rows"
OP_MODIFY_ROW = "modify_row"
OP_MODIFY_ROWS = "modify_rows"
OP_DELETE_ROW = "delete_row"
OP_DELETE_ROWS = "delete_rows"
OP_UPDATE_ROW_LINKS = "update_row_links"
OP_UPDATE_ROWS_LINKS = "update_rows_links"
OP_ARCHIVE_ROW = "archive_row"
OP_ARCHIVE_ROWS = "archive_rows"

ROW_EVENT_CAT_NAME = "row_event"
ROW_EVENTS = [
    OP_INSERT_ROW,
    OP_INSERT_ROWS,
    OP_APPEND_ROW,
    OP_APPEND_ROWS,
    OP_MODIFY_ROW,
    OP_MODIFY_ROWS,
    OP_DELETE_ROW,
    OP_DELETE_ROWS,
    OP_UPDATE_ROW_LINKS,
    OP_UPDATE_ROWS_LINKS,
    OP_ARCHIVE_ROW,
    OP_ARCHIVE_ROWS,
]


# COLUMNS
OP_INSERT_COLUMN = "insert_column"
OP_DELETE_COLUMN = "delete_column"
OP_RENAME_COLUMN = "rename_column"
OP_UPDATE_COLUMN_DESCRIPTION = "update_column_description"
OP_UPDATE_COLUMN_COLORBYS = "update_column_colorbys"
OP_MODIFY_COLUMN_TYPE = "modify_column_type"
OP_MODIFY_COLUMN_PERMISSION = "modify_column_permission"
OP_MODIFY_COLUMN_METADATA_PERMISSION = "modify_column_metadata_permission"

COLUMN_EVENT_CAT_NAME = "column_event"
COLUMN_EVENTS = [
    OP_INSERT_COLUMN,
    OP_DELETE_COLUMN,
    OP_RENAME_COLUMN,
    OP_UPDATE_COLUMN_DESCRIPTION,
    OP_UPDATE_COLUMN_COLORBYS,
    OP_MODIFY_COLUMN_TYPE,
    OP_MODIFY_COLUMN_PERMISSION,
    OP_MODIFY_COLUMN_METADATA_PERMISSION,
]


# VIEW
OP_INSERT_VIEW = "insert_view"
OP_DELETE_VIEW = "delete_view"
OP_RENAME_VIEW = "rename_view"
OP_MODIFY_VIEW_TYPE = "modify_view_type"
OP_MODIFY_VIEW_LOCK = "modify_view_lock"
OP_MODIFY_FILTERS = "modify_filters"
OP_MODIFY_SORTS = "modify_sorts"
OP_MODIFY_GROUPBYS = "modify_groupbys"
OP_MODIFY_HIDDEN_COLUMNS = "modify_hidden_columns"
OP_MODIFY_ROW_COLOR = "modify_row_color"
OP_MODIFY_ROW_HEIGHT = "modify_row_height"
OP_MOVE_VIEW = "move_view"

VIEW_EVENT_CAT_NAME = "view_event"
VIEW_EVENTS = [
    OP_INSERT_VIEW,
    OP_DELETE_VIEW,
    OP_RENAME_VIEW,
    OP_MODIFY_VIEW_TYPE,
    OP_MODIFY_VIEW_LOCK,
    OP_MODIFY_FILTERS,
    OP_MODIFY_SORTS,
    OP_MODIFY_GROUPBYS,
    OP_MODIFY_HIDDEN_COLUMNS,
    OP_MODIFY_ROW_COLOR,
    OP_MODIFY_ROW_HEIGHT,
    OP_MOVE_VIEW,
]

# TABLE
OP_INSERT_TABLE = "insert_table"
OP_RENAME_TABLE = "rename_table"
OP_DELETE_TABLE = "delete_table"
OP_MODIFY_HEADER_LOCK = "modify_header_lock"

TABLE_EVENT_CAT_NAME = "table_event"
TABLE_EVENTS = [
    OP_INSERT_TABLE,
    OP_RENAME_TABLE,
    OP_DELETE_TABLE,
    OP_MODIFY_HEADER_LOCK,
]

# PARQUET OVERWRITE CASE
PARQUET_OVERWRITE_EVENTS = [
    # COLUMNS
    OP_INSERT_COLUMN,
    OP_DELETE_COLUMN,
    OP_RENAME_COLUMN,
    # ROWS
    OP_INSERT_ROW,
    OP_INSERT_ROWS,
    OP_APPEND_ROW,
    OP_APPEND_ROWS,
    OP_MODIFY_ROW,
    OP_MODIFY_ROWS,
    OP_DELETE_ROW,
    OP_DELETE_ROWS,
    # TABLE
    OP_RENAME_TABLE,
    # VIEW
    OP_RENAME_VIEW,
    OP_MODIFY_FILTERS,
    OP_MODIFY_SORTS,
    OP_MODIFY_HIDDEN_COLUMNS,
]


# Helper
def get_event_cat_name(op_type: str):
    if op_type in ROW_EVENTS:
        return ROW_EVENT_CAT_NAME
    if op_type in COLUMN_EVENTS:
        return COLUMN_EVENT_CAT_NAME
    if op_type in VIEW_EVENTS:
        return VIEW_EVENT_CAT_NAME
    if op_type in TABLE_EVENTS:
        return TABLE_EVENT_CAT_NAME
    _msg = f"route_event_type - unknown op_type '{op_type}'"
    raise KeyError(_msg)
