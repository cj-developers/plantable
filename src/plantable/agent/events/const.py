# COLUMNS
OP_INSERT_COLUMN = "insert_column"
OP_DELETE_COLUMN = "delete_column"
OP_RENAME_COLUMN = "rename_column"
OP_UPDATE_COLUMN_DESCRIPTION = "update_column_description"
OP_UPDATE_COLUMN_COLORBYS = "update_column_colorbys"
OP_MODIFY_COLUMN_TYPE = "modify_column_type"
OP_MODIFY_COLUMN_PERMISSION = "modify_column_permission"
OP_MODIFY_COLUMN_METADATA_PERMISSION = "modify_column_metadata_permission"


# ROWS
OP_INSERT_ROW = "insert_row"
OP_INSERT_ROWS = "insert_rows"
OP_APPEND_ROW = "append_row"
OP_APPEND_ROWS = "append_rows"
OP_MODIFY_ROW = "modify_row"
OP_MODIFY_ROWS = "modify_rows"
OP_DELETE_ROW = "delete_row"
OP_DELETE_ROWS = "delete_rows"


# TABLE
OP_INSERT_TABLE = "insert_table"
OP_RENAME_TABLE = "rename_table"
OP_DELETE_TABLE = "delete_table"


# VIEW
OP_INSERT_VIEW = "insert_view"
OP_DELETE_VIEW = "delete_view"
OP_RENAME_VIEW = "rename_view"
OP_MODIFY_VIEW_LOCK = "modify_view_lock"
OP_MODIFY_FILTERS = "modify_filters"
OP_MODIFY_SORTS = "modify_sorts"
OP_MODIFY_GROUPBYS = "modify_groupbys"
OP_MODIFY_HIDDEN_COLUMNS = "modify_hidden_columns"


# TODO
TO_INSERT_ROWS = [OP_INSERT_ROW, OP_INSERT_ROWS, OP_APPEND_ROW, OP_APPEND_ROWS]
TO_UPDATE_ROWS = [OP_MODIFY_ROW, OP_MODIFY_ROWS]
TO_DELETE_ROWS = [OP_DELETE_ROW, OP_DELETE_ROWS]
