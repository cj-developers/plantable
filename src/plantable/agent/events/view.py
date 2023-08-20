from pydantic import BaseModel


class View(BaseModel):
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
sample_insert_view = {
    "op_type": "insert_view",
    "table_id": "wMtQ",
    "view_data": {
        "_id": "ht2Q",
        "name": "test",
        "type": "table",
        "private_for": None,
        "is_locked": False,
        "row_height": "default",
        "filter_conjunction": "And",
        "filters": [],
        "sorts": [],
        "groupbys": [],
        "colorbys": {},
        "hidden_columns": [],
        "rows": [],
        "formula_rows": {},
        "link_rows": {},
        "summaries": {},
        "colors": {},
        "column_colors": {},
        "groups": [],
    },
    "view_folder_id": None,
}


class InsertView(BaseModel):
    op_type: str
    table_id: str
    view_data: dict
    view_folder_id: str


sample_delete_view = {
    "op_type": "delete_view",
    "table_id": "0000",
    "view_id": "761d",
    "view_folder_id": None,
    "view_name": "abc",
}

sample_rename_view = {
    "op_type": "rename_view",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "view_name": "test_again",
}

sample_modify_view_lock = {
    "op_type": "modify_view_lock",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "is_locked": True,
}


sample_modify_filters = {
    "op_type": "modify_filters",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "filters": [{"column_key": "0000", "filter_predicate": "contains", "filter_term": "woojin"}],
    "filter_conjunction": "And",
}

sample_modify_sorts = {
    "op_type": "modify_sorts",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "sorts": [{"column_key": "HkRD", "sort_type": "up"}],
}

sample_modify_groupbys = {
    "op_type": "modify_groupbys",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "groupbys": [{"column_key": "HkRD", "sort_type": "up", "count_type": ""}],
}

sample_modify_hidden_columns = {
    "op_type": "modify_hidden_columns",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "hidden_columns": ["WD7B"],
}
