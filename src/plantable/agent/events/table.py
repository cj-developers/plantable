from pydantic import BaseModel

sample_insert_table = {
    "op_type": "insert_table",
    "table_data": {
        "_id": "D4S8",
        "name": "Table3",
        "is_header_locked": False,
        "header_settings": {},
        "summary_configs": {},
        "columns": [
            {
                "key": "0000",
                "type": "text",
                "name": "Name",
                "editable": True,
                "width": 200,
                "resizable": True,
                "draggable": True,
                "data": None,
                "permission_type": "",
                "permitted_users": [],
                "edit_metadata_permission_type": "",
                "edit_metadata_permitted_users": [],
                "description": None,
            }
        ],
        "rows": [],
        "view_structure": {"folders": [], "view_ids": ["0000"]},
        "views": [
            {
                "_id": "0000",
                "name": "Default View",
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
            }
        ],
        "id_row_map": {},
    },
}

sample_rename_table = {
    "op_type": "rename_table",
    "table_id": "0000",
    "table_name": "Table_Renamed",
}

sample_delete_table = {
    "op_type": "delete_table",
    "table_id": "CzsA",
    "table_name": "Table3(copy)",
    "deleted_table": {
        "_id": "CzsA",
        "name": "Table3(copy)",
        "is_header_locked": False,
        "columns": [
            {
                "key": "0000",
                "type": "text",
                "name": "Name",
                "editable": True,
                "width": 200,
                "resizable": True,
                "draggable": True,
                "data": None,
                "permission_type": "",
                "permitted_users": [],
                "edit_metadata_permission_type": "",
                "edit_metadata_permitted_users": [],
                "description": None,
                "editor": {"key": None, "ref": None, "props": {}, "_owner": None},
                "formatter": {"key": None, "ref": None, "props": {}, "_owner": None},
            }
        ],
        "rows": [],
        "id_row_map": {},
        "view_structure": {"folders": [], "view_ids": ["0000"]},
        "views": [
            {
                "_id": "0000",
                "name": "Default View",
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
            }
        ],
        "header_settings": {},
    },
}
