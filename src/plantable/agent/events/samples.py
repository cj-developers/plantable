################################################################
# Column Events
################################################################
# Insert Column [DONE]
sample_insert_column = {
    "op_type": "insert_column",
    "table_id": "wMtQ",
    "column_key": "_last_modifier",
    "column_data": {
        "key": "4xVF",
        "type": "text",
        "name": "hello",
        "editable": True,
        "width": 200,
        "resizable": True,
        "draggable": True,
        "data": {
            "enable_fill_default_value": False,
            "enable_check_format": False,
            "format_specification_value": None,
            "default_value": "",
            "format_check_type": "custom_format",
        },
        "permission_type": "",
        "permitted_users": [],
        "edit_metadata_permission_type": "",
        "edit_metadata_permitted_users": [],
        "description": None,
    },
    "view_id": "0000",
    "rows_datas": [],
}

# Delete Column [DONE]
sample_delete_column = {
    "op_type": "delete_column",
    "table_id": "wMtQ",
    "column_key": "4xVF",
    "old_column": {
        "rowType": "header",
        "key": "4xVF",
        "type": "text",
        "name": "hello",
        "editable": True,
        "width": 200,
        "resizable": True,
        "draggable": True,
        "data": {
            "enable_fill_default_value": False,
            "enable_check_format": False,
            "format_specification_value": None,
            "default_value": "",
            "format_check_type": "custom_format",
        },
        "permission_type": "",
        "permitted_users": [],
        "edit_metadata_permission_type": "",
        "edit_metadata_permitted_users": [],
        "description": None,
        "editor": {"key": None, "ref": None, "props": {}, "_owner": None},
        "formatter": {"key": None, "ref": None, "props": {}, "_owner": None},
        "idx": 6,
        "left": 1080,
        "last_frozen": False,
    },
    "upper_column_key": "_last_modifier",
}

# Rename Column [Done]
sample_rename_column = {
    "op_type": "rename_column",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column_name": "what",
    "old_column_name": "guest_added",
}

# Update Column Description [Done]
sample_update_column_description = {
    "op_type": "update_column_description",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "column_description": "description here",
}


# Modify Column Type [Done]
sample_modify_column_type = {
    "op_type": "modify_column_type",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column": {
        "key": "WD7B",
        "type": "email",
        "name": "guest_added",
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
    },
    "old_column": {
        "key": "WD7B",
        "type": "text",
        "name": "guest_added",
        "editable": True,
        "width": 200,
        "resizable": True,
        "draggable": True,
        "data": {
            "enable_fill_default_value": False,
            "enable_check_format": False,
            "format_specification_value": None,
            "default_value": "",
            "format_check_type": "custom_format",
        },
        "permission_type": "",
        "permitted_users": [],
        "edit_metadata_permission_type": "",
        "edit_metadata_permitted_users": [],
        "description": None,
        "editor": {"key": None, "ref": None, "props": {}, "_owner": None},
        "formatter": {"key": None, "ref": None, "props": {}, "_owner": None},
    },
    "new_rows_data": [
        {"_id": "H2Ib-4kxRWmlxIo2ft0IGQ", "WD7B": None},
        {"_id": "X-jLvmZQRq-lMgp6X1zPgw", "WD7B": None},
        {"_id": "a4MUb9F2Sb6erVSK-WWCkA", "WD7B": None},
    ],
    "old_rows_data": [
        {"_id": "H2Ib-4kxRWmlxIo2ft0IGQ", "WD7B": None},
        {"_id": "X-jLvmZQRq-lMgp6X1zPgw"},
        {"_id": "a4MUb9F2Sb6erVSK-WWCkA"},
    ],
}


# Modify Column Permission
sample_modify_column_permission = {
    "op_type": "modify_column_permission",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column_permission": {"permission_type": "admins", "permitted_users": []},
    "old_column_permission": {"permission_type": "", "permitted_users": []},
}


# Modify Column Metadata Permission
sample_modify_column_metadata_permission = {
    "op_type": "modify_column_metadata_permission",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column_permission": {
        "edit_metadata_permission_type": "admins",
        "edit_metadata_permitted_users": [],
    },
    "old_column_permission": {
        "edit_metadata_permission_type": "",
        "edit_metadata_permitted_users": [],
    },
}

# Update Column Colorbys
sample_update_column_colorybys = {
    "op_type": "update_column_colorbys",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "colorbys": {
        "type": "by_numeric_range",
        "range_settings": {
            "color_type": "color_gradation_1",
            "is_custom_start_value": True,
            "is_custom_end_value": False,
            "start_value": "",
            "end_value": "",
        },
    },
}

################################################################
# Row Events
################################################################
# Insert Row
sample_insert_row = {
    "op_type": "insert_row",
    "table_id": "wMtQ",
    "row_id": "a4MUb9F2Sb6erVSK-WWCkA",
    "row_insert_position": "insert_below",
    "row_data": {
        "_id": "FPFMs5blRR-SpLV1sk676g",
        "_participants": [],
        "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
        "_ctime": "2023-08-20T04:59:21.673+00:00",
        "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
        "_mtime": "2023-08-20T04:59:21.673+00:00",
    },
    "links_data": {},
    "key_auto_number_config": {},
}

# Insert Rows
sample_insert_rows = {
    "op_type": "insert_rows",
    "table_id": "wMtQ",
    "row_ids": ["FPFMs5blRR-SpLV1sk676g", "NLEm269CSaO3gy7Cm2ZrvQ"],
    "row_insert_position": "insert_below",
    "row_datas": [
        {
            "_id": "NLEm269CSaO3gy7Cm2ZrvQ",
            "_participants": [],
            "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_ctime": "2023-08-20T04:59:35.211+00:00",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_mtime": "2023-08-20T04:59:35.211+00:00",
            "0000": "",
        },
        {
            "_id": "I0n3PtjORke3ubWq5B54GQ",
            "_participants": [],
            "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_ctime": "2023-08-20T04:59:35.211+00:00",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_mtime": "2023-08-20T04:59:35.211+00:00",
            "0000": "",
        },
    ],
    "links_data": {},
    "key_auto_number_config": {},
}


# Append Rows (Append Row는 없음)
sample_append_rows = {
    "op_type": "append_rows",
    "table_id": "wMtQ",
    "row_datas": [
        {
            "0000": "w.cho@cj.net",
            "HkRD": "조우진",
            "sAd6": "DT플랫폼팀",
            "WD7B": "",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_id": "F7ze485HTAKfrZmCZYqb1g",
            "_ctime": "2023-08-20T05:15:42.105+00:00",
            "_mtime": "2023-08-20T05:15:42.105+00:00",
        },
        {
            "0000": "w.cho@cj.net",
            "HkRD": "a",
            "sAd6": "",
            "WD7B": "",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_id": "ONBSOt73RieaD7O0m4LiaA",
            "_ctime": "2023-08-20T05:15:42.106+00:00",
            "_mtime": "2023-08-20T05:15:42.106+00:00",
        },
    ],
    "key_auto_number_config": {},
}

# Modify Row
sample_modify_row = {
    "op_type": "modify_row",
    "table_id": "wMtQ",
    "row_id": "FPFMs5blRR-SpLV1sk676g",
    "updated": {
        "HkRD": "a",
        "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
    },
    "old_row": {},
}

# Modify Rows
sample_modify_rows = {
    "op_type": "modify_rows",
    "table_id": "wMtQ",
    "row_ids": ["X-jLvmZQRq-lMgp6X1zPgw", "a4MUb9F2Sb6erVSK-WWCkA"],
    "updated": {
        "X-jLvmZQRq-lMgp6X1zPgw": {
            "HkRD": "a",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
        },
        "a4MUb9F2Sb6erVSK-WWCkA": {
            "HkRD": "a",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
        },
    },
    "old_rows": {
        "X-jLvmZQRq-lMgp6X1zPgw": {"HkRD": None},
        "a4MUb9F2Sb6erVSK-WWCkA": {"HkRD": None},
    },
}

# Delete Row
sample_delete_row = {
    "op_type": "delete_row",
    "table_id": "wMtQ",
    "row_id": "bH1cdZboS7WOg_qpjUkOXw",
    "deleted_row": {
        "_id": "bH1cdZboS7WOg_qpjUkOXw",
        "_participants": [],
        "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
        "_ctime": "2023-08-20T04:37:21.579+00:00",
        "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
        "_mtime": "2023-08-20T04:37:21.587+00:00",
        "0000": "",
    },
    "upper_row_id": "JmsWPdqgT4irjvxAyKBxlA",
    "deleted_links_data": {},
}

# Delete Rows
sample_delete_rows = {
    "op_type": "delete_rows",
    "table_id": "wMtQ",
    "row_ids": ["NwkAMFWgTzO6A8ugLyMzMQ", "JmsWPdqgT4irjvxAyKBxlA"],
    "deleted_rows": [
        {
            "_id": "NwkAMFWgTzO6A8ugLyMzMQ",
            "_participants": [],
            "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_ctime": "2023-08-20T04:33:50.072+00:00",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_mtime": "2023-08-20T04:33:50.072+00:00",
        },
        {
            "_id": "JmsWPdqgT4irjvxAyKBxlA",
            "_participants": [],
            "_creator": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_ctime": "2023-08-20T04:37:21.579+00:00",
            "_last_modifier": "2926d3fa3a364558bac8a550811dbe0e@auth.local",
            "_mtime": "2023-08-20T04:37:21.587+00:00",
            "0000": "",
        },
    ],
    "upper_row_ids": ["H2Ib-4kxRWmlxIo2ft0IGQ", "X-jLvmZQRq-lMgp6X1zPgw"],
    "deleted_links_data": {},
}


# Update Row Links
{
    "op_type": "update_rows_links",
    "link_id": "t4hs",
    "table_id": "hNbE",
    "other_table_id": "ric6",
    "row_id_list": ["VHUU0Fx3T9qagNXpDXiQnw", "QFwE8A2fT92ZGUt_tK3s1Q"],
    "other_rows_ids_map": {
        "VHUU0Fx3T9qagNXpDXiQnw": ["Z6sr4Z5wQ_6b1AMRxQzZgg"],
        "QFwE8A2fT92ZGUt_tK3s1Q": ["Z6sr4Z5wQ_6b1AMRxQzZgg"],
    },
    "old_other_rows_ids_map": {"VHUU0Fx3T9qagNXpDXiQnw": [], "QFwE8A2fT92ZGUt_tK3s1Q": []},
}


################################################################
# Table Events
################################################################
# Insert Table
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

# Rename Table
sample_rename_table = {
    "op_type": "rename_table",
    "table_id": "0000",
    "table_name": "Table_Renamed",
}

# Delete Table
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

sample_header_lock = {
    "op_type": "modify_header_lock",
    "table_id": "hNbE",
    "is_header_locked": False,
}

################################################################
# View Events
################################################################
# Insert View
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

# Delete View
sample_delete_view = {
    "op_type": "delete_view",
    "table_id": "0000",
    "view_id": "761d",
    "view_folder_id": None,
    "view_name": "abc",
}

# Rename View
sample_rename_view = {
    "op_type": "rename_view",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "view_name": "test_again",
}

# Modify View Type (Big Data)
sample_modify_view_type = {
    "op_type": "modify_view_type",
    "table_id": "hNbE",
    "view_id": "0oPc",
    "view_type": "archive",
}

# Modify View Lock
sample_modify_view_lock = {
    "op_type": "modify_view_lock",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "is_locked": True,
}

# Modify Filter
sample_modify_filters = {
    "op_type": "modify_filters",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "filters": [{"column_key": "0000", "filter_predicate": "contains", "filter_term": "woojin"}],
    "filter_conjunction": "And",
}

# Modify Sorts
sample_modify_sorts = {
    "op_type": "modify_sorts",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "sorts": [{"column_key": "HkRD", "sort_type": "up"}],
}

# Modify Groupbys
sample_modify_groupbys = {
    "op_type": "modify_groupbys",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "groupbys": [{"column_key": "HkRD", "sort_type": "up", "count_type": ""}],
}

# Modify Hidden Columns
sample_modify_hidden_columns = {
    "op_type": "modify_hidden_columns",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "hidden_columns": ["WD7B"],
}

# Modify Row Color (Rows in Column)
sample_modify_row_color = {
    "op_type": "modify_row_color",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "colorbys": {"type": "by_duplicate_values", "color_by_duplicate_column_keys": ["HkRD", "sAd6"]},
}

# Modify Row Height
sample_modify_row_height = {
    "op_type": "modify_row_height",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "row_height": "quadruple",
}
