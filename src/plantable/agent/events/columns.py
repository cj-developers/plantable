from typing import List

from pydantic import BaseModel


class Column(BaseModel):
    rowType: str = None
    key: str  # "4xVF",
    type: str  # "text",
    name: str  # "hello",
    editable: bool  # True,
    width: int  # 200,
    resizable: bool  # True,
    draggable: bool  # True,
    data: dict
    permission_type: str = None  # ""
    permitted_users: list = None  # []
    edit_metadata_permission_type: str  # ""
    edit_metadata_permitted_users: list  # []
    description: str = None
    editor: dict = None
    formatter: dict = None
    idx: int = None
    left: int = None
    last_frozen: bool = None


################################################################
# INSERT COLUMN
################################################################
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


class InsertColumn(BaseModel):
    op_type: str
    table_id: str
    column_key: str
    column_data: Column
    view_id: dict
    rows_datas: list


################################################################
# DELETE COLUMN
################################################################
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


class DeleteColumn(BaseModel):
    op_type: str
    table_id: str
    column_key: str
    old_column: Column
    upper_column_key: str


################################################################
# RENAME COLUMN
################################################################
sample_rename_column = {
    "op_type": "rename_column",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column_name": "what",
    "old_column_name": "guest_added",
}


class RenameColumn(BaseModel):
    op_type: str
    table_id: str
    column_key: str
    new_column_name: str
    old_column_column: str


################################################################
# UPDATE COLUMN DESCRIPTION
################################################################
sample_update_column_description = {
    "op_type": "update_column_description",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "column_description": "description here",
}


class UpdateColumnDescription(BaseModel):
    op_type: str
    table_id: str
    column_key: str
    column_description: str


################################################################
# MODIFY COLUMN TYPE
################################################################
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


class ModifyColumnType(BaseModel):
    op_type: str
    table_id: str
    column_key: str
    new_column: Column
    old_column: Column
    new_dows_data: List[dict]
    old_rows_data: List[dict]


sample_modify_row_color = {
    "op_type": "modify_row_color",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "colorbys": {"type": "by_duplicate_values", "color_by_duplicate_column_keys": ["HkRD", "sAd6"]},
}

sample_modify_row_height = {
    "op_type": "modify_row_height",
    "table_id": "wMtQ",
    "view_id": "ht2Q",
    "row_height": "quadruple",
}

sample_modify_column_permission = {
    "op_type": "modify_column_permission",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column_permission": {"permission_type": "admins", "permitted_users": []},
    "old_column_permission": {"permission_type": "", "permitted_users": []},
}


sample_modify_column_metadata_permission = {
    "op_type": "modify_column_metadata_permission",
    "table_id": "wMtQ",
    "column_key": "WD7B",
    "new_column_permission": {"edit_metadata_permission_type": "admins", "edit_metadata_permitted_users": []},
    "old_column_permission": {"edit_metadata_permission_type": "", "edit_metadata_permitted_users": []},
}

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
