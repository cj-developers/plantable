from pydantic import BaseModel

from .const import OP_INSERT_ROW, OP_DELETE_ROW, OP_APPEND_ROW, OP_MODIFY_ROW


################################################################
# INSERT ROW
################################################################
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


class InsertRow(BaseModel):
    op_type: str
    table_id: str
    row_id: str  # insert 기준 row의 id
    row_insert_position: str  # 기준 row에 대한 insert position
    row_data: dict
    links_data: dict
    key_auto_number_config: dict


def split_insert_rows(msg):
    ZIP_FIELDS = ["row_ids", "row_datas"]
    return [
        InsertRow(
            op_type=OP_INSERT_ROW,
            table_id=msg["table_id"],
            row_id=row_id,
            row_insert_position=msg["row_insert_position"],
            row_data=row_data,
            links_data=msg["links_data"],
            key_auto_number_config=msg["key_auto_number_config"],
        )
        for row_id, row_data in zip(*[msg[k] for k in ZIP_FIELDS])
    ]


################################################################
# APPEND ROWS
# - append_row는 없음 (append_rows만 있음)
# - 그러나 다른 Model들과 일관성 유지 위해 AppendRow 모델 정의함
################################################################
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


class AppendRow(BaseModel):
    op_type: str
    table_id: str
    row_data: dict
    key_auto_number_config: dict


def split_append_rows(msg):
    return [
        AppendRow(
            op_type=OP_APPEND_ROW,
            table_id=msg["table_id"],
            row_data=row_data,
            key_auto_number_config=msg["key_auto_number_config"],
        )
        for row_data in msg["row_data"]
    ]


################################################################
# MODIFY ROW
################################################################
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


class ModifyRow(BaseModel):
    op_type: str
    table_id: str
    row_id: str  # insert 기준 row의 id
    updated: dict
    old_row: dict


def split_modify_rows(msg):
    return [
        ModifyRow(
            op_type=OP_MODIFY_ROW,
            table_id=msg["table_id"],
            row_id=row_id,
            updated=updated,
            old_row=old_row,
        )
        for row_id, updated, old_row in zip(
            msg["rows_ids"],
            [v for _, v in msg["updated"].items()],
            [v for _, v in msg["old_rows"].items()],
        )
    ]


################################################################
# DELETE ROW
################################################################
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


class DeleteRow(BaseModel):
    op_type: str
    table_id: str
    row_id: str
    deleted_row: dict
    upper_row_id: str
    deleted_links_data: dict


def split_delete_rows(msg):
    ZIP_FIELDS = ["row_ids", "deleted_rows", "upper_row_ids"]
    return [
        DeleteRow(
            op_type=OP_DELETE_ROW,
            table_id=msg["table_id"],
            row_id=row_id,
            deleted_row=deleted_row,
            upper_row_id=upper_row_id,
            deleted_links_data=msg["deleted_links_data"],
        )
        for row_id, deleted_row, upper_row_id in zip(*[msg[k] for k in ZIP_FIELDS])
    ]
