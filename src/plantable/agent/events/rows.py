from pydantic import BaseModel

from .const import (
    Event,
    Option,
    OP_INSERT_ROW,
    OP_INSERT_ROWS,
    OP_DELETE_ROW,
    OP_APPEND_ROW,
    OP_APPEND_ROWS,
    OP_DELETE_ROWS,
    OP_MODIFY_ROW,
    OP_MODIFY_ROWS,
    OP_UPDATE_ROW_LINKS,
    OP_UPDATE_ROWS_LINKS,
)


################################################################
# Models
################################################################
# Row Event
class RowEvent(Event):
    op_type: str
    table_id: str
    row_id: str = None  # insert 기준 row의 id, append_rows에만 없음


# Insert Row
class InsertRow(RowEvent):
    row_insert_position: str  # 기준 row에 대한 insert position
    row_data: dict
    links_data: dict = None
    key_auto_number_config: dict


# Append Row
class AppendRow(RowEvent):
    row_data: dict
    key_auto_number_config: dict


# Modify Row
class ModifyRow(RowEvent):
    updated: dict
    old_row: dict


# Delete Row
class DeleteRow(RowEvent):
    deleted_row: dict
    upper_row_id: str = None
    deleted_links_data: dict


# Update Row Link
class UpdateRowLinks(RowEvent):
    other_table_id: str
    other_row_ids_map: dict
    old_other_row_ids_map: dict


################################################################
# Parser
################################################################
def row_event_parser(data):
    op_type = data["op_type"]

    # INSERT
    if op_type == OP_INSERT_ROW:
        return [InsertRow(**data)]

    if op_type == OP_INSERT_ROWS:
        return [
            InsertRow(
                op_type=OP_INSERT_ROW,
                table_id=data["table_id"],
                row_id=row_id,
                row_insert_position=data["row_insert_position"],
                row_data=row_data,
                links_data=data["links_data"],
                key_auto_number_config=data["key_auto_number_config"],
            )
            for row_id, row_data in zip(data["row_ids"], data["row_datas"])
        ]

    # APPEND
    if op_type == OP_APPEND_ROWS:
        return [
            AppendRow(
                op_type=OP_APPEND_ROW,  # [NOTE] OP_APPEND_ROW! not OP_APPEND_ROWS
                table_id=data["table_id"],
                row_data=row_data,
                key_auto_number_config=data["key_auto_number_config"],
            )
            for row_data in data["row_datas"]
        ]

    # MODIFY
    if op_type == OP_MODIFY_ROW:
        return [ModifyRow(**data)]

    if op_type == OP_MODIFY_ROWS:
        return [
            ModifyRow(
                op_type=OP_MODIFY_ROW,
                table_id=data["table_id"],
                row_id=row_id,
                updated=updated,
                old_row=old_row,
            )
            for row_id, updated, old_row in zip(
                data["row_ids"],
                [v for _, v in data["updated"].items()],
                [v for _, v in data["old_rows"].items()],
            )
        ]

    # DELETE
    if op_type == OP_DELETE_ROW:
        return [DeleteRow(**data)]

    if op_type == OP_DELETE_ROWS:
        return [
            DeleteRow(
                op_type=OP_DELETE_ROW,
                table_id=data["table_id"],
                row_id=row_id,
                deleted_row=deleted_row,
                upper_row_id=upper_row_id,
                deleted_links_data=data["deleted_links_data"],
            )
            for row_id, deleted_row, upper_row_id in zip(
                data["row_ids"], data["deleted_rows"], data["upper_row_ids"]
            )
        ]

    # UPDATE ROW LINK
    if op_type == OP_UPDATE_ROWS_LINKS:
        return [
            UpdateRowLinks(
                op_type=OP_UPDATE_ROW_LINKS,
                table_id=data["table_id"],
                other_table_id=data["other_table_id"],
                row_id=row_id,
                other_row_ids_map={new_key: new_value},
                old_other_row_ids_map={old_key: old_value},
            )
            for row_id, (new_key, new_value), (old_key, old_value) in zip(
                data["row_id_list"],
                data["other_rows_ids_map"].items(),
                data["old_other_rows_ids_map"].items(),
            )
        ]

    _msg = f"Row Parser - Unknown op_type '{op_type}'!"
    raise KeyError(_msg)
