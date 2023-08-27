from .const import COLUMN_EVENTS, ROW_EVENTS, TABLE_EVENTS, VIEW_EVENTS
from .rows import row_event_parser
from .columns import column_event_parser
from .view import view_event_parser


def event_parser(data):
    op_type = data["op_type"]

    if op_type in COLUMN_EVENTS:
        return column_event_parser(data)

    if op_type in ROW_EVENTS:
        return row_event_parser(data)

    if op_type in TABLE_EVENTS:
        pass

    if op_type in VIEW_EVENTS:
        return view_event_parser(data)

    _msg = f"event_parser - no parser for op_type '{op_type}'!"
    raise KeyError(_msg)
