################################################################
# __sync
################################################################
SYNC_TABLE_NAME = "__sync"

SYNC_TABLE = {
    "table_name": SYNC_TABLE_NAME,
    "columns": [
        {
            "column_name": "Id",
            "column_type": "text",
            "column_data": {},
            "column_width": 200,
        },
        {
            "column_name": "Table",
            "column_type": "text",
            "column_data": {},
            "column_width": 120,
        },
        {
            "column_name": "View",
            "column_type": "text",
            "column_data": {},
            "column_width": 120,
        },
        {
            "column_name": "Managers",
            "column_type": "collaborator",
            "column_data": {},
            "column_width": 240,
        },
        {
            "column_name": "LastUpdated",
            "column_type": "date",
            "column_data": {"format": "YYYY-MM-DD HH:mm"},
            "column_width": 120,
        },
        {
            "column_name": "LastSync",
            "column_type": "date",
            "column_data": {"format": "YYYY-MM-DD HH:mm"},
            "column_width": 120,
        },
        {
            "column_name": "Sync",
            "column_type": "button",
            "column_data": {
                "button_name": "Sync",
                "button_color": "#FFFCB5",
                "button_action_list": [
                    {
                        "action_type": "send_notification",
                        "msg": "hello",
                        "to_users": ["woojin.cho@gmail.com"],
                        "user_column": "Managers",
                    }
                ],
            },
            "column_width": 120,
        },
        {
            "column_name": "Log",
            "column_type": "long-text",
            "column_data": {},
            "column_width": 360,
        },
    ],
}

SYNC_TABLE_RESIZE = list()
for c in SYNC_TABLE["columns"]:
    new_column_width = c.pop("column_width")
    SYNC_TABLE_RESIZE.append(
        {"table_name": SYNC_TABLE_NAME, "column_name": c["column_name"], "new_column_width": new_column_width}
    )
