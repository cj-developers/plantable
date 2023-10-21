SYNC_TABLE = [
    {
        "column_name": "Id",
        "column_type": "text",
        "column_data": {},
    },
    {
        "column_name": "Table",
        "column_type": "text",
        "column_data": {},
    },
    {
        "column_name": "View",
        "column_type": "text",
        "column_data": {},
    },
    {
        "column_name": "LastSync",
        "column_type": "date",
        "column_data": {"format": "YYYY-MM-DD HH:mm"},
    },
    {
        "column_name": "Managers",
        "column_type": "collaborator",
        "column_data": {},
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
                    "current_table_id": "8hrK",
                    "msg": "hello",
                    "to_users": [],
                    "user_column": "Managers",
                }
            ],
        },
    },
    {
        "column_name": "Log",
        "column_type": "long-text",
        "column_data": {},
    },
]
