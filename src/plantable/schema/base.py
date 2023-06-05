BASE = [
    {
        "column_name": "base_uuid",  # "uuid"
        "column_type": "text",
    },
    {
        "column_name": "group_name",  # "owner"
        "column_type": "text",
    },
    {
        "column_name": "base_name",  # "name"
        "column_type": "text",
    },
    {
        "column_name": "workspace_id",
        "column_type": "number",
        "column_data": {"format": "number", "decimal": "dot", "thousands": "comma"},
    },
    {
        "column_name": "created_at",
        "column_type": "date",
        "column_data": {"format": "YYYY-MM-DD HH:mm"},
    },
    {
        "column_name": "updated_at",
        "column_type": "date",
        "column_data": {"format": "YYYY-MM-DD HH:mm"},
    },
    {
        "column_name": "owner_deleted",
        "column_type": "checkbox",
    },
    {
        "column_name": "rows_count",
        "column_type": "number",
        "column_data": {"format": "number", "decimal": "dot", "thousands": "comma"},
    },
]
