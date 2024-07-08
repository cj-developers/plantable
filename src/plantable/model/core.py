from datetime import datetime
from enum import Enum
from typing import Any, List, Union

import orjson
from pydantic import BaseModel, Extra, Field, validator

__all__ = [
    "DTABLE_ICON_LIST",
    "DTABLE_ICON_COLORS",
    "ColumnType",
    "Column",
    "File",
    "Table",
    "Metadata",
    "View",
    "SharedView",
    "User",
    "UserInfo",
    "Base",
    "BaseActivity",
    "BaseInfo",
    "Group",
    "Workspace",
    "BaseExternalLink",
    "ViewExternalLink",
]


DTABLE_ICON_LIST = [
    "icon-worksheet",
    "icon-task-management",
    "icon-software-test-management",
    "icon-design-assignment",
    "icon-video-production",
    "icon-market-analysis",
    "icon-data-analysis",
    "icon-product-knowledge-base",
    "icon-asset-management",
    "icon-financial-information-record",
    "icon-dollar",
    "icon-company-inventory",
    "icon-customer-inquiry",
    "icon-customer-list",
    "icon-product-list",
    "icon-store-address",
    "icon-leave-record",
    "icon-administrative-matters-calendar",
    "icon-customer-relationship",
    "icon-teachers-list",
    "icon-book-library",
    "icon-server-management",
    "icon-time-management",
    "icon-work-log",
    "icon-online-promotion",
    "icon-research",
    "icon-user-interview",
    "icon-client-review",
    "icon-club-members",
]

DTABLE_ICON_COLORS = [
    "#FF8000",
    "#FFB600",
    "#E91E63",
    "#EB00B1",
    "#7626FD",
    "#972CB0",
    "#1DDD1D",
    "#4CAF50",
    "#02C0FF",
    "#00C9C7",
    "#1688FC",
    "#656463",
]


class _Model(BaseModel): # remove "extra=Extra.forbid" here
    @validator("*", pre=True)
    def empty_to_none(cls, v):
        if v == "":
            return None
        return v


class ColumnType(Enum):
    text = "text"
    long_text = "long-text"
    number = "number"
    collaborator = "collaborator"
    date = "date"
    duration = "duration"
    single_select = "single-select"
    multiple_select = "multiple-select"
    image = "image"
    file = "file"
    email = "email"
    url = "url"
    checkbox = "checkbox"
    rating = "rating"
    formula = "formula"
    link = "link"
    link_formula = "link-formula"
    creator = "creator"
    ctime = "ctime"
    last_modifier = "last-modifier"
    mtime = "mtime"
    auto_number = "auto-number"


class Column(_Model):
    key: str  # '0000'
    type: str  # 'text' # ColumnType
    name: str = None  # 'Hive DB'
    width: int  # 199
    editable: bool  # True
    resizable: bool  # True
    draggable: bool = None  # True
    frozen: bool = None
    colorbys: dict = None
    formatter: dict = None
    data: Any = None  # None
    permission_type: str = None  # ''
    permitted_users: List[str] = None  # []
    editor: dict = None
    edit_metadata_permission_type: str = None  # ''
    edit_metadata_permitted_users: List[str] = None  # []
    description: str = None  # None
    # [NOTE] 아래 prop들은 컬럼 삭제하였다가 undo하면 이후부터 생김...
    idx: int = None
    last_frozen: bool = None
    left: int = None
    rowType: str = None

    def to_column_info(self):
        return {
            "column_name": self.name,
            "column_type": self.type,
            "column_data": self.data,
        }


class View(_Model):
    id: str = Field(..., alias="_id")  # '0000'
    name: str  # 'Default View'
    type: str  # 'table'
    private_for: str = None  # None
    is_locked: bool = None  # False
    row_height: str = None  # 'default'
    filter_conjunction: str = None # 'And'
    filters: List[dict] = None  # []
    sorts: List[dict] = None  # []
    groupbys: List[dict] = None  # []
    colorbys: dict = None  # {}
    hidden_columns: List[str] = None  # []
    rows: List[str] = None  # []
    formula_rows: dict = None  # {}
    link_rows: dict = None  # {}
    summaries: dict = None  # {}
    colors: dict = None  # {}
    column_colors: dict = None  # {}
    groups: List[str] = None  # []


class SharedView(_Model):
    id: int
    shared_name: str
    workspace_id: int
    dtable_name: str
    table_id: str  # '0000'
    view_id: str  # '0000'
    from_user: str
    to_user: str
    from_user_name: str
    to_user_name: str
    permission: str
    color: str = None
    text_color: str = None
    icon: str = None


class Table(_Model):
    id: str = Field(..., alias="_id")
    name: str
    is_header_locked: bool = None
    columns: List[Column]
    views: List[View] = None
    summary_configs: dict = None
    header_settings: dict = None

    def to_table_info(self):
        return {
            "table_name": self.name,
            "columns": [c.to_column_info() for c in self.columns],
        }


class Metadata(_Model):
    tables: List[Table]
    version: int
    format_version: int
    scripts: List[dict] = None
    settings: dict = None


class User(_Model):
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    name: str  # 'admin'
    name_pinyin: str = None
    contact_email: str  # 'woojin.cho@gmail.com'
    login_id: str = None  # ''
    phone: str = None
    avatar_url: str = None
    unit: str = None  # ''
    id_in_org: str = None  # ''
    is_staff: bool  # True
    is_active: bool  # True
    role: str = None  # 'default'
    permission: str = None  # "r" or "rw"
    workspace_id: int = None  # 1
    create_time: datetime  # '2023-05-21T03:04:26+00:00'
    last_login: datetime = None  # '2023-05-28T11:42:01+00:00'
    storage_quota: int = None
    storage_usage: int  # 0
    row_limit: int = None
    row_usage: int = None
    rows_count: int = None  # 0
    source: str = None
    quota_total: int = None
    quota_usage: int = None


class UserInfo(_Model):
    email: str  # "876543216569491ba42905bf1647fd3f@auth.local"
    name: str
    name_pinyin: str = None
    contact_email: str = None  # "michael@example.com"
    login_id: str = None  # ""
    group_id: str = None  # 1
    id_in_org: str = None  # ''
    is_admin: bool = None  # true
    role: str = None  # "Owner
    permission: str = None  # "r" or "rw"
    avatar_url: str = (
        None  # "https://cloud.seatable.io/image-view/avatars/3/7/a0a57575a3ca0c78e8c5b6b0d0dbda/resized/80/cd7f6edd2c75afd3b7299917b3767c0f.png"
    )


class Base(_Model):
    id: int = None
    workspace_id: int  # 3
    uuid: str  # '166424ad-b023-47a0-9a35-76077f5b629b'
    name: str  # 'employee'
    creator: str = None  # "Jasmin Tee"
    creator_email: str = None
    modifier: str = None  # "Jasmin Tee"
    created_at: datetime  # '2023-05-21T04:33:18+00:00'
    updated_at: datetime  # '2023-05-21T04:33:30+00:00'
    color: str = None  # None
    text_color: str = None  # None
    icon: str = None  # None
    is_encrypted: bool  # False
    in_storage: bool  # True
    org_id: int = None  # -1
    email: str = None  # '1@seafile_group'
    group_id: int = None  # 1
    owner: str = None  # 'Employee (group)'
    owner_deleted: bool = False  # False
    from_user: str = None
    from_user_name: str = None
    from_user_avatar: str = None
    permission: str = None
    file_size: int = None  # 10577
    rows_count: int = None  # 0

    def to_record(self):
        return {
            "base_uuid": self.uuid,
            "workspace_id": self.workspace_id,
            "group_name": self.owner.replace(" (group)", ""),
            "base_name": self.name,
            "created_at": self.created_at.strftime("%Y-%m-%dT%H:%M"),
            "updated_at": self.updated_at.strftime("%Y-%m-%dT%H:%M"),
            "owner_deleted": self.owner_deleted,
            "rows_count": self.rows_count,
        }


class BaseInfo(_Model):
    id: int  # 378
    workspace_id: int  # 504
    uuid: str  # "12345678-3643-489b-880c-51c8ee2a9a20"
    name: str  # "Customers"
    creator: str = None  # "Jasmin Tee"
    modifier: str = None  # "Jasmin Tee"
    created_at: str  # "2020-11-20T11:57:30+00:00"
    updated_at: str  # "2020-11-20T11:57:30+00:00"
    color: str = None  # null
    text_color: str = None  # null
    icon: str = None  # nul
    is_encrypted: bool = None
    in_storage: bool = None
    starred: bool = None
    from_user: str = None
    from_user_name: str = None
    from_user_avatar: str = None
    permission: str = None


class Group(_Model):
    id: int
    name: str
    owner: str
    owner_name: str
    created_at: datetime
    quota: int
    parent_group_id: int
    size: int


class Workspace(_Model):
    id: int = None
    name: str
    type: str
    folders: List[dict] = None
    shared_bases: List[BaseInfo] = Field(None, alias="shared_table_list")
    shared_views: List[View] = Field(None, alias="shared_view_list")
    bases: List[BaseInfo] = Field(None, alias="table_list")
    group_id: int = None
    group_owner: str = None
    group_admins: List[str] = None
    group_member_count: int = None
    group_shared_dtables: List[dict] = None
    group_shared_views: List[dict] = None

    def to_record(self):
        bases = self.bases or self.shared_bases
        return {
            "type": self.type,
            "workspace_id": self.id,
            "workspace": self.name,
            "folders": [x["name"] for x in self.folders] if self.folders else self.folders,
            "bases": [x.name for x in bases] if bases else bases,
        }


class File(_Model):
    filename: str
    content: bytes


class BaseActivity(_Model):
    author: str = None
    app: str = None
    op_id: int
    op_time: datetime
    operation: dict

    @validator("operation", pre=True)
    def load_json(cls, v):
        return orjson.loads(v)


class BaseExternalLink(_Model):
    id: int
    from_dtable: str
    from_base_uuid: str = None
    creator: str
    creator_name: str
    token: str
    permission: str
    create_at: datetime
    view_cnt: int
    url: str


class ViewExternalLink(_Model):
    id: int
    from_dtable: str
    from_base_uuid: str = None
    creator: str
    creator_name: str
    token: str
    permission: str
    create_at: datetime
    view_cnt: int
    table_id: str
    view_id: str
    url: str
    is_custom: bool
    expire_date: datetime = None
