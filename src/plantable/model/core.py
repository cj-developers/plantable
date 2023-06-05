from datetime import datetime
from typing import List

from pydantic import BaseModel, Field, validator

__all__ = ["Base", "BaseInfo", "Workspace"]


class _Model(BaseModel):
    @validator("*", pre=True)
    def empty_to_none(cls, v):
        if v == "":
            return None
        return v


class Base(_Model):
    """
    [NOTE]
     base == dtable
     Base == Dtable
    """

    workspace_id: int  # 3
    uuid: str  # '166424ad-b023-47a0-9a35-76077f5b629b'
    name: str  # 'employee'
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
    file_size: int = None  # 10577
    rows_count: int = None  # 0

    def to_record(self):
        return {
            "base_uuid": self.uuid,
            "group_name": self.owner.replace(" (group)", ""),
            "base_name": self.name,
            "workspace_id": self.workspace_id,
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
    is_storage: bool = None
    starred: bool = None


class Workspace(_Model):
    id: int = None
    name: str
    type: str
    bases: List[BaseInfo] = None
