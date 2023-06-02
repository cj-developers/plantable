from typing import List

from pydantic import BaseModel, validator
from datetime import datetime


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
    rows_count: int  # 0

    def view(self):
        return {
            "owner": self.owner,
            "workspace_id": self.workspace_id,
            "name": self.name,
            "uuid": self.uuid,
            "rows_count": self.rows_count,
        }
