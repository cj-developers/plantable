from typing import List

from pydantic import BaseModel


class Dtable(BaseModel):
    workspace_id: str  # 3
    uuid: str  # '166424ad-b023-47a0-9a35-76077f5b629b'
    name: str  # 'employee'
    created_at: str  # '2023-05-21T04:33:18+00:00'
    updated_at: str  # '2023-05-21T04:33:30+00:00'
    color: str = None  # None
    text_color: str = None  # None
    icon: str = None  # None
    is_encrypted: str  # False
    in_storage: str  # True
    org_id: str  # -1
    email: str  # '1@seafile_group'
    group_id: str  # 1
    owner: str  # 'Employee (group)'
    owner_deleted: str  # False
    file_size: str  # 10577
    rows_count: str  # 0


class User(BaseModel):
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local',
    name: str  # 'admin',
    contact_email: str  # 'woojin.cho@gmail.com',
    unit: str  # '',
    login_id: str  # '',
    is_staff: str  # True,
    is_active: str  # True,
    id_in_org: str  # '',
    workspace_id: str  # 1,
    create_time: str  # '2023-05-21T03:04:26+00:00',
    last_login: str  # '2023-05-28T11:42:01+00:00',
    role: str  # 'default',
    storage_usage: str  # 0,
    rows_count: str  # 0


class Admin(BaseModel):
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local',
    name: str  # 'admin',
    contact_email: str  # 'woojin.cho@gmail.com',
    login_id: str  # '',
    is_staff: str  # True,
    is_active: str  # True,
    storage_usage: str  # 0,
    rows_count: str  # 0,
    create_time: str  # '2023-05-21T03:04:26+00:00',
    last_login: str  # '2023-05-28T11:42:01+00:00',
    admin_role: str  # 'default_admin'
