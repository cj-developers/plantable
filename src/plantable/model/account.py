from typing import List

from pydantic import BaseModel
from datetime import datetime


class Dtable(BaseModel):
    workspace_id: str  # 3
    uuid: str  # '166424ad-b023-47a0-9a35-76077f5b629b'
    name: str  # 'employee'
    created_at: datetime  # '2023-05-21T04:33:18+00:00'
    updated_at: datetime  # '2023-05-21T04:33:30+00:00'
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

    def view(self):
        return {
            "owner": self.owner,
            "workspace_id": self.workspace_id,
            "name": self.name,
            "uuid": self.uuid,
            "rows_count": self.rows_count,
        }


class User(BaseModel):
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    name: str  # 'admin'
    contact_email: str  # 'woojin.cho@gmail.com'
    unit: str  # ''
    login_id: str  # ''
    is_staff: str  # True
    is_active: str  # True
    id_in_org: str  # ''
    workspace_id: str  # 1
    create_time: datetime  # '2023-05-21T03:04:26+00:00'
    last_login: datetime  # '2023-05-28T11:42:01+00:00'
    role: str  # 'default'
    storage_usage: str  # 0
    rows_count: str  # 0


class Admin(BaseModel):
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    name: str  # 'admin'
    contact_email: str  # 'woojin.cho@gmail.com'
    login_id: str  # ''
    is_staff: str  # True
    is_active: str  # True
    storage_usage: str  # 0
    rows_count: str  # 0
    create_time: datetime  # '2023-05-21T03:04:26+00:00'
    last_login: datetime  # '2023-05-28T11:42:01+00:00'
    admin_role: str  # 'default_admin'


class ApiToken(BaseModel):
    app_name: str  # 'n8n', null when created with account token
    api_token: str  # 'f5ca15bf3bb64101be0a03a57feaf2289f494701'
    generated_by: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    generated_at: datetime  # '2023-05-28T13:00:31+00:00'
    last_access: datetime  # '2023-05-28T13:15:10+00:00'
    permission: str  # 'rw


class BaseToken(BaseModel):
    app_name: str = None  # 'test-api'
    access_token: str  # 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2ODU2MDg3ODcsImR0YWJsZV91dWlkIjoiMTY2NDI0YWQtYjAyMy00N2EwLTlhMzUtNzYwNzdmNWI2MjliIiwidXNlcm5hbWUiOiIiLCJwZXJtaXNzaW9uIjoicnciLCJhcHBfbmFtZSI6InRlc3QtYXBpIn0.djJ7NW67UicDmo35UodS5UJBQWydqAvt-euo1TzM2rY'
    dtable_uuid: str  # '166424ad-b023-47a0-9a35-76077f5b629b'
    dtable_server: str  # 'https://seatable.jongno.life/dtable-server/'
    dtable_socket: str  # 'https://seatable.jongno.life/'
    dtable_db: str = None  # 'https://seatable.jongno.life/dtable-db/'
    workspace_id: str = None  # 3
    dtable_name: str = None  # 'employee
