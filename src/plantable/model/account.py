from datetime import datetime
from typing import List

from pydantic import BaseModel, Field, validator

from .core import _Model

__all__ = ["AccountInfo", "Admin", "Team", "ApiToken", "BaseToken", "Webhook"]


class AccountInfo(_Model):
    space_usage: str  # '0%'
    total: int  # -2
    usage: int  # 0
    row_usage_rate: str  # '0%'
    row_total: int  # -1
    row_usage: int  # 0
    avatar_url: str  # 'https://seatable.jongno.life/media/avatars/default.png'
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    name: str  # 'admin'
    login_id: str = None  # ''
    contact_email: str  # 'woojin.cho@gmail.com'
    institution: str = None  # ''
    is_staff: bool  # True
    enable_subscription: bool  # False
    dtable_updates_email_interval: int  # 0
    dtable_collaborate_email_interval: int  # 0


class Admin(_Model):
    email: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    name: str  # 'admin'
    contact_email: str  # 'woojin.cho@gmail.com'
    login_id: str = None  # ''
    is_staff: bool  # True
    is_active: bool  # True
    storage_usage: int  # 0
    rows_count: int  # 0
    create_time: datetime  # '2023-05-21T03:04:26+00:00'
    last_login: datetime  # '2023-05-28T11:42:01+00:00'
    admin_role: str  # 'default_admin'


class Team(_Model):
    org_id: str  # 1
    org_name: str  # "Test-Admin"
    ctime: datetime  # "2020-06-01T12:46:26+00:00"
    org_url_prefix: str  # "org_8hz6uh"
    role: str  # "org_default"
    creator_email: str  # "8ca1997823b44dffbaa51e0dd0c35ac0@auth.local"
    creator_name: str  # "Christoph Dyllick"
    creator_contact_email: str  # "christoph@example.com"
    quota: int  # -2
    storage_usage: int  # 0
    storage_quota: int  # 1000000000
    max_user_number: int  # 25
    rows_count: int  # 0
    row_limit: int  # 200


class ApiToken(_Model):
    app_name: str  # 'n8n', null when created with account token
    api_token: str  # 'f5ca15bf3bb64101be0a03a57feaf2289f494701'
    generated_by: str  # '2926d3fa3a364558bac8a550811dbe0e@auth.local'
    generated_at: datetime  # '2023-05-28T13:00:31+00:00'
    last_access: datetime  # '2023-05-28T13:15:10+00:00'
    permission: str  # 'rw


class BaseToken(_Model):
    app_name: str = None  # 'test-api'
    access_token: str  # 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2ODU2MDg3ODcsImR0YWJsZV91dWlkIjoiMTY2NDI0YWQtYjAyMy00N2EwLTlhMzUtNzYwNzdmNWI2MjliIiwidXNlcm5hbWUiOiIiLCJwZXJtaXNzaW9uIjoicnciLCJhcHBfbmFtZSI6InRlc3QtYXBpIn0.djJ7NW67UicDmo35UodS5UJBQWydqAvt-euo1TzM2rY'
    dtable_uuid: str  # '166424ad-b023-47a0-9a35-76077f5b629b'
    dtable_server: str  # 'https://seatable.jongno.life/dtable-server/'
    dtable_socket: str  # 'https://seatable.jongno.life/'
    dtable_db: str = None  # 'https://seatable.jongno.life/dtable-db/'
    workspace_id: int = None  # 3
    group_id: int = None  # (manually added)
    group_name: str = None  # (manually added)
    base_name: str = Field(None, alias="dtable_name")  # 'employee
    use_api_gateway: str = None
    generated_at: datetime = datetime.now()  # (manually added)


class Webhook(_Model):
    id: int
    dtable_uuid: str
    url: str
    creator: str
    created_at: datetime
    is_valid: bool
    settings: dict = None
