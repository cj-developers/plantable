from typing import Annotated

import aioboto3
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from ...client import UserClient
from ..conf import AIOBOTO3_CONF
from ..util import upload_to_s3, view_to_parquet

session = aioboto3.Session(**AIOBOTO3_CONF)
router = APIRouter(prefix="/user", tags=["UserClient"])
security = HTTPBasic()


################################################################
# Basic Auth
################################################################
async def get_user_client(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    uc = UserClient(seatable_username=credentials.username, seatable_password=credentials.password)
    try:
        _ = await uc.login()
    except Exception as ex:
        if ex.status == 400:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Basic"},
            )
        raise ex
    return uc


################################################################
# Endpoints
################################################################
# My SeaTable Account Info
@router.get("/info")
async def info_my_seatable_account(user_client: Annotated[dict, Depends(get_user_client)]):
    return await user_client.get_account_info()


# Export View to S3 Parquet
@router.get("/export/parquet/view")
async def export_view_to_s3_with_parquet(
    user_client: Annotated[dict, Depends(get_user_client)],
    workspace_name: str,
    base_name: str,
    table_name: str,
    view_name: str,
    group: str = None,
    prod: bool = False,
):
    base_client = await user_client.get_base_client_with_account_token(
        workspace_name_or_id=workspace_name, base_name=base_name
    )
    content = await view_to_parquet(client=base_client, table_name=table_name, view_name=view_name)

    return await upload_to_s3(
        session=session,
        content=content,
        format="parquet",
        prod=prod,
        workspace_name=workspace_name,
        base_name=base_name,
        table_name=table_name,
        view_name=view_name,
        group=group,
    )
