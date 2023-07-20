import io
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from ...client import UserClient
from ..conf import AWS_S3_BUCKET_NAME, session
from ..util import generate_obj_key, generate_filename, read_table, read_view, upload_to_s3

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


# Export
@router.get("/export/parquet/table")
async def export_table_to_s3_with_parquet(
    user_client: Annotated[dict, Depends(get_user_client)],
    workspace_name: str,
    base_name: str,
    table_name: str,
    modified_before: str = None,
    modified_after: str = None,
    prod: bool = False,
):
    base_client = await user_client.get_base_client_with_account_token(
        workspace_name_or_id=workspace_name, base_name=base_name
    )
    content = await read_table(
        client=base_client, table_name=table_name, modified_before=modified_before, modified_after=modified_after
    )

    return await upload_to_s3(
        session=session,
        content=content,
        format="parquet",
        prod=prod,
        workspace_name=workspace_name,
        base_name=base_name,
        table_name=table_name,
    )


@router.get("/export/parquet/view")
async def export_view_to_s3_with_parquet(
    user_client: Annotated[dict, Depends(get_user_client)],
    workspace_name: str,
    base_name: str,
    table_name: str,
    view_name: str,
    view_group: str = None,
    prod: bool = False,
):
    base_client = await user_client.get_base_client_with_account_token(
        workspace_name_or_id=workspace_name, base_name=base_name
    )
    content = await read_view(client=base_client, table_name=table_name, view_name=view_name)

    return await upload_to_s3(
        session=session,
        content=content,
        format="parquet",
        prod=prod,
        workspace_name=workspace_name,
        base_name=base_name,
        table_name=table_name,
        view_name=view_name,
        view_group=view_group,
    )


# Export Excel
@router.get("/export/xlsx/table")
async def export_table_to_s3_with_xlsx(
    user_client: Annotated[dict, Depends(get_user_client)],
    workspace_name: str,
    base_name: str,
    table_name: str,
    prod: bool = False,
):
    results = await user_client.export_table_by_name(
        workspace_name_or_id=workspace_name, base_name=base_name, table_name=table_name
    )

    return await upload_to_s3(
        session=session,
        content=results.content,
        format=results.filename.rsplit(".", 1)[-1],
        prod=prod,
        workspace_name=workspace_name,
        base_name=base_name,
        table_name=table_name,
    )


@router.get("/export/xlsx/view")
async def export_view_to_s3_with_xlsx(
    user_client: Annotated[dict, Depends(get_user_client)],
    workspace_name: str,
    base_name: str,
    table_name: str,
    view_name: str,
    view_group: str = None,
    prod: bool = False,
):
    results = await user_client.export_view_by_name(
        workspace_name_or_id=workspace_name, base_name=base_name, table_name=table_name, view_name=view_name
    )

    return await upload_to_s3(
        session=session,
        content=results.content,
        format=results.filename.rsplit(".", 1)[-1],
        prod=prod,
        workspace_name=workspace_name,
        base_name=base_name,
        table_name=table_name,
        view_name=view_name,
        view_group=view_group,
    )
