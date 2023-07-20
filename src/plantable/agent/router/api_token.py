import io
from typing import Annotated
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Header
from fastapi.security import APIKeyHeader

from ...client import BaseClient
from ..conf import AWS_S3_BUCKET_NAME, session
from ..util import read_table, read_view, upload_to_s3

router = APIRouter(prefix="/api-token", tags=["ApiTokenClient"])

api_key_header = APIKeyHeader(name="Token")


################################################################
# Basic Auth
################################################################
async def get_base_client(api_token: str = Depends(api_key_header)) -> BaseClient:
    bc = BaseClient(api_token=api_token)
    return bc


################################################################
# Endpoints
################################################################
# My API Token
@router.get("/info")
async def info_my_seatable_api_token(base_client: BaseClient = Depends(get_base_client)) -> dict:
    return base_client.base_token


# Export
@router.get("/export/parquet/table")
async def export_table_to_s3_with_parquet(
    workspace_name: str,
    base_name: str,
    table_name: str,
    modified_before: str = None,
    modified_after: str = None,
    prod: bool = False,
    base_client: BaseClient = Depends(get_base_client),
):
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
    workspace_name: str,
    base_name: str,
    table_name: str,
    view_name: str,
    view_group: str = None,
    prod: bool = False,
    base_client: BaseClient = Depends(get_base_client),
):
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
