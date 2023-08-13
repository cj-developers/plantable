import aioboto3
from fastapi import APIRouter, Depends
from fastapi.security import APIKeyHeader

from ...client import BaseClient
from ..conf import AIOBOTO3_CONF
from ..util import upload_to_s3, view_to_parquet

session = aioboto3.Session(**AIOBOTO3_CONF)
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
async def info_my_seatable_api_token(
    base_client: BaseClient = Depends(get_base_client),
) -> dict:
    return base_client.base_token


# Export View to S3 Parquet
@router.get("/export/parquet/view")
async def export_view_to_s3_with_parquet(
    workspace_name: str,
    base_name: str,
    table_name: str,
    view_name: str,
    group: str = None,
    prod: bool = False,
    base_client: BaseClient = Depends(get_base_client),
):
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
