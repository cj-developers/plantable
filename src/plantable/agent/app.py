import io
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from ..client import UserClient
from .conf import AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PREFIX, DEV, PROD, session

app = FastAPI(title="FASTO API")
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
@app.get("/hello", tags=["System"])
async def hello():
    return {"hello": "world!"}


@app.get("/info/s3", tags=["Info"])
async def info_detination_s3():
    return {
        "bucket": AWS_S3_BUCKET_NAME,
        "obj-key-prod": f"{AWS_S3_BUCKET_PREFIX}/{PROD}/<workspace name>/<base name>/<table name>/<table or view>/",
        "obj-key-dev": f"{AWS_S3_BUCKET_PREFIX}/{DEV}/<workspace name>/<base name>/<table name>/<table or view>/",
    }


@app.get("/info/login", tags=["Info"])
async def info_my_seatable_account(user_client: Annotated[dict, Depends(get_user_client)]):
    return await user_client.get_account_info()


@app.get("/export/table", tags=["Export"])
async def export_table_to_s3(
    user_client: Annotated[dict, Depends(get_user_client)],
    workspace_name: str,
    base_name: str,
    table_name: str,
    prod: bool = False,
):
    results = await user_client.export_table_by_name(
        workspace_name_or_id=workspace_name, base_name=base_name, table_name=table_name
    )

    async with session.client("s3") as client:
        obj_key = f"{AWS_S3_BUCKET_PREFIX}/{'prod' if prod else 'dev'}/{workspace_name}/{base_name}/{table_name}/table/{results.filename}"
        _ = await client.upload_fileobj(io.BytesIO(results.content), AWS_S3_BUCKET_NAME, obj_key)

    return {"bucket": AWS_S3_BUCKET_NAME, "key": obj_key, "size": len(results.content)}


@app.get("/export/view", tags=["Export"])
async def export_view_to_s3(
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

    async with session.client("s3") as client:
        obj_key = "/".join(
            [
                AWS_S3_BUCKET_PREFIX,
                "prod" if prod else "dev",
                workspace_name,
                base_name,
                table_name,
                "=".join(["view_group", view_group]) if view_group else "=".join(["view", view_name]),
                results.filename,
            ]
        )
        _ = await client.upload_fileobj(io.BytesIO(results.content), AWS_S3_BUCKET_NAME, obj_key)

    return {"bucket": AWS_S3_BUCKET_NAME, "key": obj_key, "size": len(results.content)}
