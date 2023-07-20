import io
from typing import Annotated

from fastapi import Depends, FastAPI

from . import router

from .conf import AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PREFIX, DEV, PROD
from .util import generate_obj_key

app = FastAPI(title="FASTO API")
app.include_router(router.user.router)
app.include_router(router.api_token.router)


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
        "obj-key-table-prod": generate_obj_key(
            format="<file format>",
            prod=True,
            workspace_name="<workspace name>",
            base_name="<base name>",
            table_name="<table name>",
            filename="<filename>",
        ),
        "obj-key-table-dev": generate_obj_key(
            format="<file format>",
            prod=False,
            workspace_name="<workspace name>",
            base_name="<base name>",
            table_name="<table name>",
            filename="<filename>",
        ),
        "obj-key-view-prod": generate_obj_key(
            format="<file format>",
            prod=True,
            workspace_name="<workspace name>",
            base_name="<base name>",
            table_name="<table name>",
            view_name="<view name>",
            filename="<filename>",
        ),
        "obj-key-view-dev": generate_obj_key(
            format="<file format>",
            prod=False,
            workspace_name="<workspace name>",
            base_name="<base name>",
            table_name="<table name>",
            view_name="<view name>",
            filename="<filename>",
        ),
    }
