import io
from typing import Annotated

from fastapi import Depends, FastAPI

from . import router
from .conf import AWS_S3_BUCKET_NAME, DEV, PROD
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
    format = "<format>"
    workspace_name = "<workspace>"
    base_name = "<base>"
    table_name = "<table>"
    view_name = "<view>"
    group = "<group>"
    examples = [
        (True, view_name, None),
        (False, view_name, None),
        (True, view_name, group),
        (False, view_name, group),
    ]
    return {
        "bucket": AWS_S3_BUCKET_NAME,
        **{
            f"key {'w/' if group else 'w/o'} view group for {PROD if prod else DEV}": generate_obj_key(
                format=format,
                prod=True,
                workspace_name=workspace_name,
                base_name=base_name,
                table_name=table_name,
                view_name=view_name,
                group=group,
            )
            for prod, view_name, group, in examples
        },
    }
