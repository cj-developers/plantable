import io
from typing import List

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq

from ..client.base import BaseClient
from .conf import AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PREFIX, DEV, PROD


# Generate Filename
def generate_filename(
    workspace_name: str, base_name: str, table_name: str, view_name: str = None, format: str = "parquet"
):
    names = [workspace_name, base_name, table_name]
    if view_name:
        names.append(view_name)
    return ".".join(["_".join(names), format])


# Generate S3 Object Key
def generate_obj_key(
    format: str,
    prod: bool,
    workspace_name: str,
    base_name: str,
    table_name: str,
    filename: str = None,
    view_group: str = None,
    view_name: str = None,
    aws_s3_bucket_prefix: str = AWS_S3_BUCKET_PREFIX,
) -> str:
    keys = [aws_s3_bucket_prefix, format, PROD if prod else DEV, workspace_name, base_name, table_name]

    if not view_name:
        keys.append("table")
    else:
        keys.append("=".join(["view_group", view_group]) if view_group else "=".join(["view", view_name]))

    keys.append(filename)

    return "/".join(keys)


# Python List to Parquet Bytes
def pylist_to_parquet(records: List[dict], version: str = "1.0") -> bytes:
    tbl = pa.Table.from_pylist(records)
    with io.BytesIO() as buffer:
        pq.write_table(table=tbl, where=buffer, version=version)
        buffer.seek(0)
        content = buffer.read()
    return content


# Read Table using BaseClient as Parquet
async def read_table(client: BaseClient, table_name: str, modified_before: str, modified_after: str):
    records = await client.read_table(
        table_name=table_name,
        modified_before=modified_before,
        modified_after=modified_after,
    )
    return pylist_to_parquet(records)


# Read Table using BaseClient as Parquet
async def read_view(client: BaseClient, table_name: str, view_name: str):
    records = await client.read_view(table_name=table_name, view_name=view_name)
    return pylist_to_parquet(records)


# Upload to S3
async def upload_to_s3(
    session: aioboto3.Session,
    content: bytes,
    format: str,
    prod: bool,
    workspace_name: str,
    base_name: str,
    table_name: str,
    view_name: str = None,
    view_group: str = None,
):
    obj_key = generate_obj_key(
        format="parquet",
        prod=prod,
        workspace_name=workspace_name,
        base_name=base_name,
        table_name=table_name,
        filename=generate_filename(
            workspace_name=workspace_name,
            base_name=base_name,
            table_name=table_name,
            view_name=view_name,
            format=format,
        ),
        view_name=view_name,
        view_group=view_group,
    )

    async with session.client("s3") as client:
        await client.upload_fileobj(io.BytesIO(content), AWS_S3_BUCKET_NAME, obj_key)

    return {"bucket": AWS_S3_BUCKET_NAME, "key": obj_key, "size": len(content)}
