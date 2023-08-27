import asyncio
import io
import logging
import time
from typing import Union

import aioboto3
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import redis.asyncio as redis

from plantable.client import AdminClient, BaseClient

from .conf import (
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_BUCKET_NAME,
    AWS_S3_BUCKET_PREFIX,
    AWS_S3_REGION_NAME,
    AWS_S3_SECRET_ACCESS_KEY,
    KEY_PREFIX,
    REDIS_HOST,
    REDIS_PORT,
    SEATABLE_PASSWORD,
    SEATABLE_URL,
    SEATABLE_USERNAME,
)
from .events.const import PARQUET_OVERWRITE_EVENTS
from .ws_client.client import NEW_NOTIFICATION, UPDATE_DTABLE

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(
        self,
        redis_host: str = REDIS_HOST,
        redis_port: Union[int, str] = REDIS_PORT,
        key_prefix: str = KEY_PREFIX,
        seatable_url: str = SEATABLE_URL,
        seatable_username: str = SEATABLE_USERNAME,
        seatable_password: str = SEATABLE_PASSWORD,
        view_suffix: str = "__sync",
        aws_s3_access_key_id: str = AWS_S3_ACCESS_KEY_ID,
        aws_s3_secret_access_key: str = AWS_S3_SECRET_ACCESS_KEY,
        aws_s3_region_name: str = AWS_S3_REGION_NAME,
        aws_s3_bucket_name: str = AWS_S3_BUCKET_NAME,
        aws_s3_bucket_prefix: str = AWS_S3_BUCKET_PREFIX,
    ):
        # Redis to Subscribe
        self.redis_host = redis_host
        self.redis_port = int(redis_port) if redis_port else redis_port
        self.key_prefix = key_prefix

        self.redis_client = redis.Redis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )
        self.key_update_dtable = "|".join([self.key_prefix, UPDATE_DTABLE])
        self.key_new_notification = "|".join([self.key_prefix, NEW_NOTIFICATION])

        # Seatable
        self.seatable_url = seatable_url
        self.seatable_username = seatable_username
        self.seatable_password = seatable_password
        self.view_suffix = view_suffix

        self.seatable_admin_client = AdminClient(
            seatable_url=self.seatable_url,
            seatable_username=self.seatable_username,
            seatable_password=self.seatable_password,
        )

        # AWS S3
        self.aws_s3_access_key_id = aws_s3_access_key_id
        self.aws_s3_secret_access_key = aws_s3_secret_access_key
        self.aws_s3_region_name = aws_s3_region_name
        self.aws_s3_bucket_name = aws_s3_bucket_name
        self.aws_s3_bucket_prefix = aws_s3_bucket_prefix

        self.boto_session = aioboto3.Session(
            aws_access_key_id=self.aws_s3_access_key_id,
            aws_secret_access_key=self.aws_s3_secret_access_key,
            region_name=self.aws_s3_region_name,
        )

        # Store
        self.store = dict()
        self.last_update = None

    async def run(self):
        await self.snapshot()

        while True:
            now = self.get_offset()
            messages = await self.redis_client.xrange(
                name=self.key_update_dtable,
                min=self.last_update,
                max=now,
            )

            indices = list()
            store = dict()
            for idx, msg in messages:
                indices.append(idx)
                base = orjson.loads(msg["base"])
                data = orjson.loads(msg["data"])
                op_type = data["op_type"]
                if op_type in PARQUET_OVERWRITE_EVENTS:
                    base_uuid = base["base_uuid"]
                    if base_uuid not in store:
                        store[base_uuid] = {"base": base, "op": list()}
                    store[base_uuid]["op"].append(data)
            for base_uuid, value in store.items():
                group_id = value["base"]["group_id"]

    async def snapshot(self):
        groups = await self.seatable_admin_client.list_groups()
        for group in groups:
            if group.id not in self.store:
                self.store[group.id] = dict()
            bases = await self.seatable_admin_client.list_group_bases(group.name)
            for base in bases:
                if base.id not in self.store[group.id]:
                    self.store[group.id][base.id] = dict()
                bc = await self.seatable_admin_client.get_base_client_with_account_token(
                    group_name_or_id=group.id, base_name=base.name
                )
                tables = await bc.list_tables()
                for table in tables:
                    if table.id not in self.store[group.id][base.id]:
                        self.store[group.id][base.id][table.id] = asyncio.create_task(
                            self.overwrite_parquet_to_s3(base_client=bc, table_name=table.name)
                        )

    def get_offset(self):
        return str(int(time.time() * 1e3))

    def get_obj_key(self, base_client: BaseClient, table_name: str, format: str = "parquet"):
        base_token = base_client.base_token
        keys = [self.aws_s3_bucket_prefix, base_token.group_name, base_token.base_name, table_name]
        return "/".join([*keys, ".".join(["__".join(keys), format])])

    async def overwrite_parquet_to_s3(
        self, base_client: BaseClient, table_name: str, parquet_version: str = "1.0"
    ):
        offset = self.get_offset()
        records = await base_client.read_table(table_name=table_name)
        tbl = pa.Table.from_pylist(records)
        with io.BytesIO() as buffer:
            pq.write_table(table=tbl, where=buffer, version=parquet_version)
            buffer.seek(0)
            content = buffer.read()
        obj_key = self.get_obj_key(base_client=base_client, table_name=table_name)
        async with self.boto_session.client("s3") as client:
            await client.upload_fileobj(io.BytesIO(content), self.aws_s3_bucket_name, obj_key)
        return offset
