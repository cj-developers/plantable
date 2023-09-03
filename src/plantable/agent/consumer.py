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
from plantable.model import Base, View

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
from .events import event_parser
from .events.const import (
    COLUMN_EVENTS,
    OP_APPEND_ROW,
    OP_APPEND_ROWS,
    OP_ARCHIVE_ROW,
    OP_ARCHIVE_ROWS,
    OP_DELETE_COLUMN,
    OP_DELETE_ROW,
    OP_DELETE_ROWS,
    OP_DELETE_TABLE,
    OP_DELETE_VIEW,
    OP_INSERT_COLUMN,
    OP_INSERT_ROW,
    OP_INSERT_ROWS,
    OP_INSERT_TABLE,
    OP_INSERT_VIEW,
    OP_MODIFY_COLUMN_METADATA_PERMISSION,
    OP_MODIFY_COLUMN_PERMISSION,
    OP_MODIFY_COLUMN_TYPE,
    OP_MODIFY_FILTERS,
    OP_MODIFY_GROUPBYS,
    OP_MODIFY_HEADER_LOCK,
    OP_MODIFY_HIDDEN_COLUMNS,
    OP_MODIFY_ROW,
    OP_MODIFY_ROW_COLOR,
    OP_MODIFY_ROW_HEIGHT,
    OP_MODIFY_ROWS,
    OP_MODIFY_SORTS,
    OP_MODIFY_VIEW_LOCK,
    OP_MODIFY_VIEW_TYPE,
    OP_RENAME_COLUMN,
    OP_RENAME_TABLE,
    OP_RENAME_VIEW,
    OP_UPDATE_COLUMN_COLORBYS,
    OP_UPDATE_COLUMN_DESCRIPTION,
    OP_UPDATE_ROW_LINKS,
    OP_UPDATE_ROWS_LINKS,
    PARQUET_OVERWRITE_EVENTS,
    ROW_EVENTS,
    TABLE_EVENTS,
    VIEW_EVENTS,
    get_event_cat_name,
)
from .events.model import Event
from .ws_client.client import NEW_NOTIFICATION, UPDATE_DTABLE

logger = logging.getLogger(__name__)

# ACTIONS
ACTION_DELETE_PARQUET = "delete_parquet"
ACTION_OVERWRITE_PARQUET = "overwrite_parquet"


# Helper
def generate_offset():
    return str(int(time.time() * 1e3))


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
        batch_seconds: float = 30.0,
    ):
        # Redis to Subscribe
        self.redis_host = redis_host
        self.redis_port = int(redis_port) if redis_port else redis_port
        self.key_prefix = key_prefix

        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
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

        # batch
        self.batch_seconds = batch_seconds

        # Store
        self.watch_list = dict()
        self.do_list = dict()
        self.row_events = dict()
        self.last_offset = generate_offset()

    async def run(self):
        self.last_offset = generate_offset()
        _ = await self.init_watch_list()

        while True:
            offset = generate_offset()

            # get messages
            messages = await self.redis_client.xrange(name=self.key_update_dtable, min=self.last_offset, max=offset)

            # parse messages and submit to handlers - 순서 보장되어야 함
            for idx, msg in messages:
                base, data = orjson.loads(msg["base"]), orjson.loads(msg["data"])
                base_uuid = base["base_uuid"]

                events = event_parser(data)
                for event in events:
                    if event.op_type in TABLE_EVENTS:
                        await self.table_event_handler(idx=idx, base_uuid=base_uuid, e=event)
                    if event.op_type in VIEW_EVENTS:
                        await self.view_event_handler(idx=idx, base_uuid=base_uuid, e=event)
                    if event.op_type in COLUMN_EVENTS:
                        await self.column_event_handler(idx=idx, base_uuid=base_uuid, e=event)
                    if event.op_type in ROW_EVENTS:
                        await self.row_event_handler(idx=idx, base_uuid=base_uuid, e=event)
                    _msg = "handlers - unknown event {}".format(str(event))
                    raise KeyError(_msg)

            # sync

    ################################################################
    # Register
    ################################################################
    # init watch list
    async def init_watch_list(self):
        bases = await self.seatable_admin_client.list_bases()
        _ = await asyncio.gather(*[self.init_watch_list_for_each_base(base) for base in bases])

    # init watch list for each base
    async def init_watch_list_for_each_base(self, base: Base):
        bc = await self.seatable_admin_client.get_base_client_with_account_token_by_base_uuid(base_uuid=base.uuid)
        bigdata_status = await bc.get_bigdata_status()
        if "tables" not in bigdata_status:
            return
        for table in bigdata_status["tables"]:
            views = await bc.list_views(table["name"])
            for view in views:
                if view.type == "archive" and view.private_for is None:
                    _ = self.register_watch_list(base_uuid=base.uuid, table_id=table["id"], view=view)

    # register watch list
    def register_watch_list(self, base_uuid: str, table_id: str, view: View):
        if base_uuid not in self.watch_list:
            self.watch_list[base_uuid] = dict()
        if table_id not in self.watch_list[base_uuid]:
            self.watch_list[base_uuid][table_id] = dict()
        self.watch_list[base_uuid][table_id].update({view.id: view})

    # unregister watch list
    def unregister_watch_list(self, base_uuid: str, table_id: str, view_id: str):
        _ = self.watch_list[base_uuid][table_id].pop(view_id)
        return

    # register do list
    def register_do_list(self, base_uuid: str, table_id: str, view_id: str, action: str):
        if base_uuid not in self.do_list:
            self.do_list[base_uuid] = dict()
        if table_id not in self.do_list[base_uuid]:
            self.do_list[base_uuid][table_id] = dict()
        self.do_list[base_uuid][table_id].update({view_id: action})

    ################################################################
    # Event Handlers
    ################################################################
    # [TABLE EVENTS]
    async def table_event_handler(self, idx: str, base_uuid: str, e: Event):
        # 등록되지 않은 테이블이면 종료
        if e.table_id not in self.watch_list[base_uuid]:
            return

        # 테이블 삭제 Event
        if e.op_type in [OP_DELETE_TABLE]:
            views = [view for _, view in self.watch_list[base_uuid][e.table_id].items()]
            for view in views:
                #  (1) Watch List에서 삭제
                await self.unregister_watch_list(base_uuid=base_uuid, table_id=e.table_id, view=view.id)
                #  (2) Sink File 삭제
                await self.register_do_list(
                    base_uuid=base_uuid, table_id=e.table_id, view_id=e.view_id, action=ACTION_DELETE_PARQUET
                )
            return

        # 테이블 이름 변경 - 마지막 값만 중요
        if e.op_type in [OP_RENAME_TABLE]:
            views = [view for _, view in self.watch_list[base_uuid][e.table_id].items()]
            for view in views:
                self.register_do_list(
                    base_uuid=base_uuid, table_id=e.table_id, view_id=view.id, action=ACTION_OVERWRITE_PARQUET
                )

        # 무시
        if e.op_type in [OP_INSERT_TABLE, OP_MODIFY_HEADER_LOCK]:
            return

        # Raise Error
        _msg = "table_event_handler - miss event: {}".format(str(e))
        raise KeyError(_msg)

    # [VIEW EVENTS] Register는 이곳에서!
    async def view_event_handler(self, idx: str, base_uuid: str, e: Event):
        # REGISTER - 새로운 archive view 등록
        if e.op_type == OP_INSERT_VIEW and e.view_data.type == "archive" and e.view_data.private_for is None:
            await self.register_watch_list(base_uuid=base_uuid, table_id=e.table_id, view=e.view_data)
            return

        # 등록되지 않은 Table이면 종료
        if e.table_id not in self.watch_list[base_uuid]:
            return

        # View Type이 table -> archive 이면 등록, View Type이 archive -> table이면 등록 해제
        if e.op_type == OP_MODIFY_VIEW_TYPE:
            bc = await self.seatable_admin_client.get_base_client_with_account_token_by_base_uuid(base_uuid)
            view = await bc.get_view_by_id(table_id=e.table_id, view_id=e.view_id)
            if view.private_for:
                # private view는 pass
                return
            if e.view_type == "archive":
                _ = await self.register_watch_list(base_uuid=base_uuid, table_id=e.table_id, view=view.id)
                return
            if e.view_type == "table":
                if e.view_id in self.watch_list[base_uuid][e.table_id] and e.view_type == "table":
                    _ = await self.unregister_watch_list(base_uuid=base_uuid, table_id=e.table_id, view=view.id)
                    return
            _msg = "view_event_handler - unknown view_type '{}'.".format(e.view_type)
            raise KeyError(_msg)

        # 등록되지 않은 View면 종료
        if e.view_id not in self.watch_list[base_uuid][e.table_id]:
            return

        # View 삭제 -> 목적지에서도 삭제
        if e.op_type == OP_DELETE_VIEW:
            # (1) view 등록 해제
            _ = self.unregister_watch_list(base_uuid=base_uuid, table_id=e.table_id, view=e.view_id)
            # (2) view 삭제
            _ = self.register_do_list(
                base_uuid=base_uuid, table_id=e.table_id, view_id=e.view_id, action=ACTION_DELETE_PARQUET
            )
            return

        # View 이름 변경 -> 목적지에서 이름 변경
        if e.op_type == OP_RENAME_VIEW:
            # (1) watch list update
            self.watch_list[base_uuid][e.table_id][e.view_id].update({"name": e.view_name})
            # (2) overwrite
            _ = self.register_do_list(
                base_uuid=base_uuid, table_id=e.table_id, view_id=e.view_id, action=ACTION_OVERWRITE_PARQUET
            )

        # View Modify - Filters, Sorts, Groupbys -> 목적지 파일 덮어쓰기
        if e.op_type in [OP_MODIFY_FILTERS, OP_MODIFY_SORTS, OP_MODIFY_GROUPBYS, OP_MODIFY_HIDDEN_COLUMNS]:
            _ = self.register_do_list(
                base_uuid=base_uuid, table_id=e.table_id, view_id=e.view_id, action=ACTION_OVERWRITE_PARQUET
            )
            return

        # View Modify - Row Color, Row Heights -> Do Nothing
        if e.op_type in [OP_MODIFY_ROW_COLOR, OP_MODIFY_ROW_HEIGHT, OP_MODIFY_VIEW_LOCK]:
            # do nothing
            return

        # Raise Error
        _msg = "view_event_hander - miss event: {}".format(str(e))
        raise KeyError(_msg)

    # [COLUMN EVENT]
    async def column_event_handler(self, idx: str, base_uuid: str, e: Event):
        # 등록되어 있지 않은 Table 무시
        if e.table_id not in self.watch_list[base_uuid][e.table_id]:
            return

        if e.op_type in [OP_INSERT_COLUMN, OP_DELETE_COLUMN, OP_MODIFY_COLUMN_TYPE, OP_RENAME_COLUMN]:
            # do something
            return

        if e.op_type in [
            OP_MODIFY_COLUMN_PERMISSION,
            OP_MODIFY_COLUMN_METADATA_PERMISSION,
            OP_UPDATE_COLUMN_COLORBYS,
            OP_UPDATE_COLUMN_DESCRIPTION,
        ]:
            # do nothing
            return

        _msg = "column_event_hander - miss event: {}".format(str(e))
        raise KeyError(_msg)

    # [ROW EVENT]
    async def row_event_handler(self, idx: str, base_uuid: str, e: Event):
        # 등록되어 있지 않은 Table 무시
        if e.table_id not in self.watch_list[base_uuid][e.table_id]:
            return

        if e.op_type in [OP_INSERT_ROW, OP_DELETE_ROW, OP_APPEND_ROW, OP_MODIFY_ROW, OP_UPDATE_ROW_LINKS]:
            # Sync
            return

        if e.op_type in [OP_ARCHIVE_ROW]:
            # do nothing
            return

        _msg = "row_event_handler - miss event: {}".format(str(e))
        raise KeyError(_msg)

    ################################################################
    #
    ################################################################
    def get_obj_key(self, base_client: BaseClient, table_name: str, format: str = "parquet"):
        base_token = base_client.base_token
        keys = [self.aws_s3_bucket_prefix, base_token.group_name, base_token.base_name, table_name]
        return "/".join([*keys, ".".join(["__".join(keys), format])])

    async def overwrite_parquet_to_s3(self, base_client: BaseClient, table_name: str, parquet_version: str = "1.0"):
        offset = generate_offset()
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
