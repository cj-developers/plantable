import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Callable, Union

import orjson
import socketio

from plantable.client import AdminClient

from ..conf import SEATABLE_PASSWORD, SEATABLE_URL, SEATABLE_USERNAME

logger = logging.getLogger(__file__)

JOIN_ROOM = "join-room"
CONNECT = "connect"
DISCONNECT = "disconnect"
IO_DISCONNECT = "io-disconnect"
CONNECT_ERROR = "connect_error"
UPDATE_DTABLE = "update-dtable"
NEW_NOTIFICATION = "new-notification"


# Websocket Override
class SIO(socketio.Client):
    def _handle_disconnect(self, namespace):
        """io server disconnect"""
        self.logger.info("Engine.IO connection disconnected")
        if not self.connected:
            return
        self.disconnect()
        namespace = namespace or "/"
        self._trigger_event("io-disconnect", namespace=namespace)


################################################################
# Websocket
################################################################
class BaseWebsocketClient(socketio.AsyncClient):
    """
    [NOTE] admin user만 사용할 수 있도록 작성되어 있음
     - AdminClient 사용하여 BaseToken 얻는 구조
    """

    def __init__(
        self,
        group_name_or_id: Union[int, str],
        base_name: str,
        seatable_url: str = SEATABLE_URL,
        seatable_username: str = SEATABLE_USERNAME,
        seatable_password: str = SEATABLE_PASSWORD,
        handler: Callable = None,
        request_timeout: int = 30,
    ):
        # account
        self.seatable_url = seatable_url
        self.seatable_username = seatable_username
        self.seatable_password = seatable_password

        # group & base
        self.group_name_or_id = group_name_or_id
        self.base_name = base_name

        # handler
        self.handler = handler

        # websocket io
        super().__init__(request_timeout=request_timeout)

        # plantable client
        self.admin_client = AdminClient(
            seatable_url=self.seatable_url,
            seatable_username=self.seatable_username,
            seatable_password=self.seatable_password,
        )

        # base token
        self.base_token = None
        self.websocket_url = None
        self.key = None

    async def update_base_token(self):
        await self.admin_client.ensure_group_member(group_name_or_id=self.group_name_or_id)
        self.base_token = await self.admin_client.get_base_token_with_account_token(
            group_name_or_id=self.group_name_or_id, base_name=self.base_name
        )
        self.websocket_url = self.seatable_url + f"?dtable_uuid={self.base_token.dtable_uuid}"
        self.key = {
            "workspace_id": self.base_token.workspace_id,
            "group_id": self.base_token.group_id,
            "group_name": self.base_token.group_name,
            "base_uuid": self.base_token.dtable_uuid,
            "base_name": self.base_token.base_name,
        }

    async def run(self, on_update: Callable = None, on_notification: Callable = None):
        try:
            self.on(CONNECT, self.on_connect)
            self.on(DISCONNECT, self.on_disconnect)
            self.on(IO_DISCONNECT, self.on_io_disconnect)
            self.on(CONNECT_ERROR, self.on_connect_error)
            self.on(UPDATE_DTABLE, on_update or self.on_update)
            self.on(NEW_NOTIFICATION, on_notification or self.on_notification)
            await self.update_base_token()
            await self.connect(url=self.websocket_url)
            await self.wait()
        except asyncio.CancelledError as ex:
            await self.disconnect()
            raise ex

    async def on_connect(self):
        if not self.base_token or datetime.now() >= self.base_token.generated_at + timedelta(days=3):
            self.update_base_token()
        await self.emit(JOIN_ROOM, (self.base_token.dtable_uuid, self.base_token.access_token))
        logger.info("[ SeaTable SocketIO connection established ]")

    async def on_disconnect(self):
        logger.info("[ SeaTable SocketIO connection dropped ]")

    async def on_io_disconnect(self, sleep=3):
        logger.warning("[ SeaTable SocketIO connection disconnected ]")
        time.sleep(sleep)
        self.update_base_token()
        self.connect(self.websocket_url)

    async def on_connect_error(self, error_msg):
        logger.error("[ SeaTable SocketIO connection error ]", error_msg)

    async def on_update(self, data, index, *args):
        msg = {"index": index, "base": orjson.dumps(self.key), "data": data}
        if self.handler:
            await self.handler(UPDATE_DTABLE, msg)
        else:
            print(UPDATE_DTABLE, msg)

    async def on_notification(self, data, index, *args):
        msg = {"index": index, "base": orjson.dumps(self.key), "data": data}
        if self.handler:
            await self.handler(NEW_NOTIFICATION, msg)
        else:
            print(NEW_NOTIFICATION, msg)

    # override _handle_disconnect
    async def _handle_disconnect(self, namespace):
        """io server disconnect"""
        self.logger.info("Engine.IO connection disconnected")
        await super()._handle_disconnect(self, namespace=namespace)
