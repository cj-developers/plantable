import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Callable

import aiohttp
import orjson
import socketio

from .conf import SEATABLE_URL

logger = logging.getLogger(__file__)

JOIN_ROOM = "join-room"
UPDATE_DTABLE = "update-dtable"
NEW_NOTIFICATION = "new-notification"

PLANTABLE_AGENT_APP_NAME = "plantable-agent"
PLANTABLE_AGENT_APP_PERMISSION = "rw"


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
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        seatable_username: str = None,
        seatable_password: str = None,
        workspace_id: int = None,
        base_name: int = None,
        api_token: str = None,
        on_update_handler: Callable = None,
        on_notification_handler: Callable = None,
        request_timeout: int = 30,
    ):
        self.seatable_url = seatable_url
        self.seatable_username = seatable_username
        self.seatable_password = seatable_password
        self.workspace_id = workspace_id
        self.base_name = base_name
        self.api_token = api_token

        self.on_update_handler = on_update_handler
        self.on_notification_handler = on_notification_handler

        self.dtable_uuid = None
        self.base_token = None
        self.base_token_expired = None
        self.websocket_url = None

        super().__init__(request_timeout=request_timeout)

    async def update_base_token(self):
        if not self.api_token:
            api_token = await self.create_temp_api_token()
        else:
            api_token = self.api_token

        # update base token
        url = "/api/v2.1/dtable/app-access-token/"
        headers = {"accept": "application/json", "authorization": f"Token {api_token}"}
        results = await self.request("GET", url=url, headers=headers)

        self.workspace_id = results["workspace_id"]
        self.base_name = results["dtable_name"]
        self.dtable_uuid = results["dtable_uuid"]
        self.base_token = results["access_token"]
        self.base_token_expired = datetime.now() + timedelta(days=3)
        self.websocket_url = self.seatable_url + f"?dtable_uuid={self.dtable_uuid}"

    async def create_temp_api_token(self):
        url = f"/api/v2.1/workspace/{self.workspace_id}/dtable/{self.base_name}/temp-api-token/"
        account_token = await self.get_account_token()
        headers = {
            "accept": "application/json",
            "authorization": f"Token {account_token}",
        }
        results = await self.request("GET", url=url, headers=headers)
        return results["api_token"]

    async def get_account_token(self):
        url = "/api2/auth-token/"
        payload = {
            "username": self.seatable_username,
            "password": self.seatable_password,
        }
        results = await self.request("POST", url=url, payload=payload)
        return results["token"]

    async def request(
        self, method: str, url: str, headers: dict = None, payload: dict = None
    ):
        async with aiohttp.ClientSession(base_url=self.seatable_url) as session:
            async with session.request(
                method=method, url=url, headers=headers, json=payload
            ) as response:
                response.raise_for_status()
                results = await response.json()
        return results

    async def run(self, on_update: Callable = None, on_notification: Callable = None):
        try:
            self.on("connect", self.on_connect)
            self.on("disconnect", self.on_disconnect)
            self.on("io-disconnect", self.on_io_disconnect)
            self.on("connect_error", self.on_connect_error)
            self.on(UPDATE_DTABLE, on_update or self.on_update)
            self.on(NEW_NOTIFICATION, on_notification or self.on_notification)
            await self.update_base_token()
            await self.connect(url=self.websocket_url)
            await self.wait()
        except asyncio.CancelledError as ex:
            await self.disconnect()
            raise ex

    async def on_connect(self):
        if datetime.now() >= self.base_token_expired:
            self.update_base_token()
        await self.emit(JOIN_ROOM, (self.dtable_uuid, self.base_token))
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
        msg = {
            "key": {"workspace_id": self.workspace_id, "dtable_uuid": self.dtable_uuid},
            "value": {"index": index, "data": orjson.loads(data)},
        }
        if self.on_update_handler:
            self.on_update_handler(**msg)
        else:
            print(f"[ SeaTable SocketIO on UPDATE_DTABLE ]")
            print(msg)

    async def on_notification(self, data, index, *args):
        msg = {
            "key": {"workspace_id": self.workspace_id, "dtable_uuid": self.dtable_uuid},
            "value": {"index": index, "data": orjson.loads(data)},
        }
        if self.on_notification_handler:
            self.on_notification_handler(**msg)
        else:
            print(f"[ SeaTable SocketIO on UPDATE_DTABLE ]")
            print(msg)

    # override _handle_disconnect
    async def _handle_disconnect(self, namespace):
        """io server disconnect"""
        self.logger.info("Engine.IO connection disconnected")
        await super()._handle_disconnect(self, namespace=namespace)
