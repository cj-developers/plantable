import logging
import time
from datetime import datetime, timedelta
from typing import Callable

import requests
import socketio
from fastapi import HTTPException, status

from ..model import BaseToken
from .conf import SEATABLE_URL

logger = logging.getLogger(__file__)

JOIN_ROOM = "join-room"
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
class BaseWebsocketClient(socketio.Client):
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        api_token: str = None,
        request_timeout: int = 30,
    ):
        self.seatable_url = seatable_url
        self.api_token = api_token
        self.base_token = None
        self.base_token_expired = datetime.now() + timedelta(days=3)

        self.update_base_token()
        self.websocket_url = self.seatable_url + f"?dtable_uuid={self.base_token.dtable_uuid}"

        super().__init__(request_timeout=request_timeout)

    def update_base_token(self):
        auth_url = self.seatable_url + "/api/v2.1/dtable/app-access-token/"
        response = requests.get(auth_url, headers={"Authorization": f"Token {self.api_token}"})
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            error_msg = response.json()["error_msg"]
            if error_msg in ["Permission denied."]:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Wrong base token!",
                )
            raise ex
        results = response.json()
        self.base_token = BaseToken(**results)

    def run(self, on_update: Callable = None, on_notification: Callable = None):
        self.on("connect", self.on_connect)
        self.on("disconnect", self.on_disconnect)
        self.on("io-disconnect", self.on_io_disconnect)
        self.on("connect_error", self.on_connect_error)
        self.on(UPDATE_DTABLE, on_update or self.on_update)
        self.on(NEW_NOTIFICATION, on_notification or self.on_notification)
        self.connect(url=self.websocket_url)
        self.wait()

    def on_connect(self):
        if datetime.now() >= self.base_token_expired:
            self.update_base_token()
        self.emit(JOIN_ROOM, (self.base_token.dtable_uuid, self.base_token.access_token))
        logger.info("[ SeaTable SocketIO connection established ]")

    def on_disconnect(self):
        logger.info("[ SeaTable SocketIO connection dropped ]")

    def on_io_disconnect(self, sleep=3):
        logger.warning("[ SeaTable SocketIO connection disconnected ]")
        time.sleep(sleep)
        self.update_base_token()
        self.connect(self.websocket_url)

    def on_connect_error(self, error_msg):
        logger.error("[ SeaTable SocketIO connection error ]", error_msg)

    def on_update(self, data, index, *args):
        print(f"{datetime.now()} [ SeaTable SocketIO on UPDATE_DTABLE ]")
        print(data, index, *args)

    def on_notification(self, data, index, *args):
        """Default is print received data You can overwrite this event"""
        print(f"{datetime.now()} [ SeaTable SocketIO on NEW_NOTIFICATION ]")
        print(data, index, *args)

    # override _handle_disconnect
    def _handle_disconnect(self, namespace):
        """io server disconnect"""
        self.logger.info("Engine.IO connection disconnected")
        if not self.connected:
            return
        self.disconnect()
        namespace = namespace or "/"
        self._trigger_event("io-disconnect", namespace=namespace)
