import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from ..conf import SEATABLE_ACCOUNT_TOKEN, SEATABLE_API_TOKEN, SEATABLE_BASE_TOKEN, SEATABLE_URL
from ..model import Admin, ApiToken, Base, BaseToken, Column, Table, Team, User, Webhook

logger = logging.getLogger()
TABULATE_CONF = {"tablefmt": "psql", "headers": "keys"}


def parse_base(base: Base = None):
    return base.workspace_id, base.name


################################################################
# HttpClient
################################################################
class HttpClient:
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
    ):
        self.seatable_url = seatable_url
        self.headers = {"accept": "application/json"}

        self._request = None

    async def info(self):
        return await self.request(method="GET", url="/server-info/")

    async def ping(self):
        return await self.request(method="GET", url="/api2/ping/")

    def session_maker(self, token: str = None):
        headers = self.headers.copy()
        if token:
            headers.update({"authorization": "Token {}".format(token)})
        return aiohttp.ClientSession(base_url=self.seatable_url, headers=headers)

    async def request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        json: str = None,
        data: bytes = None,
        **params,
    ):
        # for debug
        self._request = {
            "method": method,
            "url": url,
            "json": json if not json else {k: v for k, v in json.items() if v},
            "data": data,
            "params": params if not params else {k: v for k, v in params.items() if v},
        }

        async with session.request(**self._request) as response:
            response.raise_for_status()
            try:
                if response.content_type in ["application/json"]:
                    return await response.json()
                if response.content_type in ["text/html"]:
                    logger.warning(f"! content-type: {response.content_type}")
                    body = await response.text()
                    return orjson.loads(body)
            except Exception as ex:
                raise ex

    @staticmethod
    def print(records: List[dict], tabulate_conf: dict = TABULATE_CONF):
        print(tabulate(records, **tabulate_conf))
