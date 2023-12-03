import asyncio
import logging
from datetime import datetime
from typing import List, Union

import aiohttp
import orjson
from pydantic import BaseModel
from tabulate import tabulate

from ..model import Admin, ApiToken, Base, BaseToken, Column, File, Table, Team, User, Webhook
from .conf import SEATABLE_URL

logger = logging.getLogger()

TABULATE_CONF = {"tablefmt": "psql", "headers": "keys"}


def parse_name(*name, delim: str = "/"):
    return [x for e in name for x in (e.split(delim) if e else [None])][: len(name)]


################################################################
# HttpClient
################################################################
class HttpClient:
    def __init__(self, seatable_url: str = SEATABLE_URL):
        self.seatable_url = seatable_url.rstrip("/")

        self.headers = {"accept": "application/json"}
        self.debug = False
        self._request = None

    async def info(self):
        async with self.session_maker() as session:
            return await self.request(session=session, method="GET", url="/server-info/")

    async def ping(self):
        async with self.session_maker() as session:
            return await self.request(session=session, method="GET", url="/api2/ping/")

    def session_maker(self, token: str = None):
        headers = self.headers.copy()
        if token:
            headers.update({"authorization": "Bearer {}".format(token)})
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

            if self.debug:
                print(response.headers)
                return await response.content()
            try:
                if response.content_type in ["application/json"]:
                    return await response.json()
                if response.content_type in ["text/html"]:
                    logger.warning(f"! content-type: {response.content_type}")
                    body = await response.text()
                    return orjson.loads(body)
                if response.content_type in [
                    "application/ms-excel",
                    "application/x-zip-compressed",
                ]:
                    content = b""
                    async for data in response.content.iter_chunked(2048):
                        content += data
                    if len(content) != response.content_length:
                        raise ValueError()
                    return File(filename=response.content_disposition.filename, content=content)

            except Exception as ex:
                raise ex

    @staticmethod
    def print(records: List[dict], tabulate_conf: dict = TABULATE_CONF):
        print(tabulate(records, **tabulate_conf))
