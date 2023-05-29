import aiohttp
import asyncio

from .conf import (
    SEATABLE_ACCOUNT_TOKEN,
    SEATABLE_API_TOKEN,
    SEATABLE_URL,
    SEATABLE_BASE_TOKEN,
)

from .model.account import Dtable

PAGE_INFO = "page_info"


class AccountClient:
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        account_token: str = SEATABLE_ACCOUNT_TOKEN,
    ):
        self.seatable_url = seatable_url
        self.account_token = account_token
        self.headers = {"accept": "application/json"}
        self.session = None

    async def info(self):
        return await self.request(method="GET", url="/server-info/")

    async def ping(self):
        return await self.request(method="GET", url="/api2/ping/")

    async def login(self, username: str, password: str):
        session = self.make_session()
        response = await self.request(
            session=session,
            method="POST",
            url="/api2/auth-token/",
            json={"username": username, "password": password},
        )
        await session.close()
        self.account_token = response["token"]

    def make_session(self, token: str = None):
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
        **params,
    ):
        print("HI")
        async with session.request(
            method=method, url=url, json=json, params=params
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def users(self, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/users"
        ITEM = "data"

        # 1st page
        session = self.make_session(token=self.account_token)
        response = await self.request(session=session, method=METHOD, url=URL, **params)
        users = response[ITEM]

        # all pages
        pages = range(2, response["total_count"] + 1, 1)
        coros = [
            self.request(session=session, method=METHOD, url=URL, page=page, **params)
            for page in pages
        ]
        responses = await asyncio.gather(*coros)
        users += [user for response in responses for user in response[ITEM]]

        # don't forget!
        await session.close()

        return users

    async def admins(self, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/admin-users"
        ITEM = "admin_user_list"

        # admins endpoint has no pagenation
        session = self.make_session(token=self.account_token)
        response = await self.request(session=session, method=METHOD, url=URL, **params)
        admins = response[ITEM]

        # don't forget!
        await session.close()

        return admins

    async def search_users(self, query: str, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/search-user"
        ITEM = "user_list"

        # admins endpoint has no pagenation
        session = self.make_session(token=self.account_token)
        response = await self.request(
            session=session, method=METHOD, url=URL, query=query, **params
        )
        admins = response[ITEM]

        # don't forget!
        await session.close()

        return admins

    async def bases(self, **params):
        # bases는 page_info (has_next_page, current_page)를 제공
        METHOD = "GET"
        URL = "/api/v2.1/admin/dtables"

        # 1st page
        session = self.make_session(token=self.account_token)
        response = await self.request(session=session, method=METHOD, url=URL, **params)
        dtables = response["dtables"]

        # all pages
        while response["page_info"]["has_next_page"]:
            page = response["page_info"]["current_page"] + 1
            params.update({"page": page})
            response = await self.request(
                session=session, method="GET", url=URL, **params
            )
            dtables += [response["dtables"]]

        # don't forget!
        await session.close()

        return dtables
