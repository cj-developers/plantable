import asyncio
import logging
from typing import Callable

from plantable.client import AdminClient

from .conf import SEATABLE_PASSWORD, SEATABLE_URL, SEATABLE_USERNAME
from .ws_client import BaseWebsocketClient

logger = logging.getLogger(__file__)


# Seatable Producer
class Producer:
    def __init__(
        self,
        seatable_url: str = SEATABLE_URL,
        seatable_username: str = SEATABLE_USERNAME,
        seatable_password: str = SEATABLE_PASSWORD,
        handler: Callable = None,
        wait_for: float = 10.0,
    ):
        self.seatable_url = seatable_url
        self.seatable_username = seatable_username
        self.seatable_password = seatable_password
        self.handler = handler
        self.wait_for = wait_for

        self.client = AdminClient(
            seatable_url=self.seatable_url,
            seatable_username=self.seatable_username,
            seatable_password=self.seatable_password,
        )

        self.tasks = dict()
        self.clients = dict()

    # run
    async def run(self, debug: bool = False):
        try:
            await self.watch(debug=debug)
        except asyncio.CancelledError as ex:
            logger.error("Canelled!")
            for group, bases in self.tasks.items():
                for base, task in bases.items():
                    await self.cancel_task(task)
                    _msg = f"Removed: Group {group}, Base {base}"
                    print(_msg)
            raise ex

    # list views with name
    async def watch(self, debug: bool = False):
        while True:
            tasks = dict()
            groups = await self.client.list_groups()
            for group in groups:
                if group.id not in tasks:
                    tasks[group.id] = dict()
                bases = await self.client.list_group_bases(group.name)
                for base in bases:
                    if base.id not in tasks[group.id]:
                        tasks[group.id].update({base.id: base.name})

            # remove deleted bases
            for group_id, bases in self.tasks.items():
                if group_id not in tasks:
                    for base_id, task in bases.items():
                        await self.cancel_task(task)
                        _msg = f"Removed: Group {group_id}, Base {base_id}"
                        print(_msg)
                for base_id, task in bases.items():
                    if base_id not in tasks[group_id]:
                        await self.cancel_task(task)
                        _msg = f"Removed: {group_id}, Base {base_id}"
                        print(_msg)

            # update tasks
            for group_id, bases in tasks.items():
                if group_id not in self.tasks:
                    self.tasks[group_id] = dict()
                for base_id, base_name in bases.items():
                    if base_id not in self.tasks[group_id] or self.tasks[group_id][base_id].done():
                        try:
                            self.tasks[group_id][base_id] = asyncio.create_task(
                                self.run_websocket(group_id=group_id, base_name=base_name)
                            )
                            _msg = f"Registered: Group {group_id}, Base {base_id}"
                            print(_msg)
                        except Exception as ex:
                            _msg = f"FAILED: create websocket and run - Group {group_id}, Base {base_id}"
                            logger.warning(_msg)

            if debug:
                break

            await asyncio.sleep(self.wait_for)

    # Create Websocket
    async def run_websocket(self, group_id: int, base_name: str):
        client = BaseWebsocketClient(
            group_name_or_id=group_id,
            base_name=base_name,
            seatable_url=self.seatable_url,
            seatable_username=self.seatable_username,
            seatable_password=self.seatable_password,
            handler=self.handler,
        )
        try:
            await client.run()
        except asyncio.CancelledError as ex:
            await client.disconnect()
            raise ex

    @staticmethod
    async def cancel_task(task):
        task.cancel()
        while not task.cancelled():
            await asyncio.sleep(0.1)
