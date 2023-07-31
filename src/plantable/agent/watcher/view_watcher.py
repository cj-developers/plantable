# import asyncio
# from typing import Callable

# from plantable.client import AdminClient, BaseWebsocketClient


# # Seatable Watcher
# class SeatableViewWatcher:
#     def __init__(
#         self,
#         seatable_url: str,
#         seatable_username: str,
#         seatable_password: str,
#         handler: Callable = None,
#         wait_for: float = 10,
#     ):
#         self.seatable_url = seatable_url
#         self.seatable_username = seatable_username
#         self.seatable_password = seatable_password
#         self.handler = handler
#         self.wait_for = wait_for

#         self.client = AdminClient(
#             seatable_url=self.seatable_url,
#             seatable_username=self.seatable_username,
#             seatable_password=self.seatable_password,
#         )

#         self.store = dict()

#     # run
#     async def run(self, debug: bool = False):
#         await self.client.login()
#         try:
#             await self.watch(debug=debug)
#         except asyncio.CancelledError() as ex:
#             raise ex

#     # list views with name
#     async def watch(self, view_suffix: str = None, debug: bool = False, wait_for: float = 5.0):
#         while True:
#             store = dict()
#             groups = await self.client.list_groups()
#             for group in groups:
#                 if group.id not in store:
#                     store[group.id] = dict()
#                 bases = await self.client.list_group_bases(group.name)
#                 for base in bases:
#                     if base.id not in store[group.id]:
#                         store[group.id].update({base.id: dict()})
#                     bc = await self.client.get_base_client_with_account_token(
#                         group_name_or_id=group.id, base_name=base.name
#                     )
#                     tables = await bc.list_tables()
#                     for table in tables:
#                         if table.id not in store[group.id][base.id]:
#                             store[group.id][base.id].update({table.id: dict()})
#                         views = await bc.list_views(table.name)
#                         for view in views:
#                             if view.id not in store[group.id][base.id][table.id]:
#                                 store[group.id][base.id][table.id].update({view.id: view.dict()})

#             # remove deleted bases
#             for group_id, bases in self.store.items():
#                 if group_id not in store:
#                     for base_id, task in bases.items():
#                         task.cancel()
#                         print(f"task {group_id}/{base_id} removed!")
#                 for base_id, task in bases.items():
#                     if base_id not in tasks[group_id]:
#                         task.cancel()
#                         print(f"task {group_id}/{base_id} removed!")

#             # update tasks
#             for group_id, bases in tasks.items():
#                 if group_id not in self.store:
#                     self.store[group_id] = dict()
#                 for base_id, base_name in bases.items():
#                     if base_id not in self.store[group_id] or self.store[group_id][base_id].done():
#                         try:
#                             ws = await self.create_websocket(group_id=group_id, base_name=base_name)
#                             self.store[group_id][base_id] = asyncio.create_task(ws.run())
#                             print(f"task {group_id}/{base_id} registered!")
#                         except Exception as ex:
#                             print(f"ERROR - {group_id}/{base_id} - {ex}")

#             if debug:
#                 break

#             await asyncio.sleep(wait_for)

#     # Create Websocket
#     async def create_websocket(self, group_id: int, base_name: str):
#         workspace_id = await self.client.infer_workspace_id(group_name_or_id=group_id)
#         return BaseWebsocketClient(
#             seatable_url=self.seatable_url,
#             seatable_username=self.seatable_username,
#             seatable_password=self.seatable_password,
#             workspace_id=workspace_id,
#             base_name=base_name,
#         )
