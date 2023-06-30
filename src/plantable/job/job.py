# class BaseClientGenerator:
#     def __init__(self, admin_client):
#         self.admin_client = admin_client

#         # Stores
#         self.bases = None
#         self.groups = None

#     async def update(self):
#         self.bases = await self.admin_client.list_bases()
#         self.map_wid_gid = {x.workspace_id: x.group_id for x in self.bases}

#     async def from_shared_view(self, shared_view):
#         return await self.__call__(
#             workspace_id=shared_view.workspace_id,
#             base_name=shared_view.dtable_name,
#         )

#     async def __call__(self, workspace_id, base_name):
#         if self.bases is None:
#             await self.update()
#         group_id = self.map_wid_gid[workspace_id]
#         return await self.admin_client.get_base_client_with_account_token(
#             group_name_or_id=group_id,
#             base_name=base_name,
#         )
