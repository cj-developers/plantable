from ..client import AccountClient

REF_TABLE = "test_api"


async def update_ref_table(
    client: AccountClient,
    ref_group_name: str = "GROUP_ADMIN",
    ref_base_name: str = "task",
    ref_table_name: str = "base",
):
    # ref client
    ref_base = await client.get_base_by_name(group_name=ref_group_name, base_name=ref_base_name)
    ref_base_client = await client.get_base_client(base=ref_base)

    # create base if not exists

    # list bases and update ref table
    bases = await client.list_bases()
    rows = list()
    updates = list()
    for base in bases:
        r = base.to_record()
        old = await ref_base_client.query(f"SELECT * FROM {ref_table_name} WHERE base_uuid = '{base.uuid}'")
        if not old:
            rows.append(r)
            continue
        if len(old) > 1:
            raise
        updates.append({"row_id": old[0]["_id"], "row": r})
    if rows:
        results = await ref_base_client.append_rows(table_name=ref_table_name, rows=rows)
        print(results)
    if updates:
        results = await ref_base_client.update_rows(table_name=ref_table_name, updates=updates)
        print(updates)
        print(results)

    # check deleted
    bases_in_ref_table = await ref_base_client.query(f"SELECT * FROM {ref_table_name}")
    deleted_bases = [x for x in bases_in_ref_table if x["base_uuid"] not in [y.uuid for y in bases]]
    if deleted_bases:
        await ref_base_client.delete_rows(table_name=ref_table_name, row_ids=[x["_id"] for x in deleted_bases])
