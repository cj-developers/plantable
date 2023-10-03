# Plantable

SeaTable Python SDK

- `client` SeaTable을 제어 - 사용자 및 그룹 관리, 데이터 읽기 및 쓰기 등
- `server` SeaTable의 데이터를 AWS S3 등으로 전송하는 HTTP Server



## Client

UserClient 사용 예제

```python
from plantable import UserClient

# user client 생성
uc = UserClient(
    seatable_url="https://seatable.example.com", 
    seatable_username="itsme", 
    seatable_password="youknownothing"
)

# Workspace 리스트 보기
await uc.ls()

# Workspace 내 Base 리스트 보기
await uc.ls("my-workspace")

# Workspace / Base 내 리스트 보기 (Tables, Views)
await uc.ls("my-workspace", "some-base")

# BaseClient 생성하기 (Table 읽기/쓰기 위해서는 BaseClient 필요)
bc = await uc.get_base_client_with_account_token("my-workspace", "some-base")

# Table 읽기
tbl = bc.read_table("my-table")

# View 읽기
view = bc.read_view("my-view")

# Table 또는 View를 Pandas DataFrame으로 바꾸기 
# 1. Pandas 이용
import pandas as pd
df = pd.DataFrame.from_records(tbl)

# 2. PyArrow 이용
import pyarrow as pa
df = pa.Table.from_pylist(tbl).to_pandas()
```





## Server

