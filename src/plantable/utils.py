from datetime import datetime
import sqlparse

from .const import DT_FMT, TZ


# divide chunks
def divide_chunks(x: list, chunk_size: int):
    for i in range(0, len(x), chunk_size):
        yield x[i : i + chunk_size]


# parse string datetime
def parse_str_datetime(x):
    if x.endswith("Z"):
        x = x.replace("Z", "+00:00", 1)
    try:
        x = datetime.strptime(x, DT_FMT)
    except Exception:
        x = datetime.fromisoformat(x)
    return x.astimezone(TZ)


# extract table name error
class NoTableName(Exception):
    def __init__(self, msg):
        super().__init__(msg)


# extract table name
def extract_table_name(sql):
    statements = sqlparse.parse(sql)
    statement = statements[0]

    # FROM 절을 찾습니다.
    from_seen = False
    for token in statement.tokens:
        if from_seen:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    return identifier.get_real_name()
            if isinstance(token, sqlparse.sql.Identifier):
                return token.get_real_name()
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
            from_seen = True
    else:
        _msg = "no table name found '{sql}'!"
        raise NoTableName(_msg)


# extract columns
def extract_columns_from_select(sql):
    statements = sqlparse.parse(sql)
    statement = statements[0]

    select_seen = False
    columns = []
    for token in statement.tokens:
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == "SELECT":
            select_seen = True
        if select_seen:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(identifier.get_real_name())
            elif isinstance(token, sqlparse.sql.Identifier):
                columns.append(token.get_real_name())
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                break
    return columns
