import clickhouse_connect
from .config import (
CLICKHOUSE_DATABASE ,
CLICKHOUSE_HOST ,
CLICKHOUSE_PASSWORD ,
CLICKHOUSE_PORT ,
CLICKHOUSE_POOL_BLOCK ,
CLICKHOUSE_POOL_SIZE ,
CLICKHOUSE_SECURE ,
CLICKHOUSE_USER ,
)
def get_client (
database :str |None =None ,
username :str |None =None ,
password :str |None =None ,
):
    db =database or CLICKHOUSE_DATABASE
    user =username or CLICKHOUSE_USER or "default"
    pwd =password or CLICKHOUSE_PASSWORD or ""
    pool_kwargs ={"maxsize":CLICKHOUSE_POOL_SIZE }
    if CLICKHOUSE_POOL_BLOCK :
        pool_kwargs ["block"]=True
    base_kwargs =dict (
    host =CLICKHOUSE_HOST ,
    port =CLICKHOUSE_PORT ,
    username =user ,
    password =pwd ,
    secure =CLICKHOUSE_SECURE ,
    database =db ,
    )
    attempts =[
    dict (base_kwargs ,pool_mgr_kwargs =pool_kwargs ),
    dict (base_kwargs ,pool_maxsize =CLICKHOUSE_POOL_SIZE ),
    base_kwargs ,
    ]
    for kwargs in attempts :
        try :
            return clickhouse_connect .create_client (**kwargs )
        except TypeError :
            continue
    return clickhouse_connect .create_client (**base_kwargs )
def ensure_database (client )->None :
    database =getattr (client ,"database",None )or CLICKHOUSE_DATABASE
    try :
        client .command (f"CREATE DATABASE IF NOT EXISTS {database }")
    except Exception as exc :
        message =str (exc )
        if "ACCESS_DENIED"in message or "Недостаточно привилегий"in message :
            return
        if "Сервер отключен"in message or "соединение было закрыто"in message :
            fresh =get_client (database =database )
            fresh .command (f"CREATE DATABASE IF NOT EXISTS {database }")
            return
        raise
__all__ =["get_client","ensure_database"]