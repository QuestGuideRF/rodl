from datetime import date ,datetime
import pandas as pd
try :
    from prefect_loader .orchestration .clickhouse_utils .config import (
    CLICKHOUSE_ACCESS_DATABASE ,
    CLICKHOUSE_ACCESS_PASSWORD ,
    CLICKHOUSE_ACCESS_USER ,
    CLICKHOUSE_DATABASE ,
    CLICKHOUSE_DB_VK ,
    CLICKHOUSE_PASSWORD ,
    CLICKHOUSE_USER ,
    )
    from prefect_loader .orchestration .clickhouse_utils .database import ClickhouseDatabase ,_AsyncEngine
except Exception :
    from orchestration .clickhouse_utils .config import (
    CLICKHOUSE_ACCESS_DATABASE ,
    CLICKHOUSE_ACCESS_PASSWORD ,
    CLICKHOUSE_ACCESS_USER ,
    CLICKHOUSE_DATABASE ,
    CLICKHOUSE_DB_VK ,
    CLICKHOUSE_PASSWORD ,
    CLICKHOUSE_USER ,
    )
    from orchestration .clickhouse_utils .database import ClickhouseDatabase ,_AsyncEngine
class AsyncVkDatabase :
    def __init__ (self ,database :str |None =None ):
        target_db =database or CLICKHOUSE_DB_VK or CLICKHOUSE_DATABASE
        self ._data_db =ClickhouseDatabase (
        database =target_db ,user =CLICKHOUSE_USER ,password =CLICKHOUSE_PASSWORD
        )
        self ._access_db =ClickhouseDatabase (
        database =CLICKHOUSE_ACCESS_DATABASE ,
        user =CLICKHOUSE_ACCESS_USER ,
        password =CLICKHOUSE_ACCESS_PASSWORD ,
        )
        self .engine =_AsyncEngine ()
        self .metadata =type ("Meta",(),{"tables":{}})()
    async def table_exists (self ,table_name :str )->bool :
        exists =await self ._data_db .table_exists (table_name )
        if exists :
            self .metadata .tables .setdefault (table_name ,True )
        return exists
    async def init_db (self ):
        await self ._data_db .ensure_db ()
    async def close_engine (self ):
        return None
    async def write_dataframe_to_table (
    self ,df :pd .DataFrame ,table_name :str
    ):
        if df is None or df .empty :
            return
        await self ._data_db .write_dataframe (table_name ,df )
    @staticmethod
    def _normalize_identifier (value :str |None )->str |None :
        if value is None :
            return None
        normalized =str (value ).strip ()
        return normalized or None
    @staticmethod
    def _normalize_user_id (value :str |int |None )->str |None :
        if value is None :
            return None
        normalized =str (value ).strip ()
        return normalized or None
    async def upsert_access_records (
    self ,
    user_ids :list [str |int |None ],
    token :str ,
    *,
    container :str |None ,
    type_value :str |None ,
    replace :bool ,
    ):
        normalized_ids =[self ._normalize_user_id (uid )for uid in user_ids ]
        await self ._access_db .upsert_accesses (
        normalized_ids ,
        token ,
        container =self ._normalize_identifier (container ),
        service_type ="vk",
        replace =replace ,
        type_value =type_value ,
        )
        self .metadata .tables .setdefault ("Accesses",True )
    async def fetch_vk_access_rows (
    self ,
    *,
    container :str |None =None ,
    type_values :list [str ]|None =None ,
    include_null_type :bool =False ,
    ):
        rows =await self ._access_db .fetch_access_rows (
        service_type ="vk",
        container =self ._normalize_identifier (container ),
        type_values =type_values ,
        include_null_type =include_null_type ,
        )
        if rows :
            self .metadata .tables .setdefault ("Accesses",True )
        return rows
    async def add_agency_token (
    self ,agency_user_id :str ,token :str ,container :str |None =None
    ):
        cont =container or agency_user_id or "agency"
        await self ._access_db .upsert_accesses (
        [agency_user_id ],
        token ,
        container =cont ,
        service_type ="vk",
        replace =True ,
        )
    async def add_client_token (
    self ,client_user_id :str ,token :str ,container :str |None =None
    ):
        cont =container or client_user_id or "client"
        await self ._access_db .upsert_accesses (
        [client_user_id ],
        token ,
        container =cont ,
        service_type ="vk",
        replace =True ,
        )
    async def add_agency_client_list (
    self ,client_list :list [str ],token :str ,container :str
    ):
        await self ._access_db .upsert_accesses (
        client_list ,
        token ,
        container =container ,
        service_type ="vk",
        replace =True ,
        )
    async def get_user_id_token_dictionary (
    self ,container :str |None =None ,*,include_null_type :bool =False
    )->dict [str ,str ]:
        tokens =await self ._access_db .fetch_access_tokens (
        service_type ="vk",
        container =container ,
        include_null_type =include_null_type ,
        )
        cleaned ={k :v for k ,v in tokens .items ()if k }
        if cleaned :
            self .metadata .tables .setdefault ("Accesses",True )
        return cleaned
    async def delete_records_between_dates (
    self ,table_name :str ,start_date :datetime |date ,end_date :datetime |date
    ):
        safe_table_name =table_name .replace ("-","_").replace (".","_")
        await self ._data_db .delete_between_dates (
        safe_table_name ,start_date ,end_date
        )
    async def delete_access (
    self ,
    user_id :str |None ,
    *,
    container :str |None =None ,
    type_value :str |None =None ,
    ):
        await self ._access_db .delete_access (
        self ._normalize_user_id (user_id )if user_id else None ,
        service_type ="vk",
        container =self ._normalize_identifier (container )if container else None ,
        type_value =type_value ,
        )
    async def reset_database (self )->dict [str ,int ]:
        data_tables =await self ._data_db .truncate_all_tables ()
        return {"data_db":data_tables }
    async def get_stale_clients (self ,days_threshold :int =30 ):
        return await self ._data_db .get_stale_tables ("date",days_threshold )
    async def cleanup_clients (self ,client_user_ids :list [str ])->int :
        dropped =0
        for user_id in client_user_ids :
            safe_user_id =self ._normalize_user_id (user_id )
            if not safe_user_id :
                continue
            table_name =f"vk_{safe_user_id }".replace ("-","_").replace (".","_")
            try :
                await self ._data_db .drop_table (table_name )
                dropped +=1
            except Exception :
                pass
        return dropped
__all__ =["AsyncVkDatabase"]