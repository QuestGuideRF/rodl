from dataclasses import dataclass
from datetime import datetime ,timedelta
from typing import Any ,Optional
from prefect_loader .orchestration .clickhouse_utils import AsyncMetrikaDatabase
from .access import collect_metrika_access_data
from .change_utils import format_auth_fingerprint ,mask_token
from .change_tracker import MetrikaChangeTracker
from prefect import get_run_logger as get_logger
@dataclass (frozen =True )
class MetrikaReloadJob :
    counter_id :int
    domain_name :str
    token :str
    start_date :str
    end_date :str
    source :str ="unknown"
    fact_login :str |None =None
    def as_dict (self )->dict [str ,Any ]:
        return {
        "counter_id":self .counter_id ,
        "domain_name":self .domain_name ,
        "token":self .token ,
        "start_date":self .start_date ,
        "end_date":self .end_date ,
        "source":self .source ,
        "fact_login":self .fact_login ,
        }
    @classmethod
    def from_dict (cls ,payload :dict [str ,Any ])->"MetrikaReloadJob":
        return cls (
        counter_id =int (payload ["counter_id"]),
        domain_name =str (payload ["domain_name"]),
        token =str (payload ["token"]),
        start_date =str (payload ["start_date"]),
        end_date =str (payload ["end_date"]),
        source =str (payload .get ("source","unknown")),
        fact_login =payload .get ("fact_login"),
        )
async def plan_metrika_reload_jobs (
*,
lookback_days :int =60 ,
counter_id :Optional [int ]=None ,
db :AsyncMetrikaDatabase |None =None ,
start_date :str |None =None ,
end_date :str |None =None ,
)->tuple [list [MetrikaReloadJob ],list [dict [str ,Any ]],list [dict [str ,Any ]],list [dict [str ,Any ]]]:
    async_db =db or AsyncMetrikaDatabase ()
    created_db =db is None
    if created_db :
        await async_db .init_db ()
    jobs :list [MetrikaReloadJob ]=[]
    counters_without_changes :list [dict [str ,Any ]]=[]
    failed_counters :list [dict [str ,Any ]]=[]
    counter_diagnostics :list [dict [str ,Any ]]=[]
    try :
        force_ids ={int (counter_id )}if counter_id is not None else None
        df_full_access =await collect_metrika_access_data (
        async_db ,
        favorite_only =True ,
        force_include_ids =force_ids ,
        )
        if df_full_access .empty :
            get_logger ().warning ("После фильтрации доступов токены метрики не найдены; нечего перезагружать.")
            return jobs ,counters_without_changes ,failed_counters ,counter_diagnostics
        if counter_id is not None :
            df_full_access =df_full_access [df_full_access ["id"]==counter_id ]
            if df_full_access .empty :
                get_logger ().warning ("Счетчик %s не найден среди избранных; нечего обрабатывать.",counter_id )
                return jobs ,counters_without_changes ,failed_counters ,counter_diagnostics
        for _ ,row in df_full_access .iterrows ():
            counter =int (row ["id"])
            domain_name =row ["name"]
            token =row ["token"]
            source =row .get ("source","unknown")
            fact_login =row .get ("fact_login")
            get_logger ().info (
            "%s: запустите обнаружение_изменений, используя source=%s, %s",
            domain_name ,
            source ,
            format_auth_fingerprint (fact_login ,token ),
            )
            effective_lookback =lookback_days
            if start_date :
                try :
                    end_boundary =datetime .strptime (end_date ,"%Y-%m-%d").date ()if end_date else datetime .now ().date ()-timedelta (days =1 )
                    start_boundary =datetime .strptime (start_date ,"%Y-%m-%d").date ()
                    delta_days =(end_boundary -start_boundary ).days +1
                    if delta_days >0 :
                        effective_lookback =delta_days
                except Exception :
                    pass
            tracker =MetrikaChangeTracker (
            counter_id =counter ,
            token =token ,
            db =async_db ,
            lookback_days =effective_lookback ,
            login =fact_login ,
            )
            try :
                changes_info =await tracker .detect_changes (domain_name )
            except Exception as exc :
                error_text =str (exc )
                get_logger ().error ("%s: не удалось обнаружить изменение — %s",domain_name ,error_text )
                failed_counters .append (
                {
                "counter_id":counter ,
                "domain_name":domain_name ,
                "source":source ,
                "error":error_text ,
                }
                )
                continue
            diag ={
            "counter_id":counter ,
            "domain_name":domain_name ,
            "fact_login":fact_login ,
            "token_mask":mask_token (token ),
            "api_total_visits":changes_info .get ("api_total_visits",0 ),
            "api_days_with_data":changes_info .get ("api_days_with_data",0 ),
            "db_empty":changes_info .get ("db_empty",True ),
            "db_total_visits":changes_info .get ("db_total_visits",0 ),
            "changes_detected":changes_info .get ("changes_detected",False ),
            }
            counter_diagnostics .append (diag )
            get_logger ().info (
            "%s (counter=%s, %s): API visits=%s (%s days with data), "
            "Посещения БД=%s (пусто=%s), изменения_обнаружены=%s",
            domain_name ,counter ,format_auth_fingerprint (fact_login ,token ),
            diag ["api_total_visits"],diag ["api_days_with_data"],
            diag ["db_total_visits"],diag ["db_empty"],
            diag ["changes_detected"],
            )
            days_to_update =changes_info .get ("days_to_update")or []
            if not days_to_update :
                counters_without_changes .append (
                {"counter_id":counter ,"domain_name":domain_name ,"source":source }
                )
                continue
            for start_str ,end_str in _collapse_date_ranges (days_to_update ):
                try :
                    datetime .strptime (start_str ,"%Y-%m-%d")
                    datetime .strptime (end_str ,"%Y-%m-%d")
                except ValueError :
                    get_logger ().warning ("%s: недопустимый диапазон дат %s–%s, пропуск.",domain_name ,start_str ,end_str )
                    continue
                jobs .append (
                MetrikaReloadJob (
                counter_id =counter ,
                domain_name =domain_name ,
                token =token ,
                start_date =start_str ,
                end_date =end_str ,
                source =source ,
                fact_login =fact_login ,
                )
                )
        get_logger ().info (
        "В средстве отслеживания изменений запланировано %d заданий перезагрузки; счетчики без изменений: %s",
        len (jobs ),
        [item ["counter_id"]for item in counters_without_changes ],
        )
        return jobs ,counters_without_changes ,failed_counters ,counter_diagnostics
    finally :
        if created_db :
            await async_db .close_engine ()
def _collapse_date_ranges (dates :list [str ])->list [tuple [str ,str ]]:
    valid_dates =[]
    for raw_date in set (dates ):
        try :
            valid_dates .append (datetime .strptime (raw_date ,"%Y-%m-%d"))
        except ValueError :
            get_logger ().warning (f"Skipping invalid date while collapsing ranges: {raw_date }")
    if not valid_dates :
        return []
    valid_dates .sort ()
    ranges :list [tuple [str ,str ]]=[]
    range_start =range_end =valid_dates [0 ]
    for current in valid_dates [1 :]:
        if (current -range_end ).days ==1 :
            range_end =current
        else :
            ranges .append ((range_start .strftime ("%Y-%m-%d"),range_end .strftime ("%Y-%m-%d")))
            range_start =range_end =current
    ranges .append ((range_start .strftime ("%Y-%m-%d"),range_end .strftime ("%Y-%m-%d")))
    return ranges
__all__ =["MetrikaReloadJob","plan_metrika_reload_jobs","_collapse_date_ranges"]