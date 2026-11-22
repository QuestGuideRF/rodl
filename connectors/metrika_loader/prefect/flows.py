import sys
from pathlib import Path
from prefect import flow ,get_run_logger ,task
PROJECT_ROOT =Path (__file__ ).resolve ().parents [3 ]
ROOT_PATH =PROJECT_ROOT .parent .as_posix ()
if ROOT_PATH not in sys .path :
    sys .path .insert (0 ,ROOT_PATH )
from prefect_loader .connectors .metrika_loader import (
MetrikaReloadJob ,
fetch_metrika_job_data ,
plan_metrika_reload_jobs ,
refresh_data_with_change_tracker ,
upload_data_for_all_counters ,
write_metrika_job_data ,
)
from prefect_loader .connectors .metrika_loader .change_utils import format_auth_fingerprint
MAX_PARALLEL_FETCH =9
@task (name ="Загрузчик Метрики: диапазон дат",retries =2 ,retry_delay_seconds =25 ,timeout_seconds =60 *60 *6 )
async def run_metrika_range (start_date :str ,end_date :str )->None :
    await upload_data_for_all_counters (start_date ,end_date )
@task (name ="Загрузчик Метрики: трекер изменений",retries =2 ,retry_delay_seconds =20 ,timeout_seconds =60 *60 *6 )
async def run_metrika_change_tracker (lookback_days :int =60 ,counter_id :int |None =None )->None :
    await refresh_data_with_change_tracker (lookback_days =lookback_days ,counter_id =counter_id )
@task (
name ="Загрузчик Метрики: планируйте задания по перезагрузке",
retries =2 ,
retry_delay_seconds =60 ,
timeout_seconds =600 ,
persist_result =False ,
)
async def plan_metrika_jobs_task (
lookback_days :int =60 ,
counter_id :int |None =None ,
start_date :str |None =None ,
end_date :str |None =None ,
)->dict [str ,object ]:
    logger =get_run_logger ()
    jobs ,counters_without_changes ,failed_counters ,counter_diagnostics =await plan_metrika_reload_jobs (
    lookback_days =lookback_days ,
    counter_id =counter_id ,
    start_date =start_date ,
    end_date =end_date ,
    )
    for diag in counter_diagnostics :
        logger .info (
        "Counter %s (%s, login=%s, token=%s): API visits=%s (%s days with data), DB visits=%s (empty=%s), changes_detected=%s",
        diag .get ("counter_id"),
        diag .get ("domain_name"),
        diag .get ("fact_login")or "unknown",
        diag .get ("token_mask")or "<пусто>",
        diag .get ("api_total_visits",0 ),
        diag .get ("api_days_with_data",0 ),
        diag .get ("db_total_visits",0 ),
        diag .get ("db_empty",True ),
        diag .get ("changes_detected",False ),
        )
    if counters_without_changes :
        logger .info (
        "Счетчики без изменений (%s дней): %s",
        lookback_days ,
        [item ["counter_id"]for item in counters_without_changes ],
        )
    if failed_counters :
        logger .error (
        "Не удалось обнаружить изменения для счетчиков: %s",
        [item .get ("counter_id")for item in failed_counters ],
        )
    return {
    "jobs":[job .as_dict ()for job in jobs ],
    "no_changes":counters_without_changes ,
    "failed_counters":failed_counters ,
    "counter_diagnostics":counter_diagnostics ,
    }
@task (
name ="Загрузчик Метрики: выбор диапазона",
retries =2 ,
retry_delay_seconds =60 ,
timeout_seconds =60 *60 *3 ,
persist_result =False ,
)
async def fetch_metrika_range_task (job_payload :dict )->object :
    job =MetrikaReloadJob .from_dict (job_payload )
    logger =get_run_logger ()
    logger .info (
    "Получение %s (%s → %s, %s)",
    job .domain_name ,
    job .start_date ,
    job .end_date ,
    format_auth_fingerprint (job .fact_login ,job .token ),
    )
    df =await fetch_metrika_job_data (job )
    if df is None or getattr (df ,"empty",True ):
        raise ValueError (f"{job .domain_name }: empty dataframe for {job .start_date }-{job .end_date }")
    return df
@task (
name ="Загрузчик Метрики: диапазон записи",
retries =2 ,
retry_delay_seconds =60 ,
timeout_seconds =60 *60 *2 ,
persist_result =False ,
)
async def write_metrika_range_task (job_payload :dict ,df )->int :
    job =MetrikaReloadJob .from_dict (job_payload )
    logger =get_run_logger ()
    rows =await write_metrika_job_data (job ,df ,fail_on_empty =True )
    logger .info (
    "Записано строк %s в %s для %s → %s.",
    rows ,
    job .domain_name ,
    job .start_date ,
    job .end_date ,
    )
    return rows
@flow (name ="metrika-loader-clickhouse")
async def metrika_loader_flow (
start_date :str |None =None ,
end_date :str |None =None ,
track_changes :bool =True ,
lookback_days :int =60 ,
counter_id :int |None =None ,
)->None :
    logger =get_run_logger ()
    if track_changes :
        logger .info ("Запуск трекера изменений Метрики (lookback_days=%s, counter_id=%s)",lookback_days ,counter_id )
        plan_future =plan_metrika_jobs_task .submit (
        lookback_days =lookback_days ,
        counter_id =counter_id ,
        start_date =start_date ,
        end_date =end_date ,
        )
        plan_result =plan_future .result ()
        jobs =plan_result .get ("jobs",[])
        failed_counters =plan_result .get ("failed_counters")or []
        if not jobs :
            if failed_counters :
                raise RuntimeError (f"Change detection failed for counters: {failed_counters }")
            logger .info ("Никаких изменений метрики не обнаружено; нечего перезагружать.")
            return
        inflight :list [tuple [dict [str ,object ],object ]]=[]
        def _complete_job ()->None :
            job_payload ,future =inflight .pop (0 )
            df =future .result ()
            write_metrika_range_task .submit (job_payload ,df ).result ()
        for job_payload in jobs :
            inflight .append ((job_payload ,fetch_metrika_range_task .submit (job_payload )))
            if len (inflight )>=MAX_PARALLEL_FETCH :
                _complete_job ()
        while inflight :
            _complete_job ()
        if failed_counters :
            raise RuntimeError (f"Change detection failed for counters: {failed_counters }")
        return
    if not start_date or not end_date :
        raise ValueError ("start_date и end_date необходимы, если track_changes имеет значение False")
    logger .info ("Начинаю загрузку Метрики для %s → %s",start_date ,end_date )
    await run_metrika_range (start_date =start_date ,end_date =end_date )