from __future__ import annotations
import asyncio
import json
import random as rnd
import secrets
from datetime import datetime
from typing import Optional
import aiohttp
import pandas as pd
from .config import (
DEFAULT_DB_PROFILE ,
DIRECT_BACKOFF_BASE ,
DIRECT_MAX_RETRIES ,
DIRECT_TIMEOUT ,
)
from .logging_utils import get_logger
def normalize_login_to_db (login :str )->str :
    return login .replace ("-","_")if login else ""
def normalize_login_to_api (login :str )->str :
    return login .replace ("_","-")if login else ""
def build_direct_headers (token :str ,client_login :str |None =None )->dict [str ,str ]:
    headers ={
    "Authorization":f"Bearer {token }",
    "Accept-Language":"ru",
    "Client-Login":normalize_login_to_api (client_login )if client_login else "",
    "Use-Operator-Units":"true",
    "RequestId":secrets .token_hex (8 ),
    }
    if not client_login :
        headers .pop ("Client-Login",None )
    return headers
async def direct_post_json (
session :aiohttp .ClientSession ,
url :str ,
body :dict ,
*,
token :str ,
client_login :str |None =None ,
semaphore :asyncio .Semaphore |None =None ,
max_retries :int =DIRECT_MAX_RETRIES ,
)->dict :
    headers =build_direct_headers (token ,client_login )
    headers .setdefault ("processingMode","auto")
    attempt =0
    sem =semaphore or asyncio .Semaphore (10 )
    while True :
        attempt +=1
        try :
            async with sem :
                async with session .post (url ,json =body ,headers =headers ,timeout =DIRECT_TIMEOUT )as resp :
                    status =resp .status
                    text =await resp .text ()
                    try :
                        payload =json .loads (text )
                    except json .JSONDecodeError :
                        payload ={"raw":text }
                    if status in (429 ,500 ,502 ,503 ,504 ):
                        retry_in =resp .headers .get ("RetryIn")or resp .headers .get ("retryin")
                        if retry_in :
                            delay =float (retry_in )
                        else :
                            delay =min (60 ,DIRECT_BACKOFF_BASE **attempt +rnd .random ())
                        get_logger ().warning (f"Direct API {url } throttled ({status }), retry in {delay }s")
                        if attempt >=max_retries :
                            raise RuntimeError (f"Direct API throttled ({status }) after {attempt } attempts: {text }")
                        await asyncio .sleep (delay )
                        continue
                    if isinstance (payload ,dict )and "error"in payload :
                        error =payload ["error"]
                        if not isinstance (error ,dict ):
                            raise RuntimeError (f"Direct API error (malformed): {error }")
                        code =error .get ("error_code")or error .get ("errorCode")
                        message =error .get ("error_detail")or error .get ("error_string")or error
                        if code =="9000":
                            delay =60
                            get_logger ().info (f"Direct queue full, retry in {delay }s")
                            if attempt >=max_retries :
                                raise RuntimeError (f"Direct queue full after {attempt } attempts: {message }")
                            await asyncio .sleep (delay )
                            continue
                        raise RuntimeError (f"Direct API error ({code }): {message }")
                    if status >=400 :
                        raise RuntimeError (f"Direct API HTTP {status }: {text }")
                    return payload
        except Exception as exc :
            if attempt >=max_retries :
                raise
            delay =min (60 ,DIRECT_BACKOFF_BASE **attempt +rnd .random ())
            get_logger ().warning (f"Direct request retry {attempt }/{max_retries } after error {exc }; sleeping {delay }s")
            await asyncio .sleep (delay )
async def get_goal_ids_by_client (
token :str ,
client_login :str ,
*,
timeout :Optional [aiohttp .ClientTimeout ]=None ,
)->list [int ]:
    client_login =normalize_login_to_api (client_login )
    url ="https://api.direct.yandex.com/json/v5/campaigns"
    headers ={
    "Authorization":f"Bearer {token }",
    "Accept-Language":"ru",
    "Client-Login":client_login ,
    }
    body ={
    "method":"get",
    "params":{
    "SelectionCriteria":{
    "Types":[
    "TEXT_CAMPAIGN",
    "MOBILE_APP_CAMPAIGN",
    "DYNAMIC_TEXT_CAMPAIGN",
    "CPM_BANNER_CAMPAIGN",
    "SMART_CAMPAIGN",
    "UNIFIED_CAMPAIGN",
    ],
    "States":["ON","OFF","SUSPENDED","ARCHIVED","CONVERTED","ENDED"],
    "Statuses":["ACCEPTED","DRAFT","MODERATION","REJECTED"],
    },
    "FieldNames":["Id","Name"],
    "TextCampaignFieldNames":["PriorityGoals"],
    "DynamicTextCampaignFieldNames":["PriorityGoals"],
    "SmartCampaignFieldNames":["PriorityGoals"],
    "Page":{"Limit":10_000 ,"Offset":0 },
    },
    }
    goal_ids :set [int ]=set ()
    offset =0
    session_timeout =timeout or DIRECT_TIMEOUT
    while True :
        body ["params"]["Page"]["Offset"]=offset
        try :
            async with aiohttp .ClientSession (timeout =session_timeout )as session :
                async with session .post (url ,json =body ,headers =headers )as r :
                    if r .status !=200 :
                        get_logger ().warning (
                        f"{client_login }: Campaigns HTTP {r .status }: {await r .text ()}"
                        )
                        return sorted (goal_ids )
                    try :
                        js =await r .json (content_type =None )
                    except Exception as e :
                        get_logger ().warning (
                        f"{client_login }: Campaigns non-JSON payload ({e })"
                        )
                        return sorted (goal_ids )
        except Exception as exc :
            get_logger ().error (f"{client_login }: Campaigns fetch error — {exc }")
            return sorted (goal_ids )
        if not isinstance (js ,dict ):
            get_logger ().warning (f"{client_login }: Campaigns payload not a dict")
            return sorted (goal_ids )
        result_obj =js .get ("result")
        if not isinstance (result_obj ,dict ):
            get_logger ().warning (f"{client_login }: 'result' not a dict")
            return sorted (goal_ids )
        campaigns_raw =result_obj .get ("Campaigns")or []
        campaigns :list [dict ]=(
        campaigns_raw if isinstance (campaigns_raw ,list )else []
        )
        for camp in campaigns :
            if not isinstance (camp ,dict ):
                continue
            for capkey in ("TextCampaign","DynamicTextCampaign","SmartCampaign"):
                cap =camp .get (capkey )
                if not isinstance (cap ,dict ):
                    continue
                prio =cap .get ("PriorityGoals")
                if not isinstance (prio ,dict ):
                    continue
                items =prio .get ("Items")or []
                if not isinstance (items ,list ):
                    continue
                for g in items :
                    if isinstance (g ,dict ):
                        gid =g .get ("GoalId")
                        if isinstance (gid ,int ):
                            goal_ids .add (gid )
        limited_by =result_obj .get ("LimitedBy")
        if isinstance (limited_by ,int ):
            offset =limited_by
        else :
            break
    goal_ids .discard (12 )
    return sorted (goal_ids )
def get_conversion_columns (df :pd .DataFrame ,prefix :str ="Conv")->list [str ]:
    return [col for col in df .columns if col .startswith (prefix )]
def fill_na_for_conversion_columns (df :pd .DataFrame ,prefix :str ="Conv")->pd .DataFrame :
    conversion_columns =get_conversion_columns (df ,prefix )
    if conversion_columns :
        df [conversion_columns ]=df [conversion_columns ].fillna (0 )
    return df
def cast_conversion_columns_to_int (df :pd .DataFrame ,prefix :str ="Conv")->pd .DataFrame :
    conversion_columns =get_conversion_columns (df ,prefix )
    if conversion_columns :
        df [conversion_columns ]=df [conversion_columns ].astype (int )
    return df
def process_conversion_columns (
df :pd .DataFrame ,
prefix :str ="Conv",
*,
add_sum :bool =False ,
)->pd .DataFrame :
    df =fill_na_for_conversion_columns (df ,prefix )
    df =cast_conversion_columns_to_int (df ,prefix )
    if add_sum :
        conversion_columns =get_conversion_columns (df ,prefix )
        if conversion_columns :
            df ['SumConversion']=df [conversion_columns ].sum (axis =1 )
    return df
def normalize_conversion_columns_case_insensitive (
df :pd .DataFrame ,
target_prefix :str ="Conversions",
)->list [str ]:
    return sorted (c for c in df .columns if c .lower ().startswith (target_prefix .lower ()))
def convert_days_to_date_ranges (days :list [str ])->list [tuple [str ,str ]]:
    if not days :
        return []
    dates =sorted ({datetime .strptime (d ,"%Y-%m-%d")for d in days })
    ranges :list [tuple [str ,str ]]=[]
    start =end =dates [0 ]
    for d in dates [1 :]:
        if (d -end ).days ==1 :
            end =d
        else :
            ranges .append ((start .strftime ("%Y-%m-%d"),end .strftime ("%Y-%m-%d")))
            start =end =d
    ranges .append ((start .strftime ("%Y-%m-%d"),end .strftime ("%Y-%m-%d")))
    return ranges
def parse_date (date_string :str ,format :str ="%Y-%m-%d")->datetime :
    return datetime .strptime (date_string ,format )
def format_date (date_obj :datetime ,format :str ="%Y-%m-%d")->str :
    return date_obj .strftime (format )
async def emit_prefect_event (
event :str ,
*,
login :str ,
profile :str =DEFAULT_DB_PROFILE ,
payload :dict |None =None ,
)->None :
    try :
        from prefect .events import emit_event as prefect_emit
        from prefect .context import get_run_context
        get_run_context ()
    except Exception :
        return
    resource ={
    "prefect.resource.id":f"direct.client.{login }",
    "direct.client":login ,
    "direct.profile":profile ,
    }
    try :
        await prefect_emit (
        event =event ,
        resource =resource ,
        payload =payload or {},
        )
    except Exception :
        get_logger ().debug ("Не удалось отправить событие префекта %s.",event ,exc_info =True )