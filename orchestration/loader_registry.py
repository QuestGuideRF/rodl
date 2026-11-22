import logging
import os
from pathlib import Path
from typing import Dict ,Any ,Optional
import yaml
logger =logging .getLogger (__name__ )
_loader_config_cache :Optional [Dict [str ,Any ]]=None
def _get_config_path ()->Path :
    project_root =Path (__file__ ).parent .parent
    config_path_env =os .getenv ("LOADER_CONFIG_PATH")
    if config_path_env :
        p =Path (config_path_env )
        return p if p .is_absolute ()else project_root /p
    return project_root /"config"/"loaders.yaml"
def _load_config ()->Dict [str ,Any ]:
    global _loader_config_cache
    if _loader_config_cache is not None :
        return _loader_config_cache
    config_path =_get_config_path ()
    if not config_path .exists ():
        logger .error (
        "Конфигурация загрузчика не найдена в %s. Все загрузчики будут считаться отключенными.",
        config_path ,
        )
        _loader_config_cache ={"loaders":{}}
        return _loader_config_cache
    try :
        with open (config_path ,"r",encoding ="utf-8")as config_file :
            config =yaml .safe_load (config_file )
        if not isinstance (config ,dict )or "loaders"not in config or not isinstance (config .get ("loaders"),dict ):
            logger .error (
            "Неверная конфигурация загрузчика %s. Все загрузчики будут считаться отключенными.",
            config_path ,
            )
            config ={"loaders":{}}
        _loader_config_cache =config
        logger .info ("Загружена конфигурация загрузчика из %s.",config_path )
        return _loader_config_cache
    except Exception as exc :
        logger .error (
        "Ошибка загрузки конфигурации загрузчика из %s: %s. Все загрузчики будут считаться отключенными.",
        config_path ,
        exc ,
        )
        _loader_config_cache ={"loaders":{}}
        return _loader_config_cache
def is_loader_enabled (loader_name :str )->bool :
    config =_load_config ()
    loaders =config .get ("loaders",{})
    loader_config =loaders .get (loader_name ,{})
    return bool (loader_config .get ("enabled",False ))
def get_loader_config (loader_name :str )->Dict [str ,Any ]:
    config =_load_config ()
    loaders =config .get ("loaders",{})
    return loaders .get (loader_name ,{})
def get_enabled_loaders ()->list [str ]:
    config =_load_config ()
    loaders =config .get ("loaders",{})
    return [
    loader_name
    for loader_name ,loader_config in loaders .items ()
    if bool (loader_config .get ("enabled",False ))
    ]
def get_all_loaders ()->list [str ]:
    config =_load_config ()
    loaders =config .get ("loaders",{})
    return list (loaders .keys ())
def reload_config ()->None :
    global _loader_config_cache
    _loader_config_cache =None
    _load_config ()