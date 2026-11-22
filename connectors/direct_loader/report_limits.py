from __future__ import annotations
import asyncio
import hashlib
import time
from collections import deque
from contextlib import asynccontextmanager
from .logging_utils import get_logger
def _now ()->float :
    return time .monotonic ()
def token_fingerprint (token :str )->str :
    return hashlib .sha1 (token .encode ("utf-8")).hexdigest ()[:8 ]
class GlobalRateLimiter :
    def __init__ (self ,max_requests :int ,window_seconds :float )->None :
        self .max_requests =max_requests
        self .window =window_seconds
        self ._request_times :deque [float ]=deque ()
        self ._lock :asyncio .Lock |None =None
        self ._lock_loop :asyncio .AbstractEventLoop |None =None
    def _get_lock (self )->asyncio .Lock :
        try :
            current_loop =asyncio .get_running_loop ()
        except RuntimeError :
            current_loop =None
        if self ._lock is None or self ._lock_loop is not current_loop :
            self ._lock =asyncio .Lock ()if current_loop else None
            self ._lock_loop =current_loop
        return self ._lock
    async def acquire (self ,client_id :str |None =None )->float :
        waited_total =0.0
        client_label =client_id or "unknown"
        while True :
            async with self ._get_lock ():
                now =_now ()
                while self ._request_times and now -self ._request_times [0 ]>self .window :
                    self ._request_times .popleft ()
                if len (self ._request_times )<self .max_requests :
                    self ._request_times .append (now )
                    return waited_total
                wait =max (0.01 ,self .window -(now -self ._request_times [0 ]))
                waited_total +=wait
                get_logger ().debug (
                "%s: ГЛОБАЛЬНОЕ регулирование — достигнут предел %d/%s с, ожидание %.2f с (общее количество запросов в окне: %d)",
                client_label ,
                self .max_requests ,
                self .window ,
                wait ,
                len (self ._request_times ),
                )
            await asyncio .sleep (wait )
class PerAdvertiserRateLimiter :
    def __init__ (self ,max_requests :int ,window_seconds :float )->None :
        self .max_requests =max_requests
        self .window =window_seconds
        self ._windows :dict [str ,deque [float ]]={}
        self ._lock :asyncio .Lock |None =None
        self ._lock_loop :asyncio .AbstractEventLoop |None =None
    def _get_lock (self )->asyncio .Lock :
        try :
            current_loop =asyncio .get_running_loop ()
        except RuntimeError :
            current_loop =None
        if self ._lock is None or self ._lock_loop is not current_loop :
            self ._lock =asyncio .Lock ()if current_loop else None
            self ._lock_loop =current_loop
        return self ._lock
    async def acquire (self ,advertiser :str )->float :
        key =advertiser or "unknown"
        waited_total =0.0
        while True :
            async with self ._get_lock ():
                now =_now ()
                window =self ._windows .setdefault (key ,deque ())
                while window and now -window [0 ]>self .window :
                    window .popleft ()
                if len (window )<self .max_requests :
                    window .append (now )
                    return waited_total
                wait =max (0.01 ,self .window -(now -window [0 ]))
                waited_total +=wait
                get_logger ().info (
                "%s: отчеты о регулировании – достигнуто ограничение %d/%s с, ожидание %.2f с.",
                key ,
                self .max_requests ,
                self .window ,
                wait ,
                )
            await asyncio .sleep (wait )
class OfflineReportTracker :
    def __init__ (self ,max_inflight :int =5 )->None :
        self .max_inflight =max_inflight
        self ._semaphores :dict [str ,asyncio .Semaphore ]={}
        self ._semaphore_loops :dict [str ,asyncio .AbstractEventLoop ]={}
        self ._inflight :dict [str ,int ]={}
        self ._lock :asyncio .Lock |None =None
        self ._lock_loop :asyncio .AbstractEventLoop |None =None
    def _get_lock (self )->asyncio .Lock :
        try :
            current_loop =asyncio .get_running_loop ()
        except RuntimeError :
            current_loop =None
        if self ._lock is None or self ._lock_loop is not current_loop :
            self ._lock =asyncio .Lock ()if current_loop else None
            self ._lock_loop =current_loop
        return self ._lock
    async def _get_sem (self ,key :str )->asyncio .Semaphore :
        try :
            current_loop =asyncio .get_running_loop ()
        except RuntimeError :
            current_loop =None
        async with self ._get_lock ():
            if key not in self ._semaphores or self ._semaphore_loops .get (key )is not current_loop :
                self ._semaphores [key ]=asyncio .Semaphore (self .max_inflight )
                self ._semaphore_loops [key ]=current_loop
            return self ._semaphores [key ]
    @asynccontextmanager
    async def reserve (self ,token :str ,*,owner :str |None =None ):
        user_key =token_fingerprint (token )
        sem =await self ._get_sem (user_key )
        started =_now ()
        await sem .acquire ()
        waited =_now ()-started
        async with self ._get_lock ():
            self ._inflight [user_key ]=self ._inflight .get (user_key ,0 )+1
            current =self ._inflight [user_key ]
        if waited >0.01 :
            get_logger ().debug (
            "%s: очередь офлайн-отчетов переполнена (ожидание %.2f сек., сейчас %d/%d в работе)",
            owner or user_key ,
            waited ,
            current ,
            self .max_inflight ,
            )
        else :
            get_logger ().debug (
            "%s: получен автономный слот (%d/%d в работе)",
            owner or user_key ,
            current ,
            self .max_inflight ,
            )
        try :
            yield current
        finally :
            async with self ._get_lock ():
                self ._inflight [user_key ]=max (self ._inflight .get (user_key ,1 )-1 ,0 )
                current =self ._inflight [user_key ]
            sem .release ()
            get_logger ().debug (
            "%s: слот для автономного режима освобожден (осталось %d/%d)",
            owner or user_key ,
            current ,
            self .max_inflight ,
            )