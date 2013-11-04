from datetime import datetime

from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.constants import MULTI
from django.db import connection

from aggregate.client import get_client

from profiler import _get_current_view

def execute_sql(self, *args, **kwargs):
    client = get_client()
    if client is None:
        return self.__execute_sql(*args, **kwargs)
    try:
        q, params = self.as_sql()
        if not q:
            raise EmptyResultSet
    except EmptyResultSet:
        if kwargs.get('result_type', MULTI) == MULTI:
            return iter([])
        else:
            return
    start = datetime.now()
    try:
        return self.__execute_sql(*args, **kwargs)
    finally:
        d = (datetime.now() - start)
        client.insert({'query' : q, 'view' : _get_current_view(), 'type' : 'sql'},
                      {'time' : 0.0 + d.seconds * 1000 + d.microseconds/1000, 'count' : 1})

INSTRUMENTED = False

############ code for pymongo instrumentation #############

def wrap_mongo(orig, query, force_refresh=False):
    def wrapper(self, *args, **kwargs):
        client = get_client()
        if client is None:
            return orig(self, *args, **kwargs)
        try:
            start = datetime.now()
            result = orig(self, *args, **kwargs)
            if force_refresh:
                result._refresh()
            return result
        finally:
            d = datetime.now() - start
            log_q = query(self, *args, **kwargs) if callable(query) else query
            log_key = dict(query=log_q,
                           view=_get_current_view(),
                           type='mongo')
            log_val = dict(time=(0.0 + d.seconds * 1000 + d.microseconds/1000), count=1)
            print "Logging {} : {}".format(log_key, log_val)
            client.insert(log_key, log_val)
    return wrapper


def find_action(collection, *args, **kwargs):
    return u"{} : {}".format(collection.name, kwargs.get('spec', 'ALL'))

def instrument_mongo():
    try:
        from pymongo.collection import Collection
        print "initializing pymongo"
        Collection.save = wrap_mongo(Collection.save, 'save')
        Collection.insert = wrap_mongo(Collection.insert, 'insert')
        Collection.update = wrap_mongo(Collection.update, 'update')
        Collection.remove = wrap_mongo(Collection.remove, 'remove')
        Collection.count = wrap_mongo(Collection.count, 'count')
        Collection.find = wrap_mongo(Collection.find, find_action, force_refresh=True)
    except ImportError:
        print "Failed to import pymongo"
        pass


######## This attaches the instrumentation on import ########

if not INSTRUMENTED:
    SQLCompiler.__execute_sql = SQLCompiler.execute_sql
    SQLCompiler.execute_sql = execute_sql
    instrument_mongo()
    INSTRUMENTED = True
