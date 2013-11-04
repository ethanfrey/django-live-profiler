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

def wrap_mongo(orig, query):
    def wrapper(self, *args, **kwargs):
        client = get_client()
        if client is None:
            return orig(self, *args, **kwargs)
        try:
            start = datetime.now()
            result = orig(self, *args, **kwargs)
            return result
        finally:
            d = datetime.now() - start
            # None return value is key to not log
            log_q = query(self, delay=d, *args, **kwargs) if callable(query) else query
            if log_q is not None:
                log_key = dict(query=log_q,
                            view=_get_current_view(),
                            type='mongo')
                log_val = dict(time=(0.0 + d.seconds * 1000 + d.microseconds/1000), count=1)
                client.insert(log_key, log_val)
    return wrapper


def refresh_action(cursor, delay, *args, **kwargs):
    """
    This is called whenever a cursor is accessed, and the data actually read into python.
    The Collection.find just returns a Cursor object that can be manipulated further before
    actually making the DB call, rendering timing useless there.
    Sorry to hack into private methods of cursor, but I don't know why they need to hide an accessor,
    it's not like I'm changing the query, I just want to look at the cursor.
    In mongo engine, this method is used by find and count (and maybe more?)
    """
    name = cursor.collection.name
    spec = cursor._Cursor__spec
    cursor.num_calls = getattr(cursor, 'num_calls', 0) + 1
    # don't record if 2nd or more refresh takes no time
    # refresh is called to make the call, but also when the data is all read to check if there is more data
    # usually, the refresh takes little-to-no time, at least on localhost
    # but it is nice to log it for the general case.
    if (cursor.num_calls > 1) and delay.seconds == 0 and d.microseconds < 10:
        return None
    if name == '$cmd':
        action = [k for k in spec.keys() if not k in ('fields', 'query')]
        if len(action):
            name = "{} ({})".format(spec[action[0]], action[0])
    log_spec = spec.get('query', spec).copy()
    for key in log_spec.keys():
        if not key == "_cls":
            log_spec[key] = '?'
    return u"{} #{}: {}".format(name, cursor.num_calls, log_spec)


def instrument_mongo():
    try:
        from pymongo.collection import Collection
        from pymongo.cursor import Cursor
        # no real need to profile save, it delegates to either insert or update
        # Collection.save = wrap_mongo(Collection.save, 'save')
        Collection.insert = wrap_mongo(Collection.insert, 'insert')
        Collection.update = wrap_mongo(Collection.update, 'update')
        Collection.remove = wrap_mongo(Collection.remove, 'remove')
        # count is just a wrapper around find with some more infos, we record this in refresh_action
        # Collection.count = wrap_mongo(Collection.count, 'count')
        Cursor._refresh = wrap_mongo(Cursor._refresh, refresh_action)
    except ImportError:
        pass


######## This attaches the instrumentation on import ########

if not INSTRUMENTED:
    SQLCompiler.__execute_sql = SQLCompiler.execute_sql
    SQLCompiler.execute_sql = execute_sql
    instrument_mongo()
    INSTRUMENTED = True
