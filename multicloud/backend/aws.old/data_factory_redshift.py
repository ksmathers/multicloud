"""
DataFactory classes adapt a database connection so that it can run queries originally intended for a different
platform, renaming columns and tables as needed.
"""
from hashlib import md5
from typing import Optional, Dict
import pandas as pd
import re
import os

from generic_templates.zulutime import ZuluTime
from .redshift_client import RedshiftClient
from .data_query import DataQuery, DataQueryRegistry
    
def fingerprint(sval : str) -> str:
    # DF943B29-9D5F-4C6C-A3BB-612BA5F9E1C2
    h = [f'{b:02x}' for b in md5(sval.encode('ASCII')).digest()]
    return f'{h[0]}{h[1]}{h[2]}{h[3]}-{h[4]}{h[5]}-{h[6]}{h[7]}-{h[8]}{h[9]}-{h[10]}{h[11]}{h[12]}{h[13]}{h[14]}{h[15]}'
    

class RedshiftClientDbCursor:
    def __init__(self, dbc, cachedir : str, query_registry : DataQueryRegistry):
        self.dbc : RedshiftClient = dbc
        self.cachedir = cachedir
        self.query_registry = query_registry
        self._data = None

    def cache_path(self, sql):
        query = self.registered_query(sql)
        filename = f"{self.cachedir}/{query.name}"
        return filename
    
    def execute(self, sql):
        #myhash = md5(sql.encode('ASCII')).hexdigest()
        filename = self.cache_path(sql)
        rssql = self.aws_query(sql)
        query = self.registered_query(sql)
        if query.nonquery:
            self._data = None
            self.description = None
            self.dbc.execute_cache(rssql, filename)
        else:
            self._data = self.dbc.sql_query_cache(rssql, filename, with_column_names=query.column_renames)
            self.description = [(x, self._data[x].dtype) for x in self._data.columns]

    def execute_non_query(self, sql):
        """Executes a non-query SQL statement
        Skips execution if the dependent_query is already cached and up to date

        Args:
            sql :str: A SQL statement
            dependent_query :str: A SQL query that depends on first having executed the statement
        """
        now = ZuluTime.now()
        filename = f"{self.cache_path(sql)}-{now.date()}.sql"
        rssql = self.aws_query(sql)
        self.dbc.execute_cache(rssql, filename)

    def is_cached(self, sql):
        path = self.cache_path(sql)
        return self.dbc.is_cached(path)

    def registered_query(self, sql) -> DataQuery:
        # Returns a DataQuery object given SQL text
        query = self.query_registry.reverse_data_queries[sql]
        return query

    def aws_query(self, sql: str) -> str:
        query : DataQuery = self.registered_query(sql)
        return self.translate_query(sql, column_renames = query.column_renames)

    def translate_query(self, sql : str, column_renames : Dict[str, str] = {}, table_renames : Optional[Dict[str, str]] = None, environ : Optional[Dict[str, str]] = None):
        #print("translate_query: (from) ", sql)
        if table_renames is None:
            table_renames = self.query_registry.table_renames
        if environ is None:
            environ = self.query_registry.environment

        def sql_esc(w):
            return w.replace("[",r"\[").replace("]", r"\]").replace(".", r"\.")
        
        for k in table_renames:
            #pat = r" %s([ ,.])" % k.replace("[",r"\[").replace("]", r"\]")
            pat = r"(\W)%s(\W)" % sql_esc(k)
            #print(pat)
            sql = re.sub(pat, r"\1%s\2" % table_renames[k], sql, count=999)

        for k in environ:
            pat = f"%{k}%"
            #print(pat)
            sql = re.sub(pat, environ[k], sql, count=999)

        #sql = sql.lower()
        for k in column_renames:
            pat = r"(\W)%s(\W)" % sql_esc(column_renames[k])
            #print(pat)
            sql = re.sub(pat, r"\1%s\2" % k, sql, count=999)

        #print("translate_query (to):", sql)
        return sql

    def _aws_query(self, sql : str, column_renames : Dict[str, str] = {}, table_renames : Optional[Dict[str, str]] = None, environ : Optional[Dict[str, str]] = None):
        if table_renames is None:
            table_renames = self.query_registry.table_renames
        if environ is None:
            environ = self.query_registry.environment

        # Replace Exponent table names with Redshift equivalents
        for k in table_renames:
            if k in sql:
                sql = sql.replace(k, table_renames[k])

        # Replace Exponent column names with Redshift column names
        if not column_renames is None:
            for k in column_renames:
                c = column_renames[k]
                if c in sql:
                    sql = sql.replace(c, k)

        # Interpolate environment variables that were inserted during rename 
        for k in environ:
            token = f"%{k}%"
            if token in sql:
                sql = sql.replace(token, environ[k])

        #print("query out", sql)
        return sql

    def fetchall(self):
        return self._data.values
        #return [['a', 1], ['b', 2]]

    def load_data(self, table, df : pd.DataFrame, truncate : bool):
        table = self.query_registry.translate_table(table)
        self.dbc.load_data(table, df, use_timestamp=True, truncate=truncate, tmpdir=self.cachedir)
    
    def close(self):
        pass

class RedshiftClientDb:
    def __init__(self, ctx, cachedir : str, query_registry : DataQueryRegistry):
        dbc = RedshiftClient(ctx)
        self.dbc = dbc
        self.cachedir = cachedir
        self.query_registry = query_registry

    def cursor(self):
        return RedshiftClientDbCursor(self.dbc, self.cachedir, self.query_registry)

    def execute(self, sql):
        self.cursor().execute_non_query(sql)

    def load_data(self, table, df, truncate):
        self.cursor().load_data(table, df, truncate)
    
    def rollback(self):
        pass
