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
from .postgres_client import AradDatabase, PostgresClient
from .data_query import DataQuery, DataQueryRegistry
from .pdutil import fix_column_names
    
def fingerprint(sval : str) -> str:
    # DF943B29-9D5F-4C6C-A3BB-612BA5F9E1C2
    h = [f'{b:02x}' for b in md5(sval.encode('ASCII')).digest()]
    return f'{h[0]}{h[1]}{h[2]}{h[3]}-{h[4]}{h[5]}-{h[6]}{h[7]}-{h[8]}{h[9]}-{h[10]}{h[11]}{h[12]}{h[13]}{h[14]}{h[15]}'


class PostgresClientDbCursor:
    def __init__(self, dbc, query_registry : DataQueryRegistry):
        self.dbc : PostgresClient = dbc
        self.query_registry = query_registry
        self._data = None
    
    def execute(self, sql):
        rssql = self.aws_query(sql)
        query = self.registered_query(sql)
        if query.nonquery:
            self._data = None
            self.description = None
            self.dbc.execute(rssql)
        else:
            df = self.dbc.sql_query(rssql)
            if query.column_renames is not None:
                df = fix_column_names(df, query.column_renames)
            self._data = df
            self.description = [(x, self._data[x].dtype) for x in self._data.columns]

    def execute_non_query(self, sql):
        """Executes a non-query SQL statement
        Skips execution if the dependent_query is already cached and up to date

        Args:
            sql :str: A SQL statement
            dependent_query :str: A SQL query that depends on first having executed the statement
        """
        rssql = self.aws_query(sql)
        self.dbc.execute(rssql)

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
            return w.replace("[",r"\[").replace("]", r"\]").replace(".", r"\.").replace("(",r"\(").replace(")", r"\)")
        
        for k in table_renames:
            #pat = r" %s([ ,.])" % k.replace("[",r"\[").replace("]", r"\]")
            pat = r"(\W)%s(\W)" % sql_esc(k)
            #print(pat)
            sql = re.sub(pat, r"\1%s\2" % table_renames[k], sql, count=999)

        for k in environ:
            pat = f"%{k}%"
            #print(pat)
            sql = re.sub(pat, environ[k], sql, count=999)

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

        # Postgres defaults to having all of its columns to be lowercase to work with the 
        # auto-minification of column names in SQL statements.   
        df.columns = df.columns.str.lower()
        self.dbc.load_data(table, df, truncate=truncate, index=False)
    
    def close(self):
        pass

class PostgresClientDb:
    def __init__(self, query_registry : DataQueryRegistry, ctx=None, db="POSTGIS_LOCAL"):
        db = AradDatabase[db]
        dbc = PostgresClient(ctx, db=db)
        #dbc.connect_standalone(host=host, username=username, password=password, dbname=dbname, port=port)
        self.dbc = dbc
        self.query_registry = query_registry

    def cursor(self):
        return PostgresClientDbCursor(self.dbc, self.query_registry)

    def execute(self, sql):
        self.cursor().execute_non_query(sql)

    def load_data(self, table, df, truncate):
        self.cursor().load_data(table, df, truncate)
    
    def rollback(self):
        pass
