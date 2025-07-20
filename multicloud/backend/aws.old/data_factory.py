from enum import Enum

from .data_factory_redshift import RedshiftClientDb
from .data_factory_postgres import PostgresClientDb
from .context import Context
from typing import Dict


class DataSource(Enum):
    AWS="AWS"
    POSTGRES="POSTGRES"
    REDSHIFT="REDSHIFT"
    EXPONENT="EXPONENT"
    VDB="VDB"

class DataFactory:
    CONNECTION=None
    def __init__(self, dsrc : DataSource, **kwargs):
        """Connect to a database

        dsrc :str: One of 'REDSHIFT', 'AWS', 'EXPONENT'
        kwargs :dict: parameters for the specified database constructor
            REDSHIFT -
                cachedir :str: directory in which to store downloaded query results
                query_registry :DataQueryRegistry: query translation layer
            
        """
        self.dsrc = dsrc
        self.kwargs = kwargs
    
    def __repr__(self):
        return f"DataFactory(dsrc='{self.dsrc}')"

    def connect(self, datasource : str = None):
        """Returns a new instance"""
        if datasource is not None:
            assert(datasource == str(self.dsrc))
        if DataFactory.CONNECTION is None:
            DataFactory.CONNECTION = DataFactory.make_connection(self.dsrc, **self.kwargs)
        conn = DataFactory.CONNECTION
        assert(conn is not None)
        return conn

    @classmethod
    def make_connection(_class, dsrc : DataSource, **kwargs):
        """(classmethod) Connect to a database

        dsrc :str: One of 'REDSHIFT', 'POSTGIS_LOCAL'
        kwargs :dict: parameters for the specified database constructor
            REDSHIFT -
                ctx :jaws.Context: AWS client context
                cachedir :str: directory in which to store downloaded query results
                query_registry :DataQueryRegistry: query translation layer

            VDB -
                vdbclient :Class: Class with constructor for initializing a VDB connection
                cachedir :str: directory in which to find parquet files
                query_registry :DataQueryRegistry: query translation layer

            POSTGRES -
                db :str: A database to connect to, default POSTGIS_LOCAL
                    also: POSTGIS, POSTGIS_ROOT, ARADGIS
                query_registry :DataQueryRegistry: query translation layer

            
        """
        query_registry = kwargs.get("query_registry")
        del(kwargs["query_registry"])
        if type(dsrc) is str:
            dsrc = DataSource(dsrc)
        conn = None
        if dsrc == DataSource.REDSHIFT:
            ctx = kwargs.get('ctx', None)
            if ctx is None:
                ctx = Context()
            conn = RedshiftClientDb(ctx, cachedir=kwargs['cachedir'], query_registry=query_registry)
        elif dsrc == DataSource.POSTGRES:
            db = kwargs.get("db", "POSTGIS_LOCAL")
            conn = PostgresClientDb(query_registry=query_registry, db=db)
        elif dsrc == DataSource.VDB:
            vdbclient = kwargs.get('vdbclient')
            conn = vdbclient(cachedir=kwargs['cachedir'], query_registry=query_registry)
        elif dsrc == DataSource.AWS:
            from pyodbc import create_engine
            import urllib
            conn = create_engine("mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus(
                "DRIVER=ODBC Driver 17 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(
                    kwargs['host'], kwargs['db'], kwargs['username'], kwargs['password']))),
                fast_executemany=True)
            return conn
        elif dsrc == DataSource.EXPONENT:
            import pyodbc
            conn = pyodbc.connect(driver="{SQL Server}", server=kwargs['ExpoServer'], database=kwargs['ExpoDatabase'],
                                  trusted_connection='yes')
        else:
            raise NotImplementedError("Unknown datasource:", dsrc)
        return conn