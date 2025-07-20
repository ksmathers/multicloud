from modulefinder import Module
from typing import Dict

class DataQuery:
    def __init__(self, query_name, sql, column_renames={}, nonquery=False):
        """Defines a cross platform SQL query

        Args:
            query_name :str: A name for the query, eg: the unique variable name where the query is stored
            sql :str: The original text of the SQL query
            column_renames :Dict[str,str]: Translated column names 'target' to 'original' for this query
                column_renames is used twice, once reversed for preparing the query (so that valid column names
                are used when talking to the database), and once for fixing the result set so that column
                names are returned in the form that the original code expects.
        """
        self.name = query_name
        self.sql = sql
        self.column_renames = column_renames
        self.nonquery = nonquery

class NonQuery(DataQuery):
    def __init__(self, query_name, sql):
        super().__init__(query_name, sql, nonquery=True)
    
class DataQueryRegistry:
    def __init__(self, system : str, environment : Dict[str, str], tables : Dict[str, str], queries : list, non_queries : list):
        """Creates a data query registry for processing translated queries

        Args:
            system :str: a string name for the registry
            environment :dict: a lookup table for environmental configuration parameters
            config :Module: a module the defines the know SQL queries
            tables :dict: a lookup table for tables renamed in the target database
            queries :dict: a JSON description of the query conversion parameters:
                 {
                    "config_query_symbol_name": {
                        "target column name": "original column name",
                        ...
                    },
                    ...
                 }
        """
        self.system = system
        self.environment = environment
        self.data_queries = {}
        self.reverse_data_queries = {}
        self.table_renames = tables
        for dq in queries:
            self.add(dq)
        for dq in non_queries:
            self.add(dq)

    def translate_table(self, tablename):

        for tbl in [ tablename, f'[{tablename}]' ]:
            if tbl in self.table_renames:
                return self.table_renames[tbl]
        raise ValueError(f"Unknown Exponent table {tablename}")

    def add(self, query : DataQuery):
        self.data_queries[query.name] = query
        self.reverse_data_queries[query.sql] = query
