from multicloud.postgres_client import PostgresClient
from urllib.parse import quote_plus
from .context import Context
from .secretsmanager_client import SecretsManagerClient
from .postgres_client import PostgresClient
from .postgres_client import AradDatabase
from .s3_client import S3Client
from generic_templates.zulutime import ZuluTime
from .pdutil import fix_column_names
from .context import Context

import pandas
import os

class RedshiftClient(PostgresClient):
    def __init__(self, ctx : Context, user : AradDatabase = AradDatabase.REDSHIFT):
        super().__init__(ctx, user)

    def is_cached(self, path):
        if not path.endswith(".parquet"):
            path = path+"-"+ZuluTime().date()+".parquet"
        return os.path.isfile(path)

    def cache_query(self, sql : str, path : str) -> str:
        """Caches a local copy of the specified query (but doesn't load the file)

        Returns the path of the local parquet file, which will match the path supplied but with
        the date added.
        """
        # Normalize the cache filename
        if not path.endswith(".parquet"):
            path = path+"-"+ZuluTime().date()+".parquet"

        # Fetch the file from the database if dblive is True or the file isn't cached
        if self.ctx.dblive or not os.path.isfile(path):
            print("Live update query cache at:", path)
            assert(not 'ZULUTIME' in os.environ)
            fname = os.path.basename(path).replace(".parquet","")
            arn = self.unload_query(sql, f"s3://arad-poc/jaws/{fname}.", overwrite=True)
            S3Client(self.ctx).download_arn(arn, path)
        else:
            print("Using cached query:", path)
        return path

    def cache_table(self, tblname : str, path : str) -> str:
        """Caches a local copy of the specified redshift table
        """
        assert not path is None
        return self.cache_query(f"select * from {tblname}", path)

    def sql_query_cache(self, sql, path, with_column_names=None):
        """
        Equivalent to sql_query, but unloads to s3 then downloads, then reads
        the file instead of fetching directly from Redshift.

        sql - the SQL query
        path - a filename to use for intermediate storage
        live - when True forces the query to happen, False uses local file if it exists
        """

        # Fetch the query
        path = self.cache_query(sql, path)

        # Load cached result, fix column names, and return
        df = pandas.read_parquet(path)
        if not with_column_names is None:
            fix_column_names(df, with_column_names, inplace=True)
        return df

    def execute_cache(self, sql, path):
        """
        Equivalent to execute, but only executes if the cache file is missing or if
        islive is True
        """
        # Fetch the file from the database if dblive is True or the file isn't cached
        if self.ctx.dblive or not os.path.isfile(path):
            print("Live update non-query cache at:", path)
            assert(not 'ZULUTIME' in os.environ)
            fname = os.path.basename(path)
            self.execute(sql)
            with open(path, "w+") as fout:
                print(f"Run at {ZuluTime.now()}", file=fout)
        else:
            print("Using cached non-query:", path)

    def list_tables(self, schema):
        query = f"""
select t.table_name
from information_schema.tables t
where t.table_schema = '{schema}'
    and t.table_type = 'BASE TABLE'
order by t.table_name;
"""
        return self.sql_query(query)

    def load_data(self, table : str, df : pandas.DataFrame, format : str='csv', use_timestamp : bool=False, truncate : bool=False, s3arn : str=None, tmpdir : str="/tmp"):
        """ - uploads data to S3 and import into a Redshift table

        Args:
            table :str: Redshift full table name to upload into
            df :pd.DataFrame: Data to upload.  The order and number of columns must match the DDL for the table
            format :str: Either 'csv' or 'parquet' indicating the upload conversion format
            use_timestamp :bool: Adds a timestamp to the uploaded filename both locally and in S3
            truncate :bool: Truncates the table before loading if True, default is False
            s3arn :str: Optional location to upload in S3.  Default is 's3://arad-data-prd/redshift/load/'
                ARNs that end with a '/' have the filename {table}.{now}.{format} appended if use_timestamp is True
            tmpdir :str: Optional location to store the intermediate CSV file before uploading.  Default '/tmp'
        """
        if use_timestamp:
            now=ZuluTime.now()
            tmpfile = f"{tmpdir}/{table}.{now}.{format}"
        else:
            tmpfile = f"{tmpdir}/{table}.{format}"
        if format=='csv':
            df.to_csv(tmpfile, index=False)
        elif format=='parquet':
            df.to_parquet(tmpfile, index=False, engine='pyarrow')
        else:
            raise NotImplementedError(f"Unknown format {format}")
        s3 = S3Client(self.ctx)
        if s3arn is None:
            s3arn = f"s3://arad-data-prd/redshift/load/"
        key = s3.upload_arn(tmpfile, s3arn)
        if truncate:
            self.execute(f"truncate {table}")
        self.load_table(table, key, df.columns, format=format)

    def load_table(self, table, s3arn, columns=None, ignore_header=1, format='csv'):
        """
        Loads data from an S3 key under the arad-data-prd bucket into a Redshift table.
        """
        ignore_header_opt = ""
        delimiter_opt = ""
        if format == 'csv':
            ignore_header_opt = f'IGNOREHEADER {ignore_header}'
            delimiter_opt = f"DELIMITER ','"
        elif format == 'parquet':
            pass
        else:
            raise NotImplementedError(f"unknown format {format}")

        if columns is None:
            columns = table.columns

        sql = f"""
            COPY {table} ({",".join(columns)})
            FROM '{s3arn}'
                IAM_ROLE 'arn:aws:iam::925741509387:role/RedshiftArad'
                FORMAT {format}
                timeformat 'auto'
                {ignore_header_opt} {delimiter_opt}
            """
        self.execute(sql)


    def unload_table(self, table, s3arn, s3key=None, overwrite=True):
        """
        Unloads a table or view from Redshift to the supplied S3 location using the RedshiftArad
        role.   The actual output file will include a sequence number and the '.parquet' extension,
        the corrected ARN for which is returned.
        """
        if s3arn is None:
            s3arn = "s3://arad-data-prd/redshift/oa-foundry-crosscheck"
        if s3key is None:
            if s3arn.endswith("/"):
                s3key = table.replace(".", "_")
                s3arn += s3key
        else:
            if not s3arn.endswith("/"):
                s3arn += "/"
            s3arn += s3key

        return self.unload_query(f'SELECT * from {table}', s3arn, overwrite)

    def unload_query(self, query, s3arn, overwrite=True):
        """
        Unloads the result of a select statement query to the supplied S3 location using the RedshiftArad
        role.   The actual output file will include a sequence number and the '.parquet' extention,
        the corrected ARN for which is returned.   If the file is larger than 6.2GB it will be split across
        multiple sequence numbers of which only the first is returned.
        """
        opt_overwrite=""
        if overwrite:
            opt_overwrite = "ALLOWOVERWRITE"

        # escape any literal single quotes in the query for embedding in the UNLOAD statement
        query=query.replace("'", "''")

        role = 'arn:aws:iam::925741509387:role/RedshiftArad'
        sql = f"""
UNLOAD ('{query}')
TO '{s3arn}'
IAM_ROLE '{role}'
{opt_overwrite}
FORMAT AS parquet
PARALLEL off
MAXFILESIZE AS 6.2 GB
"""
        self.execute(sql)
        return s3arn + '000.parquet'

    def unload_table_csv(self, tbl, s3key=None, s3bucket=None, delimiter="|", gzip=False, overwrite=True):
        """ unloads a Redshift table to S3

        The output is sent to the s3bucket and s3key provided.
        If s3key is an ARN then the ARN is used and s3bucket is ignored.
        """
        #print("unloading", tbl)
        if s3key is None:
            s3key = tbl.replace(".","_")
        if s3key.startswith("s3:"):
            s3bucket, s3key = S3Client.parse_arn(s3key)
        if s3bucket is None:
            s3bucket = 's3://arad-data-prd/redshift/oa-foundry-crosscheck'
        if not s3bucket.startswith("s3://"):
            s3bucket = 's3://' + s3bucket
        return self.unload_query_csv(f'SELECT * from {tbl}', s3key, s3bucket, delimiter, gzip, overwrite)

    def download_table_csv(self, tbl, dir=".", fname=None, delimiter="|", gzip=False) -> str:
        """ Dumps a database table to a CSV file and returns the full path
        where the file was saved

        Args:
            tbl :str: name of table to dump (with schema if any)
            dir :str: directory to save the file to
            fname :str: path to save to, defaults to the table name +.csv (or +.csv.gz)
            delimiter :str: separator character for CSV columns
            gzip :bool: save GZipped CSV if true
        """
        arn = self.unload_table_csv(tbl, delimiter=delimiter, gzip=gzip)
        s3 = S3Client(self.ctx)
        if fname is None:
            fname = tbl.replace('.','_')
        if not '.' in fname:
            fname = f"{fname}.csv"
        path = f"{dir}/{fname}"
        if gzip and not path.endswith(".gz"):
            path += ".gz"
        s3.download_arn(arn, path)
        return path

    def download_table_parquet(self, tbl, dir=".", fname=None) -> str:
        """ Dumps a database table to a CSV file and returns the full path
        where the file was saved

        Args:
            tbl :str: name of table to dump (with schema if any)
            dir :str: directory to save the file to
            fname :str: path to save to, defaults to the table name +.parquet
        """
        arn = self.unload_table(tbl, "s3://arad-poc", "jaws/")
        s3 = S3Client(self.ctx)
        if fname is None:
            fname = tbl.replace('.','_')
        if not '.' in fname:
            fname = f"{fname}.parquet"
        path = f"{dir}/{fname}"
        s3.download_arn(arn, path)

    def unload_query_csv(self, query, s3key, s3bucket='s3://arad-data-prd/redshift/oa-foundry-crosscheck', delimiter="|", gzip=False, overwrite=True):
        #print("unloading", s3key)
        s3bucket = s3bucket.rstrip('/')
        opt_format = f"CSV HEADER DELIMITER '{delimiter}'"
        suffix="000"
        if gzip:
            suffix += ".gz"
            opt_format += " GZIP"

        opt_overwrite = ""
        if overwrite:
            opt_overwrite = "ALLOWOVERWRITE"

        auth = "IAM_ROLE 'arn:aws:iam::925741509387:role/RedshiftArad'"
        arn = f'{s3bucket}/{s3key}.csv'
        sql = f"""
        UNLOAD ('{query}')
            TO '{arn}'
            {auth}
            {opt_format}
            {opt_overwrite}
            MAXFILESIZE AS 6.24 GB
            PARALLEL OFF
            REGION 'us-west-2'"""
        self.execute(sql)
        return arn+suffix




