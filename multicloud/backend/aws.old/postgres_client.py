from urllib.parse import quote_plus
from .secretsmanager_client import SecretsManagerClient, SecretId
from enum import Enum
from generic_templates.zulutime import ZuluTime
from .s3_client import S3Client
import pandas
import sqlalchemy


class AradDatabase(Enum):
    NONE = 0
    POSTGIS = 1
    POSTGIS_ROOT = 10
    REDSHIFT = 2
    ARADGIS = 3
    POSTGIS_LOCAL = 11

class DatabaseDriver:
    Redshift = "redshift+psycopg2"
    PostGIS = "postgresql"

# dbc = jaws.PostgresClient(db=jaws.AradDatabase.POSTGIS_LOCAL)
class PostgresClient:
    def __init__(self, ctx = None, db : AradDatabase = AradDatabase.NONE):
        self.ctx = ctx
        self.engine = None
        self.driver = None
        self.matviews = {}
        self.now = ZuluTime.now()
        if db == AradDatabase.POSTGIS:
            self.connect_as_postgis_dev()
        elif db == AradDatabase.POSTGIS_ROOT:
            self.connect_as_dcagis_dev()
        elif db == AradDatabase.REDSHIFT:
            self.connect_as_redshift_datalake_prd()
        elif db == AradDatabase.ARADGIS:
            self.connect_as_arad_gis_postgres_prd()
        elif db == AradDatabase.POSTGIS_LOCAL:
            self.connect_standalone("localhost", "postgres") #

    def materialize(self, table):
        mtable = "public." + table.replace(".","_") + str(self.now)
        sql=f"""create table {mtable} as select * from {table}"""
        self.execute(sql)
        self.matviews[table] = mtable

    def load_data(self, table : str, df : pandas.DataFrame, truncate : bool=False, index=False):
        """ - uploads data to S3 and import into a Redshift table

        Args:
            table :str: Redshift full table name to upload into
            df :pd.DataFrame: Data to upload.  The order and number of columns must match the DDL for the table
            truncate :bool: Truncates the table before loading if True, default is False
        """
        if truncate:
            mode="replace"
        else:
            mode="append"
        if "." in table:
            schema, table = table.split(".", 1)
        else:
            schema = "public"
        #print(f"PostgresClient:load_data to_sql({len(df)} rows)")
        # pandas 2.2.2 :: sean
        # DataFrame.to_sql(name, con, *, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)
        # df.to_sql(table, self.engine, schema=schema, if_exists=mode, chunksize=10000, index=index)
        with self.engine.begin() as connection:
            df.to_sql(
                name=table,
                con=connection,
                schema=schema,
                if_exists=mode,
                index=index
            )

    def load_parquet(self, table, path, truncate=True, index=False):
        df = pandas.read_parquet(path)
        self.load_data(table, df, truncate, index)

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
        df = self.sql_query(f"SELECT * FROM {tbl}")
        if fname is None:
            fname = tbl.replace('.','_')
        if not '.' in fname:
            fname = f"{fname}.csv"
        path = f"{dir}/{fname}"
        if gzip and not path.endswith(".gz"):
            path += ".gz"
        df.to_csv(path, sep=delimiter, compression='infer', index=False)
        return path

    def download_table_parquet(self, tbl, dir=".", fname=None) -> str:
        """ Dumps a database table to a CSV file and returns the full path 
        where the file was saved

        Args:
            tbl :str: name of table to dump (with schema if any)
            dir :str: directory to save the file to
            fname :str: path to save to, defaults to the table name +.parquet
        """
        df = self.sql_query(f"SELECT * FROM {tbl}")
        if fname is None:
            fname = tbl.replace('.','_')
        if not '.' in fname:
            fname = f"{fname}.parquet"
        path = f"{dir}/{fname}"
        df.to_parquet(path, index=False, engine='pyarrow')
        return path

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

        fname = f"{tbl}.csv"
        if gzip:
            fname = fname + ".gz"

        df = self.sql_query(f"SELECT * FROM {tbl}")
        df.to_csv(fname, sep=delimiter, compression='infer', index=False)
        s3 = S3Client(self.ctx)
        arn = f'{s3bucket}/{s3key}'
        s3.upload_arn(fname, arn)
        return arn

    def cleanup(self, table):
        for mtable in self.matviews.values():
            sql = f"""drop table {mtable}"""
            self.execute(sql)

    def connect_as_postgis_dev(self):
        self.connect(SecretId.POSTGIS_DEV, DatabaseDriver.PostGIS)

    def connect_as_dcagis_dev(self):
        self.connect(SecretId.DCAGIS_DEV, DatabaseDriver.PostGIS)

    def connect_as_postgis_prd(self):
        self.connect(SecretId.POSTGIS_PRD, DatabaseDriver.PostGIS)

    def connect_as_arad_gis_postgres_prd(self):
        self.connect(SecretId.ARAD_GIS_POSTGRES_PRD, DatabaseDriver.PostGIS)
    
    def connect_as_redshift_datalake_prd(self):
        self.connect(SecretId.REDSHIFT_DATALAKE_PRD, DatabaseDriver.Redshift)

    def connect_as_redshift_datalake_dev(self):
        self.connect(SecretId.REDSHIFT_DATALAKE_DEV, DatabaseDriver.Redshift)

    def connect_as_prd_oa_modal_svc(self):
        self.connect(SecretId.REDSHIFT_PRD_OA_MODAL_SVC, DatabaseDriver.Redshift)

    def connect_standalone(self, host, username, local_cred=None, dbname="postgres", port=5432):
        self.driver = DatabaseDriver.PostGIS
        if local_cred is None:
            local_cred = username
        uri = self._connection_string(username, local_cred, host, dbname, port)
        print(uri) # sean
        self.engine = sqlalchemy.create_engine(uri)
      # self.engine = sqlalchemy.create_engine(uri).connect() # sean
        # https://stackoverflow.com/questions/38332787/pandas-to-sql-to-sqlite-returns-engine-object-has-no-attribute-cursor
        # sql_engine = sqlalchemy.create_engine('sqlite:///test.db', echo=False)
        # conn = sql_engine.connect()
        # working_df.to_sql('data', conn,index=False, if_exists='append')
        
    def connect(self, secret_id, driver = None):
        """
        Connect to database using an ARN of a secret, together with a database driver.

        :param: secret_id : str - The ARN of an AWS SecretsManager secret for which you have access permissions
        :param: driver : DatabaseDriver - The driver to use for connection
        """
        self.driver = driver
        uri = self.generate_connection_string(secret_id)
        self.engine = sqlalchemy.create_engine(uri)

    def generate_connection_string(self, secret_id, escape_percent_signs=False):
        """Creates a Redshift database URL

        Args:
            secret_id (str|SecretId): Either the Amazon Resource Name (ARN) or the friendly
                name of the secret that contains the database credentials.
            escape_percent_signs (:obj:`bool`, optional): Whether to escape raw percent
                signs in the URL. This is used to overcome variable interpoliation issues
                when using Alembic for migrations. Defaults to False.

        Returns:
            str: A URL used by SQLAlchemy or Alembic to connect to the database.
        """
        smc = SecretsManagerClient(self.ctx)
        if type(secret_id) is str:
            _secrets = smc.get_secret(secret_id)
        elif type(secret_id) is SecretId:
            _secrets = smc.known_secret(secret_id)
        else:
            raise NotImplementedError("Unknown secret type: ", secret_id)
        url = self._connection_string(_secrets['username'], _secrets['password'], _secrets['host'], _secrets['dbname'], _secrets['port'], escape_percent_signs)
        if escape_percent_signs:
            return url.replace("%", "%%")
        else:
            return url

    def _connection_string(self, username, password, host, dbname="postgres", port=5432, escape_percent_signs=False):
        url = f"{self.driver}://{username}:{quote_plus(password)}@{host}:{port}/{dbname}"

        if escape_percent_signs:
            return url.replace("%", "%%")
        else:
            return url 

    def sql_query(self, sql, *params):
        sql = self.rewrite_query(sql)
     #  sql = sqlalchemy.text(sql)
        if self.engine is None:
            raise ValueError("Call connect before using sql_query()")
        if len(params) > 0:
            return pandas.read_sql_query(sql, self.engine, params=params)
        return pandas.read_sql_query(sql, self.engine)

    def rewrite_query(self, sql):
        for tbl in self.matviews:
            import re
            sql = re.sub(r"\b%s\b" % tbl, self.matviews[tbl], sql, flags=re.IGNORECASE)
        return sql
    
    def execute(self, sql, *params):
        print('pandas=',pandas.__version__)         # '2.2.2'  #
        print('sqlalchemy=',sqlalchemy.__version__) # '2.0.31' #
        sql = self.rewrite_query(sql)
        sql = sqlalchemy.text(sql) #sean sqlalchemy.exc.ObjectNotExecutableError
        if self.engine is None:
            raise ValueError("Call connect before using execute()")
     #  with self.engine.connect() as conn: # sean
        with self.engine.begin() as connection:
            if len(params) > 0:
                connection.execute(sql, *params)
            else:
                connection.execute(sql)

