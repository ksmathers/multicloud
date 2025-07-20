#from jupyter_aws.arglist import Arglist
from generic_templates.arglist import Arglist
import multicloud as jaws
import os

def cmd_notify(_ctx: jaws.Context, args: Arglist):
    """
            channel : can be a webhook URL or one of the following predefined webhooks:
            'dev' - development notifications
            'oa' - generic OA operations notifications
            'star-tline-de' - notifications associated with GIS ingestion
            'arad-delta-datalake' - notifications associated with Redshift ingestion
            'oa-application' - notifications associated with ETOA
    """
    args.shift_opts()
    channel = args.opt('channel','oa')
    level = jaws.NoteLevel[args.opt('level', 'NONE')]
    title = args.opt('title', f'{level.name} on {channel}')
    notify = jaws.Notification(channel)
    message = args.shift_all()
    notify.send_message(message, title, level)
    print(f"notify [{level.name}:{channel}] {title}/{message}")

def cmd_dbcache(ctx: jaws.Context, args: Arglist):
    dbc = jaws.RedshiftClient(ctx)
    sql = args.shift()
    path = args.shift()
    print(f"dbcache '{sql}' '{path}'")
    dbc.cache_query(sql, path)

def model_table(model: str):
    if model == "AWS21":
        table = "tline.expo_pf_calcs"
    elif model == "AWS20":
        table = "tline.aws20_pf_calcs"
    else:
        print("List unknown model:", model)
        raise ValueError("Unknown model " + model)
    return table

def cmd_oacache(ctx: jaws.Context, args: Arglist):
    dbc = jaws.RedshiftClient(ctx)
    model = args.shift()
    tbl = model_table(model)
    dttm = args.shift()
    path = f"oacache.{model}.{dttm[0:13]}"
    print(f"oacache {tbl} {dttm} {path}")
    dbc.cache_query(f"select * from {tbl} where insert_dttm = ''{dttm}'' ", path)

def cmd_dbdump(ctx: jaws.Context, args: Arglist):
    tbl = args.shift()
    s3key = args.shift()
    dbc = jaws.RedshiftClient(ctx)
    arn = dbc.unload_table_csv(tbl, s3key, delimiter=",")
    print(f"Output file: {arn}")

def cmd_tocsv(_ctx: jaws.Context, args: Arglist):
    infile = args.shift()
    outfile = args.shift()
    sep = (args.shift() or "|")
    print("tocsv", infile, outfile)
    import pandas as pd
    df = pd.read_parquet(infile)
    df.to_csv(outfile, sep="|", index=None)

def cmd_list(ctx: jaws.Context, args: Arglist):
    model = args.shift()
    dbc = jaws.RedshiftClient(ctx)
    table = model_table(model)
    df = dbc.sql_query(f"select distinct insert_dttm from {table} order by insert_dttm desc")
    print(jaws.pdutil.pp(df.insert_dttm.values, 1))

def cmd_upload(ctx : jaws.Context, args: Arglist):
    from_file = args.shift()
    to_arn = args.shift()

    s3 = jaws.S3Client(ctx)
    with open(from_file, "rb") as fin:
        data = fin.read()
        s3.put_arn(to_arn, data)

def cmd_s3find(ctx : jaws.Context, args: Arglist):
    s3arn = args.shift()
    ftype = args.shift()
    needle = args.shift()
    bucket, path = jaws.S3Client.parse_arn(s3arn)
    if needle == "%ZDATEC%":
        needle = jaws.ZuluTime().date().replace('-','')
    elif needle == "%ZDATE%":
        needle = jaws.ZuluTime().date()
    if not path.endswith("/"):
        path = path + "/"
    s3 = jaws.S3Client(ctx, bucket)
    contents = s3.list(prefix = path)
    result = jaws.grep(contents[ftype], needle)
    print(result)
    return len(result)

def usage():
    print("""
Usage: python -m jupyter_aws [command] [arguments]

Where 'command' is one of the following:

 - notify [-channel=<channel>] [-title=<title>] [-level=<level>] <message to send>
    <channel> in OA_OPERATIONS, STAR_TLINE_DE, ARAD_DELTA_DATALAKE, OA_APPLICATION, OA_DEVTEST
    <level> in DEBUG, INFO, WARN, ERROR
    Send message to OA Operations teams chat

 - dbcache <sql-query> <path>
    Performs the sql query provided and downloads the result to <path>-<date>.parquet

 - dbdump <table-name> <s3arn>
    Exports the <table-name> from Redshift to a pipe delimited CSV file '<s3arn>.csv000'  If no <s3arn> is supplied then
    the S3 arn will be 's3://arad-data-prd/redshift/oa-foundry-crosscheck/<table-shortname>.csv000' where
    <table-shortname> is the name of the table not including the database part.

 - list <table-name>
    Returns the list of <dttm> dates when data was inserted into a table.  Typically for one of the OA model input or
    output tables.

 - oacache <tablename> <dttm>
    Extracts the a dated set of data in an OA table matching <dttm> into a parquet file oacache.<tablename>-<date>.parquet

 - tocsv <parquet-file> <csv-file>
    Converts the provided <parquet-file> to pipe ('|') delimited <csv-file>.

 - upload <path> <s3arn>
    Uploads a single file from a local <path> to the provided <s3arn>

 - s3find <s3arn> [keys|dirs] <name>
    Finds keys or directories (common prefixes) that contain the substring <name>.  Name can also be a hash
    tag from the following list:
       %%ZDATE%% - searches for today's date in the form 2021-12-31
       %%ZDATEC%% - searches for today's date in the form 20211231
""")

def main():
    ecode = 0
    env = os.environ.get('JAWS_ENVIRON', 'AUTO')
    ctx = jaws.Context(environment=env)
    args = Arglist()
    cmd = args.shift()
    if cmd == "notify":
        cmd_notify(ctx, args)
    elif cmd == "dbcache":
        cmd_dbcache(ctx, args)
    elif cmd == "list":
        cmd_list(ctx, args)
    elif cmd == "oacache":
        cmd_oacache(ctx, args)
    elif cmd == "tocsv":
        cmd_tocsv(ctx, args)
    elif cmd == "upload":
        cmd_upload(ctx, args)
    elif cmd == "dbdump":
        cmd_dbdump(ctx, args)
    elif cmd == "s3find":
        ecode = cmd_s3find(ctx, args)
    else:
        print("Unknown command:",cmd)
        usage()
        ecode = 255
    return ecode

if __name__ == "__main__":
    print("main")
    ecode = main()
    exit(ecode)
