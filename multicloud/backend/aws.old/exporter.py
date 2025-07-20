from posixpath import basename
import multicloud as jaws
import os
from datetime import date


class Exporter:
    """
    Exporter is a class that exports a set of files from Redshift to CSV files in different
    S3 locations optional tagging by current date and optionally zips the files into a single
    downloadable package
    """
    def __init__(self, ctx, exportdir="export-$today", format="csv", dryrun=False, dbc=None):
        """ - begins a Redshift Export to the local filesystem
        Parameters:
          ctx : a jupyter_aws Context object
          exportdir : the local directory in which to place the exported downloads.   Interpolates the exportdir.
          format : either 'csv' or 'parquet'
          dryrun : if true then files won't actually be uploaded to S3 when upload() is called.
        """
        self.ctx = ctx
        self.format = format
        self.dryrun = dryrun
        self.dbc = dbc

        self.now = jaws.ZuluTime()

        self.basedir = self.interpolate(exportdir, {'$extension': format})
        self.destinations = []
        if not os.path.isdir(self.basedir):
            os.makedirs(self.basedir)
        else:
            os.system(f"rm -rf {self.basedir}/*")

    @property
    def today_is_thursday(self):
        return False
        thursday = False
        if date.today().weekday() == 3:
            thursday = True
        return thursday

    def add_destination(self, basearn):
        """ - adds an upload directory for the exports

        The <basearn> is the S3 bucket and prefix to use for the upload.  The value will be interpolated with the
        string '$today' replaced with the current date.
        """
        self.destinations.append(basearn)

    def export(self, tables, table_excluded=[]):
        """ - exports a table or list of tables from Redshift

        Export unloads the table as either CSV or PARQUET depending on how the Exporter was instantiated, downloads the
        files and renames them to match the tablename.

        Returns the list of files exported
        """
        if type(tables) is list:
            tablelist = tables
        elif type(tables) is str:
            tablelist = [ tables ]
        else:
            raise ValueError("Invalid type for 'tables' parameter, must be list or string")
        exports = []
        for tablepath in tablelist:
            print(f"Export {tablepath}")
            if table_excluded and tablepath in table_excluded:
                print(f'{table_excluded=}')
            else:
                fpath = self.export_table(tablepath)
                exports.append(fpath)
        return exports


    def export_table(self, tablepath):
        type = self.format
        if "." in tablepath:
            schema, tablename = tuple(tablepath.split("."))
        else:
            schema = "public"
            tablename = tablepath

        print("schema =", schema)
        print("tablename =", tablename)
        s3bucket = f"s3://arad-poc/temp/{tablename}/"

        if type == "csv" or type == "csv.gz":
            use_gzip = type.endswith(".gz")
            fname = self.dbc.download_table_csv(tablepath, self.basedir, f"{tablename}.csv", delimiter=",", gzip=use_gzip)
        elif type == "parquet":
            fname = self.dbc.download_table_parquet(tablepath, self.basedir, f"{tablename}.parquet")
        else:
            raise ValueError(f"Invalid type: {type}, accepted types are 'csv', or 'parquet'")
        return fname

    def publish(self):
        for fname in os.listdir(self.basedir):
            print(fname)
            fpath = os.path.join(self.basedir, fname)
            for d in self.destinations:
                self.upload(d, fpath)

    def interpolate(self, template, kvlist={}):
        """ Replaces string contents with values from the kvlist

        Common variables:
            $today - the current date in the format 20210131
            $date - the current date in the format 2021-01-31
            $today_met - the current date in the format 20210131_163059
            $format - the dump format 'csv', 'csv.gz' or 'parquet'

        Other variables provided by upload():
            $extension - the uploaded file extension, 'csv', 'parquet', or 'zip'
            $fpath - the upload or download file path
            $fname - the upload or download filename
            $tablename - the name of the table being dumped not including the database namespace
        """
        import re
        kvlist['$today'] = self.now.expo_ts()
        kvlist['$date'] = self.now.date()
        kvlist['$today_met'] = self.now.met_ts()
        kvlist['$format'] = self.format
        while True:
            m = re.search(r"\$([A-Za-z_]+)", template)
            if not m:
                m = re.search(r"\$({[A-Za-z_]+})", template)
            if not m:
                break
            var = "$"+m.group(1)
            #print(f"var={var}")
            if var in kvlist:
                #print(f"kvlist[var]={kvlist[var]}")
                template = template.replace(var, kvlist[var])
            else:
                raise ValueError(f"Unknown template variable {var}")
        return template

    def upload(self, basearn, fpath):
        """ - upload <fpath> to a <basearn> on S3

        The <basearn> will be interpolated with the string '$today' replaced with the current date
        """
        s3 = jaws.S3Client(self.ctx)
        fname = os.path.basename(fpath)
        tablename = fname.split(".", 1)[0]
        dryrun_flag = " [DRYRUN] " if self.dryrun else ""
        arn = self.interpolate(basearn, {'$fname': fname, '$tablename': tablename})
        print(f" -> {dryrun_flag}{arn}")
        with open(fpath, 'rb') as f:
            data = f.read()
        if not self.dryrun:
            s3.put_arn(arn, data)

    def manifest(self, basearn, fpath):
        """ - upload <fpath> as manifest.json to a <basearn> on S3

        The <basearn> will be interpolated with the string '$today' replaced with the current date
        """
        s3 = jaws.S3Client(self.ctx)
        fname = os.path.basename(fpath)
        tablename = fname.split(".", 1)[0]
        dryrun_flag = " [DRYRUN] " if self.dryrun else ""
        arn = self.interpolate(basearn, {'$fname': fname, '$tablename': tablename})
        print(f" -> {dryrun_flag}{arn}")

        json_data = "{'meteorology':'$today_met'}"
        if not self.dryrun:
            s3.put_arn(arn, json_data)

    def zip_upload(self, basearn, rename="$tablename-$today.$format", exclude=[]):
        """ - dates and zips the previously exported files and uploads the zip to an S3 location

        basearn : the S3 prefix that the ZIP file should be dropped into
        exclude : optional list of strings to avoid in uploaded filenames
        """
        zipfile = f"{self.basedir}.zip"
        for fn in os.listdir(self.basedir):
            if any(xcl in fn for xcl in exclude): continue
            if fn.endswith("."+self.format):
                tablename, extension = fn.split(".", 1)
                os.rename(
                    os.path.join(self.basedir, fn),
                    os.path.join(self.basedir, self.interpolate(rename, {'$tablename':tablename, '$extension':extension}))
                    )
        os.system(f"zip -r {zipfile} {self.basedir}")
        print(zipfile)
        self.upload(basearn, zipfile)
        return zipfile

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception_value, _traceback):
        pass

