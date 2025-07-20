import os
import pathlib
from generic_templates.text_finder import TextFinder
from .tablenames import TableNames

class AradSqlViews(TableNames):
    def __init__(self, arad_delta_datalake = None, verbose = False):
        super().__init__()
        if arad_delta_datalake is None:
            arad_delta_datalake = "~/git/arad-delta-datalake"

        arad_delta_datalake = os.path.expanduser(str(arad_delta_datalake))
        
        assert(os.path.isdir(arad_delta_datalake))
        self.arad_delta_datalake = arad_delta_datalake
        for schema in [ 'etgis', 'sap', 'tline', 'pronto' ]:
            for fname, sql in self.load_sql(schema):
                if verbose:
                    print(fname)
                self._intern(fname, sql)

    def find_sql(self, schema):
        base = self.arad_delta_datalake + "/redshift"
        for fname in list(pathlib.Path(base).glob(f"{schema}/**/views/*sql")):
            yield fname
            
    def load_sql(self, schema):
        for fname in self.find_sql(schema):
            text = open(fname, 'r').read()
            yield fname, text

    def find_tables(self, sql:str):
        table_name = r"(tline|etgis|sap|td|plscadd|pi|outage|locate_and_mark|edgis)\.vw[A-Z_a-z][A-Z_a-z0-9]+"
        tlist = []
        tf = TextFinder(sql)
        while True:
            try:
                tbl = tf.findx(table_name).mark().spanx(table_name).copy()
                if tbl not in tlist:
                    tlist.append(tbl) 
            except ValueError:
                break
        return tlist

    def _intern(self, fname, sql):
        tlist = self.find_tables(sql)
        if len(tlist)==0: return
        mytable = tlist.pop(0)
        others = sorted(list(set(tlist) - set(mytable)))
        for o in others:
            self.add(fname, mytable, o)


        

