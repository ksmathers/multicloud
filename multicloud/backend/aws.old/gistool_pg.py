from .postgres_client import PostgresClient
from .postgres_client import AradDatabase
from .context import Context

class Geometry:
    def __init__(self, gistool, wkt, refsys):
        self.gistool = gistool
        self.wkt = wkt
        self.refsys = refsys

    def ExportToWkt(self):
        return self.wkt

    def ExportToGeoJson(self):
        tbl = self.gistool.dbc.sql_query("select ST_AsGeoJson(ST_GeomFromText(%s,%s))", self.wkt, self.refsys.AsRsid())
        if len(tbl)==0:
            raise SystemError(f"Unable to get GeoJson from {self.wkt}")
        return tbl.st_asgeojson.values[0]

    def TransformTo(self, to_refsys) -> str:
        tbl = self.gistool.dbc.sql_query("select ST_AsText(ST_Transform(ST_GeomFromText(%s,%s),%s))", self.wkt, self.refsys.AsRsid(), to_refsys.AsRsid())
        if len(tbl)==0:
            raise SystemError(f"Unable to transform from {self.refsys.refname} to {to_refsys.refname}")
        return tbl.st_astext.values[0]

    def Length(self):
        tbl = self.gistool.dbc.sql_query("select ST_Length(ST_GeomFromText(%s,%s))", self.wkt, self.refsys.AsRsid())
        if len(tbl)==0:
            raise SystemError(f"Unable to calculate length from {self.wkt}")
        return tbl.st_length.values[0]

class Refsys:
    REFSYSTEMS = {
        "EPSG:26910": 26910,
        "WGS84": 4326,
    }

    def __init__(self, gistool, refname):
        self.gistool = gistool
        self.refname = refname

    def AsRsid(self):
        if self.refname in Refsys.REFSYSTEMS:
            return Refsys.REFSYSTEMS[self.refname]
        raise NotImplementedError(f"refsys name {self.refname}")

    def ExportToWkt(self):
        tbl = self.gistool.dbc.sql_query("select srtext from public.spatial_ref_sys where srid=%s", self.AsRsid())
        if len(tbl)==0:
            raise FileNotFoundError(f"refsys name {self.refname}")
        return tbl.srtext.values[0]

class GisToolPG:
    def __init__(self, ctx : Context):
        self.dbc = PostgresClient(ctx, AradDatabase.POSTGIS_ROOT)

    def CreateGeometryFromTwkb(self, bdata, refsys):
        tbl = self.dbc.sql_query("select ST_AsText(ST_GeomFromTWKB(%s))", bdata)
        if len(tbl)==0:
            raise ValueError("Unable to parse TWKB geometry")
        return Geometry(self, tbl.st_astext.values[0], refsys)

    def LoadGeometriesForTline(self, sap_func_loc_no, refsys):
        rsid = refsys.AsRsid()
        tbl = self.dbc.sql_query("select etgis_id, sap_func_loc_no, ST_AsText(ST_Transform(shape,%s)) as shape_wkt from etgis.t_ohlinesegment where sap_func_loc_no=%s", rsid, sap_func_loc_no)
        segments = []
        for i in tbl.index:
            row = tbl.iloc[i].to_dict()
            #print(row.keys())
            segments.append( { 'etgis_id': row['etgis_id'], 'sap_func_loc_no': row['sap_func_loc_no'], 'geometry': Geometry(self, row['shape_wkt'], refsys)})
        return segments