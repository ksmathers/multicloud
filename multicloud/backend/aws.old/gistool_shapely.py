import shapely
import shapely.wkt
import shapely.geometry
import shapely.ops
import pyproj
import json

from .context import Context
from twkbpy import Twkb

class Geometry:
    def __init__(self, gistool, wkt, refsys):
        self.gistool = gistool
        self.wkt = wkt
        self.geom = shapely.wkt.loads(wkt)
        self.refsys = refsys

    def ExportToWkt(self):
        return self.wkt

    def ExportToGeoJson(self):
        return json.dumps(shapely.geometry.mapping(self.geom))

    def TransformTo(self, to_refsys):
        if type(to_refsys) is str:
            prj = pyproj.Proj(to_refsys)
            fn_transform = pyproj.Transformer.from_proj(self.refsys.AsProj(), prj, always_xy=True).transform
        else:
            fn_transform = pyproj.Transformer.from_crs(self.refsys.crs, to_refsys.crs, always_xy=True).transform
        newgeom = shapely.ops.transform(fn_transform, self.geom)
        return newgeom.wkt

    def Length(self):
        return self.geom.length

class Refsys:
    REFSYSTEMS = {
        "EPSG:26910": 26910,
        "WGS84": 4326,
        "PGE": "+proj=tmerc +lat_0=0 +lon_0=-123.0 +k_0=0.9996 +x_0=500000 +y_0=0 +units=us-ft"
    }

    def __init__(self, gistool, refname):
        self.gistool = gistool
        self.refname = refname
        self.crs = pyproj.CRS(self.refname)

    def AsProj(self):
        return self.crs.to_proj4()

    def AsRsid(self):
        if self.refname in Refsys.REFSYSTEMS:
            return Refsys.REFSYSTEMS[self.refname]
        raise NotImplementedError(f"refsys name {self.refname}")

    def ExportToWkt(self):
        return self.crs.to_wkt()

class GisToolShapely:
    def __init__(self, ctx : Context):
        self.ctx = ctx

    def CreateGeometryFromTwkb(self, bdata, refsys):
        reader = Twkb.from_binary(bdata)
        return Geometry(self, reader.to_ogr().ExportToWkt(), refsys)

    def LoadGeometriesForTline(self, sap_func_loc_no, refsys):
        pass
        # rsid = refsys.AsRsid()
        # tbl = self.dbc.sql_query("select etgis_id, sap_func_loc_no, ST_AsText(ST_Transform(shape,%s)) as shape_wkt from etgis.t_ohlinesegment where sap_func_loc_no=%s", rsid, sap_func_loc_no)
        # segments = []
        # for i in tbl.index:
        #     row = tbl.iloc[i].to_dict()
        #     #print(row.keys())
        #     segments.append( { 'etgis_id': row['etgis_id'], 'sap_func_loc_no': row['sap_func_loc_no'], 'geometry': Geometry(self, row['shape_wkt'], refsys)})
        # return segments
