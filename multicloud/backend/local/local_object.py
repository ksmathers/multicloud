import os
from io import IOBase
from ..object import Object
from ...autocontext import Context

class LocalObject(Object):
    def __init__(self, ctx:Context, key:str, basedir:str):
        super().__init__(ctx, key)
        self.basedir = ctx.environment.interpolate(basedir)
        #print(f"LocalObject<{self.basedir}>")

    def fullpath(self):
        path = os.path.join(self.basedir, f"{self.key}.object")
        return path

    def prepare(self, fullpath):
        subdir = os.path.dirname(fullpath)
        os.makedirs(subdir, exist_ok=True)

    def put_bytes(self, data : bytes):
        fullpath = self.fullpath()
        self.prepare(fullpath)
        with open(fullpath, "wb") as f:
            f.write(data)

    def put_file(self, binary:bool = True) -> IOBase:
        fullpath = self.fullpath()
        return open(fullpath, f"w{'b' if binary else 't'}")

    def get_bytes(self):
        fullpath = self.fullpath()
        with open(fullpath, "rb") as f:
            return f.read()

    def get_file(self, binary:bool = True) -> IOBase:
        fullpath = self.fullpath()
        return open(fullpath, "r{'b' if binary else 't'}")

    def exists(self) -> bool:
        fullpath = self.fullpath()
        return os.path.exists(fullpath)

