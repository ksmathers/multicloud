import os
from io import IOBase, BytesIO
from ..object import Object
from ...autocontext import Context

from webdav4.client import Client


client = Client("https://drivep.ank.com:5006", auth=("kevin", "zany0Tiger!"), verify=False)
#client.exists("Documents/Readme.md")

client.ls("Archive", detail=False)


class NasObject(Object):
    def __init__(self, ctx:Context, key:str, server:str, port:int, creds_secret:str):
        super().__init__(ctx, key)
        self.server = ctx.environment.interpolate(server)
        creds = ctx.backend.secret(creds_secret).get()
        self.port = port
        self.client = Client(f"https://{self.server}:{self.port}", auth=creds, verify=ctx.network.verify)
        self.fullpath = key
        #print(f"LocalObject<{self.server}:{self.port}>")

    def put_bytes(self, data : bytes):
        ios = BytesIO(data)
        with open(self.fullpath, "wb") as f:
            self.client.upload_fileobj(f, self.fullpath)

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


