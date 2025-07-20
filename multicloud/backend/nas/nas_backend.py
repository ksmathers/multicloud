from ..backend import Backend
from .nas_secret import NasSecret
from .nas_object import NasObject
from ..secret import Secret
from ..object import Object
from ...errors import ConfigurationError

class NasBackend(Backend):
    def __init__(self, ctx, server : str, port : int, webdav_secret : str):
        super().__init__(ctx, "LocalBackend")
        self.server = server
        self.port = port
        self.webdav_secret = webdav_secret

    def secret(self, name) -> Secret:
        """Returns a secret by using ssh to store and retrieve files from the server"""
        return NasSecret(self.ctx, name)

    def object(self, key) -> Object:
        """Store and retrieve objects over WebDAV"""
        return NasObject(self.ctx, key, self.server, self.port, self.webdav_secret)


