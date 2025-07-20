from ..backend import Backend
from .local_secret import LocalSecret
from .local_object import LocalObject
from ..secret import Secret
from ..object import Object
from ...errors import ConfigurationError

class LocalBackend(Backend):
    def __init__(self, ctx, basedir):
        super().__init__(ctx, "LocalBackend")
        self.basedir = basedir

    def secret(self, name) -> Secret:
        return LocalSecret(self.ctx, name)
    
    def object(self, key) -> Object:
        if self.basedir is None:
            raise ConfigurationError("object service requires the 'basedir' configuration setting")
        return LocalObject(self.ctx, key, self.basedir)


    