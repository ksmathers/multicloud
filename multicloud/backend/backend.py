from ..common.runtime import Runtime, detect_runtime
from .secret import Secret
from .object import Object

class Backend:
    def __init__(self, ctx, name="BaseBackend"):
        self.name = name
        self.ctx = ctx

    def __repr__(self):
        return f"{self.name}<>"

    def secret(self, name) -> Secret:
        raise NotImplementedError("base class")
    
    def object(self, key) -> Object:
        raise NotImplementedError("base class")

