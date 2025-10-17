from ..backend import Backend
from .portable_secret import PortableSecret
#from .portable_object import PortableObject
from ..secret import Secret
from ..object import Object
from ...errors import ConfigurationError
from ...autocontext import Context
from typing import Optional

class PortableBackend(Backend):
    def __init__(self, ctx : Context, basedir, keyring_impl : Optional[str] = None, keyring_path : Optional[str] = None):
        """A local filesystem based backend
        
        Args:
            ctx : Context : The context this backend is part of
            basedir : str : The base directory to store objects in
            keyring_impl : Optional[str] : The keyring backend to use for secrets, e.g. "fernet", default None uses the system keyring
            keyring_file : Optional[str] : The file to use for file-based keyrings, e.g. for "fernet" backends, default None uses the default location
        """
        super().__init__(ctx, "LocalBackend")
        self.basedir = basedir
        self.keyring_impl = keyring_impl
        self.keyring_path = keyring_path

    def secret(self, name) -> Secret:
        """Returns an abstraction to access a secret stored in the local keyring

        Args:
            name : str : The name of the secret to access
        """
        return PortableSecret(self.ctx, name, self.keyring_impl, self.keyring_path)

    def object(self, key) -> Object:
        """Returns an abstraction to access an object stored in the local filesystem
        
        Args:
            key : str : The key of the object to access
        """
        raise NotImplementedError("Portable object storage not yet implemented")
        if self.basedir is None:
            raise ConfigurationError("object service requires the 'basedir' configuration setting")
        return PortableObject(self.ctx, key, self.basedir)


    