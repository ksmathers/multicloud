from ..backend import Backend
from .local_secret import LocalSecret
from .local_object import LocalObject
from ..secret import Secret
from ..object import Object
from ...errors import ConfigurationError
from ...autocontext import Context
from typing import Optional

class LocalBackend(Backend):
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
        return LocalSecret(self.ctx, name, self.keyring_impl, self.keyring_path)

    def object(self, key) -> Object:
        """Returns an abstraction to access an object stored in the local filesystem
        
        Args:
            key : str : The key of the object to access
        """
        if self.basedir is None:
            raise ConfigurationError("object service requires the 'basedir' configuration setting")
        return LocalObject(self.ctx, key, self.basedir)


    