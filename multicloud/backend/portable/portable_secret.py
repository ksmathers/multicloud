import os
import json
from keyring.backend import KeyringBackend
from ..secret import Secret
from ...autocontext import Context
from typing import Optional
from cryptography.fernet import InvalidToken


class PortableSecret(Secret):
    """Fetches secrets automatically by redirecting the secret request to the 
    appropriate service based on the detected runtime environment"""

    def __init__(self, ctx : Context, name : str, keyring_backend : KeyringBackend):
        """Initializes a local secret access object
        Args:
            ctx : Context : The context this secret is part of
            name : str : The name of the secret to access
        """
        #print("Initializing PortableSecret for", name)
        super().__init__(ctx, name)
        self.keyring_backend = keyring_backend

    def get(self) -> dict:
        """Fetches localhost secrets from keyring"""
        #print("PortableSecret.get called")
        try:
            pw = self.keyring_backend.get_password(self.ctx.service, self.name)
            if pw is None:
                raise KeyError(f"Secret '{self.name}' not found in keyring for service '{self.ctx.service}'")
            return json.loads(pw)
        except InvalidToken as e:
            raise RuntimeError(f"Failed to decrypt secret, check that the bootstrap password is correct") from e

    def set(self, value : dict):
        """Stores localhost secrets into keyring"""
        #print("PortableSecret.set called")
        self.keyring_backend.set_password(self.ctx.service, self.name, json.dumps(value))


