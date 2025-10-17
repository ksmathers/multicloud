import os
import json
import keyring
from ..secret import Secret
from ...autocontext import Context
from typing import Optional
from cryptography.fernet import InvalidToken


class LocalSecret(Secret):
    """Fetches secrets automatically by redirecting the secret request to the 
    appropriate service based on the detected runtime environment"""

    def __init__(self, ctx : Context, name : str):
        """Initializes a local secret access object
        Args:
            ctx : Context : The context this secret is part of
            name : str : The name of the secret to access
            backend_type : Optional[str] : The keyring backend to use for secrets, e.g. "fernet", default None uses the system keyring
            keyring_path : Optional[str] : The file to use for file-based keyrings, e.g. for "fernet" backends, default None uses the default location
        """
        super().__init__(ctx, name)
        keyring_impl = keyring.get_keyring()

    def get(self) -> dict:
        """Fetches localhost secrets from keyring"""
        try:
            pw = keyring.get_password(self.ctx.service, self.name)
            if pw is None:
                raise KeyError(f"Secret '{self.name}' not found in keyring for service '{self.ctx.service}'")
            return json.loads(pw)
        except InvalidToken as e:
            raise RuntimeError(f"Failed to decrypt secret, check that the bootstrap password is correct") from e

    def set(self, value : dict):
        """Stores localhost secrets into keyring"""
        keyring.set_password(self.ctx.service, self.name, json.dumps(value))


