import os
import json
import keyring
from ..secret import Secret
from typing import Optional


class LocalSecret(Secret):
    """Fetches secrets automatically by redirecting the secret request to the 
    appropriate service based on the detected runtime environment"""

    def __init__(self, ctx, name : str, backend : Optional[str] = None):
        super().__init__(ctx, name)
        if backend is not None and backend == "fernet":
            from .fernet_keyring import FernetKeyring
            assert(ctx.bootstrap_password is not None), "Fernet keyring requires a bootstrap password"
            backend = FernetKeyring(ctx.bootstrap_password)
            backend.activate()

    def get(self) -> dict:
        """Fetches localhost secrets from keyring"""
        return json.loads(keyring.get_password(self.ctx.service, self.name))
    
    def set(self, value : dict):
        """Stores localhost secrets into keyring"""
        keyring.set_password(self.ctx.service, self.name, json.dumps(value))


