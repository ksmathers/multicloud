import os
import json
from keyring import get_password, set_password
from ..secret import Secret


class LocalSecret(Secret):
    """Fetches secrets automatically by redirecting the secret request to the 
    appropriate service based on the detected runtime environment"""

    def __init__(self, ctx, name : str):
        super().__init__(ctx, name)

    def get(self) -> dict:
        """Fetches localhost secrets from keyring"""
        return json.loads(get_password(self.ctx.service, self.name))
    
    def set(self, value : dict):
        """Stores localhost secrets into keyring"""
        set_password(self.ctx.service, self.name, json.dumps(value))
    

