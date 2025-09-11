import os
import json
from keyring import get_password, set_password
from multicloud.backend.secret import Secret
import boto3
import sys
from botocore.exceptions import ClientError


class AwsSecret(Secret):
    """Fetches secrets automatically by redirecting the secret request to the 
    appropriate service based on the detected runtime environment"""

    def __init__(self, ctx, name : str, client : any):
        super().__init__(ctx, name)
        self.client = client

    def get(self) -> dict:
        """Fetches AWS secrets"""
        try:
            get_secret_value_response = self.client.get_secret_value(SecretId=self.name)
            return json.loads(get_secret_value_response['SecretString'])
        except ClientError as e:
            print("ERROR: Unable to get secret {self.name}", file=sys.stderr)
            raise ValueError(f"Unable to get AWS secret {self.name}")
    
    def set(self, value : dict):
        """Stores AWS secrets"""
        try:
            self.client.put_secret_value(SecretId=self.name, SecretString=json.dumps(value))
        except ClientError as e:
            print("ERROR: Unable to set secret {self.name}", file=sys.stderr)
            raise ValueError(f"Unable to set AWS secret {self.name}")
    

