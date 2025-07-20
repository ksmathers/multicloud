import os
from .docker_util import detect_runtime, DockerRuntime
import generic_templates.secret as gtsecret
import json

JAWS_RUNTIME = os.environ.get("JAWS_RUNTIME", None)

class Secret(gtsecret.Secret):
    """Fetches secrets automatically by redirecting the secret request to the 
    appropriate service based on the detected runtime environment"""

    def __init__(self, name : str, system : str = JAWS_RUNTIME):
        super().__init__(name, system)

    def to_arn(self):
        """The value of the Enum member as a SecretsManager ARN."""
        return f"arn:aws:secretsmanager:us-west-2:925741509387:secret:{self.name}"
   
    def get_secret_aws(self) -> dict:
        """Fetches AWS secrets from EKS"""
        from .secretsmanager_client import SecretsManagerClient
        from .context import Context
        ctx = Context("AUTO")
        secrets = SecretsManagerClient(ctx)
        return secrets.get_secret(self.to_arn())
