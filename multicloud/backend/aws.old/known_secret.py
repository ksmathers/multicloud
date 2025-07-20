import os
from .docker_util import detect_runtime, DockerRuntime
from .secretsmanager_client import SecretId
import json

JAWS_RUNTIME = os.environ.get("JAWS_RUNTIME", None)

class KnownSecret:
    def __init__(self, system = JAWS_RUNTIME):
        if system is None:
            system = detect_runtime()
        else:
            system = DockerRuntime[system]
        self.system = system

    def __repr__(self):
        return f"KnownSecret({self.system.name})"

    def get_secret(self, known_secret : SecretId):
        #print(f"Fetch secret {known_secret.name}")
        if self.system == DockerRuntime.KUBERNETES:
            return self.get_secret_aws(known_secret)
        elif self.system == DockerRuntime.OTHER:
            return self.get_secret_keyring(known_secret)
        elif self.system == DockerRuntime.DOCKER:
            return self.get_secret_server(known_secret)
        else:
            raise NotImplementedError(f"Unknown secret system: {self.system}")
    
    def get_secret_aws(self, known_secret : SecretId):
        from .secretsmanager_client import SecretsManagerClient
        from .context import Context
        ctx = Context("AUTO")
        secrets = SecretsManagerClient(ctx)
        return secrets.get_secret(known_secret.arn)

    def get_secret_keyring(self, known_secret : SecretId):
        from keyring import get_password
        return json.loads(get_password("aws", known_secret.value))

    def get_secret_server(self, known_secret : SecretId):
        import requests
        url = f"http://host.docker.internal:4443/secret/aws/{known_secret.value}"
        r = requests.get(url)
        assert(r.status_code == 200)
        return r.json()

