import os
import yaml

from .virtual import create_backend, create_network, create_environment
from .backend.object import Object
from .backend.secret import Secret
from typing import Optional


class Context:
    def __init__(self, service="default", config=None, password=None, credentials: Optional[Secret] = None):
        """
        Reads service descriptions from a $HOME/.jaws and instantiates one of the services

        Args:
           service :str: The name of the service to create
           config :config: Optionally a dictionary of service names and settings

                <servicename>:
                    environment:
                        <ENVVAR>: <value>
                        MULTICLOUD_BOOTSTRAP_PASSWORD: <optional-password-to-unlock-keyring>
                        ...
                    network:
                        cacerts: <optional-root-ssl-certificate-bundle-file>
                    backend:
                        type: [local|aws|tiny|nas]
                        basedir: <base-directory-for-local>
                        server: <server-hostname-for-nas>
                        port: <webdav-port>

                        bucket: <bucket-name-for-aws>

        If not specified, environment defaults to the running unix environment variables.
        Network defaults to the certifi certificate bundle.
        Backend type must be specified.  For 'aws' the bucket must be specified.  For 'local' the basedir must be specified.
        """
        if config is None:
            with open(os.path.expanduser("~/.jaws"), "rt") as f:
                config = yaml.load(f.read(), yaml.loader.SafeLoader)
        config_group = config.get(service)
        self.service = service
        self.credentials = credentials
        self.environment = create_environment(self, config_group.get("environment"))
        #self.bootstrap_password = self.environment.getenv("MULTICLOUD_BOOTSTRAP_PASSWORD", password)
        self.network = create_network(self, config_group.get("network"))
        self.backend = create_backend(self, config_group.get("backend"))
        


    def object(self, key:str) -> Object:
        return self.backend.object(key)

    def secret(self, name:str) -> Secret:
        return self.backend.secret(name)

    def __repr__(self):
        return f"Context<{self.service}>({self.backend},{self.network},{self.environment})"



