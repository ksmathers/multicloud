from .common.runtime import Runtime, detect_runtime
from .backend import Backend
from .common.network import Network
from .common.environment import Environment
from .common.config import Config
from .backend.secret import Secret
import os
from typing import Optional



def create_backend(ctx, backend_config : Config) -> Backend:
    if backend_config is None:
        rt = detect_runtime()
        if rt == Runtime.KUBERNETES:
            backend_config = Config({ "type": "aws", "creds": "auto" })
        elif rt == Runtime.DOCKER:
            backend_config = Config({ "type": "tinyserver" })
        elif rt == Runtime.MACOS:
            backend_config = Config({ "type": "local", "basedir": "/tmp" })
        elif rt == Runtime.WINDOWS:
            backend_config = Config({ "type": "local", "basedir": "C:\\tmp" })
        else:
            raise NotImplementedError(f"Unable to create backend for {rt}")

    backend_type = backend_config.get_value(ctx, 'type', 'local')
    if backend_type == 'aws':
        #my_backend = __import__(f"multicloud.aws.{backend_config['type']}_backend")
        #my_backend.create_backend(ctx, backend_config)
        from multicloud_aws.aws_backend import AwsBackend
        from multicloud_aws.aws_options import AwsOptions
        options = AwsOptions(backend_config)
        return AwsBackend(ctx, options)
    elif backend_type == 'tinyserver':
        from .backend.tiny.tiny_backend import TinyBackend
        return TinyBackend(ctx)
    elif backend_type == 'local':
        from .backend.local.local_backend import LocalBackend
        return LocalBackend(ctx, 
                            basedir=backend_config.get_value(ctx, 'basedir'))
    elif backend_type == 'portable':
        from .backend.portable.portable_backend import PortableBackend
        return PortableBackend(ctx,
                               fernet_password=backend_config.get_value(ctx, 'fernet_password'),
                               keyring_path=backend_config.get_value(ctx, 'keyring_path')
                               )
    elif backend_type == 'nas':
        from .backend.nas.nas_backend import NasBackend
        return NasBackend(ctx, 
                          backend_config.get_value(ctx, 'server'), 
                          backend_config.get_value(ctx, 'port'), 
                          backend_config.get_value(ctx, 'secret'))
    else:
        library_name = backend_config.get_value(ctx, 'library')
        if library_name:
            try:
                external_library = __import__(library_name)
                return external_library.create_backend(ctx, backend_config)
            except ImportError:
                raise ImportError(f"Unable to import library '{library_name}'")
        else:
            raise ValueError("Unsupported backend type and no library specified")


def create_network(ctx, network_config : Config) -> Network:
    if network_config is None:
        import certifi
        rt = detect_runtime()
        if rt == Runtime.KUBERNETES:
            network_config = Config({ "cacerts": certifi.where() })
        elif rt == Runtime.DOCKER:
            network_config = Config({ "cacerts": certifi.where() })
        elif rt == Runtime.MACOS:
            network_config = Config({ "cacerts": "~/etc/CombinedCA.cer" })
        elif rt == Runtime.WINDOWS:
            network_config = Config({ "cacerts": certifi.where() })
        else:
            raise NotImplementedError(f"Unable to create network for {rt}")
    return Network(ctx, network_config)

def create_environment(ctx, environment_config : Config) -> Environment:
    if environment_config is None:
        environment_config = {}
    else:
        environment_config = environment_config.to_dict()
    return Environment(ctx, environment_config)