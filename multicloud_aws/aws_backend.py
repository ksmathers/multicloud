import boto3.session
from multicloud.backend import Backend
from multicloud.backend.secret import Secret
from multicloud.backend.object import Object
from .aws_secret import AwsSecret
from .aws_object import AwsObject
from .aws_options import AwsOptions
import boto3

class AwsBackend(Backend):
    def __init__(self, ctx, options : AwsOptions):
        super().__init__(ctx, "LocalBackend")
        self.bucket = options.bucket
        self.region = options.region
        self.session = boto3.session.Session()

    def secret(self, name) -> Secret:
        client = self.session.client(
            service_name='secretsmanager',
            region_name=self.region
        )
        return AwsSecret(self.ctx, self.session, name, client)

    def object(self, key) -> Object:
        client = self.session.client(
            service_name='s3',
            region_name=self.region
        )
        return AwsObject(self.ctx, key, self.bucket, client)

def create_backend(ctx, backend_config : dict) -> Backend:
    if backend_config is None:
        raise ValueError("Backend configuration is required")

    assert('type' in backend_config)
    if backend_config['type'] == 'aws':
        options = AwsOptions(backend_config)
        return AwsBackend(ctx, backend_config['region'], backend_config['bucket'])
    else:
        raise ValueError(f"Unsupported backend type: {backend_config['type']}")


