from io import IOBase, BytesIO
from ..object import Object
from ...autocontext import Context
from .aws_options import AwsOptions

class ObjectIO(BytesIO):
    def __init__(self, object, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object = object

    def close(self):
        object.put_bytes(self.getvalue())
        super().close()

class AwsObject(Object):
    options = AwsOptions

    def __init__(self, ctx:Context, key:str, bucket:str, client):
        super().__init__(ctx, key)
        self.client = client
        self.bucket = ctx.environment.interpolate(bucket)
        #print(f"AwsObject<{self.bucket}>({key})")

    def put_bytes(self, data : bytes):
        response = self.client.put_object(
            Body=data,
            Bucket=self.bucket,
            Key=self.key,
            #ServerSideEncryption='AES256'|'aws:kms'|'aws:kms:dsse',
            #RequestPayer='requester',
        )

    def put_file(self, binary:bool = True) -> IOBase:
        ios = ObjectIO(self)
        return ios

    def get_bytes(self):
        response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        return response['Body'].read()

    def get_file(self, binary:bool = True) -> IOBase:
        """Reads an object and returns a file-like object.

        Args:
            binary (bool, optional): Whether to read the file in binary mode. Defaults to True.

        Returns:
            IOBase: A file-like object for reading the object data.
        """
        response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        return response['Body']

    def exists(self) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self.key)
            return True
        except self.client.exceptions.ClientError:
            return False
