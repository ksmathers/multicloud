import os
from io import IOBase
from ..object import Object
from ...autocontext import Context
from .aws_options import AwsOptions

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
        fullpath = self.fullpath()
        return open(fullpath, f"w{'b' if binary else 't'}")


    def get_bytes(self):
        fullpath = self.fullpath()
        with open(fullpath, "rb") as f:
            return f.read()
        
    def get_file(self, binary:bool = True) -> IOBase:
        fullpath = self.fullpath()
        return open(fullpath, "r{'b' if binary else 't'}")


            