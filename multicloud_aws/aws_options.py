from enum import Enum
from typing import Optional

class S3Sse(Enum):
    #ServerSideEncryption='AES256'|'aws:kms'|'aws:kms:dsse',
    AES256 = "AES256"
    KMS = "aws:kms"
    KMS_DSSE = "aws:kms:dsse"

class S3Payer(Enum):
    REQUESTER = "requester"

class AwsOptions:
    def __init__(self, opts):
        self.ServerSideEncryption = opts.get('ServerSideEncryption', None)
        self.RequestPayer = opts.get('RequestPayer', None)
        self.region = opts.get('Region', None)
        self.bucket = opts.get('Bucket', None)

    def populate(self, opts):
        dd = {}
        for opt in opts:
            v = self.__getattribute__(self, opt)
            if v is not None:
                dd[opt] = v.value
        return dd

    def s3args_put_object(self):
        return self.populate(['ServerSideEncryption', 'RequestPayer'])
