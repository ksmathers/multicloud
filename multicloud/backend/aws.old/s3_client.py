from .context import Context
from .aws_client import AwsClient
from typing import Optional
import os

class S3Client(AwsClient):
    def __init__(self, ctx : Context, bucket : str = "arad-data-prd"):
        super().__init__(ctx, 's3')
        self.bucket = bucket

    def check_response(self, resp):
        code = resp['ResponseMetadata']['HTTPStatusCode']
        if code >= 200 and code < 300:
            return
        raise Exception(resp)

    def put(self, key: str, data: bytearray, bkt=None):
        #istream = open(data, 'r')
        #o = self.client.Object
        if bkt is None:
            bkt = self.bucket

        opts = {
            'ServerSideEncryption': 'AES256'
        }
        if not self.ctx.s3sse:
            del opts['ServerSideEncryption']

        resp = self.client.put_object(
            ACL='private',
            Body=data,
            ContentLength = len(data),
            Bucket=bkt,
            Key=key,
            **opts
        )
        self.check_response(resp)

    def copy(self, from_arn, to_key, bkt=None):
        if bkt is None:
            bkt = self.bucket

        opts = {
            'ServerSideEncryption': 'AES256'
        }
        if not self.ctx.s3sse:
            del opts['ServerSideEncryption']

        self.client.copy_object(
            ACL='private',
            Bucket=bkt,
            CopySource=from_arn, 
            Key=to_key,
            **opts)

    def copy_arn(self, from_arn, to_arn):
        bkt,key = self.parse_arn(to_arn)
        self.copy(from_arn, key, bkt)

    def put_arn(self, arn: str, data: bytearray):
        """
        Creates an S3 storage object containing the supplied data using an ARN
        for the destination
        """
        bkt,key = self.parse_arn(arn)
        self.put(key, data, bkt)

    def delete(self, key: str, bkt: str = None):
        if bkt is None:
            bkt = self.bucket
        resp = self.client.delete_object(
            Bucket=bkt,
            Key=key
        )
        self.check_response(resp)

    def delete_arn(self, arn: str):
        bkt,key = self.parse_arn(arn)
        self.delete_arn(key, bkt)

    def get(self, key: str, bkt: Optional[str]=None):
        """
        Issues a get request for a key (optionally with respect to another bucket)

        :param: key - the name of the object to access
        :param: bkt - optionally a bucket name, by default the bucket used at S3Client init
        """
        if bkt is None:
            bkt = self.bucket
        resp = self.client.get_object(
            Bucket=bkt,
            Key=key
        )
        self.check_response(resp)
        return resp    

    def get_arn(self, arn: str):
        bkt,key = self.parse_arn(arn)
        return self.get(key, bkt)

    def download(self, key: str, path: Optional[str]=None, bkt: Optional[str]=None):
        """
        Downloads a file from S3, returning the path to the downloaded object
        """
        if bkt is None:
            bkt = self.bucket
        if path is None:
            path = key.replace("/", "_")        
        self.client.download_file(bkt, key, path)
        return path

    @staticmethod
    def parse_arn(arn: str):
        # s3://arad-data-prd/oa/...
        parts = arn.split('/')
        bucket = parts[2]
        key = "/".join(parts[3:])
        return bucket,key

    def download_arn(self, arn: str, path: Optional[str]=None):
        bkt,key = self.parse_arn(arn)
        if path is None:
            parts = arn.split('/')
            path = parts[-1]
        # print(f"download {bkt}/{key} -> {path}")
        self.download(key, path, bkt)

    @staticmethod
    def guess_binary(path, binary='auto'):
        text_suffixes = [ '.csv' ]
        binary_suffixes = [ '.parquet' ]
        if binary == 'auto':
            for s in text_suffixes:
                if path.endswith(s): return False
            for s in binary_suffixes:
                if path.endswith(s): return True
        else:
            if binary == 'binary':
                return True
            if binary == 'text':
                return False
        raise NotImplementedError(f"Can't guess binary for {path}")

    def upload_arn(self, path: str, arn: str, binary='auto'):
        """ - Uploads a file from disk to S3

        path : the path the the file to upload
        arn : an S3 destination.  If the destination ends in '/' then the filename being
             read will automatically be appended to the upload location.

        Returns the ARN uploaded
        """
        bkt,key = self.parse_arn(arn)
        if key.endswith('/'):
            key += os.path.basename(path)
        if not os.path.isfile(path):
            raise ValueError(f"Invalid file path {path}")

        opts = {
            'ServerSideEncryption': 'AES256'
        }
        if not self.ctx.s3sse:
            del opts['ServerSideEncryption']
        self.client.upload_file(path, bkt, key, ExtraArgs=opts)
        return f"s3://{bkt}/{key}"

    def list_l(self, prefix: str="", bucket: Optional[str]=None, delimiter: str="/"):
        if prefix.startswith("s3://") and bucket is None:
            bucket, prefix = self.parse_arn(prefix)
        if bucket is None:
            bucket = self.bucket
        result = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
        self.check_response(result)
        if 'Contents' in result:
            keys = [{'key': x['Key'], 'size': x['Size'], 'modified': x['LastModified'].strftime('%Y-%m-%dT%H:%M:%S')} for x in result['Contents']]
        else:
            keys = []
            
        if 'CommonPrefixes' in result:
            dirs = [x['Prefix'] for x in result['CommonPrefixes']]
        else:
            dirs = []
        return {
            'dirs': dirs,
            'keys': keys
        }       

    def list(self, prefix: str="", bucket: Optional[str]=None, delimiter: str="/"):
        res = self.list_l(prefix, bucket, delimiter)
        res['keys'] = [x['key'] for x in res['keys']]
        return res

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        pass
