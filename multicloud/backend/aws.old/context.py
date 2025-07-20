import os
from .aws_token2 import AwsToken

class Context:
    def __init__(self, environment=None, dblive=True):
        """
        Initialize an AWS connection context.  The Context creates a set of connection credentials working from 
        one of the following sources:

        :param: environment : one of USER, AUTO (default)

        USER - Loads the credentials from ~/.aws/credentials as written by Cyber samlapi binary
        AUTO - Uses boto3.Session to load credentials using the built in credential search (default)

        The benefit of USER environment over AUTO is for local development.  AUTO credentials will pick up
        short duration credentials granted through STS for use with the aws CLI command while 'USER' will
        look for the 8-hour credentials that the 'saml' binary produces.  AUTO is the default so that 
        Kubernetes jobs can obtain valid credentials since they do not use 'saml'.  

        To default to 'USER' environment on MacOS use:
           $ export JAWS_ENVIRON=USER 

        :param: dblive : when True requires queries to go to the database. False allows them to be cached.
        """

        if environment is None:
            environment = os.environ.get("JAWS_ENVIRON", "AUTO")

        self.dblive = dblive
        self.s3sse = True
        self._debug = False
        self._force = False
        self._credentials = None
        self._region = "us-west-2"
        self._environ = environment
        self._aws_token = None
        self.login()

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, value):
        self._debug = value

    @property
    def force(self):
        return self._force

    @force.setter
    def force(self, value):
        self._force = value

    @property
    def credentials(self):
        return self._credentials

    def login(self, rolename = "de", renew=False):
        if self._aws_token is None:
            self._aws_token = AwsToken(self._environ, role=None)
        self._credentials = self._aws_token.assume_role(rolename, renew)
        print(f"Expiration in {self._credentials.expiration_hrs} hours")

    def client(self, svc, region='us-west-2'):
        if self._aws_token is None:
            import boto3
            if region is None:
                return boto3.client(svc)
            else:
                return boto3.client(svc, region_name=region)
        else:
            return self._aws_token.client(svc, region)

    def __str__(self):
        sval = f"Context({self._aws_token})"
        return sval

    @property
    def region(self):
        return self._region
