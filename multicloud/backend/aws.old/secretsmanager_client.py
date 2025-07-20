import ast
import json
from types import DynamicClassAttribute

from .aws_client import AwsClient
from enum import Enum

class SecretId(Enum):
    ARTIFACTORY_ARAD_SERVICE="artifactory-arad-service"
    POSTGIS_DEV="postgis-dev"
    DCAGIS_DEV="dcagis-dev"
    POSTGIS_PRD="postgis-prd"
    ARAD_GIS_POSTGRES_PRD="ARAD-GIS-Postgres-prd"
    REDSHIFT_DATALAKE_PRD="redshift-datalake-prd"
    REDSHIFT_DATALAKE_DEV="redshift-datalake-dev"
    REDSHIFT_PRD_OA_MODAL_SVC="redshift-prd-oa_modal_svc"
    EKS_PRA_PROD="eks-pra-prod"
    EKS_PRA_DEV="eks-pra-dev"
    EKS_PROD="eks-prod"
    EKS_NONPROD="eks-nonprod"
    FOUNDRY_K0SF_TEST="foundry-k0sf-test"
    K0SF_VFS_SCAN="k0sf_vfs_scan"
    JFROG_ARAD_DOCKER="jfrog-arad-micro-svcs-docker"
    FOUNDRY_SERVICE_ACCOUNT="foundry_service_account_token"  
    FOUNDRY_API="foundry-api"
    LOCAL_SONARQUBE_TOKEN="local-sonarqube-token"
    LOCAL_SONARQUBE_DATABASE="local-sonarqube-database"
    SONARQUBE_TOKEN="sonarqube-token"
    DEV_FOUNDRY_SONARQUBE_METRICS="dev-foundry-sonarqube-metrics"
    PROD_FOUNDRY_SONARQUBE_METRICS="prod-foundry-sonarqube-metrics"
    
    @classmethod
    def by_value(cls, value):
        try:
            return cls(value)
        except:
            return None
    
    @classmethod
    def by_name(cls, name):
        for n in list(cls):
            if n.name == name: return n
        raise NotImplementedError(f"No secret named {name}")
        return None

    @DynamicClassAttribute
    def arn(self):
        """The value of the Enum member as a SecretsManager ARN."""
        return f"arn:aws:secretsmanager:us-west-2:925741509387:secret:{self._value_}"


class SecretsManagerClient(AwsClient):
    def __init__(self, ctx):
        if not ctx is None:
            super().__init__(ctx, "secretsmanager", ctx.region)
        else:
            self.ctx = None

    def known_secret(self, secret : SecretId, force_update : bool = False):
        """Fetches a known secret from local cached copy, or from SecretsManager if the secret isn't cached, 
        and reduces the service invocation load on the SecretsManager service.  In a service context the 
        cache will be empty so the first call will always result in a clean fetch.   In the development context
        it will default to your local keyring, so you will need to set 'force_update' to True when the secret
        is changed.
        
        Args:
            secret :SecretId: one of the known secrets
            force_update :bool: forces the local cache to be updated from SecretsManager
        """
        arn = secret.arn
        pwd = None
        if not force_update:
            #print("cached secret")
            pwd = self.get_cached_secret(arn)
        if pwd is None:
            #print("smc secret")
            pwd = self.get_secret(arn)
            self.set_cached_secret(arn, pwd)
        return pwd

    @staticmethod
    def unpack(pwd):
        if pwd is None:
            return None
        while type(pwd) is str:
            try:
                pwd = json.loads(pwd)
            except:
                pwd = ast.literal_eval(pwd)
        return pwd

    def get_cached_secret(self, arn):
        keyname = arn.split(":")[-1]
        try:
            import keyring
            pwd = keyring.get_password("aws", keyname)
            return self.unpack(pwd)
        except:
            pass
        return None

    def set_cached_secret(self, arn, pwd):
        keyname = arn.split(":")[-1]
        try:
            import keyring
            if not type(pwd) is str:
                pwd = json.dumps(pwd)
            keyring.set_password("aws", keyname, pwd)    
        except:
            # no keyring backend installed
            pass

    def get_secret(self, arn):
        result = self.client.get_secret_value(SecretId=arn)
        self.check_response(result)
        pwd = json.loads(result['SecretString'])
        return pwd
