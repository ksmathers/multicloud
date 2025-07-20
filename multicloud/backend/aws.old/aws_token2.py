from configparser import ConfigParser
import os
import sys
from .aws_creds import AwsCreds
from generic_templates.zulutime import ZuluTime
from generic_templates.docker_util import detect_runtime, DockerRuntime

class StopExecution(Exception):
    def _render_traceback_(self):
        print("ERROR:", str(self), file=sys.stderr)

class AwsToken:
    #DATA_ENGINEER_ROLE_387="arn:aws:iam::925741509387:role/ARAD_Data_Engineer"
    ITOA_USER_ROLE_387="arn:aws:iam::925741509387:role/Transmission_Operability_Assessment_User"
    METEO_DEV_ROLE="arn:aws:iam::967517989313:role/OAModelS3WriteRole"
    METEO_PROD_ROLE="arn:aws:iam::865440211801:role/OAModelS3WriteRole"
    METEO_QA_ROLE="arn:aws:iam::850755775074:role/OAModelS3WriteRole"
    TLINE_PROD_ROLE="arn:aws:iam::925741509387:role/arad-tline-prod-cf"
    TLINE_DEV_ROLE="arn:aws:iam::925741509387:role/arad-tline-dev-cf"

    def __init__(self, profile_nm='AUTO', role="de"):
        """
        Initializes an AwsToken loading the initial credentials from your ~/.aws/credentials file
        and then uses the STS service to obtain temporary credentials for the Data Engineer role.

        Params:
            profile_nm - name of the profile to load from ~/.aws/credentials.   If left blank the
            default is to use the first unexpired profile found.   There are special values for 
            'profile_nm':
                'USER' - Chooses the best unexpired credential in ~/.aws/credentials
                'AUTO' - Chooses the credential using 'boto3.Session' which has internal search logic
            role - the name of the role to assume.  Defaults to 'de' but can be overridden using the 
            AWS_DE_ROLE environment variable (low priority) or by calling the constructor with the 
            role you want:
                'de' - Arad Data Engineer
                'meteo-qa' - Meteorology QA S3 bucket access
                'meteo-dev' - Meteorology DEV S3 bucket access
                'meteo-prod' - Meteorology PROD S3 bucket access
                'tline-prod' - Kubernetes enabled access to ARAD PROD resources
                'tline-dev' - Kubernetes enabled access to ARAD DEV resources

        Errors:
            PermissionError - thrown when the initial credentials are expired
            ValueError - thrown when profile_nm refers to a profile not listed in credentials

        """
        import boto3
        self.docker_runtime = detect_runtime()
        if "AWS_CA_BUNDLE" not in os.environ:
            if self.docker_runtime == DockerRuntime.KUBERNETES:
                import certifi
                os.environ["AWS_CA_BUNDLE"] = certifi.where()
            else:
                os.environ["AWS_CA_BUNDLE"] = os.path.expanduser("~/etc/CombinedCA.cer")
            print(f'Unset AWS_CA_BUNDLE initialized to {os.environ["AWS_CA_BUNDLE"]}')

        if profile_nm == "AUTO":
            self.root_session = boto3.Session()
        else:
            self.credentials = os.path.expanduser("~/.aws/credentials")
            config = ConfigParser()
            config.read(self.credentials)
            if profile_nm == "USER":
                active_role_fn = os.path.expanduser("~/.aws/active_role")
                if os.path.isfile(active_role_fn):
                    with open(active_role_fn, "rt") as f:
                        for line in f:
                            if "profile" in line:
                                profile_nm = line.split("=")[1].strip()
                    if profile_nm == "USER":
                        raise StopExecution("~/.aws/active_role isn't formatted as expected")
                    print(f"Using profile {profile_nm}")
                else:
                    profiles = self.active_profiles(config)
                    if len(profiles) == 0:
                        raise StopExecution("No unexpired profiles in ~/.aws/credentials")

                    profile_nm = profiles[0]
                    print("Found unexpired profile", profile_nm)
            self.profile_name = profile_nm
            if not profile_nm in config:
                raise ValueError(f"Unknown profile '{profile_nm}'")
        
            self.profile = AwsCreds(profile_nm, config[profile_nm])
            if self.profile.is_expired():
                raise PermissionError(f"Profile {profile_nm} has expired.  Please renew the login and try again.")
            self.root_session = boto3.Session(
                aws_access_key_id=self.profile.access_id,
                aws_secret_access_key=self.profile.secret_key,
                aws_session_token=self.profile.token
            )

        self.role_arn = None
        self.role_name = None
        self.role = None
        self.root_arn = None
        if role is not None:
            self.assume_role(role)
        self.primary_role = self.role

    def client(self, svc, region = None, role=None):
        """
        Creates a boto3 client for the requested svc (and optionally region)
        using the active role.   Renews the role first if needed.
        """
        import boto3
        if role is None:
            role = self.role
        if role.is_expired():
            raise Exception(f"role expired {role.profile_nm}")

        args = {
            'aws_access_key_id':role.access_id,
            'aws_secret_access_key':role.secret_key,
            'aws_session_token':role.token
        }
        if region is not None:
            args['region_name'] = region
        return boto3.client(svc, **args)

    def root_ident(self):
        """ - requests the current role from AWS STS
        """
        sts = self.root_session.client("sts")
        root_ident = sts.get_caller_identity()
        root_arn = root_ident.get('Arn')
        return root_arn
    
    def assume_role(self, rolename, renew=False):
        """ - activates the specified rolename
        rolename : One of 'de','meteo-dev','meteo-qa','meteo-prod', or an AWS arn.   If None then the 
        base SAML role is returned.

        After assume_role() the client() method will return clients bound to the activated role.
        """
        role_arn = None

        if rolename == "de":
            ident = self.root_ident()
            if "AWS-A2854-All-Transmission_Operability_Assessment_User" in ident:
                role_arn = AwsToken.ITOA_USER_ROLE_387
            elif "EKS-Nonprod-Node-Role" in ident:
                role_arn = AwsToken.TLINE_DEV_ROLE
            elif "EKS-Prod-Node-Role" in ident:
                role_arn = AwsToken.TLINE_PROD_ROLE
            elif "arad-tline-dev-cf" in ident:
                role_arn = AwsToken.TLINE_DEV_ROLE
            elif "arad-tline-prod-cf" in ident:
                role_arn = AwsToken.TLINE_PROD_ROLE
            else:
                rolename = os.environ["AWS_DE_ROLE"]

        sts_origin = 'root'
        if rolename == "meteo-dev":
            role_arn = AwsToken.METEO_DEV_ROLE
            sts_origin = 'primary'
        elif rolename == "meteo-qa":
            role_arn = AwsToken.METEO_QA_ROLE
            sts_origin = 'primary'
        elif rolename == "meteo-prod":
            role_arn = AwsToken.METEO_PROD_ROLE
            sts_origin = 'primary'
        elif rolename == "tline-prod":
            role_arn = AwsToken.TLINE_PROD_ROLE
        elif rolename == "tline-dev":
            role_arn = AwsToken.TLINE_DEV_ROLE
        elif rolename.startswith("arn:aws:iam::"):
            role_arn = rolename
        
        if role_arn is None and rolename is not None:
            print("Unknown role:", rolename)
            raise NotImplementedError("Unable to assume unknown role: " + rolename)

        if ((role_arn == self.role_arn) 
                and (role_arn is not None) 
                and (not self.role.is_expired()) 
                and (not renew) 
            ):
            # role is already active
            return self.role

        if sts_origin == 'root':
            sts = self.root_session.client("sts")
        elif sts_origin == 'primary':
            sts = self.client("sts",role=self.primary_role)
        root_ident = sts.get_caller_identity()
        root_arn = root_ident.get('Arn')
        if role_arn is None or root_arn == role_arn:
            self.role = AwsCreds.from_session(self.root_session)
            role_arn = root_arn
        else:
            result = sts.assume_role(RoleArn=role_arn, RoleSessionName=f"aws-jupyter")
            if 'Credentials' not in result:
                raise PermissionError(f"Unable to assume {rolename} role")
            self.role = AwsCreds("aws-jupyter", result['Credentials'])
        self.role_arn = role_arn
        self.role_name = rolename
        return self.role

    def __str__(self):
        sval = f"AwsToken2(root_session={self.root_session}, role_arn={self.role_arn})"
        return sval

    def active_profiles(self, config):
        profiles = []
        sections = config.sections()
        for s in sections:
            section = config[s]
            if ('aws_access_key_id' in section and
                'aws_secret_access_key' in section and
                'aws_session_token' in section and
                'session_expires' in section):
                if not AwsCreds.date_is_expired(section['session_expires']):
                    profiles.append(s)
        return profiles
                
