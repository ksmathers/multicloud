from configparser import ConfigParser
import os
import sys
from .aws_creds import AwsCreds
from generic_templates.zulutime import ZuluTime
from enum import Enum

class StopExecution(Exception):
    def _render_traceback_(self):
        print("ERROR:", str(self), file=sys.stderr)

class Role(Enum):
    OA = "arn:aws:iam::925741509387:role/Transmission_Operability_Assessment_User"
    METEO_DEV = "arn:aws:iam::967517989313:role/OAModelS3WriteRole"
    METEO_PROD = "arn:aws:iam::865440211801:role/OAModelS3WriteRole"
    METEO_QA = "arn:aws:iam::850755775074:role/OAModelS3WriteRole"
    TLINE_PROD = "arn:aws:iam::925741509387:role/arad-tline-prod-cf"
    TLINE_DEV ="arn:aws:iam::925741509387:role/arad-tline-dev-cf"

    @property
    def arn(self):
        return self.value

class SessionManager:
    def __init__(self, role=None):
        """
        Creates a SessionManager for managing AWS Sessions and activates the initial session by either
        loading the profile that was last activated through the 'msentraid' command, or by loading the
        default session, if a logged in session isn't available.  

        If role is supplied then an STS session for that role will be started as soon as the SessionManager
        has been initialized.

        role :str: Either a short name for a role, or a full role ARN
                    'de': A base role for most AWS operations. The actual role used is selected automatically
                        based on the initial session, so it adapts to current runtime environment
        """
        self.sts_sessions = {}
        self._activate_default_session()
        if role:
            self.assume_role(role)

    def _activate_session(self, role, session=None):
        if session is None:
            if role == 'root':
                self.active_session = self.root_session
            elif role in self.sts_sessions:
                self.active_session = self.sts_sessions[role]
            else:
                raise RuntimeError(f"Not an activated session {role}")
        else:
            self.active_session = session
            if role == 'root':
                self.root_session = session
            else:
                self.sts_sessions[role] = session

    def _activate_default_session(self):
        import boto3
        active_role_fn = os.path.expanduser("~/.aws/active_role")
        if os.path.isfile(active_role_fn):
            profile_nm=None
            with open(active_role_fn) as f:
                for line in f:
                    if "profile=" in line:
                        profile_nm = line.split("=")[1].strip()
            if profile_nm is None:
                raise RuntimeError(f"Unable to find profile in {active_role_fn}")
            self.root_profile = profile_nm
            self._activate_session('root', boto3.Session(profile_name=profile_nm))
            print(f"Started session for profile {profile_nm}")
        else:
            self._activate_session('root', boto3.Session())
            print("Started default session")

    def client(self, svc, region=None, session=None):
        """
        Creates a boto3 client for the requested svc (and optionally region)
        using the active role.
        """
        if session is None:
            session = self.active_session

        args = { }
        if region is not None:
            args['region_name'] = region
        return session.client(svc, **args)

    def who_am_i(self, session=None):
        """ - requests the current role from AWS STS
        """
        if session is None:
            session = self.active_session
        sts = session.client("sts")
        ident = sts.get_caller_identity()
        arn = ident.get('Arn')
        return arn
    
    def get_de_role(self):
        ident = self.who_am_i(self.root_session)
        role_arn = None
        if "AWS-A2854-All-Transmission_Operability_Assessment_User" in ident:
            role_arn = Role.OA.value
        elif "EKS-Nonprod-Node-Role" in ident:
            role_arn = Role.TLINE_DEV.value
        elif "EKS-Prod-Node-Role" in ident:
            role_arn = Role.TLINE_PROD.value
        elif "arad-tline-dev-cf" in ident:
            role_arn = Role.TLINE_DEV.value
        elif "arad-tline-prod-cf" in ident:
            role_arn = Role.TLINE_PROD.value
        return role_arn
    
    def expand_short_role_arn(self, rolename):
        short_roles = {
            'meteo-dev': Role.METEO_DEV,
            'meteo-qa': Role.METEO_QA,
            'meteo-prod': Role.METEO_PROD,
            'tline-dev': Role.TLINE_DEV,
            'tline-prod': Role.TLINE_PROD
        }
        if rolename == 'de':
            role_arn = self.get_de_role()
        elif rolename in short_roles:
            role_arn = short_roles[rolename].value
        else:
            role_arn = rolename
        return role_arn

    def _make_session_from_credentials(self, creds):
        import boto3
        return boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )
    
    def assume_role(self, rolename, refresh=False):
        """ - activates the specified rolename
        rolename : One of 'de','meteo-dev','meteo-qa','meteo-prod', or an AWS arn.   If None then the 
        base SAML role is returned.

        After assume_role() the client() method will return clients bound to the activated role.
        """

        role_arn = self.expand_short_role_arn(rolename)
        if rolename == 'root':
            self._activate_default_session()
            return
        if (rolename in self.sts_sessions and not refresh):
            self._activate_session(rolename)
            return

        sts = self.client('sts')
        result = sts.assume_role(RoleArn=role_arn, RoleSessionName=f"aws-jupyter-{rolename}")
        if 'Credentials' not in result:
            raise PermissionError(f"Unable to assume {rolename} role")
        self._activate_session(rolename, self._make_session_from_credentials(result['Credentials']))

    def __str__(self):
        sval = f"SessionManager(root_session={self.who_am_i(self.root_session)}, active_session={self.who_am_i(self.active_session)})"
        return sval

                
