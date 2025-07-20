
from dateutil.parser import isoparse
import datetime
import pytz
import re

from generic_templates.zulutime import ZuluTime

class AwsCreds:
    def __init__(self, profile_nm, credential_dict):
        _expires = None
        if 'AccessKeyId' in credential_dict:
            self.access_id = credential_dict['AccessKeyId']
            self.secret_key = credential_dict['SecretAccessKey']
            self.token = credential_dict['SessionToken']
            _expires = str(credential_dict['Expiration'])
        elif 'aws_access_key_id' in credential_dict:
            self.access_id = credential_dict['aws_access_key_id']
            self.secret_key = credential_dict['aws_secret_access_key']
            self.token = credential_dict['aws_session_token']
            if 'session_expires' in credential_dict:
                _expires = credential_dict['session_expires']
        elif  'access_key' in credential_dict:
            self.access_id = credential_dict['access_key']
            self.secret_key = credential_dict['secret_key']
            self.token = credential_dict['token']
            _expires = None
        else:
            raise NotImplementedError(f"Unknown credential dictionary {credential_dict.keys()}")

        self.profile_nm = profile_nm
        # Format normalization
        if _expires is None:
            self.expires = ZuluTime.now()+86400  # assume expiration in 24 hours if unknown
        else:
            self.expires = ZuluTime(_expires)
        #print(f"Credentials for {profile_nm} expire at {self.expires}")

    @classmethod 
    def from_session(cls, session):
        credentials = session.get_credentials()
        return cls(credentials.__dict__)

    @staticmethod
    def date_is_expired(isodate):
        expiration = ZuluTime(isodate)
        now = ZuluTime.now()
        return expiration < now

    @property
    def expiration_hrs(self):
        now = ZuluTime.now()
        print("Current time:", now)
        expiration = self.expires-now
        hrs = expiration.seconds // 3600
        min = (expiration.seconds % 3600) // 60
        return "%d:%02d" % (hrs, min)

    def is_expired(self):
        if self.expires is None:
            # unknown expiration
            return False
        return self.date_is_expired(self.expires)

