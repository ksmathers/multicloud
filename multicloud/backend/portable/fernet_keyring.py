from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from keyring.backend import KeyringBackend
import base64
import json
import os

class FernetKeyring(KeyringBackend):
    def __init__(self, password, keystore_path="fernet-keyring.json"):
        #print("Initializing FernetKeyring with keystore:", keystore_path)
        self.keystore_path = keystore_path
        self.keystore = self.load_data()
        self.codec = Fernet(self.superkey(bytes(password, 'UTF-8')))

    def get_password(self, service, user):
        #print("Getting password for", user, "@", service, "from", self.keystore_path)
        black_text = self.keystore.get(service, {}).get(user, None)
        red_text = None
        if black_text:
            red_text = self.codec.decrypt(bytes.fromhex(black_text)).decode('UTF-8')
        return red_text

    def set_password(self, service, user, password):
        #print(f"Storing password for {user}@{service} in {self.keystore_path}")
        if not 'service' in self.keystore:
            self.keystore[service] = {}
        red_text = password
        black_text = self.codec.encrypt(bytes(red_text, 'UTF-8')).hex()
        self.keystore[service][user] = black_text
        self.save_data()
        
    def superkey(self, password):
        kdf = PBKDF2HMAC(hashes.SHA256(), 32, bytes.fromhex(self.keystore['salt']), iterations=1_200_000)
        key = base64.urlsafe_b64encode(kdf.derive(password))
        return key

    def load_data(self):
        if os.path.exists(self.keystore_path):
            with open(self.keystore_path, "rt") as f:
                keystore = json.loads(f.read())
        else:
            self.salt = os.urandom(16)
            keystore = { 'salt': self.salt.hex() }
        assert 'salt' in keystore, f"Invalid keystore {self.keystore_path}, missing salt"
        return keystore

    def save_data(self):
        basedir = os.path.dirname(self.keystore_path)
        if basedir and not os.path.isdir(basedir):
            os.makedirs(basedir)
        with open(self.keystore_path, "wt") as f:
            print(json.dumps(self.keystore), file=f)

    def activate(self):
        print("Activating FernetKeyring as the default keyring backend")
        import keyring
        keyring.set_keyring(self)