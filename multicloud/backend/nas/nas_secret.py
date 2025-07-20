import os
import json
from keyring import get_password, set_password
from ..secret import Secret
from paramiko import SSHClient


class NasSecret(Secret):
    """Fetches secrets from a NAS device by using an SSH keypair to connect and set or retrieve the value using remote shell commands"""
    cache = {}

    def __init__(self, ctx, server : str, name : str):
        super().__init__(ctx, name)
        self.server = server

    def get(self) -> dict:
        """Fetches localhost secrets from keyring"""
        if self.name in NasSecret.cache:
            return NasSecret.cache[self.name]
        secret = json.loads(rssh_get_secret(self.server, self.ctx.service, self.name))
        NasSecret.cache[self.name] = secret
        return secret

    def set(self, value : dict):
        """Stores localhost secrets into keyring"""
        rssh_set_secret(self.server, self.ctx.service, self.name, json.dumps(value))


def rssh_get_secret(server, service, user):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(server)
    xin, xout, xerr = ssh.exec_command(f"cat '.keys/{service}/{user}'\necho $?")
    lines = xout.read()[:-1].decode('ASCII').split('\n')
    #print(f"lines '{lines}'")
    secret = lines[0]
    success = lines[-1]
    ssh.close()
    return secret if success == '0' else None

def rssh_set_secret(server, service, user, password):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(server)
    xin, xout, xerr =  ssh.exec_command(f"mkdir -p '{service}'&&cat >'.keys/{service}/{user}' <<XYZZY\n{password}\nXYZZY\necho $?")
    lines = xout.read()[:-1].decode('ASCII').split('\n')
    success = lines[-1]
    if success != '0':
        raise ValueError(f"Unable to write secret '.keys/{service}/{user}'")
