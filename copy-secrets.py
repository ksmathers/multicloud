import yaml
import keyring
import os
from multicloud.autocontext import Context
import getpass
mc_portable = Context(service="portable", config={
    "portable": {
        "backend": {
            "type": "portable",
            "keyring_backend": "fernet",
                "fernet_password": getpass.getpass("Enter Fernet password: ")
            }
    }
})

    
with open(os.path.expanduser("~/.secrets.yaml"), "rt") as f:
    secrets = yaml.safe_load(f)
                             
for group in secrets:
    for name in secrets[group]:
        value = keyring.get_password(group, name)
        if value is None:
            print(f"!! Unable to read secret '{name}' from group '{group}'")
            continue

        try:
            yvalue = yaml.safe_load(value)
        except yaml.YAMLError:
            print(f"!! Warning: Secret '{name}' in group '{group}' is not valid YAML, storing as default string")
            yvalue = { "value": value }


        print(f"Storing secret '{name}' in portable keyring")
        mc_portable.secret(name).set(yvalue)
