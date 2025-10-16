import multicloud
import yaml
from getpass import getpass

config = yaml.safe_load("""
multicloud:
  backend:
    type: local
default:
  backend:
    type: aws
    Bucket: frubious-bandersnatch
    Region: us-west-2
""")
mc_multi = multicloud.Context("multicloud", config=config)
creds = mc_multi.secret("aws")

mc_default = multicloud.Context(config=config, credentials=creds)
ksecret = mc_default.secret('myname')

#ksecret.set({"name": "Kevin Smathers"})

print(ksecret.get())
