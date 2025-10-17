# MultiCloud

Multicloud is an abstract API for cloud services with multiple implementations including AWS, NAS, and local machine.
The current implementation abstracts Object Storage (S3), and Secrets Storage (SecretsManager).

Implementation specifics:

  - Object Storage (multicloud.object):
    - AWS: Uses S3
    - Local: Uses local filesystem
    - NAS: Uses WebDAV

  - Secrets Manager (multicloud.secret):
    - AWS: Uses SecretsManager
    - Local: Uses keyring Python library
    - NAS: Uses SSH to fetch keys stored in plain text in from ~/.keys/<service>/<user>
    - Tiny: Intended for use from Docker containers running on the same system, retrieves keyring secrets as a REST API call to a tiny secret server

# Configuration

Multiple services can be mixed together from various sources all within the same configuration file.  The configuration associates a symbolic name
for the service with one or more service backends, all within the same YAML config file:

```yaml
<servicename>:
    environment:
        <ENVVAR>: <value>
        ...
    network:
        cacerts: <optional-root-ssl-certificate-bundle-file>
    backend:
        type: [local|aws|tiny|nas]
        ...backend specific key value pairs...
```

The environment section of the config file takes precedence over the system environment variables when looking up a value, so environment 
settings can be written to either location.  In the examples below we use the `yaml` file to set environment variables to keep the examples
cohesive.

The network section is used to override standard networking settings.  Currently the only option is `cacerts` which can be used to override
the certificate bundle used to authenticate HTTPS connections.  You would point it at your private self-signed root certificate when talking 
to private services.

The backend section includes options for configuring the `secrets` and `object` services so that a multicloud context can instantiate those
service handles correctly.   

# Synopsis


```python
import multicloud
cfg = multicloud.Config.from_yaml("""
  default:
    backend:
      type: local
  fernet:
    environment:
      FERNET_PASSWORD: changeit
    backend:
      type: portable
""")
mc = multicloud.Context(config=cfg) # initializes the 'default' service
mc_local = multicloud.Context('local', cfg) # Initializes the 'local' service
mc_portable_secrets = multicloud.Context('fernet', cfg)

# object storage
obj = mc.object('path/to/object')
obj.put_text('this is some content')

# secret storage
sec = mc.secret('secret_name')
key = os.urandom(16)
sec.set({'key': key.hex()})
restored_key = bytes.fromhex(sec.get()['key'])
```

# Backends

## Portable Services

The `portable` backend supports services where the storage can be copied from system to system and reused without having to be reinstantiated.  Most services require the underlying secrets
and objects to be copied individually to the new system.  Portable services allow the entire
state to be copied as a single file.  

The portable secrets store uses the Fernet library to store secrets as encrypted data in a JSON
file stored in a file identified by the `keyring_path` configuration property, and encrypted using the password identified by the `keyring_password` property.  

The `keyring_path` is optional, and if not specified will use `./fernet_keyring.json`.  If `keyring_password` is unspecified it will default to the environment variable `FERNET_PASSWORD`.

```yaml
portable:
  environment:
    FERNET_PASSWORD: changeit
  backend:
    type: portable
    keyring: fernet
    keyring_path: ${env:HOME}/etc/fernet.keyring
    keyring_password: ${env:FERNET_PASSWORD}
```

## Local Services

The `local` services backend uses Python's `keyring` library to store and retrieve system 
secrets.  If not previously authorized to read the system keyring then running your code will
open a system dialog box requesting permission to access the keyring.



```yaml
system_secrets:
  backend:
    type: local
```

The `local` services backend also supports object storage.  Objects are saved and retrieved 
from the filesystem using file paths that match the key being used.  The directory structure
is automatically created as needed.  To use local object storage you will need to set the 
`basedir` configuration property to be the root of the object storage directory tree.

```yaml
local:
  backend:
    type: local
    basedir: ${env:HOME}/.multicloud/objects
```

## NAS Services

The `nas` services backend is intended for use with a local NAS device.  Object storage uses 
WebDAV4, while secrets are stored using 'ssh' to set and retrieve files in the target system's
local filesystem.

Configuration of the 'ssh' connection credentials should be set up outside of Multicloud using
the standard tooling for installing your local public key in the NAS server.

WebDAV configuration includes the server name, TCP port number, and the secret to use for basic authentication.

```yaml
local:
  environment:
    WEBDAV_TOKEN: "..."
  backend:
    type: nas
    server: synology.local
    port: 8080
    secret: ${env:WEBDAV_TOKEN}
```

## AWS Services

AWS Secret and Object Service with AWS Credentials in keyring 

```yaml
local:
  backend:
    type: local
aws:
  backend:
    type: aws
    Bucket: my-bucket-name
    Region: us-west-2
```

Using keyring, setup the AWS credentials as follows:

```bash
$ keyring set local aws <<!
>>> { "access_id": "<aws-access-id>", "secret_key": "<aws-secret-key>" }
>>> !
```

The AWS context can be initialized by providing a local secret to the AWS Context from which to load the AWS credentials:

```python
mc_local = multicloud.Context("local")
creds = mc_local.secret("aws")
mc_aws = multicloud.Context("aws", credentials=creds)
```

At that point the 'mc_aws' context can be used to create handles to read and write objects or secrets, but neither the object bucket
nor the secrets are automatically provisioned.   Those must be instantiated separately using your tool of choice, after which multicloud
will read and write to the contents.


