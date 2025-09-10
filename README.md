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
        basedir: <base-directory-for-local>
        server: <server-hostname-for-nas>
        port: <webdav-port>
        bucket: <bucket-name-for-aws>
        keyring: [fernet|None]
```

# Synopsis

```python
import multicloud
mc = multicloud.Context() # initializes the 'default' service
mc_local = multicloud.Context('local') # Initializes the 'local' service
mc_portable_secrets = multicloud.Context(
  password='changeit',
  config={
    'default':{
      'backend':{
        'type':'local', 
        'keyring':'fernet'
      }}}    
  )

# object storage
obj = mc.object('path/to/object')
obj.put_text('this is some content')

# secret storage
sec = mc.secret('secret_name')
key = os.urandom(16)
sec.set({'key': key.hex()})
restored_key = bytes.fromhex(sec.get()['key'])
```

# Examples

Local Secret Service using fernet_keyring.  Password can optionally come from the shell environment instead by removing the 'environment' section.   Alternatively the bootstrap password can be supplied when the multicloud Context is instantiated.

```yaml
portable_secrets:
  environment:
    MULTICLOUD_BOOTSTRAP_PASSWORD: changeit
  backend:
    type: local
    keyring: fernet
```

Local Secret Service using system keyring.  Password is from the system dialog box if not preauthorized.

```yaml
system_secrets:
  backend:
    type: local
```

Local Secret Service and Object Store

```yaml
local:
  backend:
    type: local
    basedir: ~/.multicloud/objects
```




