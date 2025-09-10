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

