import os
from enum import Enum

class DockerRuntime(Enum):
    DOCKER = 1
    KUBERNETES = 2
    OTHER = 3

def detect_runtime():
    runtime = DockerRuntime.OTHER
    if os.path.exists("/.dockerenv"):
        runtime = DockerRuntime.DOCKER
    if os.path.exists("/var/run/secrets/kubernetes.io") or "KUBERNETES_SERVICE_HOST" in os.environ:
        runtime = DockerRuntime.KUBERNETES
    return runtime