import platform
import os
from enum import Enum


class Runtime(Enum):
    OTHER = -1
    DOCKER = 1
    KUBERNETES = 2
    MACOS = 3
    WINDOWS = 4
    
g_runtime = None

def detect_runtime():
    global g_runtime
    if not g_runtime:
        uname = platform.uname()
        if os.path.exists("/var/run/secrets/kubernetes.io") or "KUBERNETES_SERVICE_HOST" in os.environ:
            g_runtime = Runtime.KUBERNETES
        elif os.path.exists("/.dockerenv"):
            g_runtime = Runtime.DOCKER
        elif uname.system == 'Darwin':
            g_runtime = Runtime.MACOS
        elif uname.system == 'Windows':
            g_runtime = Runtime.WINDOWS
        else:
            g_runtime = Runtime.OTHER
    return g_runtime