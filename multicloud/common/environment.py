from copy import copy
import os

class Environment:
    def __init__(self, ctx, environment : dict):
        self.ctx = ctx
        if environment is not None:
            self._environ = copy(environment)
        else:
            raise ValueError(f"Unknown environment config: {environment}")

    def __repr__(self):
        return f"Environment<{self.getenv('ENV','dev')}>"
    
    def getenv(self, varname, default=None):
        # First check in the provided environment, then fallback to os.environ
        return self._environ.get(varname, os.environ.get(varname, default))
    
    def interpolate(self, sval):
        environ = self._environ
        for var in self._environ:
            sval = sval.replace(f"%{var}%", self._environ[var])
        return sval