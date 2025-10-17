from copy import copy
import os
import re
#from ..autocontext import Context

class Environment:
    def __init__(self, ctx : 'Context', environment : dict):
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
        pattern = re.compile(r"\${(\w+)\.(\w+)}")
        while True:
            match = pattern.search(sval)
            if not match:
                break
            text = match.group(0)
            sys = match.group(1)
            var = match.group(2)
            if sys == "env":
                sval = sval.replace(text, self.getenv(var, ""))
            else:
                raise ValueError(f"Unknown interpolation system '{sys}' in '{sval}'")
        return sval