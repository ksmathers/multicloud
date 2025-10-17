from copy import copy
import os
import re

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
        pattern = re.compile(r"\${(\w+)\.(\w+)}")
        while True:
            match = pattern.search(sval)
            if not match:
                break
            sys = match.group(1)
            var = match.group(2)
            text = match.group(0)
            if sys == "env":
                sval = sval.replace(text, environ.get(var, ""))
            elif sys == "secret":
                var, attr = (var.split('.', 1) + [None])[:2]
                secret = self.ctx.secret(var)
                sval = sval.replace(text, secret.get(var, {}).get(attr, ""))
            else:
                raise ValueError(f"Unknown interpolation system '{sys}' in '{sval}'")
        return sval