from copy import copy

class Environment:
    def __init__(self, ctx, environment : dict):
        self.ctx = ctx
        if environment:
            self._environ = copy(environment)
        else:
            raise ValueError(f"Unknown environment config: {environment}")

    def __repr__(self):
        return f"Environment<{self.getenv('ENV','dev')}>"
    
    def getenv(self, varname, default=None):
        return self._environ.get(varname, default)
    
    def interpolate(self, sval):
        environ = self._environ
        for var in self._environ:
            sval = sval.replace(f"%{var}%", self._environ[var])
        return sval