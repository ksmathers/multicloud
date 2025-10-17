import os
import certifi
from ..common.config import Config
#from ..autocontext import Context

class Network:

    def __init__(self, ctx : 'Context', network_config : Config):
        self.ctx = ctx
        self.cacerts = certifi.where()
        self.verify = network_config.get_value(ctx, 'verify', True)
        self.cacerts = network_config.get_value(ctx, 'cacerts', certifi.where())

    def __repr__(self):
        return f'Network<{self.cacerts}>'
