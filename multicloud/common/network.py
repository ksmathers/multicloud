import os
import certifi

class Network:

    def __init__(self, ctx, network_config : dict):
        self.ctx = ctx
        self.cacerts = certifi.where()
        self.verify = network_config.get('verify', True)
        if network_config and 'cacerts' in network_config:
            self.cacerts = os.path.expanduser(network_config['cacerts'])
        else:
            self.cacerts = certifi.where()

    def __repr__(self):
        return f'Network<{self.cacerts}>'
