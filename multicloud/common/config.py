import yaml
from ..autocontext import Context

class Config:
    def __init__(self, ctx: Context, config_dict: dict):
        self.config = config_dict
        self.ctx = ctx

    @classmethod
    def from_yaml(cls, ctx: Context, config_yaml_str: str):
        config = yaml.load(config_yaml_str, yaml.loader.SafeLoader)
        return cls(ctx, config)

    def get_section(self, section: str, default=None):
        section_data = self.config.get(section, default)
        if type(section_data) is dict:
            return Config(self.ctx, section_data)
        raise ValueError(f"Config section '{section}' is not a dictionary")
    
    def get_value(self, key, default=None):
        value = self.config.get(self.ctx.service, default)
        if type(value) is str:
            return self.ctx.environment.interpolate(value)
        raise ValueError(f"Unsupported config value type for {key}: {type(value)}")
    

