from typing import Optional
import yaml
#from ..autocontext import Context

class Config:
    def __init__(self, config_dict: dict, parent: 'Config' = None):
        self.config = config_dict
        self.parent = parent

    @classmethod
    def from_yaml(cls, config_yaml_str: str):
        config = yaml.load(config_yaml_str, yaml.loader.SafeLoader)
        return cls(config)
    
    def to_dict(self):
        return self.config

    def get_section(self, section: str, default=None) -> Optional['Config']:
        section_data = self.config.get(section, default)
        if section_data is None:
            return None
        if type(section_data) is dict:
            return Config(section_data, parent=self)
        raise ValueError(f"Config section '{section}' is not a dictionary")
    
    def get_value(self, ctx : 'Context', key : str, default=None) -> Optional[str]:
        value = self.config.get(key, default)
        if value is None:
            return None
        if type(value) is str:
            return ctx.environment.interpolate(value)
        if type(value) in [int, float, bool]:
            return value
        raise ValueError(f"Unsupported config value type for {key}: {type(value)}")
    

