class Secret:
    def __init__(self, ctx, name : str):
        self.ctx = ctx
        self.name = name

    def __repr__(self):
        return f"{type(self).__name__}<{self.name}>"

    def get(self) -> dict:
        """fetches a secret value
        
        Returns:
           :dict: A JSON serializable object that was stored in the secret"""
        raise NotImplementedError("base class")
    
    def set(self, _value : dict):
        """stores a value in the secret
        
        Args:
           _value :dict: A JSON serializable object to store
        """
        raise NotImplementedError("base class")