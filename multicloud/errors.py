class ConfigurationError(Exception):
    """Custom exception class for configuration-related errors."""
    def __init__(self, message="Configuration error occurred"):
        self.message = message
        super().__init__(self.message)