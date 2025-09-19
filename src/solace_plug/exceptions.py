class SolaceError(Exception):
    """Base class for all Solace-related errors."""


class SolaceConnectionError(SolaceError):
    """Raised when connection to Solace fails."""

class PublishError(SolaceError):
    """Raised when a publish operation times out."""


class SubscribeError(SolaceError):
    """Raised when a subscribe operation times out."""

class CircuitBreakerError(SolaceError):
    """Raised when the circuit breaker is open and blocks operations."""