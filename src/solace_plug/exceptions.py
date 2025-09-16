class SolaceError(Exception):
    """Base class for all Solace-related errors."""


class SolaceConnectionError(SolaceError):
    """Raised when connection to Solace fails."""


class PublishTimeoutError(SolaceError):
    """Raised when a publish operation times out."""


class CircuitBreakerError(SolaceError):
    """Raised when the circuit breaker is open and blocks operations."""