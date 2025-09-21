class SolaceError(Exception):
    """Base class for all Solace-related errors."""

class ClientError(SolaceError):
    """Raised when connection to Solace fails."""

class IllegalStateClientError(ClientError):
    """When another connect/disconnect operation is ongoing."""

class PublishError(SolaceError):
    """Raised when a publish operation times out."""

class SubscribeError(SolaceError):
    """Raised when a subscribe operation times out."""

class CircuitBreakerError(SolaceError):
    """Raised when the circuit breaker is open and blocks operations."""