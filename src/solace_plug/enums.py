from enum import Enum

class PublishStatus(str, Enum):
    """Result of a publish operation."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class CircuitState(str, Enum):
    """State of the circuit breaker (for fault tolerance)."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"  