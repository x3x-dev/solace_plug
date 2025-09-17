from enum import StrEnum

class PublishStatus(StrEnum):
    """Result of a publish operation."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class CircuitState(StrEnum):
    """State of the circuit breaker (for fault tolerance)."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"  