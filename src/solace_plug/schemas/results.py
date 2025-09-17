from pydantic import BaseModel
from solace_plug.enums import PublishStatus


class ConnectionResult(BaseModel):
    """
    Represents the outcome of attempting to connect to Solace.

    Fields:
        success: True if connection succeeded, False otherwise.
        details: Optional description or error details about the connection attempt.
    """
    success: bool
    details: str | None = None


class PublishResult(BaseModel):
    """
    Represents the outcome of a publish operation.

    Fields:
        status: The result status (SUCCESS, FAILED, TIMEOUT).
        message_id: Optional broker-assigned message identifier, if available.
        error: Optional error message if publishing failed.
    """
    status: PublishStatus
    message_id: str | None = None
    error: str | None = None


class SubscriptionResult(BaseModel):
    """
    Represents the status of a subscription attempt.

    Fields:
        topic: The topic or queue name being subscribed to.
        active: True if subscription is active, False if it failed.
        error: Optional error message if the subscription failed.
    """
    topic: str
    active: bool
    error: str | None = None
