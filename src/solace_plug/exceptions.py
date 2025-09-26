from solace.messaging.errors.pubsubplus_client_error import (
    PubSubPlusClientError,
    PubSubTimeoutError,
    MessageRejectedByBrokerError,
    PublisherOverflowError as SolacePublisherOverflowError,
    MessageDestinationDoesNotExistError,
    MessageNotAcknowledgedByBrokerError,
    IllegalArgumentError,
    InvalidDataTypeError,
)


class SolaceError(Exception):
    """Base class for all Solace-related errors."""


# ---- Client errors ----

class ClientError(SolaceError):
    """Raised when a client connection to Solace fails."""


class IllegalStateClientError(ClientError):
    """Raised when an illegal client state transition occurs."""


# ---- Publish errors ----

class PublishError(SolaceError):
    """Base error for publish-related failures."""


class TimeoutError(PublishError):
    """Raised when a publish operation times out waiting for broker acknowledgment."""


class PublisherOverflowError(PublishError):
    """Raised when the publisher sends messages faster than Solace I/O or buffer limits."""


class InvalidMessageError(PublishError):
    """Raised when a message or its properties are invalid."""


class DestinationNotFoundError(PublishError):
    """Raised when publishing to a destination that does not exist."""


class MessageNotAcknowledgedError(PublishError):
    """Raised when the broker could not acknowledge message persistence."""


class MessageRejectedError(PublishError):
    """Raised when the broker explicitly rejects a message."""


# ---- Subscribe errors ----

class SubscribeError(SolaceError):
    """Base error for subscribe-related failures."""


def _translate_exception(err: Exception) -> "SolaceError":
    if isinstance(err, SolacePublisherOverflowError):
        raise PublisherOverflowError(str(err)) from err
    if isinstance(err, InvalidDataTypeError):
        raise InvalidMessageError(str(err)) from err
    if isinstance(err, MessageDestinationDoesNotExistError):
        raise DestinationNotFoundError(str(err)) from err
    if isinstance(err, MessageNotAcknowledgedByBrokerError):
        raise MessageNotAcknowledgedError(str(err)) from err
    if isinstance(err, PubSubTimeoutError):
        raise TimeoutError(str(err)) from err
    if isinstance(err, MessageRejectedByBrokerError):
        raise MessageRejectedError(str(err)) from err
    if isinstance(err, IllegalArgumentError):
        raise InvalidMessageError(str(err)) from err
    if isinstance(err, PubSubPlusClientError):
        raise ClientError(str(err)) from err
    raise SolaceError(str(err)) from err