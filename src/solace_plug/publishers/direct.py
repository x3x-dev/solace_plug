import logging
import uuid
import asyncio
import typing as t
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import (
    DirectMessagePublisher,
    PublishFailureListener,
    FailedPublishEvent,
)
from solace_plug.exceptions import PublishError
from solace_plug.schemas.base import BaseEvent
from solace_plug.utils.decorators import retry_on_failure

log = logging.getLogger("solace_plug")


class _PublishFailureListener(PublishFailureListener):
    def __init__(self, callback: t.Callable[[FailedPublishEvent], None]):
        self._callback = callback

    def on_failed_publish(self, event: FailedPublishEvent):
        self._callback(event)


class DirectPublisher:
    """
    Publishes non-persistent (direct) messages to Solace topics.

    A Solace message has three parts:
      - Header: destination (topic) and message ID
      - Properties: key/value metadata (optional)
      - Body: the actual payload (JSON from BaseEvent)

    This publisher sends fire-and-forget event messages:
      - No delivery guarantees
      - No acknowledgments
      - Optimized for speed

    Args:
        client: A connected SolaceClient instance.
        default_properties: Properties applied to all messages by default.
        on_failure: Optional Callable(event) to handle publish failures.

    Usage:
        publisher = DirectPublisher(
            client,
            default_properties={"app": "order-service"}
        )

        with publisher:
            event = BaseEvent(source="order-service", payload={"order_id": "123"})
            publisher.publish(
                topic="orders.created",
                event=event,
                properties={"trace_id": "abc123"}
            )
    """

    def __init__(
        self,
        client,
        default_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        on_failure: t.Callable[[FailedPublishEvent], None] | None = None,
    ):
        if not client._connected or not client._service:
            raise PublishError("Solace client is not connected.")

        self._client = client
        self._service = client.get_messaging_service()
        self._publisher: DirectMessagePublisher | None = None
        self._default_properties = default_properties or {}
        self._on_failure = on_failure or self._default_failure_handler

    def _default_failure_handler(self, event):
        log.error("Direct publisher error event: %s", event)

    @retry_on_failure
    def start(self):
        """Start the direct publisher and attach the failure listener."""
        try:
            self._publisher = (
                self._service.create_direct_message_publisher_builder().build()
            )
            self._publisher.set_publish_failure_listener(
                _PublishFailureListener(self._on_failure)
            )
            self._publisher.start()
            log.info("Direct publisher started.")
        except Exception as e:
            raise PublishError(f"Failed to start direct publisher: {e}") from e
    
    @retry_on_failure
    def stop(self):
        """Stop and clean up the publisher."""
        if self._publisher:
            try:
                self._publisher.terminate()
                self._publisher = None
                log.info("Direct publisher stopped.")
            except Exception as e:
                raise PublishError(f"Failed to stop publisher: {e}") from e

    @retry_on_failure
    def publish(
        self,
        topic: str,
        event: BaseEvent,
        properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        message_id: str | None = None,
    ):
        """
        Publish a BaseEvent to the given topic.

        Args:
            topic: The topic to publish to.
            event: The BaseEvent (or subclass) to send.
            properties: Optional per-message application properties.
            message_id: Optional application message ID (defaults to UUID4).

        Raises:
            PublishError: If publishing fails immediately on the client side.
                          (Broker delivery errors are handled via on_failure.)
        """
        if not self._publisher:
            raise PublishError("Publisher is not started. Call .start() first.")

        try:
            # Merge default and per-message properties
            merged_props = {**self._default_properties, **(properties or {})}
            msg_id = message_id or str(uuid.uuid4())

            # Build outbound message
            builder = self._service.message_builder()
            builder = builder.with_application_message_id(msg_id)
            for key, value in merged_props.items():
                builder = builder.with_property(key, value)

            payload_str = event.to_json()
            message = builder.build(payload_str)

            # Publish
            self._publisher.publish(destination=Topic.of(topic), message=message)
            log.info(
                "Published event %s to topic '%s' with msg_id=%s props=%s",
                event.id,
                topic,
                msg_id,
                merged_props or "{}",
            )

        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    # Context manager support
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


# -----------------------------
# Async direct publisher
# -----------------------------


class _AsyncPublishFailureListener(PublishFailureListener):
    def __init__(
        self,
        async_callback: t.Callable[[FailedPublishEvent], t.Awaitable[None]],
        event_loop,
    ):
        self.async_callback: t.Callable[[FailedPublishEvent], t.Awaitable[None]] = (
            async_callback
        )
        self.event_loop: asyncio.AbstractEventLoop = event_loop

    def on_failed_publish(self, event: FailedPublishEvent):
        # Bridge from Solace thread to async world
        asyncio.run_coroutine_threadsafe(self.async_callback(event), self.event_loop)


class AsyncDirectPublisher:
    """
    Async-friendly direct message publisher for Solace.
    """

    def __init__(
        self,
        client,
        default_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        on_failure: t.Callable[[FailedPublishEvent], t.Awaitable[None]] | None = None,
    ):
        if not client._connected or not client._service:
            raise PublishError("Solace client is not connected.")

        self._client = client
        self._service = client.get_messaging_service()
        self._publisher: DirectMessagePublisher | None = None
        self._default_properties = default_properties or {}
        self._on_failure = on_failure or self._default_failure_handler
        self._event_loop = None

    async def _default_failure_handler(self, event):
        log.error("Async Direct publisher error event: %s", event)

    @retry_on_failure
    async def start(self):
        """Start the direct publisher and attach the failure listener."""
        self._event_loop = asyncio.get_running_loop()

        try:
            self._publisher = (
                self._service.create_direct_message_publisher_builder().build()
            )
            self._publisher.set_publish_failure_listener(
                _AsyncPublishFailureListener(self._on_failure, self._event_loop)
            )
            self._publisher.start()
            log.info("Direct publisher started.")
        except Exception as e:
            raise PublishError(f"Failed to start direct publisher: {e}") from e

    @retry_on_failure
    async def stop(self):
        """Stop and clean up the publisher."""
        if self._publisher:
            try:
                self._publisher.terminate()
                self._publisher = None
                log.info("Direct publisher stopped.")
            except Exception as e:
                raise PublishError(f"Failed to stop publisher: {e}") from e

    @retry_on_failure
    async def publish(
        self,
        topic: str,
        event: BaseEvent,
        properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        message_id: str | None = None,
    ):
        """
        Publish a BaseEvent to the given topic.

        Args:
            topic: The topic to publish to.
            event: The BaseEvent (or subclass) to send.
            properties: Optional per-message application properties.
            message_id: Optional application message ID (defaults to UUID4).

        Raises:
            PublishError: If publishing fails immediately on the client side.
                          (Broker delivery errors are handled via on_failure.)
        """
        if not self._publisher:
            raise PublishError("Publisher is not started. Call .start() first.")

        try:
            # Merge default and per-message properties
            merged_props = {**self._default_properties, **(properties or {})}
            msg_id = message_id or str(uuid.uuid4())

            # Build outbound message
            builder = self._service.message_builder()
            builder = builder.with_application_message_id(msg_id)
            for key, value in merged_props.items():
                builder = builder.with_property(key, value)

            payload_str = event.to_json()
            message = builder.build(payload_str)

            # Publish
            self._publisher.publish(destination=Topic.of(topic), message=message)
            log.info(
                "Published event %s to topic '%s' with msg_id=%s props=%s",
                event.id,
                topic,
                msg_id,
                merged_props or "{}",
            )

        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    # Context manager support
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
