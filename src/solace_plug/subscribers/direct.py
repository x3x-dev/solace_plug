import logging
import asyncio
import typing as t
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace_plug.exceptions import SubscribeError
from solace_plug.schemas.base import IncomingDirectMessage, BaseEvent

log = logging.getLogger("solace_plug")


class _MessageHandler(MessageHandler):
    def __init__(self, callback: t.Callable[[IncomingDirectMessage], None]):
        self._callback = callback

    def on_message(self, message: InboundMessage):
        raw = message.get_payload_as_string()
        event = BaseEvent.model_validate_json(raw)

        incoming_msg = IncomingDirectMessage(
            topic=message.get_destination_name(),
            message_id=message.get_application_message_id(),
            properties=message.get_properties() or {},
            event=event,
        )
        self._callback(incoming_msg)


class DirectSubscriber:
    """
    Subscribes to non-persistent (direct) messages from Solace topics.

    Direct messages are fire-and-forget:
      - No persistence
      - Only received by active subscribers
      - Optimized for speed

    Args:
        client: A connected SolaceClient instance.
        topics: List of topic strings to subscribe to.
        handler: Optional custom MessageHandler to process inbound messages.

    Usage:
        def on_msg(msg: InboundMessage):
            print("Got message:", msg.get_payload_as_string())

        sub = DirectSubscriber(client, topics=["orders.created"])
        with sub:
            # stays active until stopped
            while True: ...
    """

    def __init__(
        self,
        client,
        topics: list[str],
        on_message: t.Callable[[IncomingDirectMessage], None] | None = None,
    ):
        if not client._connected or not client._service:
            raise SubscribeError("Solace client is not connected.")

        self._client = client
        self._service = client.get_messaging_service()
        self._topics = topics
        self._receiver = None
        self._handler = _MessageHandler(on_message or self._default_message_handler)

    def _default_message_handler(self, message: IncomingDirectMessage):
        log.info(
            "[DirectSubscriber] New message:\n%s", message.model_dump_json(indent=4)
        )

    def start(self):
        """Start the direct subscriber and begin receiving messages."""
        try:
            subs = [TopicSubscription.of(t) for t in self._topics]
            self._receiver = (
                self._service.create_direct_message_receiver_builder()
                .with_subscriptions(subs)
                .build()
            )
            self._receiver.start()
            self._receiver.receive_async(self._handler)
            log.info("Direct subscriber started on topics: %s", self._topics)
        except Exception as e:
            raise SubscribeError(f"Failed to start direct subscriber: {e}") from e

    def stop(self):
        """Stop and clean up the subscriber."""
        if self._receiver:
            try:
                self._receiver.terminate()
                self._receiver = None
                log.info("Direct subscriber stopped.")
            except Exception as e:
                raise SubscribeError(f"Failed to stop subscriber: {e}") from e

    # Context manager support
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


# -----------------------------
# Async direct subscriber
# -----------------------------


class _AsyncMessageHandler(MessageHandler):
    def __init__(
        self,
        async_callback: t.Callable[[IncomingDirectMessage], t.Awaitable[None]],
        event_loop: asyncio.AbstractEventLoop,
    ):
        self._async_callback = async_callback
        self._event_loop = event_loop

    def on_message(self, message: InboundMessage):
        raw = message.get_payload_as_string()
        event = BaseEvent.model_validate_json(raw)

        incoming_msg = IncomingDirectMessage(
            topic=message.get_destination_name(),
            message_id=message.get_application_message_id(),
            properties=message.get_properties() or {},
            event=event,
        )

        # Bridge from Solace thread to async world
        asyncio.run_coroutine_threadsafe(
            self._async_callback(incoming_msg), self._event_loop
        )


class AsyncDirectSubscriber:
    """
    Async-friendly direct message subscriber for Solace.
    """

    def __init__(
        self,
        client,
        topics: list[str],
        on_message: (
            t.Callable[[IncomingDirectMessage], t.Awaitable[None]] | None
        ) = None,
    ):
        if not client._connected or not client._service:
            raise SubscribeError("Solace client is not connected.")

        self._client = client
        self._service = client.get_messaging_service()
        self._topics = topics
        self._receiver = None
        self._on_message = on_message or self._default_message_handler
        self._event_loop = None

    async def _default_message_handler(self, message: IncomingDirectMessage):
        """Default async message handler that just logs the message."""
        log.info(
            "[AsyncDirectSubscriber] New message:\n%s",
            message.model_dump_json(indent=4),
        )

    async def start(self):
        """Start the async subscriber."""
        # Capture current event loop
        self._event_loop = asyncio.get_running_loop()

        try:
            subs = [TopicSubscription.of(t) for t in self._topics]
            self._handler = _AsyncMessageHandler(self._on_message, self._event_loop)

            self._receiver = (
                self._service.create_direct_message_receiver_builder()
                .with_subscriptions(subs)
                .build()
            )
            self._receiver.start()
            self._receiver.receive_async(self._handler)
            log.info("Async direct subscriber started on topics: %s", self._topics)
        except Exception as e:
            raise SubscribeError(f"Failed to start async subscriber: {e}") from e

    async def stop(self):
        """Stop the async subscriber."""
        if self._receiver:
            try:
                self._receiver.terminate()
                self._receiver = None
                log.info("Async direct subscriber stopped.")
            except Exception as e:
                raise SubscribeError(f"Failed to stop async subscriber: {e}") from e

    # Async context manager support
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
