import logging
import uuid
import asyncio
import typing as t
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.persistent_message_publisher import (
    PersistentMessagePublisher,
    MessagePublishReceiptListener,
    PublishReceipt,
)

from solace_plug.exceptions import (
    ClientError,
    PublishError,
    _translate_exception,
)
from solace_plug.schemas.base import BaseEvent
from solace_plug.schemas.publish import PublishReceiptModel
from solace_plug.utils.decorators import retry_on_failure

log = logging.getLogger("solace_plug")


class _PublishReceiptListener(MessagePublishReceiptListener):
    """
    Internal listener bridging Solace PublishReceipt -> PublishReceiptModel.
    """

    def __init__(self, callback: t.Callable[[PublishReceiptModel], None]):
        self._callback = callback
        self._receipt_count = 0

    @property
    def receipt_count(self) -> int:
        return self._receipt_count

    def on_publish_receipt(self, receipt: PublishReceipt):
        self._receipt_count += 1
        try:
            model = PublishReceiptModel.from_solace(receipt)
            self._callback(model)
        except Exception as e:
            log.error("Failed to process publish receipt: %s", e, exc_info=True)


class PersistentPublisher:
    """
    Publishes persistent (guaranteed delivery) messages to Solace topics.

    Features:
      - At-least-once delivery
      - Broker persistence & durability
      - Publish receipts via callbacks (converted to PublishReceiptModel)
      - Blocking or async publish APIs

    Typical usage:

        publisher = PersistentPublisher(client)

        with publisher:
            event = BaseEvent(source="order-service", payload={"order_id": "123"})
            publisher.publish(
                topic="service/order/created",
                event=event,
                properties={"trace_id": "abc123"}
            )
    """

    def __init__(
        self,
        client,
        default_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        on_receipt: t.Callable[[PublishReceiptModel], None] | None = None,
    ):
        if not client._connected or not client._service:
            raise ClientError("Solace client is not connected.")

        self._client = client
        self._service = client.get_messaging_service()
        self._publisher: PersistentMessagePublisher | None = None
        self._default_properties = default_properties or {}
        self._on_receipt = on_receipt or self._default_receipt_handler
        self._receipt_listener: _PublishReceiptListener | None = None

    def _default_receipt_handler(self, receipt: PublishReceiptModel):
        """Default handler: just log the receipt."""
        log.info("Persistent publish receipt: %s", receipt.json())

    @property
    def receipt_count(self) -> int:
        """
        Get the total number of publish receipts received since publisher started.
        
        This includes both successful and failed publish attempts. Use this
        for monitoring delivery confirmation rates and publisher health.
        
        Returns:
            int: Total receipt count, or 0 if publisher hasn't been started.
            
        Example:
            publisher.publish("topic", event1)
            publisher.publish("topic", event2)
            # ... wait for receipts ...
            print(f"Received {publisher.receipt_count} confirmations")
        """
        return self._receipt_listener.receipt_count if self._receipt_listener else 0

    @retry_on_failure
    def start(self):
        """
        Start the persistent message publisher.
        
        Creates and starts the underlying Solace publisher, then attaches
        the receipt listener to handle delivery confirmations.
        
        Must be called before publishing messages.
        
        Raises:
            ClientError: If the publisher fails to start.
        """
        try:
            self._publisher = (
                self._service.create_persistent_message_publisher_builder().build()
            )
            # Start the publisher FIRST
            self._publisher.start()
            
            # THEN set the receipt listener (after publisher is started)
            self._receipt_listener = _PublishReceiptListener(self._on_receipt)
            self._publisher.set_message_publish_receipt_listener(self._receipt_listener)
            
            log.info("Persistent publisher started.")
        except Exception as e:
            _translate_exception(e)

    @retry_on_failure
    def stop(self):
        """
        Stop the persistent message publisher and clean up resources.
        
        Terminates the underlying Solace publisher and clears all listeners.
        Safe to call multiple times.
        
        Raises:
            ClientError: If the publisher fails to stop cleanly.
        """
        if self._publisher:
            try:
                self._publisher.terminate()
                self._publisher = None
                self._receipt_listener = None
                log.info("Persistent publisher stopped.")
            except Exception as e:
                _translate_exception(e)


    @retry_on_failure
    def publish(
        self,
        topic: str,
        event: BaseEvent,
        additional_message_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        message_id: str | None = None,
        user_context: t.Any = None,
    ):
        """
        Publish a BaseEvent with guaranteed delivery to a Solace topic.

        This method returns immediately after sending the message to Solace.
        Delivery confirmations arrive asynchronously via the on_receipt callback.

        Args:
            topic: The Solace topic to publish to (e.g., "orders/created").
            event: The BaseEvent (or subclass) containing the message payload.
            additional_message_properties: Optional message properties (merged with default_properties).
                                         These become message metadata visible to subscribers.
            message_id: Optional application message ID. If None, a UUID is generated.
            user_context: Optional data passed back in the receipt callback.
                         Use this to correlate receipts with your application state.

        Example:
            publisher.publish(
                topic="orders/created",
                event=OrderCreatedEvent(order_id="123", amount=99.99),
                additional_message_properties={"priority": "high", "trace_id": "abc123"},
                user_context={"order_id": "123", "action": "ship"}
            )

        Raises:
            ClientError: If publisher is not started.
            PublisherOverflowError: If publishing faster than I/O capabilities.
            MessageRejectedError: If broker rejects the message.
            InvalidMessageError: If message properties are invalid.
            DestinationNotFoundError: If topic doesn't exist on broker.
        """
        if not self._publisher:
            raise ClientError("Publisher is not started. Call .start() first.")

        try:
            # Merge default and additional properties
            merged_props = {**self._default_properties, **(additional_message_properties or {})}
            msg_id = message_id or str(uuid.uuid4())
            
            # Build message
            message = (self._service.message_builder()
                      .with_application_message_id(msg_id)
                      .build(event.to_json()))
            
            self._publisher.publish(
                message, 
                Topic.of(topic), 
                user_context, 
                additional_message_properties=merged_props
            )
            log.info(
                "Published event %s to '%s' msg_id=%s props=%s",
                event.id, topic, msg_id, merged_props,
            )
        except Exception as e:
            _translate_exception(e)

    @retry_on_failure
    def publish_await_acknowledgement(
        self,
        topic: str,
        event: BaseEvent,
        timeout_ms: int = 5000,
        additional_message_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        message_id: str | None = None,
    ):
        """
        Publish a BaseEvent and block until broker acknowledgment is received.

        This method blocks the calling thread until the Solace broker confirms
        it has received and persisted the message, or until the timeout expires.
        Use this when you need immediate confirmation before proceeding.

        Args:
            topic: The Solace topic to publish to (e.g., "orders/created").
            event: The BaseEvent (or subclass) containing the message payload.
            timeout_ms: Maximum time in milliseconds to wait for acknowledgment.
                        Default is 5000ms (5 seconds).
            additional_message_properties: Optional message properties (merged with default_properties).
            message_id: Optional application message ID. If None, a UUID is generated.

        Example:
            # Block until confirmed or timeout
            try:
                publisher.publish_await_acknowledgement(
                    topic="critical/payment/processed",
                    event=PaymentEvent(amount=1000.00),
                    timeout_ms=10000  # Wait up to 10 seconds
                )
                print("Payment confirmed by broker!")
            except SolaceTimeoutError:
                print("Broker didn't respond in time")

        Raises:
            ClientError: If publisher is not started.
            SolaceTimeoutError: If no acknowledgment received within timeout_ms.
            MessageRejectedError: If broker rejects the message.
            PublisherOverflowError: If publishing faster than I/O capabilities.
            InvalidMessageError: If timeout_ms is negative or properties are invalid.
            DestinationNotFoundError: If topic doesn't exist on broker.
            MessageNotAcknowledgedError: If broker can't acknowledge persistence.
        """
        if not self._publisher:
            raise ClientError("Publisher is not started. Call .start() first.")
        if timeout_ms < 0:
            raise PublishError("Timeout cannot be negative")

        try:
            # Merge default and additional properties
            merged_props = {**self._default_properties, **(additional_message_properties or {})}
            msg_id = message_id or str(uuid.uuid4())
            
            # Build message
            message = (self._service.message_builder()
                      .with_application_message_id(msg_id)
                      .build(event.to_json()))
            
            self._publisher.publish_await_acknowledgement(
                message, 
                Topic.of(topic), 
                timeout_ms,
                additional_message_properties=merged_props
            )
            log.info(
                "Published+ACK event %s to '%s' msg_id=%s props=%s",
                event.id, topic, msg_id, merged_props,
            )
        except Exception as e:
            _translate_exception(e)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


class _AsyncPublishReceiptListener(MessagePublishReceiptListener):
    """
    Internal async listener bridging Solace PublishReceipt -> PublishReceiptModel.
    """

    def __init__(
        self,
        async_callback: t.Callable[[PublishReceiptModel], t.Awaitable[None]],
        event_loop,
    ):
        self.async_callback: t.Callable[[PublishReceiptModel], t.Awaitable[None]] = (
            async_callback
        )
        self.event_loop: asyncio.AbstractEventLoop = event_loop
        self._receipt_count = 0

    @property
    def receipt_count(self) -> int:
        return self._receipt_count

    def on_publish_receipt(self, receipt: PublishReceipt):
        self._receipt_count += 1
        try:
            model = PublishReceiptModel.from_solace(receipt)
            # Bridge from Solace thread to async world
            asyncio.run_coroutine_threadsafe(
                self.async_callback(model), self.event_loop
            )
        except Exception as e:
            log.error("Failed to process async publish receipt: %s", e, exc_info=True)


class AsyncPersistentPublisher:
    """
    Async-friendly persistent message publisher for Solace.
    
    Provides guaranteed delivery messaging with async lifecycle management.

    Args:
        client: A connected SolaceClient instance.
        default_properties: Properties applied to all messages by default.
        on_receipt: Optional async Callable(receipt) to handle publish receipts.
    """

    def __init__(
        self,
        client,
        default_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        on_receipt: t.Callable[[PublishReceiptModel], t.Awaitable[None]] | None = None,
    ):
        if not client._connected or not client._service:
            raise PublishError("Solace client is not connected.")

        self._client = client
        self._service = client.get_messaging_service()
        self._publisher: PersistentMessagePublisher | None = None
        self._default_properties = default_properties or {}
        self._on_receipt = on_receipt or self._default_receipt_handler
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._receipt_listener: _AsyncPublishReceiptListener | None = None

    async def _default_receipt_handler(self, receipt: PublishReceiptModel):
        """Default handler: just log the receipt."""
        log.info("Async persistent publish receipt: %s", receipt.json())

    @property
    def receipt_count(self) -> int:
        """
        Get the total number of publish receipts received since publisher started.
        
        This includes both successful and failed publish attempts. Use this
        for monitoring delivery confirmation rates and publisher health.
        
        Returns:
            int: Total receipt count, or 0 if publisher hasn't been started.
            
        Example:
            await publisher.publish("topic", event1)
            await publisher.publish("topic", event2)
            # ... wait for receipts ...
            print(f"Received {publisher.receipt_count} confirmations")
        """
        return self._receipt_listener.receipt_count if self._receipt_listener else 0

    @retry_on_failure
    async def start(self):
        """
        Start the persistent message publisher.
        
        Creates and starts the underlying Solace publisher, then attaches
        the receipt listener to handle delivery confirmations.
        
        Must be called before publishing messages.
        
        Raises:
            ClientError: If the publisher fails to start.
        """
        self._event_loop = asyncio.get_running_loop()

        try:
            self._publisher = (
                self._service.create_persistent_message_publisher_builder().build()
            )
            # Start the publisher FIRST
            await self._event_loop.run_in_executor(None, self._publisher.start)
            
            # THEN set the receipt listener (after publisher is started)
            self._receipt_listener = _AsyncPublishReceiptListener(
                self._on_receipt, self._event_loop
            )
            self._publisher.set_message_publish_receipt_listener(self._receipt_listener)
            
            log.info("Async persistent publisher started.")
        except Exception as e:
            _translate_exception(e)

    @retry_on_failure
    async def stop(self):
        """
        Stop the persistent message publisher and clean up resources.
        
        Terminates the underlying Solace publisher and clears all listeners.
        Safe to call multiple times.
        
        Raises:
            ClientError: If the publisher fails to stop cleanly.
        """
        if self._publisher:
            try:
                await self._event_loop.run_in_executor(None, self._publisher.terminate)
                self._publisher = None
                self._receipt_listener = None
                log.info("Async persistent publisher stopped.")
            except Exception as e:
                _translate_exception(e)

    @retry_on_failure
    async def publish(
        self,
        topic: str,
        event: BaseEvent,
        additional_message_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        message_id: str | None = None,
        user_context: t.Any = None,
    ):
        """
        Publish a BaseEvent with guaranteed delivery to a Solace topic.

        This method returns immediately after sending the message to Solace.
        Delivery confirmations arrive asynchronously via the on_receipt callback.

        Args:
            topic: The Solace topic to publish to (e.g., "orders/created").
            event: The BaseEvent (or subclass) containing the message payload.
            additional_message_properties: Optional message properties (merged with default_properties).
                                         These become message metadata visible to subscribers.
            message_id: Optional application message ID. If None, a UUID is generated.
            user_context: Optional data passed back in the receipt callback.
                         Use this to correlate receipts with your application state.

        Example:
            await publisher.publish(
                topic="orders/created",
                event=OrderCreatedEvent(order_id="123", amount=99.99),
                additional_message_properties={"priority": "high", "trace_id": "abc123"},
                user_context={"order_id": "123", "action": "ship"}
            )

        Raises:
            ClientError: If publisher is not started.
            PublisherOverflowError: If publishing faster than I/O capabilities.
            MessageRejectedError: If broker rejects the message.
            InvalidMessageError: If message properties are invalid.
            DestinationNotFoundError: If topic doesn't exist on broker.
        """
        if not self._publisher:
            raise ClientError("Publisher is not started. Call .start() first.")

        try:
            # Merge default and additional properties
            merged_props = {**self._default_properties, **(additional_message_properties or {})}
            msg_id = message_id or str(uuid.uuid4())
            
            # Build message
            message = (self._service.message_builder()
                      .with_application_message_id(msg_id)
                      .build(event.to_json()))
            
            # No need to run in executor here
            # Peristent publishers are non-blocking by design 
            self._publisher.publish(
                message, 
                Topic.of(topic), 
                user_context, 
                additional_message_properties=merged_props
            )
            log.info(
                "Published async event %s to '%s' msg_id=%s props=%s",
                event.id, topic, msg_id, merged_props,
            )
        except Exception as e:
            _translate_exception(e)

    @retry_on_failure
    async def publish_await_acknowledgement(
        self,
        topic: str,
        event: BaseEvent,
        timeout_ms: int = 5000,
        additional_message_properties: dict[str, t.Union[str, int, float, bool]] | None = None,
        message_id: str | None = None,
    ):
        """
        Publish a BaseEvent and block until broker acknowledgment is received.

        This method blocks the calling coroutine until the Solace broker confirms
        it has received and persisted the message, or until the timeout expires.
        Use this when you need immediate confirmation before proceeding.

        Args:
            topic: The Solace topic to publish to (e.g., "orders/created").
            event: The BaseEvent (or subclass) containing the message payload.
            timeout_ms: Maximum time in milliseconds to wait for acknowledgment.
                        Default is 5000ms (5 seconds).
            additional_message_properties: Optional message properties (merged with default_properties).
            message_id: Optional application message ID. If None, a UUID is generated.

        Raises:
            ClientError: If publisher is not started.
            SolaceTimeoutError: If no acknowledgment received within timeout_ms.
            MessageRejectedError: If broker rejects the message.
            PublisherOverflowError: If publishing faster than I/O capabilities.
            InvalidMessageError: If timeout_ms is negative or properties are invalid.
            DestinationNotFoundError: If topic doesn't exist on broker.
            MessageNotAcknowledgedError: If broker can't acknowledge persistence.
        """
        if not self._publisher or not self._event_loop:
            raise ClientError("Publisher is not started. Call .start() first.")
        if timeout_ms < 0:
            raise PublishError("Timeout cannot be negative")

        try:
            # Merge default and additional properties
            merged_props = {**self._default_properties, **(additional_message_properties or {})}
            msg_id = message_id or str(uuid.uuid4())
            
            # Build message
            message = (self._service.message_builder()
                      .with_application_message_id(msg_id)
                      .build(event.to_json()))
            
            # Run the blocking publish in executor
            publisher = self._publisher  # Type narrowing for mypy
            await self._event_loop.run_in_executor(
                None,
                lambda: publisher.publish_await_acknowledgement(
                    message, 
                    Topic.of(topic), 
                    timeout_ms,
                    additional_message_properties=merged_props
                )
            )
            log.info(
                "Published+ACK async event %s to '%s' msg_id=%s props=%s",
                event.id, topic, msg_id, merged_props,
            )
        except Exception as e:
            _translate_exception(e)

    # Context manager support
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()