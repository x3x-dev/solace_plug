
import pytest 
import asyncio
import time
import threading
from solace_plug.publishers.direct import AsyncDirectPublisher, DirectPublisher
from solace_plug.subscribers.direct import AsyncDirectSubscriber, DirectSubscriber
from solace_plug.schemas.base import BaseEvent


@pytest.mark.integration
def test_direct_publish_and_subscribe_flow(client):
    received = []
    event = threading.Event()

    def on_message(msg):
        received.append(msg)
        event.set()

    sub = DirectSubscriber(client, topics=["test"], on_message=on_message)
    pub = DirectPublisher(client)

    with pub, sub:
        pub.publish("test", BaseEvent(source="test", payload={"x": 1}))
        # block until callback fires or timeout
        assert event.wait(timeout=2)

    assert len(received) == 1
    assert received[0].event.source == "test"
    assert received[0].event.payload == {"x": 1}


@pytest.mark.integration
@pytest.mark.parametrize("topics, publish_topics, expected_count", [
    (["test", "test2"], ["test", "test2"], 2), # multiple topics
    (["orders/*"], ["orders/created", "orders/fulfilled"], 2), # wildcard
    (["orders/*"], ["users/created"], 0), # no match
])
def test_subscriber_handles_wildcard_and_multiple_topics(client, topics, publish_topics, expected_count):
    received = []

    def on_message(msg):
        received.append(msg)

    sub = DirectSubscriber(client, topics=topics, on_message=on_message)
    pub = DirectPublisher(client)
    with pub, sub:
        for publish_topic in publish_topics:
            pub.publish(publish_topic, BaseEvent(source="t", payload={"x": 1}))
        time.sleep(1)

    assert len(received) == expected_count


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_direct_publish_and_subscribe_flow(async_client):
    received = []
    event = asyncio.Event()

    async def on_message(msg):
        received.append(msg)
        event.set()

    subscriber = AsyncDirectSubscriber(async_client, topics=["test"], on_message=on_message)
    publisher = AsyncDirectPublisher(async_client)

    async with subscriber, publisher:
        await publisher.publish("test", BaseEvent(source="test", payload={"x": 1}))
        await asyncio.wait_for(event.wait(), timeout=2)
        
    assert len(received) == 1
    assert received[0].event.source == "test"
    assert received[0].event.payload == {"x": 1}
