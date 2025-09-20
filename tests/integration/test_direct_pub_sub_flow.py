import pytest 
import asyncio
from solace_plug.client import SolaceClient
from solace_plug.publishers.direct import AsyncDirectPublisher
from solace_plug.subscribers.direct import AsyncDirectSubscriber
from solace_plug.schemas.base import BaseEvent



@pytest.mark.integration
def test_direct_pub_sub_flow():
    pass

@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_direct_pub_sub_flow():
    received_messages = []
    async def on_message(msg):
        received_messages.append(msg)


    with SolaceClient().session() as client:
        subscriber = AsyncDirectSubscriber(client, topics=["test"], on_message=on_message)
        publisher = AsyncDirectPublisher(client)

        async with subscriber, publisher:
            await publisher.publish("test", BaseEvent(source="test", payload={"test": "test"}))
            await asyncio.sleep(1)

            assert len(received_messages) == 1
            assert received_messages[0].event.source == "test"
            assert received_messages[0].event.payload == {"test": "test"}

