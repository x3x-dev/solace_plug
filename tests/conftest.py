import pytest
import logging
import pytest_asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient
from solace_plug.exceptions import ClientError
import time
import asyncio
log = logging.getLogger("solace_plug")

@pytest.fixture
def client():
    # try:
    #     with SolaceClient().session() as c:
    #         yield c
    # except ClientError as e:
    #     logging.error("Error connecting to Solace: %s", e)
    #     pytest.skip("Solace Broker not available")
    # Retry for 2 minutes

    timeout = time.time() + 120  # 2 minutes
    last_error = None

    while time.time() < timeout:
        try:
            c = SolaceClient()
            c.connect()
            yield c
            c.disconnect()
            return
        except ClientError as e:
            last_error = e
            logging.warning("Retrying connection: %s", e)
            time.sleep(1)

    pytest.skip(f"Solace Broker not available: {last_error}")

@pytest_asyncio.fixture
async def async_client():
    # try:
    #     async with AsyncSolaceClient().session() as c:
    #         yield c
    # except ClientError as e:
    #     logging.error("Error connecting to Solace: %s", e)
    #     pytest.skip("Solace Broker not available")
    # Retry for 2 minutes
    timeout = time.time() + 120  # 2 minutes
    last_error = None

    while time.time() < timeout:
        try:
            c = AsyncSolaceClient()
            await c.connect()
            yield c
            await c.disconnect()
            return
        except ClientError as e:
            last_error = e
            logging.warning("Retrying async connection: %s", e)
            await asyncio.sleep(1)

    pytest.skip(f"Solace Broker not available: {last_error}")
