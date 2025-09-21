import pytest
import logging
import pytest_asyncio
import time
import asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient
from solace_plug.exceptions import ClientError

log = logging.getLogger("solace_plug")


@pytest.fixture
def client():
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
