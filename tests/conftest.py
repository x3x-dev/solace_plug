import pytest
import logging
import pytest_asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient
from solace_plug.exceptions import ClientError
import time

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
    for i in range(120):
        try:
            with SolaceClient().session() as c:
                yield c
        except ClientError as e:
            logging.error("Error connecting to Solace: %s", e)
            time.sleep(1)

   

@pytest_asyncio.fixture
async def async_client():
    # try:
    #     async with AsyncSolaceClient().session() as c:
    #         yield c
    # except ClientError as e:
    #     logging.error("Error connecting to Solace: %s", e)
    #     pytest.skip("Solace Broker not available")
    # Retry for 2 minutes
    for i in range(120):
        try:
            async with AsyncSolaceClient().session() as c:
                yield c
        except ClientError as e:
            logging.error("Error connecting to Solace: %s", e)
            time.sleep(1)
