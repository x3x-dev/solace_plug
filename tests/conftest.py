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
    c = None
    timeout = time.time() + 120 # 2 minutes
    while c is None and time.time() < timeout:
        try:
            with SolaceClient().session() as c:
                c = c
        except ClientError as e:
            logging.error("Error connecting to Solace: %s", e)
            time.sleep(1)
    
    if c is None:
        pytest.skip("Solace Broker not available")
    
    yield c


@pytest_asyncio.fixture
async def async_client():
    # try:
    #     async with AsyncSolaceClient().session() as c:
    #         yield c
    # except ClientError as e:
    #     logging.error("Error connecting to Solace: %s", e)
    #     pytest.skip("Solace Broker not available")
    # Retry for 2 minutes
    c = None
    timeout = time.time() + 120 # 2 minutes
    while c is None and time.time() < timeout:
        try:
            async with AsyncSolaceClient().session() as c:
                c = c
        except ClientError as e:
            logging.error("Error connecting to Solace: %s", e)
            time.sleep(1)
    
    if c is None:
        pytest.skip("Solace Broker not available")
    
    yield c
