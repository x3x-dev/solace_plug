import pytest
import logging
import pytest_asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient
from solace_plug.exceptions import ClientError

log = logging.getLogger("solace_plug")

@pytest.fixture
def client():
    # try:
    #     with SolaceClient().session() as c:
    #         yield c
    # except ClientError as e:
    #     logging.error("Error connecting to Solace: %s", e)
    #     pytest.skip("Solace Broker not available")
    with SolaceClient().session() as c:
        yield c
   

@pytest_asyncio.fixture
async def async_client():
    # try:
    #     async with AsyncSolaceClient().session() as c:
    #         yield c
    # except ClientError as e:
    #     logging.error("Error connecting to Solace: %s", e)
    #     pytest.skip("Solace Broker not available")
       
    async with AsyncSolaceClient().session() as c:
        yield c
