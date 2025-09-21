import pytest
import logging
import pytest_asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient
from solace_plug.exceptions import SolaceConnectionError 

log = logging.getLogger("solace_plug")

@pytest.fixture
def client():
    try:
        with SolaceClient().session() as c:
            yield c
    except SolaceConnectionError as e:
        logging.error("Error connecting to Solace: %s", e)
        pytest.skip("Solace Broker not available")


@pytest_asyncio.fixture
async def async_client():
    try:
        async with AsyncSolaceClient().session() as c:
            yield c
    except SolaceConnectionError:
        logging.error("Error connecting to Solace: %s", e)
        pytest.skip("Solace Broker not available")
    