import pytest
import pytest_asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient
from solace_plug.exceptions import SolaceConnectionError 

@pytest.fixture
def client():
    try:
        with SolaceClient().session() as c:
            yield c
    except SolaceConnectionError:
        pytest.skip("Solace Broker not available")


@pytest_asyncio.fixture
async def async_client():
    try:
        async with AsyncSolaceClient().session() as c:
            yield c
    except SolaceConnectionError:
        pytest.skip("Solace Broker not available")
    