import pytest
import asyncio
from solace_plug.client import SolaceClient, AsyncSolaceClient


@pytest.fixture
def client():
    with SolaceClient().session() as c:
        yield c

@pytest.fixture
async def async_client():
    async with AsyncSolaceClient().session() as c:
        yield c