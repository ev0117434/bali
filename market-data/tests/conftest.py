import os
import sys
import pytest
import pytest_asyncio
import fakeredis.aioredis

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest_asyncio.fixture
async def redis():
    """Async fakeredis client — isolated per test."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()
