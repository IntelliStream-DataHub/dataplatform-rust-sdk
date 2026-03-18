from pickle import MARK

import pytest
import datahub_sdk



@pytest.fixture()
def async_client():
    # Create an AsyncClient instance from your env
    client = datahub_sdk.AsyncClient.from_env("/home/jgjesdal/RustroverProjects/dataplatform-rust-sdk/.env")
    yield client

def test_async_client(async_client):
    assert async_client is not None
    assert async_client.api_url is not None
    assert async_client.api_key is not None

class TestTimeseries:
    ts = datahub_sdk.PyTimeseries(
        external_id="test",
        name="test",
        value_type="BIGINT"
    )
    @pytest.mark.asyncio
    async def test_create_timeseries(self,async_client):
        created_ts = await async_client.create_timeseries(self.ts)
        assert(created_ts.external_id == self.ts.external_id)
        assert(created_ts.name == self.ts.name)
        assert(created_ts.value_type == self.ts.value_type)