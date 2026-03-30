from datetime import datetime
from time import sleep

import datahub_sdk
import numpy as np
import pandas as pd
import pytest

@pytest.fixture(scope="module")
def async_client():
    # Create an AsyncClient instance from your env
    client = datahub_sdk.AsyncDataHubClient.from_envfile("/home/jgjesdal/RustroverProjects/dataplatform-rust-sdk/.env")
    yield client

@pytest.fixture(scope="module")
def sync_client():
    client = datahub_sdk.DataHubClient.from_envfile("/home/jgjesdal/RustroverProjects/dataplatform-rust-sdk/.env")
    yield client


@pytest.fixture(scope="module")
def ts_decimal(sync_client):
    ts = datahub_sdk.TimeSeries(
        external_id=f"test_decimal_{datetime.now().isoformat()}",
        name="test_decimal",
        value_type="decimal",
        unit ="a.u")
    sync_client.timeseries.delete([ts])
    created_ts = sync_client.timeseries.create([ts])

    return created_ts[0]
    pd.Timestamp('2023-01-01', tz='UTC')
@pytest.fixture(scope="module")
def ts_bigint(sync_client):
    ts = datahub_sdk.TimeSeries(
        external_id=f"test_bigint_{datetime.now().isoformat()}",
        name="test_bigint",
        value_type="bigint",
        unit ="a.u"
    )
    ts = datahub_sdk.TimeSeries(
        external_id="None",
        name=None,
        value_type="bigint",
        unit ="a.u"
    )
    sync_client.timeseries.delete([ts])
    created_ts = sync_client.timeseries.create([ts])
    yield created_ts[0]
    sync_client.timeseries.delete([ts])

@pytest.fixture(scope="module")
def test_data():
    dates = pd.date_range(start='2023-01-01', periods=100, freq='D',tz='UTC')
    # 2. Generate random "steps" (standard normal distribution)
    seed_value = 42 # Or any other integer
    rng = np.random.default_rng(seed_value)
    steps = rng.standard_normal(100)
    # 3. Create the walk by taking the cumulative sum
    walk = steps.cumsum()
    walk = np.arange(0,100).astype(float)

    # 4. Combine into a Series or DataFrame
    return pd.Series(walk, index=dates, name="Random Walk")

@pytest.fixture(scope="module")
def inserted_data(sync_client,test_data,ts_decimal):
    inserted_data = sync_client.timeseries.insert_from_lists(timestamps= test_data.index,values=test_data.values,ts=ts_decimal)
    sleep(0.5)
    yield inserted_data


@pytest.fixture(scope="function")
def fresh_inserted_data(sync_client,test_data,ts_decimal):
    inserted_data = sync_client.timeseries.insert_from_lists(timestamps= test_data.index,values=test_data.values,ts=ts_decimal)
    sleep(0.5)
    yield inserted_data
