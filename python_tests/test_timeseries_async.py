import asyncio

import numpy as np
import pytest
import datahub_sdk
import pandas as pd

from python_tests.fixtures import *


def test_async_client(async_client):
    assert async_client is not None
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start, end",
    [
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-31T00:00:01', tz='UTC')),
        (pd.Timestamp('2023-02-01', tz='UTC'), pd.Timestamp('2023-02-27T23:59:59', tz='UTC')),
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-02-28T00:00:01', tz='UTC')),
        (pd.Timestamp('2022-01-01', tz='UTC'), pd.Timestamp('2025-01-1', tz='UTC')),
    ]
)
async def test_retrieve_datapoints(async_client,start,end,inserted_data,ts_decimal,test_data):
    datapoints_filter = datahub_sdk.RetrieveFilter(start=start,end=end,ts= ts_decimal)
    datapoints = await async_client.timeseries.retrieve_datapoints(datapoints_filter)
    datapoints = datapoints[0].as_dict()
    s = pd.Series(datapoints["values"], index=datapoints["timestamps"])
    print(s)

    print(s.describe())
    assert datapoints
    assert np.allclose(s,test_data[start:end])

@pytest.mark.asyncio
async def test_create_timeseries(async_client,ts_decimal,ts_bigint):
    ts_list= [ts_bigint,ts_decimal]
    await async_client.timeseries.delete(ts_list)
    created_ts = await async_client.timeseries.create(ts_list)
    for created,original in zip(created_ts,ts_list):
        assert(created.external_id == original.external_id)
        assert(created.name == original.name)
        assert(created.value_type == original.value_type)

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start, end",
    [
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-30T00:00:00', tz='UTC')),
        (pd.Timestamp('2023-02-01', tz='UTC'), pd.Timestamp('2023-02-27T23:59:59', tz='UTC')),
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-28T00:00:01', tz='UTC')),
        (pd.Timestamp('2023-01-01', tz='+01:00'), pd.Timestamp('2023-01-28T00:00:01', tz='UTC')),
    ]
)
async def test_delete_datapoints(async_client,fresh_inserted_data,ts_decimal,start,end,test_data):
    #start = pd.Timestamp('2023-04-01', tz='UTC')
    #send = pd.Timestamp('2023-04-03', tz='UTC')
    delete_target = datahub_sdk.DeleteFilter(ts=ts_decimal, inclusive_begin=start,exclusive_end=end)
    async_client.timeseries.delete_datapoints([delete_target])
    #await asyncio.sleep(20)
    """ # commented out because it takes a while to delete datapoints making test flaky
     fix could be to create special test query with final
    datapoints_filter = datahub_sdk.RetrieveFilter(start=pd.Timestamp("2023-01-01",tz='UTC'),
                                                     end=pd.Timestamp("2023-05-01",tz='UTC'),
                                                     ts= ts_decimal)
    datapoints = await async_client.timeseries.retrieve_datapoints(datapoints_filter)
    datapoints = datapoints[0].get_datapoints()
    s = pd.Series(datapoints["values"], index=datapoints["timestamps"])
    to_drop = test_data[(test_data.index >= start) & (test_data.index < end)].index
    remaining = test_data.drop(to_drop)
    print(remaining)
    print(len(remaining),len(test_data),len(to_drop))
    assert np.allclose(s, remaining)
    """

@pytest.mark.asyncio
async def test_retrieve_latest_datapoint(async_client,inserted_data,test_data,ts_decimal):
    latest_datapoint = await async_client.timeseries.retrieve_latest_datapoints(input=[ts_decimal])
    latest_datapoint = latest_datapoint[0].as_dict()
    ts,val = test_data.tail(1).index[0], test_data.tail(1).values[0]
    assert latest_datapoint["values"][0] == val
    assert latest_datapoint["timestamps"][0] == ts



@pytest.mark.asyncio
@pytest.mark.parametrize(
    "timestamps,values,value_type",
    [
        (pd.date_range("2020-01-01",periods=100,tz="UTC"), pd.Series(np.random.randn(100),dtype="float64"), "decimal"),
        (pd.date_range("2020-01-01",periods=100,tz="UTC"), pd.Series(np.random.randint(100),dtype="int64"), "bigint"),
        (pd.date_range("2020-01-01",periods=100,tz="UTC"), pd.Series(np.random.randint(100),dtype="int64"), "decimal"),
    ]
)
async def test_insert(async_client,timestamps,values,value_type):
    test_insert_ts = datahub_sdk.TimeSeries(name="test insert",value_type=value_type,unit="a.u")
    await async_client.timeseries.delete([test_insert_ts])

    await async_client.timeseries.create([test_insert_ts])
    if value_type == "bigint":
        data = [datahub_sdk.DatapointString.from_int(ind,val) for ind,val in zip(timestamps,values)]
    elif value_type == "decimal":
        data = [datahub_sdk.DatapointString.from_float(ind,val) for ind,val in zip(timestamps,values)]

    vals=datahub_sdk.DatapointsCollectionString(datapoints=data,ts=test_insert_ts)
    inserted_datapoints = await async_client.timeseries.insert_datapoints(input=[vals])
    retrieved_datapoints = await async_client.timeseries.retrieve_datapoints(datahub_sdk.RetrieveFilter(
        start=pd.Timestamp("2019-01-01",tz="UTC"),
        end=pd.Timestamp("2025-01-01",tz="UTC"),
        ts=test_insert_ts))
    assert retrieved_datapoints





"""
TODO! determine what is invalid input and codify in tests
Bellow are draft tests for invalid input 

@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [{"vec": [0,1,2]},{"value_params": {"nested": {}}},{"nonstringable": print}])
async def test_reject_invalid_timeseries_metadata(async_client,metadata):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            metadata=metadata,
        )
        await async_client.timeseries.delete([test_insert_ts])

@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["valid name"])
async def test_reject_invalid_timeseries_name(async_client,name):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name=name,
        )
        await async_client.timeseries.delete([test_insert_ts])

@pytest.mark.asyncio
@pytest.mark.parametrize("external_id", [1,12,"a","ab"])
async def test_reject_invalid_timeseries_metadata(async_client,external_id):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            external_id=external_id,
        )
        await async_client.timeseries.delete([test_insert_ts])


@pytest.mark.asyncio
@pytest.mark.parametrize("value_type", ["",None,"big_int","strings","hex"])
async def test_reject_invalid_timeseries_metadata(async_client,value_type):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            value_type=value_type,
        )
        await async_client.timeseries.delete([test_insert_ts])


@pytest.mark.asyncio
@pytest.mark.parametrize("unit", ["",None,0],marks=pytest.mark.xfail(reason="TBD what are invalid units"))
@pytest.mark.parametrize("unit_external_id", ["",None,0],marks=pytest.mark.xfail(reason="TBD what are invalid units"))
async def test_reject_invalid_timeseries_unit(async_client,unit,unit_external_id):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            unit=unit,
            unit_external_id=unit_external_id,
        )
        await async_client.timeseries.delete([test_insert_ts])


"""