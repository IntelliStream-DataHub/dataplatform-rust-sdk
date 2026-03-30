import asyncio
from datetime import tzinfo, timezone, timedelta

import numpy as np
import pytest
import datahub_sdk
import pandas as pd
import polars as pl
from python_tests.fixtures import *


def test_sync_client(sync_client):
    assert sync_client is not None
@pytest.mark.parametrize(
    "start, end",
    [
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-31T00:00:01', tz='UTC')),
        (pd.Timestamp('2023-02-01', tz='UTC'), pd.Timestamp('2023-02-27T23:59:59', tz='UTC')),
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-02-28T00:00:01', tz='UTC')),
        (pd.Timestamp('2022-01-01', tz='UTC'), pd.Timestamp('2025-01-1', tz='UTC')),
    ]
)
def test_retrieve_datapoints(sync_client,start,end,inserted_data,ts_decimal,test_data):
    datapoints_filter = datahub_sdk.RetrieveFilter(start=start,end=end,ts= ts_decimal)
    datapoints =  sync_client.timeseries.retrieve_datapoints(datapoints_filter)
    datapoints = datapoints[0].as_dict()
    s = pd.Series(datapoints["values"], index=datapoints["timestamps"])
    print(s)

    print(s.describe())
    assert datapoints
    assert np.allclose(s,test_data[start:end])
def test_create_timeseries_invalid_value_type(sync_client):
    with pytest.raises(ValueError):
        invalid = datahub_sdk.TimeSeries(name="test insert",value_type="invalid_string",unit="a.u")
        sync_client.timeseries.create([invalid])

@pytest.mark.parametrize(
    "start, end",
    [
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-30T00:00:00', tz='UTC')),
        (pd.Timestamp('2023-02-01', tz='UTC'), pd.Timestamp('2023-02-27T23:59:59', tz='UTC')),
        (pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-28T00:00:01', tz='UTC')),
        (pd.Timestamp('2023-01-01', tz='+01:00'), pd.Timestamp('2023-01-28T00:00:01', tz='UTC')),
        (datetime(2023,1,1,tzinfo=timezone(timedelta(hours=1))), pd.Timestamp('2023-01-28T00:00:01', tz='UTC')),

    ]
)
def test_delete_datapoints(sync_client,fresh_inserted_data,ts_decimal,start,end,test_data):
    #start = pd.Timestamp('2023-04-01', tz='UTC')
    #send = pd.Timestamp('2023-04-03', tz='UTC')
    delete_target = datahub_sdk.DeleteFilter(ts=ts_decimal, inclusive_begin=start,exclusive_end=end)
    sync_client.timeseries.delete_datapoints([delete_target])
    # asyncio.sleep(20)
    """ # commented out because it takes a while to delete datapoints making test flaky
     fix could be to create special test query with final
    datapoints_filter = datahub_sdk.RetrieveFilter(start=pd.Timestamp("2023-01-01",tz='UTC'),
                                                     end=pd.Timestamp("2023-05-01",tz='UTC'),
                                                     ts= ts_decimal)
    datapoints =  sync_client.timeseries.retrieve_datapoints(datapoints_filter)
    datapoints = datapoints[0].get_datapoints()
    s = pd.Series(datapoints["values"], index=datapoints["timestamps"])
    to_drop = test_data[(test_data.index >= start) & (test_data.index < end)].index
    remaining = test_data.drop(to_drop)
    print(remaining)
    print(len(remaining),len(test_data),len(to_drop))
    assert np.allclose(s, remaining)
    """

def test_retrieve_latest_datapoint(sync_client,inserted_data,test_data,ts_decimal):
    latest_datapoint =  sync_client.timeseries.retrieve_latest_datapoints(input=[ts_decimal])
    latest_datapoint = latest_datapoint[0].as_dict()
    ts,val = test_data.tail(1).index[0], test_data.tail(1).values[0]
    assert latest_datapoint["values"][0] == val
    assert latest_datapoint["timestamps"][0] == ts



@pytest.mark.parametrize(
    "timestamps,values,value_type",
    [
        (pd.date_range("2020-01-01",periods=100,tz="UTC"), pd.Series(np.random.randn(100),dtype="float64"), "decimal"),
        (pd.date_range("1-09-21 ",periods=100,tz="UTC"), pd.Series(np.random.randint(100),dtype="int64"), "bigint"),
        (pd.date_range("2262-04-11",periods=100,tz="UTC"), pd.Series(np.random.randint(100),dtype="int64"), "decimal"),
    ]
)
def test_insert(sync_client,timestamps,values,value_type):
    test_insert_ts = datahub_sdk.TimeSeries(name="test insert",value_type=value_type,unit="a.u")
    sync_client.timeseries.delete([test_insert_ts])

    sync_client.timeseries.create([test_insert_ts])
    if value_type == "bigint":
        data = [datahub_sdk.DatapointString.from_int(ind,val) for ind,val in zip(timestamps,values)]
    elif value_type == "decimal":
        data = [datahub_sdk.DatapointString.from_float(ind,val) for ind,val in zip(timestamps,values)]

    vals=datahub_sdk.DatapointsCollectionString(datapoints=data,ts=test_insert_ts)
    inserted_datapoints = sync_client.timeseries.insert_datapoints(input=[vals])
    retrieved_datapoints = sync_client.timeseries.retrieve_datapoints(datahub_sdk.RetrieveFilter(
        start=pd.Timestamp("2019-01-01",tz="UTC"),
        end=pd.Timestamp("2025-01-01",tz="UTC"),
        ts=test_insert_ts))
    assert retrieved_datapoints

def test_invalid_retrieve_latest_datapoint(sync_client):
    with pytest.raises(IndexError):
        nonexistant_ts = datahub_sdk.TimeSeries(external_id="nonexistent_ts",value_type="bigint",unit="a.u")
        sync_client.timeseries.retrieve_latest_datapoints(input=[nonexistant_ts])[0]

"""
TODO! determine what is invalid input and codify in tests
Bellow are draft tests for invalid input 

@pytest.mark.parametrize("metadata", [{"vec": [0,1,2]},{"value_params": {"nested": {}}},{"nonstringable": print}])
def test_reject_invalid_timeseries_metadata(sync_client,metadata):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            metadata=metadata,
        )
         sync_client.timeseries.delete([test_insert_ts])

@pytest.mark.parametrize("name", ["valid name"])
def test_reject_invalid_timeseries_name(sync_client,name):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name=name,
        )
         sync_client.timeseries.delete([test_insert_ts])

@pytest.mark.parametrize("external_id", [1,12,"a","ab"])
def test_reject_invalid_timeseries_metadata(sync_client,external_id):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            external_id=external_id,
        )
         sync_client.timeseries.delete([test_insert_ts])


@pytest.mark.parametrize("value_type", ["",None,"big_int","strings","hex"])
def test_reject_invalid_timeseries_metadata(sync_client,value_type):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            value_type=value_type,
        )
         sync_client.timeseries.delete([test_insert_ts])


@pytest.mark.parametrize("unit", ["",None,0],marks=pytest.mark.xfail(reason="TBD what are invalid units"))
@pytest.mark.parametrize("unit_external_id", ["",None,0],marks=pytest.mark.xfail(reason="TBD what are invalid units"))
def test_reject_invalid_timeseries_unit(sync_client,unit,unit_external_id):
    with pytest.raises(ValueError):
        test_insert_ts = datahub_sdk.TimeSeries(
            name="valid name",
            unit=unit,
            unit_external_id=unit_external_id,
        )
         sync_client.timeseries.delete([test_insert_ts])


"""



def test_timeseries_update_with_fields(sync_client):
    # 1. Create a new unique TS for updating
    ext_id = f"test_update_{datetime.now().timestamp()}"
    ts = datahub_sdk.TimeSeries(
        name="Original Name",
        external_id=ext_id,
        value_type="bigint",
        unit="a.u"
    )
    sync_client.timeseries.delete([ts])
    created_ts = sync_client.timeseries.create([ts])[0]

    # 2. Prepare Update Fields using Field structs
    # Note: TimeSeriesUpdate.__init__ expects these types for specific fields
    new_name = datahub_sdk.FieldStr(value="Updated Name")
    new_unit = datahub_sdk.FieldStr(value="Updated Unit")
    new_metadata = datahub_sdk.MapField(add={"status": "updated", "version": "2"})

    # 3. Create the Update object
    # The first argument 'ts' is the Identifyable (the created_ts itself)
    ts_update = datahub_sdk.TimeSeriesUpdate(
        created_ts,
        name=new_name,
        unit=new_unit,
        metadata=new_metadata
    )

    # 4. Perform the update
    # The sync_client.timeseries.update likely takes a list of updates or objects
    updated_tss = sync_client.timeseries.update([ts_update])
    updated_ts = updated_tss[0]

    # 5. Assertions
    assert updated_ts.name == "Updated Name"
    assert updated_ts.unit == "Updated Unit"
    # Metadata in Datahub usually merges, verify the keys exist
    assert updated_ts.metadata["status"] == "updated"

    # Clean up
    sync_client.timeseries.delete([updated_ts])

def test_timeseries_update_set_null(sync_client):
    # Create TS with a description
    ext_id = f"test_null_{datetime.now().timestamp()}"
    ts = datahub_sdk.TimeSeries(
        name="Null Test",
        external_id=ext_id,
        description="I should be deleted",
        unit="a.u",
        value_type="text"
    )
    created_ts = sync_client.timeseries.create([ts])[0]

    # Use FieldStr with set_null=True to clear the description
    null_description = datahub_sdk.FieldStr(set_null=True)

    ts_update = datahub_sdk.TimeSeriesUpdate(
        created_ts,
        description=null_description
    )

    updated_ts = sync_client.timeseries.update([ts_update])[0]

    assert updated_ts.description is None

    # Clean up
    sync_client.timeseries.delete([updated_ts])