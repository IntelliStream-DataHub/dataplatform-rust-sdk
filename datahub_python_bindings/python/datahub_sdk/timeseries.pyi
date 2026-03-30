import datetime
from typing import Sequence, Literal, Union

from datahub_sdk import FieldStr
from python.datahub_sdk import IdCollection, TimeSeries


class TimeSeriesServiceAsync:
    def create(self, items: list[TimeSeries]) -> list[TimeSeries]: ...
    def delete(self, items: list[TimeSeries | IdCollection]) -> None: ...
    def insert_from_lists(self, timestamps: Sequence[datetime.datetime] ,values: Sequence[float|int]) -> list[str]: ...
    def retrieve_datapoints(self, filter: RetrieveFilter) -> list[DatapointsCollectionDatapoints]: ...
    def insert_datapoints(self,datapoints:DatapointsCollectionString)-> None: ...

def delete_datapoints(self,filter:DeleteFilter)-> None: ...
class DatapointsCollectionDatapoints:
    def get_datapoints(self) -> dict[str,Sequence[ datetime.datetime | float]]: ...
class DatapointsCollectionString:
    def __init__(
            self,
            id:int,
            external_id:str,
            datapoints:Sequence[tuple[datetime.datetime,float]]): ...
class DeleteFilter:
    ts: TimeSeries | IdCollection
    inclusive_begin: datetime.datetime
    exclusive_end: datetime.datetime
    def __init__(
            self,
            ts: TimeSeries| IdCollection,
            inclusive_begin: datetime.datetime,
            exclusive_end: datetime.datetime
    ): ...

class RetrieveFilter:
    ts: TimeSeries| IdCollection
    start: datetime.datetime
    end: datetime.datetime
    limit: int
    aggregates: str
    granularity: str
    cursor: str
    def __init__(
            self,
            ts: TimeSeries| IdCollection,
            start: datetime.datetime| None = None,
            end: datetime.datetime| None = None,
            limit:int | None = None,
            aggregates: str | None = None,
            granularity: str | None = None,
            cursor: str | None = None,
    ): ...


class TimeseriesUpdate:
    """
    Update request for timeseries

    parameters
    ----------
    ts: Timeseries or IdCollection
        The Timeseries to update

    external_id: FieldStr | None
    name: FieldStr | None
    metadata: MapField | None
    unit: FieldStr | None
    description: FieldStr | None
    unit_external_id: FieldStr | None
    security_categories: ListField[int] | None
    data_set_id: FieldU64 | None

    """
    ts: Union[TimeSeries, IdCollection]
    external_id: FieldStr | None
    name: FieldStr |None
    metadata: dict[str,str] | None
    unit: FieldStr |None
    description: FieldStr | None
    unit_external_id: FieldStr | None
    security_categories: ListFieldU64 | None
    data_set_id: FieldInt | None
    relations_from: ListFieldU64 | None
    value_type: Literal["BIGINT", "DESCIMAL", "TEXT"] | None
    def __init__(
            ts: TimeSeries | IdCollection,
            external_id: str | None,
            name: str |None,
            metadata: dict[str,str] | None,
            unit: str |None,
            description: str | None,
            unit_external_id: str | None,
            security_categories: Sequence[int] | None,
            data_set_id: int | None,
            relations_from: Sequence[int] |None,
            is_string: bool | None,
            is_step: bool | None,
            value_type: Literal["BIGINT", "DESCIMAL", "TEXT"],
            mode: Literal["replace_ignore_nulls","replace"],
    ): ...
