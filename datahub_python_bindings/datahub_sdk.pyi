import datetime
import uuid
from typing import Sequence, Literal, Optional, Mapping


class TimeSeries:

    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str: ...
    @property
    def name(self) -> str: ...


class IdCollection:
    @property
    def id(self)-> int: ...
    def external_id(self)-> str: ...

class EventServiceAsync:
    def by_ids(self,EventIdentifyable): ...

class AsyncDataHubClient:
    @property
    def timeseries(self) -> TimeSeriesServiceAsync: ...
    @property
    def events(self) -> EventServiceAsync: ...





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
    ts: TimeSeries | IdCollection
    external_id: str | None
    name: str |None
    metadata: dict[str,str] |None
    unit: str |None
    description: str | None
    unit_external_id: str | None
    security_categories: Sequence[int] | None
    data_set_id: int | None
    relations_from: Sequence[int] |None
    is_string: bool | None
    is_step: bool | None
    value_type: Literal["BIGINT", "DESCIMAL", "TEXT"]
    mode: Literal["replace_ignore_nulls","replace"]
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


class Unit:
    pass
class RetrieveFilter:
    pass

class FieldStr:
    set: str
    set_null: bool

class ListFieldU64:
    set: list[int]
    add: list[int]
    remove: list[int]

class MapField:

    set: dict[str,str]
    add: dict[str,str]
    remove: list[str]
    def set(self):

from src.events.event_stubs import *