import datetime
from typing import Sequence, Literal, Union, Optional, List

from datahub_sdk import FieldStr, ListFieldU64, IdCollection



class TimeSeries:
    """
    Represents a time series object in the data hub.

    Attributes
    ----------
    id : int or None
        The internal ID of the time series.
    external_id : str
        The external ID of the time series.
    name : str
        The name of the time series.
    metadata : dict[str, str] or None
        Metadata associated with the time series.
    description : str or None
        A description of the time series.
    unit : str or None
        The unit of measurement for the time series values.
    unit_external_id : str or None
        The external ID of the unit.
    security_categories : str or None
        Security categories for the time series.
    data_set_id : int or None
        The ID of the datasets this time series belongs to.
    relations_from : str or None
        Relations from this time series.
    created_time : datetime.datetime or None
        The timestamp when the time series was created.
    last_updated_time : datetime.datetime or None
        The timestamp when the time series was last updated.
    """

    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def metadata(self)-> Optional[dict[str,str]]: ...
    @property
    def description(self)-> Optional[str]: ...
    @property
    def unit(self)-> Optional[str]: ...
    @property
    def unit_external_id(self)-> Optional[str]: ...
    @property
    def security_categories(self)-> Optional[str]: ...
    @property
    def data_set_id(self)-> Optional[int]: ...
    @property
    def relations_from(self)-> Optional[str]: ...
    @property
    def created_time(self)-> Optional[datetime.datetime]: ...
    @property
    def last_updated_time(self)-> Optional[datetime.datetime]: ...
    def __init__(self,
        name: Optional[str],
        external_id: Optional[str],
        value_type: Optional[str],
        metadata: Optional[dict[str,str]],
        description: Optional[str],
        unit: Optional[str],
        unit_external_id: Optional[str],
        security_categories: Optional[list[int]],
        data_set_id: Optional[int],
        relations_from: Optional[str],
                 ):
        """
        Initialize a TimeSeries object.

        Parameters
        ----------
        name : str or None
            The name of the time series.
        external_id : str or None
            The external ID of the time series.
        value_type : str or None
            The type of values stored in the time series.
        metadata : dict[str, str] or None
            Metadata associated with the time series.
        description : str or None
            A description of the time series.
        unit : str or None
            The unit of measurement for the time series values.
        unit_external_id : str or None
            The external ID of the unit.
        security_categories : list[int] or None
            Security categories for the time series.
        data_set_id : int or None
            The ID of the datasets this time series belongs to.
        relations_from : str or None
            Relations from this time series.
        """
        ...


class SearchAndFilterForm:
    """
    Form for searching and filtering time series.

    This class is used to construct search and filter criteria for querying time series.
    """
    pass


class TimeSeriesServiceAsync:
    """
    Asynchronous service for managing time series operations.

    This service provides asynchronous methods for creating, updating, retrieving,
    and deleting time series, as well as managing their datapoints.
    """

    def create(self, items: Sequence[TimeSeries]) -> list[TimeSeries]:
        """
        Create new time series.

        Parameters
        ----------
        items : Sequence[TimeSeries]
            A sequence of TimeSeries objects to create.

        Returns
        -------
        list[TimeSeries]
            A list of the created TimeSeries objects.
        """
        ...

    def search(self,input: SearchAndFilterForm) -> list[TimeSeries]: ...
    def update(self,input: Sequence[TimeseriesUpdate])-> List[TimeSeries]:...
    def by_ids(self, inputs: Sequence[TimeSeries | IdCollection]) -> list[TimeSeries]: ...
    def delete(self, items: Sequence[TimeSeries | IdCollection]) -> None: ...
    def list(self) -> list[TimeSeries]: ...
    def insert_from_lists(self, timestamps: Sequence[datetime.datetime] ,values: Sequence[float|int]) -> list[str]: ...
    def retrieve_datapoints(self, filter: RetrieveFilter) -> list[DatapointsCollectionDatapoints]: ...
    def insert_datapoints(self,datapoints:DatapointsCollectionString)-> None: ...
    def retrieve_latest_datapoints(self, input: Sequence[Union[TimeSeries , IdCollection]]) -> list[DatapointsCollectionDatapoints]: ...
    def delete_datapoints(self, filter:Sequence[DeleteFilter])-> None: ...

class TimeSeriesServiceSync:
    """
    Synchronous service for managing time series operations.

    This service provides synchronous methods for creating, updating, retrieving,
    and deleting time series, as well as managing their datapoints.
    """

    def create(self, items: Sequence[TimeSeries]) -> list[TimeSeries]:
        """
        Create new time series.

        Parameters
        ----------
        items : Sequence[TimeSeries]
            A sequence of TimeSeries objects to create.

        Returns
        -------
        list[TimeSeries]
            A list of the created TimeSeries objects.
        """
        ...

    def search(self, input: SearchAndFilterForm) -> list[TimeSeries]:
        """
        Search for time series using a search form.

        Parameters
        ----------
        input : SearchAndFilterForm
            The search and filter criteria.

        Returns
        -------
        list[TimeSeries]
            A list of TimeSeries objects matching the search criteria.
        """
        ...

    def update(self, input: Sequence[TimeseriesUpdate]) -> List[TimeSeries]:
        """
        Update existing time series.

        Parameters
        ----------
        input : Sequence[TimeseriesUpdate]
            A sequence of TimeseriesUpdate objects specifying the updates.

        Returns
        -------
        List[TimeSeries]
            A list of the updated TimeSeries objects.
        """
        ...

    def by_ids(self, inputs: Sequence[TimeSeries | IdCollection]) -> list[TimeSeries]:
        """
        Retrieve time series by their IDs.

        Parameters
        ----------
        inputs : Sequence[TimeSeries | IdCollection]
            A sequence of TimeSeries objects or IdCollection objects identifying the time series.

        Returns
        -------
        list[TimeSeries]
            A list of the retrieved TimeSeries objects.
        """
        ...

    def delete(self, items: Sequence[TimeSeries | IdCollection]) -> None:
        """
        Delete time series.

        Parameters
        ----------
        items : Sequence[TimeSeries | IdCollection]
            A sequence of TimeSeries objects or IdCollection objects identifying the time series to delete.

        Returns
        -------
        None
        """
        ...

    def list(self) -> list[TimeSeries]:
        """
        List all time series.

        Returns
        -------
        list[TimeSeries]
            A list of all TimeSeries objects.
        """
        ...

    def insert_from_lists(self, timestamps: Sequence[datetime.datetime], values: Sequence[float | int]) -> list[str]:
        """
        Insert datapoints from lists of timestamps and values.

        Parameters
        ----------
        timestamps : Sequence[datetime.datetime]
            A sequence of timestamps for the datapoints.
        values : Sequence[float | int]
            A sequence of values corresponding to the timestamps.

        Returns
        -------
        list[str]
            A list of strings indicating the result of the insertion.
        """
        ...

    def retrieve_datapoints(self, filter: RetrieveFilter) -> list[DatapointsCollectionDatapoints]:
        """
        Retrieve datapoints based on a filter.

        Parameters
        ----------
        filter : RetrieveFilter
            The filter criteria for retrieving datapoints.

        Returns
        -------
        list[DatapointsCollectionDatapoints]
            A list of DatapointsCollectionDatapoints objects containing the retrieved datapoints.
        """
        ...

    def insert_datapoints(self, datapoints: DatapointsCollectionString) -> None:
        """
        Insert datapoints into a time series.

        Parameters
        ----------
        datapoints : DatapointsCollectionString
            A collection of datapoints to insert.

        Returns
        -------
        None
        """
        ...

    def retrieve_latest_datapoints(self, input: Sequence[Union[TimeSeries, IdCollection]]) -> list[
        DatapointsCollectionDatapoints]:
        """
        Retrieve the latest datapoints for time series.

        Parameters
        ----------
        input : Sequence[Union[TimeSeries, IdCollection]]
            A sequence of TimeSeries objects or IdCollection objects identifying the time series.

        Returns
        -------
        list[DatapointsCollectionDatapoints]
            A list of DatapointsCollectionDatapoints objects containing the latest datapoints.
        """
        ...

    def delete_datapoints(self, filter: Sequence[DeleteFilter]) -> None:
        """
        Delete datapoints based on filters.

        Parameters
        ----------
        filter : Sequence[DeleteFilter]
            A sequence of DeleteFilter objects specifying which datapoints to delete.

        Returns
        -------
        None
        """
        ...


class Datapoint:
    """
    Represents a single datapoint in a time series.

    Attributes
    ----------
    timestamp : datetime.datetime or None
        The timestamp of the datapoint.
    value : float or None
        The value of the datapoint.
    min : float or None
        The minimum value (for aggregated datapoints).
    max : float or None
        The maximum value (for aggregated datapoints).
    average : float or None
        The average value (for aggregated datapoints).
    sum : float or None
        The sum value (for aggregated datapoints).
    """

    @property
    def timestamp(self)-> Optional[datetime.datetime]: ...
    @property
    def value(self)-> Optional[float]: ...
    @property
    def min(self)-> Optional[float]: ...
    @property
    def max(self)-> Optional[float]: ...
    @property
    def average(self)-> Optional[float]: ...
    @property
    def sum(self)-> Optional[float]: ...
    def __str__(self) -> str: ...
class DatapointsCollectionDatapoints:
    """
    A collection of datapoints for a time series.

    Attributes
    ----------
    id : int or None
        The internal ID of the time series.
    external_id : str or None
        The external ID of the time series.
    datapoints : Datapoint or None
        The datapoints in the collection.
    next_cursor : str
        The cursor for pagination.
    unit : str
        The unit of measurement for the datapoints.
    unit_external_id : str
        The external ID of the unit.
    """

    def get_datapoints(self) -> dict[str, Sequence[datetime.datetime | float]]:
        """
        Get datapoints as a dictionary.

        Returns
        -------
        dict[str, Sequence[datetime.datetime | float]]
            A dictionary containing the datapoints with timestamps and values.
        """
        ...

    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> Optional[str]: ...
    @property
    def datapoints(self) -> Optional[Datapoint]: ...
    @property
    def next_cursor(self) -> str: ...
    @property
    def unit(self) -> str: ...
    @property
    def unit_external_id(self) -> str: ...

class DatapointString:
    """
    Represents a datapoint with string-formatted timestamp and value.

    Attributes
    ----------
    timestamp : str
        The timestamp of the datapoint as a string.
    value : str
        The value of the datapoint as a string.
    """
    timestamp: str
    value: str

    def __init__(self, timestamp: str, value: str):
        """
        Initialize a DatapointString object.

        Parameters
        ----------
        timestamp : str
            The timestamp of the datapoint as a string.
        value : str
            The value of the datapoint as a string.
        """
        ...


class DatapointsCollectionString:
    """
    A collection of datapoints with string-formatted values.

    Attributes
    ----------
    id : int or None
        The internal ID of the time series.
    external_id : str or None
        The external ID of the time series.
    datapoints : Datapoint or None
        The datapoints in the collection.
    next_cursor : str
        The cursor for pagination.
    unit : str
        The unit of measurement for the datapoints.
    unit_external_id : str
        The external ID of the unit.
    """

    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> Optional[str]: ...
    @property
    def datapoints(self) -> Optional[Datapoint]: ...
    @property
    def next_cursor(self) -> str: ...
    @property
    def unit(self) -> str: ...
    @property
    def unit_external_id(self) -> str: ...
    def __init__(
            self,
            datapoints: Sequence[DatapointString],
            ts: Union[TimeSeries,IdCollection]
    ):
        """
        Initialize a DatapointsCollectionString object.

        Parameters
        ----------
        datapoints : Sequence[DatapointString]
            A sequence of DatapointString objects.
        ts : Union[TimeSeries, IdCollection]
            The TimeSeries or IdCollection this collection belongs to.
        """
        ...


class DeleteFilter:
    """
    Filter for deleting datapoints from a time series.

    Attributes
    ----------
    ts : TimeSeries or IdCollection
        The time series to delete datapoints from.
    inclusive_begin : datetime.datetime
        The inclusive start of the time range.
    exclusive_end : datetime.datetime
        The exclusive end of the time range.
    """
    ts: TimeSeries | IdCollection
    inclusive_begin: datetime.datetime
    exclusive_end: datetime.datetime

    def __init__(
            self,
            ts: TimeSeries | IdCollection,
            inclusive_begin: datetime.datetime,
            exclusive_end: datetime.datetime
    ):
        """
        Initialize a DeleteFilter object.

        Parameters
        ----------
        ts : TimeSeries or IdCollection
            The time series to delete datapoints from.
        inclusive_begin : datetime.datetime
            The inclusive start of the time range.
        exclusive_end : datetime.datetime
            The exclusive end of the time range.
        """
        ...


class RetrieveFilter:
    """
    Filter for retrieving datapoints from a time series.

    Attributes
    ----------
    ts : TimeSeries or IdCollection
        The time series to retrieve datapoints from.
    start : datetime.datetime
        The start of the time range.
    end : datetime.datetime
        The end of the time range.
    limit : int
        The maximum number of datapoints to retrieve.
    aggregates : str
        The aggregation functions to apply.
    granularity : str
        The granularity for aggregation.
    cursor : str
        The cursor for pagination.
    """
    ts: TimeSeries | IdCollection
    start: datetime.datetime
    end: datetime.datetime
    limit: int
    aggregates: str
    granularity: str
    cursor: str

    def __init__(
            self,
            ts: TimeSeries | IdCollection,
            start: datetime.datetime | None = None,
            end: datetime.datetime | None = None,
            limit: int | None = None,
            aggregates: str | None = None,
            granularity: str | None = None,
            cursor: str | None = None,
    ):
        """
        Initialize a RetrieveFilter object.

        Parameters
        ----------
        ts : TimeSeries or IdCollection
            The time series to retrieve datapoints from.
        start : datetime.datetime or None, optional
            The start of the time range.
        end : datetime.datetime or None, optional
            The end of the time range.
        limit : int or None, optional
            The maximum number of datapoints to retrieve.
        aggregates : str or None, optional
            The aggregation functions to apply (e.g., "average,min,max").
        granularity : str or None, optional
            The granularity for aggregation (e.g., "1h", "1d").
        cursor : str or None, optional
            The cursor for pagination.
        """
        ...


class TimeseriesUpdate:
    """
    Update request for time series.

    Attributes
    ----------
    external_id : str or None
        The external ID to update.
    name : str or None
        The name to update.
    metadata : dict[str, str] or None
        The metadata to update.
    unit : str or None
        The unit to update.
    description : str or None
        The description to update.
    unit_external_id : str or None
        The unit external ID to update.
    security_categories : Sequence[int] or None
        The security categories to update.
    data_set_id : int or None
        The data set ID to update.
    relations_from : Sequence[int] or None
        The relations to update.
    is_string : bool or None
        Whether the time series stores string values.
    is_step : bool or None
        Whether the time series is a step series.
    value_type : Literal["BIGINT", "DESCIMAL", "TEXT"]
        The type of values stored in the time series.
    mode : Literal["replace_ignore_nulls", "replace"]
        The update mode.
    """
    @property
    def external_id(self)-> str | None: ...
    @property
    def name(self)-> str | None: ...
    @property
    def metadata(self)-> dict[str,str] |None: ...
    @property
    def unit(self)-> str |None: ...
    @property
    def description(self)-> str | None: ...
    @property
    def unit_external_id(self)-> str | None: ...
    @property
    def security_categories(self)-> Sequence[int] | None: ...
    @property
    def data_set_id(self)-> int | None: ...
    @property
    def relations_from(self)-> Sequence[int] |None: ...
    @property
    def is_string(self)-> bool | None: ...
    @property
    def is_step(self)-> bool | None: ...
    @property
    def value_type(self)-> Literal["BIGINT", "DESCIMAL", "TEXT"]: ...
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
    ):
        """
        Initialize a TimeseriesUpdate object.

        Parameters
        ----------
        ts : TimeSeries or IdCollection
            The time series to update.
        external_id : str or None
            The external ID to update.
        name : str or None
            The name to update.
        metadata : dict[str, str] or None
            The metadata to update.
        unit : str or None
            The unit to update.
        description : str or None
            The description to update.
        unit_external_id : str or None
            The unit external ID to update.
        security_categories : Sequence[int] or None
            The security categories to update.
        data_set_id : int or None
            The data set ID to update.
        relations_from : Sequence[int] or None
            The relations to update.
        is_string : bool or None
            Whether the time series stores string values.
        is_step : bool or None
            Whether the time series is a step series.
        value_type : Literal["BIGINT", "DESCIMAL", "TEXT"]
            The type of values stored in the time series.
        mode : Literal["replace_ignore_nulls", "replace"]
            The update mode; "replace_ignore_nulls" updates only non-null fields,
            "replace" updates all specified fields.
        """
        ...
