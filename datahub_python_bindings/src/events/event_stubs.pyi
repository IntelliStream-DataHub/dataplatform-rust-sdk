import datetime
import uuid
from typing import Mapping, Optional

from _pytest._code import source


class Event:
    """
    Event with related metadata.

    Attributes
    ----------
    id : uuid.UUID
        Unique identifier for the event.
    external_id : str
        A user defined identifier for the Event, should be unique for event chain.
    name : str
        Name of the event.
    metadata : dict[str, str]
        A Key-value store of metadata related to the event.
    description : str
        A user defined description of the event.
    type : str
        The type of event.
    sub_type : str
        The sub_type of event.
    status : str
        The current status of the event.
    data_set_id : int
        Identity of the datasets this id is part of.
    related_resource_ids : list[int]
        Id of the resources that are related to this Event.
    related_resource_external_ids : list[str]
        External Id of the resources that are related to this Event.
    source : str
        Source of the Event.
    event_time : datetime.datetime
        The time at which the event occurred, distinct from the created and last_updated auditing timestamps.

    Parameters
    ----------
    external_id : str
        A user defined identifier for the Event, should be unique for event chain.
        Updates to events should be stored as new events with the same external_id.
    name : str
        Name of the event.
    metadata : dict[str, str]
        A Key-value store of metadata related to the event.
    description : str
        A user defined description of the event.
    type : str
        The type of event.
    sub_type : str
        The sub_type of event.
    status : str
        The current status of the event.
    data_set_id : int
        Identity of the datasets this id is part of.
    related_resource_ids : list[int]
        Id of the resources that are related to this Event.
    related_resource_external_ids : list[str]
        External Id of the resources that are related to this Event.
    source : str
        Source of the Event.
    event_time : datetime.datetime
        The time at which the event occurred, distinct from the created and last_updated auditing timestamps.
    """
    id: uuid.UUID
    external_id: str
    name: str
    metadata: dict[str, str]
    description: str
    type: str
    sub_type: str
    status: str
    data_set_id: int
    related_resource_ids: list[int]
    related_resource_external_ids: list[str]
    source: str
    event_time: datetime.datetime

    def __init__(
            self,
            external_id: str,
            name: str,
            metadata: dict[str, str],
            description: str,
            type: str,
            sub_type: str,
            status: str,
            data_set_id: int,
            related_resource_ids: list[int],
            related_resource_external_ids: list[str],
            source: str,
            event_time: datetime.datetime,

    ): ...


class EventId:
    """
    Event Id Container.

    In difference to all the other Datahub entities Events have a UUID as their primary key instead of an integer.
    Contains the id and/or external Id of an event.

    Attributes
    ----------
    id : uuid.UUID
        Unique identifier for the event.
    external_id : str
        External identifier for the event.
    """
    id: uuid.UUID
    external_id: str


class AdvancedEventFilter:
    """
    Advanced Event Filter object.

    Notes
    -----
    Not implemented yet.
    """
    pass


class EventFilter:
    """
    Event Filter object for querying events.

    Attributes
    ----------
    basic_filter : BasicEventFilter
        Basic filtering criteria for events.
    limit : int
        Maximum number of events to return.
    cursor : str
        Cursor for pagination.
    advanced_filter : AdvancedEventFilter
        Advanced filtering criteria for events.

    Parameters
    ----------
    basic_filter : BasicEventFilter, optional
        Basic filtering criteria for events.
    limit : int, optional
        Maximum number of events to return.
    advanced_filter : AdvancedEventFilter, optional
        Advanced filtering criteria for events.
    """

    basic_filter: BasicEventFilter
    limit: int
    cursor: str
    advanced_filter: AdvancedEventFilter

    def __new__(
            cls,
            basic_filter: Optional[BasicEventFilter],
            limit: Optional[int],
            advanced_filter: Optional[AdvancedEventFilter]
    ): ...


class BasicEventFilter:
    """
    Basic Event Filter for querying events.

    Parameters
    ----------
    external_id_prefix : str, optional
        Filter events by external_id prefix.
    description : str, optional
        Filter events by description.
    source : str, optional
        Filter events by source.
    type : str, optional
        Filter events by type.
    sub_type : str, optional
        Filter events by sub_type.
    event_time : TimeFilter, optional
        Filter events by event time range.
    metadata : Mapping, optional
        Filter events by metadata key-value pairs.
    related_resource_ids : list[int], optional
        Filter events by related resource ids.
    related_resource_external_ids : list[str], optional
        Filter events by related resource external ids.
    created_time : datetime.datetime, optional
        Filter events by creation time.
    last_updated_time : datetime.datetime, optional
        Filter events by last updated time.
    """

    def __new__(
            cls,
            external_id_prefix: Optional[str],
            description: Optional[str],
            source: Optional[str],
            type: Optional[str],
            sub_type: Optional[str],
            event_time: Optional[TimeFilter],
            metadata: Optional[Mapping],
            related_resource_ids: Optional[list[int]],
            related_resource_external_ids: Optional[list[str]],
            created_time: Optional[datetime.datetime],
            last_updated_time: Optional[datetime.datetime],
    ): ...

class TimeFilter:
    """
    Time Filter for filtering by time range.

    Parameters
    ----------
    start : datetime.datetime, optional
        Start time of the filter range (inclusive).
    end : datetime.datetime, optional
        End time of the filter range (exclusive).
    """

    def __new__(cls,
                start: Optional[datetime.datetime],
        end: Optional[datetime.datetime],
    ): ...


class EventsServiceAsync:
    """
    Asynchronous Event service for CRD operations for events.

    In datahub Events are not updated, if an event needs to be changed you should create a new event instead.
    Example: if an Event represents a step in a process, let the External Id represent the process instance.

    Methods
    -------
    create
        Save events to the DataHub Data platform.
    delete
        Delete events from the DataHub Data platform.
    filter
        Filter events from the DataHub Data platform.
    by_ids
        Retrieve events by their identifiers from the DataHub Data platform.
    """

    def create(self, input: list[Event]) -> list[Event]:
        """
        Save events to the DataHub Data platform.

        :param input:

        List of events to be created
        :return:
        List of events created
        """
    def delete(self, input: list[Event | EventId | str]) -> None:
        """
        Delete events from the DataHub Data platform.

        Parameters
        ----------
        input : list[Event | EventId | str]
            Accepts a list of events, event ids, or external ids.

        Returns
        -------
        None
        """

    def by_ids(self, input: list[Event | EventId | str]) -> list[Event]:
        """
        Retrieve events from the DataHub Data platform.

        Parameters
        ----------
        input : list[Event | EventId | str]
            Events, event ids, or external ids.

        Returns
        -------
        list[Event]
            List of events.
        """

    def filter(self, input: EventFilter) -> list[Event]:
        """
        Filter events from the DataHub Data platform.

        Parameters
        ----------
        input : EventFilter
            Event Filter criteria.

        Returns
        -------
        list[Event]
            Filtered list of events.
        """


class EventsServiceSync:
    """
    Synchronous Event service for CRD operations for events.

    In datahub Events are not updated, if an event needs to be changed you should create a new event instead.
    Example: if an Event represents a step in a process, let the External Id represent the process instance.

    Methods
    -------
    create
        Save events to the DataHub Data platform.
    delete
        Delete events from the DataHub Data platform.
    filter
        Filter events from the DataHub Data platform.
    by_ids
        Retrieve events by their identifiers from the DataHub Data platform.
    """

    def create(self, input: list[Event]) -> list[Event]:
        """
        Save events to the DataHub Data platform.

        Parameters
        ----------
        input : list[Event]
            List of events to be created.

        Returns
        -------
        list[Event]
            List of events created.
        """

    def delete(self, input: list[Event | EventId | str]) -> None:
        """
        Delete events to the DataHub Data platform.

        :param input:
            Accepts a list of events, event ids, or external ids

        :return:
        None

        """
    def by_ids(self, input: list[Event | EventId | str]) -> list[Event]:
        """
        Retrieve events from the DataHub Data platform.

        Parameters
        ----------
        input : list[Event | EventId | str]
            Events, event ids, or external ids.

        Returns
        -------
        list[Event]
            List of events with selected ids.
        """

    def filter(self, input: EventFilter) -> list[Event]:
        """
        Filter events from the DataHub Data platform.

        Parameters
        ----------
        input : EventFilter
            Event Filter criteria.

        Returns
        -------
        list[Event]
            Filtered list of events.
        """
