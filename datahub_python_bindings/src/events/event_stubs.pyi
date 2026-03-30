import datetime
import uuid
from typing import Mapping, Optional


class Event:
    id: uuid.UUID
    external_id: str
    name: str

    def __init__(
            self,
            id: str,
            external_id: str,
            name: str

    ): ...

class EventId:
    """
    Event Id Container

    In difference to all the other Datahub entities Events have a UUID as their primary key instead of an integer.



    """
    id: uuid.UUID
    external_id: str


class AdvancedEventFilter:
    pass


class EventFilter:
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

    def __new__(cls,
        start: Optional[datetime.datetime],
        end: Optional[datetime.datetime],
    ): ...


class EventsServiceAsync:
    """
    Asyncronous Event service for CRD operations for events.

    In datahub Events are not updated, if an event needs to be changed you should create a new event instead

    Example if an Event represents a step in a process. let the External Id represent the process instance and

    Endpoints:
        create:
        delete:
        filter:
        by_ids:

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
        Delete events to the DataHub Data platform.

        :param input:
            Accepts a list of events, event ids, or external ids

        :return:
        None

        """
    def by_ids(self, input: list[Event | EventId | str]) -> list[Event]:
        """
        Retrieve events from the DataHub Data platform.
        :param input:
        Events, event ids, or external ids
        :return:
        list of events
        """
    def filter(self, input: EventFilter) -> list[Event]:
        """
        Filter events from the DataHub Data platform.
        :param input:
            Event Filter
        :return:
        """


class EventsServiceSync:
    """
    Syncronous Event service for CRD operations for events.

    In datahub Events are not updated, if an event needs to be changed you should create a new event instead

    Example if an Event represents a step in a process. let the External Id represent the process instance and

    Endpoints:
        create:
        delete:
        filter:
        by_ids:

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
        Delete events to the DataHub Data platform.

        :param input:
            Accepts a list of events, event ids, or external ids

        :return:
        None

        """
    def by_ids(self, input: list[Event | EventId | str]) -> list[Event]:
        """
        Retrieve events from the DataHub Data platform.
        :param input: Event, EventID, External Id, UUID
        Events, event ids, or external ids
        :return:
            list of events with selected ids
        """
    def filter(self, input: EventFilter) -> list[Event]:
        """
        Filter events from the DataHub Data platform.
        :param input:
            Event Filter
        :return:
            Filtered list of events
        """