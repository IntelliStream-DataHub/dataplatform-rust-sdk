import datetime
from typing import AsyncIterator, Iterator, Optional, Union


class IdCollection:
    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> Optional[str]: ...


# Re-declared loosely so the typings don't pull in the full TimeSeries class.
class TimeSeries:
    @property
    def id(self) -> int: ...
    @property
    def external_id(self) -> str: ...


SubscriptionTimeseriesId = Union[TimeSeries, IdCollection, str, int]
SubscriptionId = Union["Subscription", IdCollection, str, int]


class Subscription:
    """
    A DataHub subscription. Binds a name + external_id to a list of timeseries; once created,
    datapoint writes against any of the bound timeseries are fanned out to subscribers connected
    to the listen WebSocket.

    Attributes
    ----------
    id : int | None
        Server-assigned numeric id (None until the subscription has been created).
    external_id : str
        User-supplied identifier; must be unique per subscription.
    name : str
        Human-readable name.
    timeseries : list[IdCollection]
        Timeseries this subscription is bound to.
    date_created : datetime.datetime | None
        Server-assigned creation timestamp.
    last_updated : datetime.datetime | None
        Server-assigned update timestamp.
    """
    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def timeseries(self) -> list[IdCollection]: ...
    @property
    def date_created(self) -> Optional[datetime.datetime]: ...
    @property
    def last_updated(self) -> Optional[datetime.datetime]: ...

    def __init__(
        self,
        external_id: str,
        name: str,
        timeseries: list[SubscriptionTimeseriesId],
    ) -> None: ...


class SubscriptionFilter:
    """Filter passed to SubscriptionsService.list — currently only filters by bound timeseries."""
    @property
    def timeseries(self) -> list[IdCollection]: ...
    def __init__(
        self,
        timeseries: Optional[list[SubscriptionTimeseriesId]] = None,
    ) -> None: ...


class DataSort:
    """Sort spec for SubscriptionRetriever — property/order/nulls map to backend sort fields."""
    @property
    def property(self) -> Optional[list[str]]: ...
    @property
    def order(self) -> Optional[str]: ...
    @property
    def nulls(self) -> Optional[str]: ...
    def __init__(
        self,
        property: Optional[list[str]] = None,
        order: Optional[str] = None,
        nulls: Optional[str] = None,
    ) -> None: ...


class SubscriptionRetriever:
    """
    Full retriever for SubscriptionsService.list. You can also call list() with kwargs
    (timeseries=, limit=, sort=) instead of building this explicitly, but not both.
    """
    @property
    def filter(self) -> SubscriptionFilter: ...
    @property
    def limit(self) -> int: ...
    @property
    def sort(self) -> DataSort: ...
    def __init__(
        self,
        filter: Optional[SubscriptionFilter] = None,
        limit: Optional[int] = None,
        sort: Optional[DataSort] = None,
    ) -> None: ...


# --- WebSocket message types ---

class EventAction:
    """Enum: CREATE | UPDATE | DELETE | RENAME"""
    Create: "EventAction"
    Update: "EventAction"
    Delete: "EventAction"
    Rename: "EventAction"


class EventObject:
    """Enum: LABEL | RELATION | RESOURCE | TIMESERIES | FUNCTION | EVENT | DATAPOINTS | RESOURCE_AND_RELATION"""
    Label: "EventObject"
    Relation: "EventObject"
    Resource: "EventObject"
    Timeseries: "EventObject"
    Function: "EventObject"
    Event: "EventObject"
    Datapoints: "EventObject"
    ResourceAndRelation: "EventObject"


class WsDatapoint:
    """A single datapoint delivered over the listen stream. The value is always a string so
    numeric and string-typed timeseries share one schema; use as_float() for numeric ones."""
    @property
    def timestamp(self) -> str: ...
    @property
    def value(self) -> str: ...
    def as_float(self) -> float:
        """Parse the value as a float. Raises ValueError if the value isn't numeric."""
        ...


class DataCollectionString:
    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> Optional[str]: ...
    @property
    def value_type(self) -> Optional[str]: ...
    @property
    def inclusive_begin(self) -> Optional[str]: ...
    @property
    def exclusive_end(self) -> Optional[str]: ...
    @property
    def datapoints(self) -> list[WsDatapoint]: ...


class DataWrapperMessage:
    @property
    def event_action(self) -> EventAction: ...
    @property
    def event_object(self) -> EventObject: ...
    @property
    def tenant_id(self) -> Optional[str]: ...
    @property
    def items(self) -> list[DataCollectionString]: ...


class SubscriptionMessage:
    """One message delivered by the backend. Echo message_id back through ack() / nack() so
    Pulsar tracks delivery; anything left unacked at close is redelivered to the next listener."""
    @property
    def message_id(self) -> str: ...
    @property
    def payload(self) -> DataWrapperMessage: ...


class SubscriptionListener:
    """
    Synchronous WebSocket listener. Drive it by iterating (`for msg in listener:`) — the loop
    blocks until the next message and exits cleanly when the server closes. Use `next_message()`
    instead if you'd rather treat a clean close as `None` than as StopIteration.

    The server pings every 15s and closes idle sessions after ~45s. Keep iterating frequently
    enough that pongs are flushed. Heavy per-message work belongs on another thread/task fed
    through a queue.
    """
    def __iter__(self) -> Iterator[SubscriptionMessage]: ...
    def __next__(self) -> SubscriptionMessage: ...
    def next_message(self) -> Optional[SubscriptionMessage]: ...
    def ack(self, message_ids: list[str]) -> None: ...
    def nack(self, message_ids: list[str]) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> "SubscriptionListener": ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...


class SubscriptionListenerAsync:
    """Asyncio variant. Use `async for msg in listener:` to drive it."""
    def __aiter__(self) -> AsyncIterator[SubscriptionMessage]: ...
    async def __anext__(self) -> SubscriptionMessage: ...
    async def next_message(self) -> Optional[SubscriptionMessage]: ...
    async def ack(self, message_ids: list[str]) -> None: ...
    async def nack(self, message_ids: list[str]) -> None: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> "SubscriptionListenerAsync": ...
    async def __aexit__(self, exc_type, exc_value, traceback) -> None: ...


class SubscriptionsServiceSync:
    """
    Synchronous CRUD + listen for subscriptions.

    `list()` accepts either a SubscriptionRetriever positionally or `timeseries`/`limit`/`sort`
    as kwargs — but not both. With neither, the default retriever is used.
    """
    def create(self, input: list[Subscription]) -> list[Subscription]: ...
    def list(
        self,
        retriever: Optional[SubscriptionRetriever] = None,
        *,
        timeseries: Optional[list[SubscriptionTimeseriesId]] = None,
        limit: Optional[int] = None,
        sort: Optional[DataSort] = None,
    ) -> list[Subscription]: ...
    def delete(self, input: list[SubscriptionId]) -> None: ...
    def listen(self, subscription_external_id: str) -> SubscriptionListener: ...


class SubscriptionsServiceAsync:
    """Asyncio variant of SubscriptionsServiceSync."""
    async def create(self, input: list[Subscription]) -> list[Subscription]: ...
    async def list(
        self,
        retriever: Optional[SubscriptionRetriever] = None,
        *,
        timeseries: Optional[list[SubscriptionTimeseriesId]] = None,
        limit: Optional[int] = None,
        sort: Optional[DataSort] = None,
    ) -> list[Subscription]: ...
    async def delete(self, input: list[SubscriptionId]) -> None: ...
    async def listen(self, subscription_external_id: str) -> SubscriptionListenerAsync: ...
