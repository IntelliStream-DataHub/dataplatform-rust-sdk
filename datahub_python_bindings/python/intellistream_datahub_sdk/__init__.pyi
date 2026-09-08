"""Type stubs for the intellistream_datahub_sdk pyo3 extension module.

The runtime is a single flat module: every class is exported at the top level.
This stub matches that structure; do not introduce submodules unless the Rust
registration in src/lib.rs::intellistream_datahub_sdk() also adds them.
"""

from __future__ import annotations

import datetime
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence, Union
from uuid import UUID


# One node of any type, as the /resources endpoints return them. Which class you get is decided
# by the node's own intrinsic type-label, so `isinstance(n, TimeSeries)` works and every object
# is the same class its own endpoint would hand back. `n.node_type` gives the name as a string
# when dispatching from data rather than by branching.
Node = Union["Asset", "TimeSeries", "Function", "Resource", "Dataset", "Policy"]

# Convenience alias: every entity-like input accepts either the entity itself,
# an IdCollection wrapper, a numeric id, or an external_id string.
Identifiable = Union["TimeSeries", "Resource", "Unit", "Event", "IdCollection", int, str]

# A filter's pattern list. `*` and `%` are wildcards, `_` is literal, matching is
# case-insensitive, and an entry with no wildcard matches exactly. Entries OR together.
# A bare string means the same as a one-element list.
PatternList = Union[str, Sequence[str]]

# Metadata criteria: every entry must be present on the matched row. A `None` value matches
# the key alone, whatever it carries.
MetadataFilter = Mapping[str, Optional[str]]

# How a filter names a data set: by numeric id, by external id, or by an explicit IdCollection.
DataSetRef = Union[int, str, "IdCollection"]

# A sort property, as one name or a one-element list. Only the first recognised entry is used.
SortBy = Union[str, Sequence[str]]


class Page(Sequence[Any]):
    """One page of a filter result: the rows, plus where to continue from.

    Behaves as a list — ``len()``, indexing, slicing, iteration, ``in`` and ``==`` against a plain
    list all work — so code written before paging existed keeps working. What it adds is
    ``next_cursor``, the only way to reach the next page: the api hands out an opaque cursor and
    does not accept one a caller assembled.

    The whole loop is::

        page = client.timeseries.filter(limit=100, sort_by="name")
        while True:
            for ts in page:
                ...
            if page.next_cursor is None:
                break
            page = client.timeseries.filter(
                limit=100, sort_by="name", cursor=page.next_cursor)

    ``next_cursor`` is ``None`` on the last page. A *full* page may still be the last one — the
    server does not count the rows twice — so a walk ends with one request that comes back empty.

    Not a ``list`` subclass, so ``isinstance(page, list)`` is ``False``; use ``page.items`` when
    something demands a real list.
    """
    @property
    def items(self) -> list[Any]: ...
    @property
    def next_cursor(self) -> str | None:
        """Send back as the next request's ``cursor``, with the same sort that produced it."""


# ====================== Errors ======================

class DataHubException(Exception):
    """Raised when the DataHub API returns an error. `status_code` is the HTTP
    status (e.g. 400, 409) and `message` is the raw response body, so callers can
    branch on the code:

        try:
            client.resources.create(nodes, relations)
        except DataHubException as e:
            if e.status_code == 409:
                ...  # already exists
            elif e.status_code == 400:
                print(e.message)
    """
    status_code: int
    message: str


# ====================== Clients ======================

class DataHubClient:
    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        token_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        project_name: str | None = None,
        enable_buffering: bool = False,
        buffer_retention_secs: int | None = None,
        buffer_max_bytes: int | None = None,
        buffer_dir: str | None = None,
        scope: str | None = None,
        audience: str | None = None,
        assertion: str | None = None,
        assertion_token_url: str | None = None,
        assertion_client_id: str | None = None,
        assertion_client_secret: str | None = None,
        assertion_scope: str | None = None,
        assertion_audience: str | None = None,
        assertion_grant: str | None = None,
    ) -> None:
        """Durable ingest buffering (off by default): when the API is unreachable, datapoint and
        event ingestion spools to disk and is flushed on a later call. Enable it with
        `enable_buffering=True` or by setting `buffer_retention_secs` / `buffer_max_bytes`
        (unset bounds default to 72h / 5 GiB). `buffer_dir` defaults to `.datahub-spool`.
        `from_env`/`from_envfile` read ENABLE_BUFFERING / BUFFER_RETENTION_SECS /
        BUFFER_MAX_BYTES / BUFFER_DIR from the environment instead.

        `scope` and `audience` (env: SCOPE / AUDIENCE) are added to the token request only when
        set. Against a DataHub realm using Keycloak Organizations, `scope` is required: use
        `organization:*`, or `organization:<alias>` to pin one tenant. That claim comes from a
        dynamic client scope, so without a selector the token carries no tenant and every call
        fails `401 invalid_token`. Not needed where the realm produces the `organization` claim
        with a protocol mapper. Entra ID instead requires `api://<app-id-uri>/.default`, Auth0
        requires an audience.

        Setting an assertion source switches the token request to the RFC 7523 `jwt-bearer`
        grant, which exchanges a JWT issued by one provider for a token from another — how an
        Entra ID service principal reaches a Keycloak-backed API. Either pass a ready-made
        `assertion` (env: ASSERTION), or all three of `assertion_client_id` /
        `assertion_client_secret` / `assertion_token_url` (env: ASSERTION_CLIENT_ID /
        ASSERTION_CLIENT_SECRET / ASSERTION_TOKEN_URI) to have the SDK fetch one, narrowed by
        `assertion_scope` / `assertion_audience` (env: ASSERTION_SCOPE / ASSERTION_AUDIENCE).
        With no `client_secret`, `assertion_grant` picks the federated grant (env: ASSERTION_GRANT):
        "client_credentials" (default, service-account identity) or "jwt-bearer" (identity chaining).
        `client_id` / `client_secret` / `token_url` then describe the client performing the
        exchange. The assertion is re-fetched per exchange rather than cached."""
        ...
    @classmethod
    def from_env(cls) -> DataHubClient: ...
    @classmethod
    def from_envfile(cls, path: str | None = None) -> DataHubClient: ...
    @property
    def timeseries(self) -> TimeSeriesServiceSync: ...
    @property
    def units(self) -> UnitServiceSync: ...
    @property
    def events(self) -> EventsServiceSync: ...
    @property
    def files(self) -> FilesServiceSync: ...
    @property
    def resources(self) -> ResourcesServiceSync: ...
    @property
    def datasets(self) -> DatasetsServiceSync: ...
    @property
    def subscriptions(self) -> SubscriptionsServiceSync: ...
    @property
    def functions(self) -> FunctionsServiceSync: ...
    @property
    def labels(self) -> LabelsServiceSync: ...
    @property
    def edges(self) -> EdgesServiceSync: ...


class AsyncDataHubClient:
    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        token_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        project_name: str | None = None,
        enable_buffering: bool = False,
        buffer_retention_secs: int | None = None,
        buffer_max_bytes: int | None = None,
        buffer_dir: str | None = None,
        scope: str | None = None,
        audience: str | None = None,
        assertion: str | None = None,
        assertion_token_url: str | None = None,
        assertion_client_id: str | None = None,
        assertion_client_secret: str | None = None,
        assertion_scope: str | None = None,
        assertion_audience: str | None = None,
        assertion_grant: str | None = None,
    ) -> None:
        """See `DataHubClient.__init__` for the durable-buffering parameters."""
        ...
    @classmethod
    def from_env(cls) -> AsyncDataHubClient: ...
    @classmethod
    def from_envfile(cls, path: str | None = None) -> AsyncDataHubClient: ...
    @property
    def timeseries(self) -> TimeSeriesServiceAsync: ...
    @property
    def units(self) -> UnitServiceAsync: ...
    @property
    def events(self) -> EventsServiceAsync: ...
    @property
    def files(self) -> FilesServiceAsync: ...
    @property
    def resources(self) -> ResourcesServiceAsync: ...
    @property
    def subscriptions(self) -> SubscriptionsServiceAsync: ...
    @property
    def functions(self) -> FunctionsServiceAsync: ...
    @property
    def labels(self) -> LabelsServiceAsync: ...
    @property
    def edges(self) -> EdgesServiceAsync: ...
    @property
    def datasets(self) -> DatasetsServiceAsync: ...


# ====================== Identifiers & search ======================

class IdCollection:
    """Names an entity by `id`, `external_id`, or both. Building one with neither raises."""
    def __init__(self, id: int | None = None, external_id: str | None = None) -> None: ...
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str | None: ...


class TimeSeriesFilter:
    """AND-combined criteria for ``timeseries.filter`` (``POST /timeseries/filter``) and the
    ``filter`` of ``timeseries.search``.

    Criteria only: ``limit``, ``sort_by``, ``sort_order`` and ``cursor`` are arguments of the
    call, not fields here, so one filter can be reused across ``filter()`` and ``search()`` and
    paged differently each time.

    ``external_id``, ``name``, ``source``, ``unit`` and ``unit_external_id`` are pattern
    lists — see ``PatternList``. Each is singular because each also takes a bare string, though a
    list is always accepted. ``labels`` keeps its plural: its entries must **all** be present, and
    so must every ``metadata`` entry, where a ``None`` value matches the key alone. ``value_type``
    is matched exactly (case-insensitively) against ``BIGINT``, ``FLOAT``, ``FLOAT32``,
    ``NUMERIC``, ``DECIMAL32``, ``TEXT``, ``MIXED``.

    ``data_set_id`` expands down the dataset hierarchy server-side, so a master dataset matches
    the timeseries of its child datasets too. **``None`` and ``[]`` differ here**: ``None`` places
    no restriction, ``[]`` narrows to no datasets and matches nothing. Every other list places no
    restriction when empty.

    """
    def __init__(
        self,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        unit: PatternList | None = None,
        unit_external_id: PatternList | None = None,
        value_type: PatternList | None = None,
    ) -> None: ...


# ====================== Field update wrappers ======================

class FieldStr:
    def __init__(self, value: str | None = None, set_null: bool = False) -> None: ...
    @property
    def value(self) -> str | None: ...
    @property
    def set_null(self) -> bool: ...


class FieldU64:
    def __init__(self, value: int | None = None, set_null: bool = False) -> None: ...
    @property
    def value(self) -> int | None: ...
    @property
    def set_null(self) -> bool: ...


class FieldBool:
    def __init__(self, value: bool | None = None, set_null: bool = False) -> None: ...
    @property
    def value(self) -> bool | None: ...
    @property
    def set_null(self) -> bool: ...


class FieldGeoJson:
    """The `set`/`set_null` pair for a geolocation: the value is a GeoJSON geometry dict,
    e.g. `{"type": "Point", "coordinates": [10.75, 59.91]}`."""
    def __init__(self, value: dict[str, Any] | None = None, set_null: bool = False) -> None: ...
    @property
    def value(self) -> dict[str, Any] | None: ...
    @property
    def set_null(self) -> bool: ...


# An update is either a replace (`set`) or a delta (`add`/`remove`), never both. The two
# constructors make the illegal mix unrepresentable; there is no bare initializer.
class ListFieldStr:
    @classmethod
    def set(cls, values: list[str]) -> ListFieldStr: ...
    @classmethod
    def delta(cls, add: list[str] | None = None, remove: list[str] | None = None) -> ListFieldStr: ...


# Entries name a resource by id, external_id, or both; `remove` matches on whichever side is given.
class ListFieldIdCollection:
    @classmethod
    def set(cls, values: list[IdCollection]) -> ListFieldIdCollection: ...
    @classmethod
    def delta(
        cls,
        add: list[IdCollection] | None = None,
        remove: list[IdCollection] | None = None,
    ) -> ListFieldIdCollection: ...


class MapField:
    @classmethod
    def set(cls, values: dict[str, str]) -> MapField: ...
    @classmethod
    def delta(cls, add: dict[str, str] | None = None, remove: list[str] | None = None) -> MapField: ...


# ====================== Time series ======================

class TimeSeries:
    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        value_type: str | ValueType | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        data_set_id: int | None = None,
        id: int | None = None,
        related_resources: list[RelatedNode] | None = None,
        source: str | None = None,
    ) -> None: ...
    @property
    def node_type(self) -> str:
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy"). Present on every node class, for dispatching from data rather
        than with an isinstance ladder."""
    @property
    def labels(self) -> list[str] | None:
        """Always includes the intrinsic "TIMESERIES" type-label."""
    @labels.setter
    def labels(self, value: list[str] | None) -> None: ...
    @property
    def table_engine(self) -> str | None:
        """The ClickHouse table engine. On a series reached through `neighbors()` this is the
        API's default rather than data — re-read the series by id for the real value."""
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def unit(self) -> str | None: ...
    @unit.setter
    def unit(self, value: str | None) -> None: ...
    @property
    def unit_external_id(self) -> str | None: ...
    @unit_external_id.setter
    def unit_external_id(self, value: str | None) -> None: ...
    @property
    def value_type(self) -> str | None: ...
    @value_type.setter
    def value_type(self, value: str | ValueType) -> None: ...
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @metadata.setter
    def metadata(self, value: dict[str, str] | None) -> None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @data_set_id.setter
    def data_set_id(self, value: int | None) -> None: ...
    @property
    def related_resources(self) -> list[RelatedNode]: ...
    @related_resources.setter
    def related_resources(self, value: list[RelatedNode] | None) -> None: ...
    # --- navigation (only on timeseries returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    def related_events(self, limit: int = 100) -> list[Event]: ...
    async def related_events_async(self, limit: int = 100) -> list[Event]: ...

class RelatedNode:
    """The unified node-centric relation, mirroring server-side `RelatedNode`: a node
    this one is connected to, with `relationship_type` and (on read) `direction` /
    `edge_id`. On input pass `id` or `external_id` plus a `relationship_type`."""

    def __init__(
        self,
        *,
        relationship_type: str | None = None,
        id: int | None = None,
        external_id: str | None = None,
    ) -> None: ...
    @classmethod
    def from_id(cls, id: int, relationship_type: str) -> RelatedNode: ...
    @classmethod
    def from_external_id(
        cls, external_id: str, relationship_type: str
    ) -> RelatedNode: ...
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str | None: ...
    @property
    def relationship_type(self) -> str | None: ...
    @property
    def direction(self) -> str | None: ...
    @property
    def edge_id(self) -> int | None: ...


class TimeSeriesUpdate:
    def __init__(
        self,
        ts: Identifiable,
        external_id: FieldStr | None = None,
        name: FieldStr | None = None,
        metadata: MapField | None = None,
        unit: FieldStr | None = None,
        description: FieldStr | None = None,
        unit_external_id: FieldStr | None = None,
        data_set_id: FieldU64 | None = None,
        source: FieldStr | None = None,
    ) -> None: ...
    @property
    def target_external_id(self) -> str | None: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def external_id(self) -> FieldStr: ...
    @property
    def name(self) -> FieldStr: ...
    @property
    def metadata(self) -> MapField: ...
    @property
    def unit(self) -> FieldStr: ...
    @property
    def description(self) -> FieldStr: ...
    @property
    def unit_external_id(self) -> FieldStr: ...
    @property
    def data_set_id(self) -> FieldU64: ...
    @property
    def source(self) -> FieldStr: ...


class DeleteFilter:
    def __init__(
        self,
        ts: Identifiable,
        inclusive_begin: datetime.datetime | None = None,
        exclusive_end: datetime.datetime | None = None,
    ) -> None: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def target_external_id(self) -> str | None: ...
    @property
    def inclusive_begin(self) -> datetime.datetime | None: ...
    @property
    def exclusive_end(self) -> datetime.datetime | None: ...


class ValueType:
    def __init__(self, value: str) -> None: ...
    def __repr__(self) -> str: ...


class Datapoint:
    def __init__(
        self,
        timestamp: datetime.datetime,
        value: float | None = None,
        min: float | None = None,
        max: float | None = None,
        average: float | None = None,
        sum: float | None = None,
    ) -> None: ...
    @property
    def timestamp(self) -> datetime.datetime: ...
    @property
    def value(self) -> float | None: ...
    @property
    def min(self) -> float | None: ...
    @property
    def max(self) -> float | None: ...
    @property
    def average(self) -> float | None: ...
    @property
    def sum(self) -> float | None: ...
    def __str__(self) -> str: ...


class DatapointString:
    def __init__(self, ts: datetime.datetime, value: str) -> None: ...
    @classmethod
    def from_int(cls, ts: datetime.datetime, value: int) -> DatapointString: ...
    @classmethod
    def from_float(cls, ts: datetime.datetime, value: float) -> DatapointString: ...
    @property
    def timestamp(self) -> str: ...
    @timestamp.setter
    def timestamp(self, value: str) -> None: ...
    @property
    def value(self) -> str: ...
    @value.setter
    def value(self, value: str) -> None: ...


class DatapointsCollectionString:
    def get_datapoints(self) -> list[Datapoint]: ...
    def as_dict(self) -> dict[str, Any]: ...
    def __len__(self) -> int: ...
    @property
    def next_cursor(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


class DatapointsCollectionDatapoints:
    def get_datapoints(self) -> list[Datapoint]: ...
    def as_dict(self) -> dict[str, Any]: ...
    def __len__(self) -> int: ...
    @property
    def next_cursor(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


class RetrieveFilter:
    def __init__(
        self,
        ts: Identifiable,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        limit: int | None = None,
        aggregates: list[str] | None = None,
        granularity: str | None = None,
        cursor: str | None = None,
    ) -> None: ...
    @property
    def start(self) -> datetime.datetime | None: ...
    @property
    def end(self) -> datetime.datetime | None: ...
    @property
    def limit(self) -> int | None: ...
    @property
    def aggregates(self) -> list[str] | None: ...
    @property
    def granularity(self) -> str | None: ...
    @property
    def cursor(self) -> str | None: ...


class TimeSeriesServiceSync:
    def list(self, limit: int | None = None) -> list[TimeSeries]: ...
    def create(self, input: list[TimeSeries]) -> list[TimeSeries]: ...
    def by_ids(self, input: list[Identifiable]) -> list[TimeSeries]: ...
    def delete(self, input: list[Identifiable]) -> None: ...
    def update(self, input: list[TimeSeriesUpdate]) -> list[TimeSeries]: ...
    def search(
        self,
        query: str,
        filter: TimeSeriesFilter | None = None,
        limit: int | None = None,
    ) -> list[TimeSeries]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` takes the same criteria as ``filter()`` and only ever removes hits from the
        phrase's — it cannot widen them, so omitting it returns them as found. ``limit`` caps what
        survives, defaulting to 100 and capping at 1000; the ``filter`` endpoints use 1000/10000,
        which is easy to conflate.
        """
    def filter(
        self,
        *,
        filter: TimeSeriesFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        unit: PatternList | None = None,
        unit_external_id: PatternList | None = None,
        value_type: PatternList | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    def insert_datapoints(self, input: list[DatapointsCollectionString]) -> list[str]: ...
    def insert_from_lists(
        self,
        timestamps: list[datetime.datetime],
        values: list[float],
        ts: Identifiable,
    ) -> list[str]: ...
    def retrieve_datapoints(self, input: RetrieveFilter) -> list[DatapointsCollectionDatapoints]: ...
    def delete_datapoints(self, input: list[DeleteFilter]) -> None: ...
    def retrieve_latest_datapoints(
        self, input: list[Identifiable]
    ) -> list[DatapointsCollectionDatapoints]: ...


class TimeSeriesServiceAsync:
    async def list(self, limit: int | None = None) -> list[TimeSeries]: ...
    async def create(self, input: list[TimeSeries]) -> list[TimeSeries]: ...
    async def by_ids(self, input: list[Identifiable]) -> list[TimeSeries]: ...
    async def delete(self, input: list[Identifiable]) -> None: ...
    async def update(self, input: list[TimeSeriesUpdate]) -> list[TimeSeries]: ...
    async def search(
        self,
        query: str,
        filter: TimeSeriesFilter | None = None,
        limit: int | None = None,
    ) -> list[TimeSeries]: ...
    async def filter(
        self,
        *,
        filter: TimeSeriesFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        unit: PatternList | None = None,
        unit_external_id: PatternList | None = None,
        value_type: PatternList | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    async def insert_datapoints(self, input: list[DatapointsCollectionString]) -> list[str]: ...
    async def insert_from_lists(
        self,
        timestamps: list[datetime.datetime],
        values: list[float],
        ts: Identifiable,
    ) -> list[str]: ...
    async def retrieve_datapoints(
        self, input: RetrieveFilter
    ) -> list[DatapointsCollectionDatapoints]: ...
    async def delete_datapoints(self, input: list[DeleteFilter]) -> None: ...
    async def retrieve_latest_datapoints(
        self, input: list[Identifiable]
    ) -> list[DatapointsCollectionDatapoints]: ...


# ====================== Events ======================

class Event:
    def __init__(
        self,
        external_id: str,
        type: str,
        event_time: datetime.datetime,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        sub_type: str | None = None,
        status: str | None = None,
        data_set_id: int | None = None,
        related_resources: list[IdCollection] | None = None,
        source: str | None = None,
    ) -> None:
        """``type`` is required: the API rejects a blank one with status 400."""
        ...
    @property
    def id(self) -> UUID | None: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def type(self) -> str: ...
    @type.setter
    def type(self, value: str) -> None: ...
    @property
    def sub_type(self) -> str | None: ...
    @sub_type.setter
    def sub_type(self, value: str | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def status(self) -> str | None: ...
    @status.setter
    def status(self, value: str | None) -> None: ...
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @metadata.setter
    def metadata(self, value: dict[str, str] | None) -> None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @data_set_id.setter
    def data_set_id(self, value: int | None) -> None: ...
    # Resources this event is attached to, each named by id, external_id, or both.
    # Events returned by the API carry both sides, resolved server-side.
    @property
    def related_resources(self) -> list[IdCollection]: ...
    @related_resources.setter
    def related_resources(self, value: list[IdCollection]) -> None: ...
    @property
    def event_time(self) -> datetime.datetime: ...
    @event_time.setter
    def event_time(self, value: datetime.datetime) -> None: ...
    @property
    def created_time(self) -> datetime.datetime | None: ...
    @property
    def last_updated_time(self) -> datetime.datetime | None: ...
    # --- navigation (only on events returned by the API; raises otherwise) ---
    def related_resource_nodes(self) -> list[Node]: ...
    async def related_resource_nodes_async(self) -> list[Node]: ...


class TimeFilter:
    def __init__(
        self,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
    ) -> None: ...


class EventFilter:
    """AND-combined criteria for ``events.filter`` (``POST /events/filter``).

    ``external_id``, ``source``, ``type``, ``sub_type`` and ``status`` are pattern lists — see
    ``PatternList`` — so ``type=["alarm", "warning"]`` is one call. They are named in the singular
    because each also takes a bare string. Every ``metadata`` entry must be present, and a ``None``
    value matches the key alone. ``related_resources`` keeps its plural: every entry of it must be
    attached to the event.

    ``data_set_id`` expands down the dataset hierarchy, so naming a parent covers its children.
    **``None`` and ``[]`` differ here**: ``None`` places no restriction, ``[]`` narrows to no
    datasets and matches nothing.

    There is no ``id``: events are keyed by UUID, and the field the api used to declare was typed
    as a long that nothing read. Use ``events.by_ids`` to look one up.
    """
    def __init__(
        self,
        external_id: PatternList | None = None,
        source: PatternList | None = None,
        type: PatternList | None = None,
        sub_type: PatternList | None = None,
        status: PatternList | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        event_time: TimeFilter | None = None,
        metadata: MetadataFilter | None = None,
        related_resources: list[IdCollection] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
    ) -> None: ...


class EventIdCollection:
    def __init__(
        self,
        id: UUID | None = None,
        external_id: str | None = None,
    ) -> None: ...
    @property
    def id(self) -> UUID | None: ...
    @property
    def external_id(self) -> str | None: ...


EventIdentifiable = Union[Event, EventIdCollection, UUID, str]


class EventUpdate:
    """Field-level changes for one event.

    There is deliberately no ``event_time``: an event's time is immutable after creation. The
    server's events table is partitioned by it, so the mutation cannot move the row and is refused
    outright; the api dropped the field from its update form, and sending it now is a ``400``
    naming the field. Record a corrected time as a new event, or delete and re-create.
    """

    def __init__(
        self,
        event: EventIdentifiable,
        external_id: FieldStr | None = None,
        description: FieldStr | None = None,
        type: FieldStr | None = None,
        sub_type: FieldStr | None = None,
        status: FieldStr | None = None,
        data_set_id: FieldU64 | None = None,
        metadata: MapField | None = None,
        source: FieldStr | None = None,
        related_resources: ListFieldIdCollection | None = None,
    ) -> None: ...
    @property
    def target_id(self) -> UUID | None: ...
    @property
    def target_external_id(self) -> str | None: ...


class EventDimension:
    """Categorical event fields with a queryable vocabulary.

    Served from dimension tables the write path maintains, so they are cheap enough for a
    typeahead but *eventually consistent* with the events themselves.
    """

    TYPE: EventDimension
    SUB_TYPE: EventDimension
    STATUS: EventDimension
    SOURCE: EventDimension


class EventsServiceSync:
    def list(self, limit: int | None = None) -> list[Event]: ...
    def create(self, input: list[Event]) -> list[Event]: ...
    def by_ids(self, input: list[EventIdentifiable]) -> list[Event]: ...
    def get(self, id: UUID) -> Event | None: ...
    def delete(self, input: list[EventIdentifiable]) -> None: ...
    def update(self, input: list[EventUpdate]) -> list[Event]: ...
    def filter(
        self,
        *,
        filter: EventFilter | None = None,
        external_id: PatternList | None = None,
        source: PatternList | None = None,
        type: PatternList | None = None,
        sub_type: PatternList | None = None,
        status: PatternList | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        event_time: TimeFilter | None = None,
        metadata: MetadataFilter | None = None,
        related_resources: Sequence[IdCollection] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    def search(
        self,
        query: str,
        filter: EventFilter | None = None,
        limit: int | None = None,
    ) -> list[Event]: ...
    def count(self) -> int: ...
    def list_dimension(
        self,
        dimension: EventDimension,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[str]: ...
    def list_types(self, limit: int | None = None) -> list[str]: ...
    def search_types(self, query: str, limit: int | None = None) -> list[str]: ...
    def list_sub_types(self, limit: int | None = None) -> list[str]: ...
    def search_sub_types(self, query: str, limit: int | None = None) -> list[str]: ...
    def list_statuses(self, limit: int | None = None) -> list[str]: ...
    def search_statuses(self, query: str, limit: int | None = None) -> list[str]: ...
    def list_sources(self, limit: int | None = None) -> list[str]: ...
    def search_sources(self, query: str, limit: int | None = None) -> list[str]: ...


class EventsServiceAsync:
    async def list(self, limit: int | None = None) -> list[Event]: ...
    async def create(self, input: list[Event]) -> list[Event]: ...
    async def by_ids(self, input: list[EventIdentifiable]) -> list[Event]: ...
    async def get(self, id: UUID) -> Event | None: ...
    async def delete(self, input: list[EventIdentifiable]) -> None: ...
    async def update(self, input: list[EventUpdate]) -> list[Event]: ...
    async def filter(
        self,
        *,
        filter: EventFilter | None = None,
        external_id: PatternList | None = None,
        source: PatternList | None = None,
        type: PatternList | None = None,
        sub_type: PatternList | None = None,
        status: PatternList | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        event_time: TimeFilter | None = None,
        metadata: MetadataFilter | None = None,
        related_resources: Sequence[IdCollection] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    async def search(
        self,
        query: str,
        filter: EventFilter | None = None,
        limit: int | None = None,
    ) -> list[Event]: ...
    async def count(self) -> int: ...
    async def list_dimension(
        self,
        dimension: EventDimension,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[str]: ...
    async def list_types(self, limit: int | None = None) -> list[str]: ...
    async def search_types(self, query: str, limit: int | None = None) -> list[str]: ...
    async def list_sub_types(self, limit: int | None = None) -> list[str]: ...
    async def search_sub_types(self, query: str, limit: int | None = None) -> list[str]: ...
    async def list_statuses(self, limit: int | None = None) -> list[str]: ...
    async def search_statuses(self, query: str, limit: int | None = None) -> list[str]: ...
    async def list_sources(self, limit: int | None = None) -> list[str]: ...
    async def search_sources(self, query: str, limit: int | None = None) -> list[str]: ...


# ====================== Datasets ======================

class Dataset:
    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        id: int | None = None,
        description: str | None = None,
        policies: list[str] | None = None,
        metadata: dict[str, str] | None = None,
        connected_data_sets: list[int] | None = None,
    ) -> None: ...
    @property
    def node_type(self) -> str:
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy"). Present on every node class, for dispatching from data rather
        than with an isinstance ladder."""
    @property
    def labels(self) -> list[str] | None:
        """Always includes the intrinsic "DATASET" type-label."""
    @labels.setter
    def labels(self, value: list[str] | None) -> None: ...
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def related_resources(self) -> list[RelatedNode]: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def id(self) -> int | None: ...
    @id.setter
    def id(self, value: int | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def policies(self) -> list[str] | None: ...
    @policies.setter
    def policies(self, value: list[str] | None) -> None: ...
    @property
    def metadata(self) -> dict[str, str]: ...
    @metadata.setter
    def metadata(self, value: dict[str, str]) -> None: ...
    @property
    def connected_data_sets(self) -> list[int]: ...
    @connected_data_sets.setter
    def connected_data_sets(self, value: list[int]) -> None: ...
    # --- navigation (only on datasets returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    def related_events(self, limit: int = 100) -> list[Event]: ...
    async def related_events_async(self, limit: int = 100) -> list[Event]: ...


# Criteria for `datasets.filter`. Every field is optional and they AND together, so an
# argument-free DatasetFilter() places no restriction. An *empty* list or dict is likewise
# no restriction rather than "match nothing".
#
# See the class docstring below for the pattern, label and metadata rules.
class DatasetFilter:
    """AND-combined criteria for ``datasets.filter``.

    ``external_id``, ``name`` and ``source`` are pattern lists — see ``PatternList`` — so
    ``external_id=["sap_*"]`` replaces the retired ``external_id_prefix`` and can be combined
    with exact ids in the same list. ``labels`` must **all** be present; names are canonicalised,
    so ``"pump a"`` finds the label stored as ``PUMP_A``. Every ``metadata`` entry must be present,
    and a ``None`` value matches the key alone.

    There is no ``data_set_id``: a dataset is the thing other nodes are scoped by, and no
    ``write_protected`` / ``deactivated`` either — both were removed server-side as inert, so a
    filter carrying them looked like it was narrowing and was not.
    """
    def __init__(
        self,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
    ) -> None: ...


# `limit` defaults to the server's 1000 and may not exceed 10000. There is no paging, so a filter
# broad enough to exceed the cap is truncated — narrow it instead.
# A partial update for one dataset. `dataset` names the target; only the fields you pass are sent,
# anything omitted is left untouched. There is deliberately no `policies` or `connected_data_sets`
# — the update endpoint does not accept them, whatever a Dataset can carry on create.
class DatasetUpdate:
    def __init__(
        self,
        dataset: Identifiable,
        external_id: FieldStr | None = None,
        name: FieldStr | None = None,
        description: FieldStr | None = None,
        metadata: MapField | None = None,
        labels: ListFieldStr | None = None,
    ) -> None: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def target_external_id(self) -> str | None: ...


# `search(query, ...)`: query is 3-140 chars and Latin letters/spaces/digits only, so an external
# id with underscores is a 400 — search on words and use filter() to look up by id. Results are
# unranked.
#
# `update(...)`: there is no write_protected/deactivated — both were removed server-side as inert.
class DatasetsServiceSync:
    def list(self, limit: int | None = None) -> list[Dataset]: ...
    def create(self, input: list[Dataset]) -> list[Dataset]: ...
    def by_ids(self, input: list[Identifiable]) -> list[Dataset]: ...
    def delete(self, input: list[Identifiable]) -> None: ...
    def filter(
        self,
        *,
        filter: DatasetFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    def search(
        self,
        query: str,
        filter: DatasetFilter | None = None,
        limit: int | None = None,
    ) -> list[Dataset]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` takes the same criteria as ``filter()`` and only ever removes hits from the
        phrase's — it cannot widen them, so omitting it returns them as found. ``limit`` caps what
        survives, defaulting to 100 and capping at 1000; the ``filter`` endpoints use 1000/10000,
        which is easy to conflate.
        """
    def update(self, input: list[DatasetUpdate]) -> list[Dataset]: ...
    def policies(self) -> list[Resource]: ...


class DatasetsServiceAsync:
    async def list(self, limit: int | None = None) -> list[Dataset]: ...
    async def create(self, input: list[Dataset]) -> list[Dataset]: ...
    async def by_ids(self, input: list[Identifiable]) -> list[Dataset]: ...
    async def delete(self, input: list[Identifiable]) -> None: ...
    async def filter(
        self,
        *,
        filter: DatasetFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    async def search(
        self,
        query: str,
        filter: DatasetFilter | None = None,
        limit: int | None = None,
    ) -> list[Dataset]: ...
    async def update(self, input: list[DatasetUpdate]) -> list[Dataset]: ...
    async def policies(self) -> list[Resource]: ...


# ====================== Resources ======================

class Resource:
    def __init__(
        self,
        name: str | None = None,
        external_id: str | None = None,
        id: int | None = None,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        is_root: bool = False,
        data_set_id: int | None = None,
        source: str | None = None,
        labels: list[str] | None = None,
        related_resources: list[RelatedNode] | None = None,
        geolocation: dict[str, Any] | None = None,
    ) -> None: ...
    @property
    def node_type(self) -> str:
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy"). Present on every node class, for dispatching from data rather
        than with an isinstance ladder."""
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def id(self) -> int | None: ...
    @id.setter
    def id(self, value: int | None) -> None: ...
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @metadata.setter
    def metadata(self, value: dict[str, str] | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def is_root(self) -> bool: ...
    @is_root.setter
    def is_root(self, value: bool) -> None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @data_set_id.setter
    def data_set_id(self, value: int | None) -> None: ...
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def labels(self) -> list[str] | None: ...
    @labels.setter
    def labels(self, value: list[str] | None) -> None: ...
    @property
    def related_resources(self) -> list[RelatedNode]: ...
    @related_resources.setter
    def related_resources(self, value: list[RelatedNode] | None) -> None: ...
    @property
    def geolocation(self) -> dict[str, Any] | None: ...
    @geolocation.setter
    def geolocation(self, value: dict[str, Any] | None) -> None: ...
    @property
    def created_time(self) -> datetime.datetime | None: ...
    @property
    def last_updated_time(self) -> datetime.datetime | None: ...
    # --- navigation (only on resources returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    def related_events(self, limit: int = 100) -> list[Event]: ...
    async def related_events_async(self, limit: int = 100) -> list[Event]: ...


class Asset:
    """A resource that carries a geographic location.

    Assets and plain resources share a field set; the API tells them apart by the intrinsic
    "ASSET" type-label, and only an asset ever has its `geolocation` echoed back on a read.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id: str | None = None,
        id: int | None = None,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        is_root: bool = False,
        data_set_id: int | None = None,
        source: str | None = None,
        labels: list[str] | None = None,
        related_resources: list[RelatedNode] | None = None,
        geolocation: dict[str, Any] | None = None,
    ) -> None: ...
    @property
    def node_type(self) -> str:
        """Always "asset"."""
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def id(self) -> int | None: ...
    @id.setter
    def id(self, value: int | None) -> None: ...
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @metadata.setter
    def metadata(self, value: dict[str, str] | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def is_root(self) -> bool: ...
    @is_root.setter
    def is_root(self, value: bool) -> None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @data_set_id.setter
    def data_set_id(self, value: int | None) -> None: ...
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def labels(self) -> list[str] | None: ...
    @labels.setter
    def labels(self, value: list[str] | None) -> None: ...
    @property
    def related_resources(self) -> list[RelatedNode]: ...
    @related_resources.setter
    def related_resources(self, value: list[RelatedNode] | None) -> None: ...
    @property
    def geolocation(self) -> dict[str, Any] | None:
        """GeoJSON geometry. On an asset reached through `neighbors()` this is rebuilt from the
        graph's native point and is lossy for anything that is not a Point — read the asset by
        id when the geometry matters."""
    @geolocation.setter
    def geolocation(self, value: dict[str, Any] | None) -> None: ...
    @property
    def created_time(self) -> datetime.datetime | None: ...
    @property
    def last_updated_time(self) -> datetime.datetime | None: ...
    # --- navigation (only on assets returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    def related_events(self, limit: int = 100) -> list[Event]: ...
    async def related_events_async(self, limit: int = 100) -> list[Event]: ...


class Policy:
    """An access policy, as a node.

    Sparse on every read: the API never sends a policy's `value`, `template_id` or
    `data_set_id` back, so those are always None on an object that came from the server.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id: str | None = None,
        id: int | None = None,
        type: str | None = None,
        value: Any | None = None,
        deactivated: bool | None = None,
        template_id: int | None = None,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        data_set_id: int | None = None,
        source: str | None = None,
        labels: list[str] | None = None,
    ) -> None: ...
    @property
    def node_type(self) -> str:
        """Always "policy"."""
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def id(self) -> int | None: ...
    @id.setter
    def id(self, value: int | None) -> None: ...
    @property
    def type(self) -> str | None:
        """The policy kind, e.g. "IS_WRITE_PROTECTED"."""
    @type.setter
    def type(self, value: str | None) -> None: ...
    @property
    def value(self) -> Any | None:
        """Never populated on a read — the API does not send it back."""
    @property
    def deactivated(self) -> bool | None: ...
    @deactivated.setter
    def deactivated(self, value: bool | None) -> None: ...
    @property
    def template_id(self) -> int | None:
        """Never populated on a read."""
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @metadata.setter
    def metadata(self, value: dict[str, str] | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def data_set_id(self) -> int | None:
        """Never populated on a read."""
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def labels(self) -> list[str] | None: ...
    @labels.setter
    def labels(self, value: list[str] | None) -> None: ...
    @property
    def related_resources(self) -> list[RelatedNode]: ...
    @property
    def created_time(self) -> datetime.datetime | None: ...
    @property
    def last_updated_time(self) -> datetime.datetime | None: ...
    # --- navigation (only on policies returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    def related_events(self, limit: int = 100) -> list[Event]: ...
    async def related_events_async(self, limit: int = 100) -> list[Event]: ...


class ResourceNetwork:
    """Connected sub-graph returned by `Resource.neighbors` (and the timeseries/dataset/
    function equivalents): the reachable `nodes`, the `edges` between them, and their
    `labels`."""
    @property
    def nodes(self) -> list[Node]: ...
    @property
    def edges(self) -> list[EdgeProxy]: ...
    @property
    def labels(self) -> list[Label]: ...


# ====================== Relations ======================
# NOTE: `RelForm` is the request-side edge form for resource create. `RelatedNode`
# is the unified node-centric relation carried by every node type (Resource,
# TimeSeries, Function, ...). `EdgeProxy` is the full edge detail in graph responses.

class EdgeProxy:
    """Server-assigned edge between two resources. The `relationship_type`
    attribute maps to the wire field `"type"`."""
    def __init__(
        self,
        id: int | None = None,
        start: int | None = None,
        end: int | None = None,
        relationship_type: str | None = None,
        description: str | None = None,
        relationship_type_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None: ...
    @property
    def id(self) -> int | None: ...
    @property
    def start(self) -> int | None: ...
    @property
    def end(self) -> int | None: ...
    @property
    def relationship_type(self) -> str | None: ...
    @property
    def description(self) -> str | None: ...
    @property
    def relationship_type_id(self) -> int | None: ...
    @property
    def metadata(self) -> dict[str, str]: ...


class RelForm:
    """Request-side edge form. Pair with a list of `Resource` and pass both to
    `ResourcesService.create()`. `relationship_type` is keyword-required."""
    def __init__(
        self,
        *,
        relationship_type: str,
        from_external_id: str | None = None,
        to_external_id: str | None = None,
        from_id: int | None = None,
        to_id: int | None = None,
        id: int | None = None,
        relationship_type_id: int | None = None,
        metadata: dict[str, str] | None = None,
        data_set_id: int | None = None,
        description: str | None = None,
    ) -> None: ...
    @classmethod
    def by_external_ids(
        cls, from_external_id: str, to_external_id: str, relationship_type: str
    ) -> RelForm: ...
    @classmethod
    def by_ids(cls, from_id: int, to_id: int, relationship_type: str) -> RelForm: ...
    @property
    def id(self) -> int | None: ...
    @property
    def from_external_id(self) -> str | None: ...
    @property
    def to_external_id(self) -> str | None: ...
    @property
    def from_id(self) -> int | None: ...
    @property
    def to_id(self) -> int | None: ...
    @property
    def relationship_type(self) -> str: ...
    @property
    def relationship_type_id(self) -> int | None: ...
    @property
    def metadata(self) -> dict[str, str]: ...
    @property
    def data_set_id(self) -> int | None: ...
    @property
    def description(self) -> str | None: ...


class GraphResult:
    """Nodes and relations returned from a graph operation."""
    @property
    def nodes(self) -> list[Node]: ...
    @property
    def relations(self) -> list[EdgeProxy]: ...


# Any node object, an external id, or a numeric id. Takes every node class, not just Resource,
# because /resources spans them all — a Dataset from filter() can be handed straight to delete().
ResourceIdentifiable = Union["Node", str, int]


class ResourceUpdate:
    """One resource's update for `resources.update`. Target the resource by a `Resource`, its
    numeric id, or its external id; every field is optional and uses the same wrappers as the
    other update APIs (`FieldStr` for scalars, `ListFieldStr` for labels, `MapField` for
    metadata). Mirrors `TimeSeriesUpdate`."""
    def __init__(
        self,
        resource: ResourceIdentifiable,
        external_id: FieldStr | None = None,
        name: FieldStr | None = None,
        description: FieldStr | None = None,
        data_set_id: FieldU64 | None = None,
        metadata: MapField | None = None,
        source: FieldStr | None = None,
        labels: ListFieldStr | None = None,
        geolocation: FieldGeoJson | None = None,
    ) -> None: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def target_external_id(self) -> str | None: ...
    @property
    def labels(self) -> ListFieldStr | None: ...


class ResourceFilter:
    """AND-combined criteria for ``resources.filter`` and the ``filter`` of ``resources.search``.

    The same arguments ``resources.filter`` takes as keywords, in an object — which is what
    ``search`` needs, since a filter passed positionally there would be indistinguishable from the
    search form.
    """
    def __init__(
        self,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        node_type: PatternList | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
    ) -> None: ...


class ResourcesServiceSync:
    def list(self, limit: int | None = None) -> list[Node]: ...
    def create(
        self, nodes: list[Node], relations: list[RelForm] | None = None
    ) -> GraphResult: ...
    def by_ids(self, input: list[ResourceIdentifiable]) -> list[Node]: ...
    def delete(self, input: list[ResourceIdentifiable]) -> None: ...
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> list[Node]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` takes the same criteria as ``filter()`` and only ever removes hits from the
        phrase's — it cannot widen them, so omitting it returns them as found. ``limit`` caps what
        survives, defaulting to 100 and capping at 1000; the ``filter`` endpoints use 1000/10000,
        which is easy to conflate.
        """
    def update(self, input: list[ResourceUpdate]) -> GraphResult: ...
    def get_by_id(self, id: int) -> Node | None: ...
    def filter(
        self,
        filter: ResourceFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        node_type: PatternList | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """``POST /resources/filter`` — the generic node query; criteria combine with AND.

        Spans **every node type** unless narrowed with ``node_type`` (``asset``, ``timeseries``,
        ``function``, ``resource``, ``dataset``, ``policy``); every node carries its type as a
        label so you can tell what came back.

        ``external_id``, ``name`` and ``source`` are pattern lists; ``labels`` must all be
        present; a ``None`` ``metadata`` value matches the key alone. ``data_set_id`` expands
        down the dataset hierarchy, and ``None`` (no restriction) differs from ``[]``
        (narrow to no datasets, matching nothing). See ``TimeSeriesFilter`` for the sort and
        cursor rules.
        """


class ResourcesServiceAsync:
    async def list(self, limit: int | None = None) -> list[Node]: ...
    async def create(
        self, nodes: list[Node], relations: list[RelForm] | None = None
    ) -> GraphResult: ...
    async def by_ids(self, input: list[ResourceIdentifiable]) -> list[Node]: ...
    async def delete(self, input: list[ResourceIdentifiable]) -> None: ...
    async def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> list[Node]: ...
    async def update(self, input: list[ResourceUpdate]) -> GraphResult: ...
    async def get_by_id(self, id: int) -> Node | None: ...
    async def filter(
        self,
        filter: ResourceFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: PatternList | None = None,
        name: PatternList | None = None,
        source: PatternList | None = None,
        labels: PatternList | None = None,
        metadata: MetadataFilter | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        node_type: PatternList | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Async twin of ``ResourcesServiceSync.filter``."""


# ====================== Labels ======================

class Label:
    """A DataHub label. `name` is the identifier you set (3–512 chars, canonicalised to
    SNAKE_UPPER_CASE server-side); `id`/`color` are usually assigned by the server. Also the
    shape returned inside a `ResourceNetwork` from `resources.fetch_related` (there
    `color`/`i18n_code` are `None`)."""
    def __init__(
        self,
        name: str | None = None,
        id: int | None = None,
        description: str | None = None,
        color: str | None = None,
        i18n_code: str | None = None,
    ) -> None: ...
    @property
    def id(self) -> int | None: ...
    @id.setter
    def id(self, value: int | None) -> None: ...
    @property
    def name(self) -> str | None: ...
    @name.setter
    def name(self, value: str | None) -> None: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def color(self) -> str | None: ...
    @color.setter
    def color(self, value: str | None) -> None: ...
    @property
    def i18n_code(self) -> str | None: ...
    @i18n_code.setter
    def i18n_code(self, value: str | None) -> None: ...


# Accepted as a label identifier when deleting: a Label, its numeric id, or its name.
LabelIdentifiable = Union["Label", int, str]


class LabelsServiceSync:
    def list(self) -> list[Label]: ...
    def get(self, id: int) -> Label | None: ...
    def create(self, input: list[Label]) -> list[Label]: ...
    def update(self, input: list[Label]) -> list[Label]: ...
    def delete(self, input: list[LabelIdentifiable]) -> None: ...


class LabelsServiceAsync:
    async def list(self) -> list[Label]: ...
    async def get(self, id: int) -> Label | None: ...
    async def create(self, input: list[Label]) -> list[Label]: ...
    async def update(self, input: list[Label]) -> list[Label]: ...
    async def delete(self, input: list[LabelIdentifiable]) -> None: ...


# ====================== Units ======================

class Unit:
    def __init__(
        self,
        id: int,
        external_id: str,
        name: str,
        long_name: str,
        symbol: str,
        description: str,
        alias_names: list[str],
        quantity: str,
        conversion: dict[str, float],
        source: str,
        source_reference: str,
    ) -> None: ...
    @property
    def id(self) -> int: ...
    @id.setter
    def id(self, value: int) -> None: ...
    @property
    def external_id(self) -> str: ...
    @external_id.setter
    def external_id(self, value: str) -> None: ...
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def long_name(self) -> str: ...
    @long_name.setter
    def long_name(self, value: str) -> None: ...
    @property
    def symbol(self) -> str: ...
    @symbol.setter
    def symbol(self, value: str) -> None: ...
    @property
    def description(self) -> str: ...
    @description.setter
    def description(self, value: str) -> None: ...
    @property
    def alias_names(self) -> list[str]: ...
    @alias_names.setter
    def alias_names(self, value: list[str]) -> None: ...
    @property
    def quantity(self) -> str: ...
    @quantity.setter
    def quantity(self, value: str) -> None: ...
    @property
    def conversion(self) -> dict[str, float]: ...
    @conversion.setter
    def conversion(self, value: dict[str, float]) -> None: ...
    @property
    def source(self) -> str: ...
    @source.setter
    def source(self, value: str) -> None: ...
    @property
    def source_reference(self) -> str: ...
    @source_reference.setter
    def source_reference(self, value: str) -> None: ...


class UnitServiceSync:
    def list(self) -> list[Unit]: ...
    def by_ids(self, input: list[IdCollection]) -> list[Unit]: ...
    def by_external_ids(self, input: str) -> list[Unit]: ...


class UnitServiceAsync:
    async def list(self) -> list[Unit]: ...
    async def by_ids(self, input: list[IdCollection]) -> list[Unit]: ...
    async def by_external_id(self, input: str) -> list[Unit]: ...


# ====================== Files ======================

class INode:
    def __init__(
        self,
        name: str,
        external_id: str,
        path: str,
        size: int,
        id: int | None = None,
        description: str | None = None,
        checksum: str | None = None,
        source: str | None = None,
        type: str | None = None,
        mime_type: str | None = None,
        source_date_created: datetime.datetime | None = None,
        source_last_updated: datetime.datetime | None = None,
        parent_id: int | None = None,
        parent_external_id: str | None = None,
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
        related_resources: list[int] | None = None,
        security_categories: list[int] | None = None,
    ) -> None: ...
    @property
    def id(self) -> int | None: ...
    @property
    def name(self) -> str: ...
    @property
    def description(self) -> str | None: ...
    @property
    def external_id(self) -> str: ...
    @property
    def path(self) -> str: ...
    @property
    def size(self) -> int: ...
    @property
    def checksum(self) -> str | None: ...
    @property
    def source(self) -> str | None: ...
    @property
    def type(self) -> str | None: ...
    @property
    def mime_type(self) -> str | None: ...
    @property
    def source_date_created(self) -> datetime.datetime | None: ...
    @property
    def source_last_updated(self) -> datetime.datetime | None: ...
    @property
    def date_created(self) -> datetime.datetime: ...
    @property
    def last_updated(self) -> datetime.datetime: ...
    @property
    def parent_id(self) -> int | None: ...
    @property
    def parent_external_id(self) -> str | None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @property
    def related_resources(self) -> list[int] | None: ...
    @property
    def security_categories(self) -> list[int] | None: ...
    # --- navigation (only on inodes returned by the API; raises otherwise) ---
    # `related_resources` (above) returns the raw ids; these resolve them to Resource objects.
    def related_resource_nodes(self) -> list[Node]: ...
    async def related_resource_nodes_async(self) -> list[Node]: ...


class FileUpload:
    def __init__(
        self,
        path: str,
        destination_path: str | None = None,
        external_id: str | None = None,
        name: str | None = None,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        source: str | None = None,
        data_set_id: int | None = None,
        related_resources: list[int] | None = None,
    ) -> None: ...
    @classmethod
    def from_path(cls, path: str) -> FileUpload: ...
    @classmethod
    def new_with_destination_path(cls, path: str, destination_path: str) -> FileUpload: ...
    @property
    def external_id(self) -> str: ...
    @property
    def file_path(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def destination_path(self) -> str | None: ...
    @property
    def metadata(self) -> dict[str, str] | None: ...
    @property
    def description(self) -> str | None: ...
    @property
    def source(self) -> str | None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @property
    def mime_type(self) -> str | None: ...
    @property
    def related_resources(self) -> list[int] | None: ...
    @property
    def source_date_created(self) -> datetime.datetime | None: ...
    @property
    def source_last_updated(self) -> datetime.datetime | None: ...


class FileUpdate:
    """A partial update for one file or folder.

    Identify the node with ``external_id`` or ``id``; every other argument is optional and only
    sent when given, so an omitted field is left unchanged.
    """

    def __init__(
        self,
        external_id: str | None = None,
        id: int | None = None,
        name: str | None = None,
        path: str | None = None,
        data_set_id: int | None = None,
        description: str | None = None,
        source: str | None = None,
        metadata: dict[str, str] | None = None,
        related_resources: list[int] | None = None,
    ) -> None: ...
    @property
    def external_id(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


class FileDownload:
    """A downloaded file's bytes plus what the server said they are."""

    @property
    def file_name(self) -> str | None: ...
    @property
    def mime_type(self) -> str | None: ...
    @property
    def content(self) -> bytes: ...
    def __len__(self) -> int: ...


FileIdentifiable = Union[INode, IdCollection, int, str]


class FilesServiceSync:
    def upload_file(self, file_upload: FileUpload) -> list[INode]: ...
    def list_root_directory(self) -> list[INode]: ...
    def delete(self, input: list[FileIdentifiable]) -> None: ...
    def list_directory_by_path(self, path: str) -> list[INode]: ...
    def get_by_id(self, id: int) -> list[INode]: ...
    def get_by_external_id(self, external_id: str) -> list[INode]: ...
    def search(self, query: str) -> list[INode]: ...
    def list_trash(self) -> list[INode]: ...
    def restore(self, input: list[FileIdentifiable]) -> list[INode]: ...
    def update(self, update: FileUpdate) -> list[INode]: ...
    def download(self, id: int) -> FileDownload: ...
    def download_to_path(self, id: int, destination: str) -> int: ...


class FilesServiceAsync:
    async def upload_file(self, file_upload: FileUpload) -> list[INode]: ...
    async def list_root_directory(self) -> list[INode]: ...
    async def delete(self, input: list[FileIdentifiable]) -> None: ...
    async def list_directory_by_path(self, path: str) -> list[INode]: ...
    async def get_by_id(self, id: int) -> list[INode]: ...
    async def get_by_external_id(self, external_id: str) -> list[INode]: ...
    async def search(self, query: str) -> list[INode]: ...
    async def list_trash(self) -> list[INode]: ...
    async def restore(self, input: list[FileIdentifiable]) -> list[INode]: ...
    async def update(self, update: FileUpdate) -> list[INode]: ...
    async def download(self, id: int) -> FileDownload: ...
    async def download_to_path(self, id: int, destination: str) -> int: ...


# ====================== Subscriptions ======================

class Subscription:
    def __init__(
        self,
        external_id: str,
        name: str,
        timeseries: list[Identifiable],
        id: int | None = None,
    ) -> None: ...


SubscriptionTimeseriesId = Union[TimeSeries, IdCollection, int, str]


class SubscriptionFilter:
    def __init__(self, timeseries: list[SubscriptionTimeseriesId] | None = None) -> None: ...
    @property
    def timeseries(self) -> list[IdCollection]: ...


class DataSort:
    def __init__(
        self,
        property: list[str] | None = None,
        order: str | None = None,
        nulls: str | None = None,
    ) -> None: ...
    @property
    def property(self) -> list[str] | None: ...
    @property
    def order(self) -> str | None: ...
    @property
    def nulls(self) -> str | None: ...


class SubscriptionFilterForm:
    def __init__(
        self,
        filter: SubscriptionFilter | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> None: ...
    @property
    def filter(self) -> SubscriptionFilter: ...
    @property
    def limit(self) -> int: ...
    @property
    def sort(self) -> DataSort: ...


class EventAction:
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


class EventObject:
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


class WsDatapoint:
    @property
    def timestamp(self) -> str: ...
    @property
    def value(self) -> str: ...
    def as_float(self) -> float: ...


class DataCollectionString:
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str | None: ...
    @property
    def value_type(self) -> str | None: ...
    @property
    def inclusive_begin(self) -> str | None: ...
    @property
    def exclusive_end(self) -> str | None: ...
    @property
    def datapoints(self) -> list[WsDatapoint]: ...


class DataWrapperMessage:
    @property
    def event_action(self) -> EventAction: ...
    @property
    def event_object(self) -> EventObject: ...
    @property
    def tenant_id(self) -> str | None: ...
    @property
    def items(self) -> list[DataCollectionString]: ...


class SubscriptionMessage:
    @property
    def subscription_external_id(self) -> str: ...
    @property
    def message_id(self) -> str: ...
    @property
    def payload(self) -> DataWrapperMessage: ...


SubscriptionIdentifiable = Union[Subscription, IdCollection, int, str]


class SubscriptionListener:
    def __iter__(self) -> SubscriptionListener: ...
    def __next__(self) -> SubscriptionMessage: ...
    def next_message(self) -> SubscriptionMessage | None: ...
    def ack(self, message_ids: list[str]) -> None: ...
    def nack(self, message_ids: list[str]) -> None: ...
    def subscribe(self, external_ids: list[str]) -> None: ...
    def unsubscribe(self, external_ids: list[str]) -> None: ...
    def set_subscriptions(self, external_ids: list[str]) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> SubscriptionListener: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...


class SubscriptionListenerAsync:
    def __aiter__(self) -> SubscriptionListenerAsync: ...
    async def __anext__(self) -> SubscriptionMessage: ...
    async def next_message(self) -> SubscriptionMessage | None: ...
    async def ack(self, message_ids: list[str]) -> None: ...
    async def nack(self, message_ids: list[str]) -> None: ...
    async def subscribe(self, external_ids: list[str]) -> None: ...
    async def unsubscribe(self, external_ids: list[str]) -> None: ...
    async def set_subscriptions(self, external_ids: list[str]) -> None: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> SubscriptionListenerAsync: ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...


class SubscriptionsServiceSync:
    def create(self, input: list[Subscription]) -> list[Subscription]: ...
    def list(
        self,
        form: SubscriptionFilterForm | None = None,
        timeseries: list[SubscriptionTimeseriesId] | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> list[Subscription]: ...
    def delete(self, input: list[SubscriptionIdentifiable]) -> None: ...
    def listen(self, subscription_external_ids: list[str]) -> SubscriptionListener: ...


class SubscriptionsServiceAsync:
    async def create(self, input: list[Subscription]) -> list[Subscription]: ...
    async def list(
        self,
        form: SubscriptionFilterForm | None = None,
        timeseries: list[SubscriptionTimeseriesId] | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> list[Subscription]: ...
    async def delete(self, input: list[SubscriptionIdentifiable]) -> None: ...
    async def listen(self, subscription_external_ids: list[str]) -> SubscriptionListenerAsync: ...


# ====================== Functions ======================

class Function:
    def __init__(
        self,
        external_id: str,
        name: str | None = None,
    ) -> None: ...
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str: ...
    @property
    def name(self) -> str | None: ...
    @property
    def node_type(self) -> str:
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy"). Present on every node class, for dispatching from data rather
        than with an isinstance ladder."""
    @property
    def labels(self) -> list[str]: ...
    @property
    def metadata(self) -> dict[str, str]: ...
    @property
    def description(self) -> str | None: ...
    @description.setter
    def description(self, value: str | None) -> None: ...
    @property
    def source(self) -> str | None: ...
    @source.setter
    def source(self, value: str | None) -> None: ...
    @property
    def data_set_id(self) -> int | None: ...
    @data_set_id.setter
    def data_set_id(self, value: int | None) -> None: ...
    @property
    def created_time(self) -> datetime.datetime | None: ...
    @property
    def last_updated_time(self) -> datetime.datetime | None: ...
    @property
    def related_resources(self) -> list[RelatedNode]: ...
    # --- navigation (only on functions returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork: ...
    def related_events(self, limit: int = 100) -> list[Event]: ...
    async def related_events_async(self, limit: int = 100) -> list[Event]: ...


FunctionIdentifiable = Union[Function, IdCollection, int, str]


class FunctionsServiceSync:
    def create(self, input: list[Function]) -> list[Function]: ...
    def list(self, limit: int | None = None) -> list[Function]: ...
    def by_ids(self, input: list[FunctionIdentifiable]) -> list[Function]: ...
    def by_external_id(self, external_id: str) -> Function: ...
    def delete(self, input: list[FunctionIdentifiable]) -> None: ...


class FunctionsServiceAsync:
    async def create(self, input: list[Function]) -> list[Function]: ...
    async def list(self, limit: int | None = None) -> list[Function]: ...
    async def by_ids(self, input: list[FunctionIdentifiable]) -> list[Function]: ...
    async def by_external_id(self, external_id: str) -> Function: ...
    async def delete(self, input: list[FunctionIdentifiable]) -> None: ...


# ====================== Edges ======================

class RelationshipType:
    """A relationship type in the tenant's catalogue."""

    @property
    def id(self) -> int | None: ...
    @property
    def name(self) -> str: ...
    @property
    def description(self) -> str | None: ...
    @property
    def i18n_code(self) -> str | None: ...


class RelTypeForm:
    """Register a relationship type up front.

    The server uppercase-snake-cases ``name`` (``"Flows To"`` -> ``FLOWS_TO``); a name that
    normalises to nothing is rejected with status 400.
    """

    def __init__(
        self,
        name: str,
        description: str | None = None,
        i18n_code: str | None = None,
    ) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def description(self) -> str | None: ...
    @property
    def i18n_code(self) -> str | None: ...


# Edges have no external id, so — unlike other identifiables — a string is not accepted.
EdgeIdentifiable = Union[EdgeProxy, int]


class EdgesServiceSync:
    def get(self, id: int) -> EdgeProxy | None: ...
    def by_ids(self, input: list[EdgeIdentifiable]) -> GraphResult: ...
    def create(self, input: list[RelForm]) -> list[EdgeProxy]: ...
    def delete(self, input: list[EdgeIdentifiable]) -> None: ...
    def types(self) -> list[RelationshipType]: ...
    def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]: ...


class EdgesServiceAsync:
    async def get(self, id: int) -> EdgeProxy | None: ...
    async def by_ids(self, input: list[EdgeIdentifiable]) -> GraphResult: ...
    async def create(self, input: list[RelForm]) -> list[EdgeProxy]: ...
    async def delete(self, input: list[EdgeIdentifiable]) -> None: ...
    async def types(self) -> list[RelationshipType]: ...
    async def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]: ...
