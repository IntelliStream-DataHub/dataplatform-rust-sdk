# Type stubs for the intellistream_datahub_sdk pyo3 extension module.
#
# The runtime is a single flat module: every class is exported at the top level.
# This stub matches that structure; do not introduce submodules unless the Rust
# registration in src/lib.rs::intellistream_datahub_sdk() also adds them.
"""Python client for the IntelliStream DataHub.

Services are reached through a configured client, never imported: ``client.timeseries``,
``client.events``, ``client.datasets`` and so on. ``AsyncDataHubClient`` exposes the same
services with every method a coroutine.
"""

from __future__ import annotations

import builtins

import datetime
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence, Union, final, overload
from uuid import UUID





@final
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
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, index: int) -> Any: ...
    @overload
    def __getitem__(self, index: slice) -> list[Any]: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __contains__(self, item: object) -> bool: ...
    @property
    def items(self) -> list[Any]:
        """
        The rows, as a plain list.
        """
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

    When the API explained itself with an RFC 9457 problem document, `problem` is
    that document as a dict, `problem_type` its `type` URI and `problem_slug` the
    kebab-case tail of it. Branch on the slug, never on `title`/`detail` — those
    are prose and may be reworded at any time:

        except DataHubException as e:
            if e.problem_slug == "would-strand":
                for blocker in e.problem["blockedBy"]:
                    print(blocker["externalId"])
            elif e.problem_slug == "validation-failed":
                for field in e.problem["fields"]:
                    print(field["field"], field["message"])

    All three are `None` when the API answered with something that is not a
    problem document — an empty 401, a stack trace, or plain text.
    """
    status_code: int
    message: str
    problem: dict | None
    problem_type: str | None
    problem_slug: str | None


# ====================== Clients ======================

@final
class DataHubClient:
    def __new__(
        cls,
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
    ) -> DataHubClient:
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
    def assets(self) -> AssetsServiceSync: ...
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


@final
class AsyncDataHubClient:
    def __new__(
        cls,
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
    ) -> AsyncDataHubClient:
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
    def assets(self) -> AssetsServiceAsync: ...
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

@final
class IdCollection:
    """Names an entity by `id`, `external_id`, or both. Building one with neither raises."""
    def __new__(cls, id: int | None = None, external_id: str | None = None) -> IdCollection: ...
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str | None: ...


@final
class TimeSeriesFilter:
    """AND-combined criteria for ``timeseries.filter`` (``POST /timeseries/filter``) and the
    ``filter`` of ``timeseries.search``.

    Criteria only: ``limit``, ``sort_by``, ``sort_order`` and ``cursor`` are arguments of the
    call, not fields here, so one filter can be reused across ``filter()`` and ``search()`` and
    paged differently each time.

    ``external_id``, ``name``, ``source``, ``unit`` and ``unit_external_id`` are pattern
    lists: one string or a list of them, where ``*`` and ``%`` are wildcards, ``_`` is literal,
    and matching ignores case. An entry without a wildcard matches exactly. ``labels`` keeps its plural: its entries must **all** be present, and
    so must every ``metadata`` entry, where a ``None`` value matches the key alone. ``value_type``
    is matched exactly (case-insensitively) against ``BIGINT``, ``FLOAT``, ``FLOAT32``,
    ``NUMERIC``, ``DECIMAL32``, ``TEXT``, ``MIXED``.

    ``data_set_id`` expands down the dataset hierarchy server-side, so a master dataset matches
    the timeseries of its child datasets too. **``None`` and ``[]`` differ here**: ``None`` places
    no restriction, ``[]`` narrows to no datasets and matches nothing. Every other list places no
    restriction when empty.

    """
    def __new__(
        cls,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        unit: str | Sequence[str] | None = None,
        unit_external_id: str | Sequence[str] | None = None,
        value_type: str | Sequence[str] | None = None,
    ) -> TimeSeriesFilter: ...


# ====================== Field update wrappers ======================

@final
class FieldStr:
    def __new__(cls, value: str | None = None, set_null: bool = False) -> FieldStr: ...
    @property
    def value(self) -> str | None: ...
    @property
    def set_null(self) -> bool: ...


@final
class FieldU64:
    def __new__(cls, value: int | None = None, set_null: bool = False) -> FieldU64: ...
    @property
    def value(self) -> int | None: ...
    @property
    def set_null(self) -> bool: ...


@final
class FieldBool:
    def __new__(cls, value: bool | None = None, set_null: bool = False) -> FieldBool: ...
    @property
    def value(self) -> bool | None: ...
    @property
    def set_null(self) -> bool: ...


@final
class FieldGeoJson:
    """The `set`/`set_null` pair for a geolocation: the value is a GeoJSON geometry dict,
    e.g. `{"type": "Point", "coordinates": [10.75, 59.91]}`."""
    def __new__(cls, value: dict[str, Any] | None = None, set_null: bool = False) -> FieldGeoJson: ...
    @property
    def value(self) -> dict[str, Any] | None: ...
    @property
    def set_null(self) -> bool: ...


# An update is either a replace (`set`) or a delta (`add`/`remove`), never both. The two
# constructors make the illegal mix unrepresentable; there is no bare initializer.
@final
class ListFieldStr:
    @classmethod
    def set(cls, values: list[str]) -> ListFieldStr:
        """
        Replace the whole list.
        """
    @classmethod
    def delta(cls, add: list[str] | None = None, remove: list[str] | None = None) -> ListFieldStr:
        """
        Add and/or remove entries, keeping the rest. Pass ``add``, ``remove``, or both.
        """


# Entries name a resource by id, external_id, or both; `remove` matches on whichever side is given.
@final
class ListFieldIdCollection:
    """
    The related-resource list of an ``EventUpdate``. Entries are ``IdCollection`` objects, so a resource can
    be named by id, external_id, or both; ``remove`` matches on whichever side is given.
    """
    @classmethod
    def set(cls, values: list[IdCollection]) -> ListFieldIdCollection:
        """
        Replace the whole list.
        """
    @classmethod
    def delta(
        cls,
        add: list[IdCollection] | None = None,
        remove: list[IdCollection] | None = None,
    ) -> ListFieldIdCollection:
        """
        Add and/or remove entries, keeping the rest. Pass ``add``, ``remove``, or both.
        """


@final
class MapField:
    @classmethod
    def set(cls, values: dict[str, str]) -> MapField:
        """
        Replace all entries.
        """
    @classmethod
    def delta(cls, add: dict[str, str] | None = None, remove: list[str] | None = None) -> MapField:
        """
        Add and/or remove entries, keeping the rest. Pass ``add``, ``remove``, or both.
        """


# ====================== Time series ======================

@final
class TimeSeries:
    """A time series: a named, typed sequence of ``(timestamp, value)`` datapoints.

    A ``TimeSeries`` describes the series -- its identity, value type, unit and metadata.
    The datapoints themselves are written and read through ``client.timeseries``
    (:meth:`timeseries.insert_datapoints`,
    :meth:`timeseries.retrieve_datapoints`). Build one locally and pass it to
    ``client.timeseries.create``; objects returned by the client also carry the
    server-assigned ``id`` and timestamps.

    Parameters
    ----------
    name : str, optional
        Display name. If omitted, ``external_id`` is used.
    external_id : str, optional
        Your identifier for the series, unique among time series in the tenant and 3--512
        characters long. If omitted, it is derived from ``name`` in lower snake case.
        At least one of ``name`` and ``external_id`` is required.
    value_type : {"bigint", "float", "text"}, default "bigint"
        What the datapoints hold, case-insensitive; ``"decimal"`` is accepted as an alias
        for ``"float"``. Fixed at creation: the server refuses to change it, and refuses
        datapoints that do not parse as this type.
    metadata : dict of str to str, optional
        Free-form key/value pairs, filterable with ``metadata=`` in
        :meth:`timeseries.filter`.
    description : str, optional
        Free text, matched by :meth:`timeseries.search`.
    unit : str, optional
        The unit as free text, for example ``"bar"`` or ``"m3/h"``.
    unit_external_id : str, optional
        A unit from the tenant's unit catalogue, for example ``"pressure_bar"``; see
        ``client.units``.
    data_set_id : int, optional
        The data set the series belongs to, which also decides who may read it.
    related_resources : list of RelatedNode, optional
        Nodes to connect to on create. Each becomes a relationship edge server-side.
    source : str, optional
        Where the series comes from, for example the name of the system that feeds it.

    Raises
    ------
    ValueError
        If neither ``name`` nor ``external_id`` is given, or ``value_type`` is not one of
        the accepted spellings.

    See Also
    --------
    timeseries.create : Store new series.
    TimeSeriesUpdate : Change a stored series.

    Examples
    --------
    >>> from intellistream_datahub_sdk import TimeSeries
    >>> ts = TimeSeries(
    ...     external_id="pump_a_pressure",
    ...     name="Pump A discharge pressure",
    ...     value_type="float",
    ...     unit_external_id="pressure_bar",
    ...     metadata={"site": "north"},
    ... )
    >>> [created] = client.timeseries.create([ts])
    >>> created.id is not None
    True
    """
    def __new__(
        cls,
        name: str | None = None,
        external_id: str | None = None,
        value_type: str = "bigint",
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        data_set_id: int | None = None,
        related_resources: list[RelatedNode] | None = None,
        source: str | None = None,
    ) -> TimeSeries: ...
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
    def value_type(self) -> str | None:
        """
        ``None`` on a series reached through ``neighbors()`` — the graph does not carry the column.
        Re-read the series by id when the value type matters.
        """
    @value_type.setter
    def value_type(self, value: str) -> None: ...
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
    ) -> ResourceNetwork:
        """
        Walk the graph from this timeseries and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters which
        edge types to follow (``None`` = all); ``limit`` caps the node count. Neighbour nodes are
        modelled as ``Resource``. Blocking; see [``neighbors_async``] for the awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """
        Awaitable variant of [``neighbors``].
        """
    def related_events(self, limit: int = 100) -> list[Event]:
        """
        Fetch events whose ``related_resources`` include this
        timeseries (matched by graph-node id when present, else external id), via ``events.filter``.
        ``limit`` caps the results (default 100). Blocking; see [``related_events_async``].
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """
        Awaitable variant of [``related_events``].
        """

@final
class RelatedNode:
    """The unified node-centric relation, mirroring server-side `RelatedNode`: a node
    this one is connected to, with `relationship_type` and (on read) `direction` /
    `edge_id`. On input pass `id` or `external_id` plus a `relationship_type`."""

    def __new__(
        cls,
        *,
        relationship_type: str | None = None,
        id: int | None = None,
        external_id: str | None = None,
    ) -> RelatedNode: ...
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
    def direction(self) -> str | None:
        """
        ``"OUTBOUND"`` / ``"INBOUND"`` on read; ``None`` on input.
        """
    @property
    def edge_id(self) -> int | None: ...


@final
class TimeSeriesUpdate:
    """
    Python wrapper for TimeseriesUpdate, represents a request for change to a timeseries

    Parameters
    ----------
    ts: Timeseries
    """
    def __new__(
        cls,
        ts: int | str | TimeSeries | IdCollection,
        external_id: FieldStr | None = None,
        name: FieldStr | None = None,
        metadata: MapField | None = None,
        unit: FieldStr | None = None,
        description: FieldStr | None = None,
        unit_external_id: FieldStr | None = None,
        data_set_id: FieldU64 | None = None,
        source: FieldStr | None = None,
    ) -> TimeSeriesUpdate: ...
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


@final
class DeleteFilter:
    """
    One series, and the window of datapoints to remove from it, for ``delete_datapoints``.

    Both bounds are optional and the window is half-open, so:
    give both to clear the window between them, ``inclusive_begin`` alone to clear everything from
    that instant onward, ``exclusive_end`` alone to clear everything before it, and neither to clear
    every datapoint of the series while keeping its definition, edges and subscriptions.

    The purge is asynchronous: the call returns once the request is accepted, and a read straight
    afterwards can still see the datapoints. It cannot be undone.

    Parameters
    ----------
    ts : int, str, TimeSeries or IdCollection
        The series, as an external id, an id, or a TimeSeries.
    inclusive_begin : datetime | None
        Start of the window, included. Must be timezone-aware.
    exclusive_end : datetime | None
        End of the window, excluded. Must be timezone-aware.

    Examples
    --------
    >>> # everything recorded before 2026 goes; the series itself stays
    >>> f = DeleteFilter(ts="engine_temperature",
    ...                  exclusive_end=pd.Timestamp("2026-01-01", tz="UTC"))
    >>> client.timeseries.delete_datapoints([f])
    """
    def __new__(
        cls,
        ts: int | str | TimeSeries | IdCollection,
        inclusive_begin: datetime.datetime | None = None,
        exclusive_end: datetime.datetime | None = None,
    ) -> DeleteFilter: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def target_external_id(self) -> str | None: ...
    @property
    def inclusive_begin(self) -> datetime.datetime | None: ...
    @property
    def exclusive_end(self) -> datetime.datetime | None: ...


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


@final
class DatapointString:
    def __new__(cls, ts: datetime.datetime, value: str) -> DatapointString: ...
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


@final
class DatapointsCollectionString:
    """Datapoints to write to one time series, for ``insert_datapoints``.

    Parameters
    ----------
    datapoints : list of DatapointString
    ts : int, str, TimeSeries or IdCollection
        The target series, by id, external id, ``IdCollection`` or ``TimeSeries``.
    """
    def __new__(cls, datapoints: list[DatapointString], ts: int | str | TimeSeries | IdCollection) -> DatapointsCollectionString: ...


@final
class DatapointsCollectionDatapoints:
    def get_datapoints(self) -> list[Datapoint]: ...
    def as_dict(self) -> dict[str, Any]: ...
    def __len__(self) -> int: ...
    @property
    def next_cursor(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


@final
class RetrieveFilter:
    def __new__(
        cls,
        ts: int | str | TimeSeries | IdCollection,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        limit: int | None = None,
        aggregates: list[str] | None = None,
        granularity: str | None = None,
        cursor: str | None = None,
    ) -> RetrieveFilter: ...
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
    """Create, find, change and delete time series, and write and read their datapoints.

    Reached as ``client.timeseries`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """
    def list(self, limit: int | None = None) -> builtins.list[TimeSeries]:
        """List time series, newest created first.

        This is a first page and nothing more: there is no cursor to continue from. To go
        further, narrow the query with :meth:`timeseries.filter` rather than raising
        ``limit``.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of TimeSeries

        See Also
        --------
        timeseries.filter : Select by criteria, with paging.

        Examples
        --------
        >>> recent = client.timeseries.list(limit=20)
        """
    def create(self, input: builtins.list[TimeSeries]) -> builtins.list[TimeSeries]:
        """Store new time series.

        The batch is all-or-nothing: if any series fails validation, none is created.

        Parameters
        ----------
        input : list of TimeSeries
            The series to create. Each ``external_id`` must be unused among the tenant's
            time series.

        Returns
        -------
        list of TimeSeries
            The stored series, with ``id`` and timestamps filled in by the server.

        Raises
        ------
        DataHubException
            ``status_code`` 409 if an ``external_id`` is already taken; the problem's
            ``duplicated`` member names it.

        Examples
        --------
        >>> from intellistream_datahub_sdk import TimeSeries
        >>> [ts] = client.timeseries.create(
        ...     [TimeSeries(external_id="pump_a_pressure", value_type="float", unit="bar")]
        ... )
        """
    def by_ids(self, input: builtins.list[int | str | TimeSeries | IdCollection]) -> builtins.list[TimeSeries]:
        """Fetch time series by id or external id.

        What is not found is left out of the result rather than raising, so compare the
        result with what you asked for to detect missing series.

        Parameters
        ----------
        input : list of int, str, TimeSeries or IdCollection
            Each entry is a numeric id, an external id, a ``TimeSeries`` or an
            ``IdCollection``. Mix freely.

        Returns
        -------
        list of TimeSeries

        Examples
        --------
        >>> found = client.timeseries.by_ids([42, "pump_a_pressure"])
        """
    def delete(self, input: builtins.list[int | str | TimeSeries | IdCollection]) -> None:
        """Delete time series, and every datapoint they hold.

        This cannot be undone. Deleting a series that is already gone is a no-op. To clear a
        series' datapoints but keep the series, use :meth:`timeseries.delete_datapoints`.

        Parameters
        ----------
        input : list of int, str, TimeSeries or IdCollection
            The series to delete, by id, external id, ``TimeSeries`` or ``IdCollection``.

        Raises
        ------
        DataHubException
            ``status_code`` 409 with ``problem_slug`` ``"referenced"`` if a series is still
            bound to a subscription, or ``"would-strand"`` if deleting it would disconnect
            another node from the graph. The problem's ``blockedBy`` names what is in the way;
            remove that first.

        Examples
        --------
        >>> client.timeseries.delete(["pump_a_pressure"])
        """
    def update(self, input: builtins.list[TimeSeriesUpdate]) -> builtins.list[TimeSeries]:
        """Change fields on existing time series.

        Only the fields named in each ``TimeSeriesUpdate`` change. ``value_type`` cannot be
        changed; create a new series instead. The batch is all-or-nothing.

        Parameters
        ----------
        input : list of TimeSeriesUpdate

        Returns
        -------
        list of TimeSeries
            The series as they stand after the update.

        Examples
        --------
        >>> from intellistream_datahub_sdk import FieldStr, MapField, TimeSeriesUpdate
        >>> client.timeseries.update([
        ...     TimeSeriesUpdate(
        ...         "pump_a_pressure",
        ...         description=FieldStr("Discharge side, after the check valve"),
        ...         metadata=MapField.delta(add={"calibrated": "2026-09"}),
        ...     )
        ... ])
        """
    def search(
        self,
        query: str,
        filter: TimeSeriesFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[TimeSeries]:
        """Free-text search over name, external id and description.

        Matching is word-aware and fuzzy -- ``"temp"`` also finds ``"temperature"`` -- and
        results are ranked, best match first. For exact lookups use
        :meth:`timeseries.by_ids`; for structured queries without a phrase, use
        :meth:`timeseries.filter`.

        Parameters
        ----------
        query : str
            The phrase, 3--140 characters.
        filter : TimeSeriesFilter, optional
            Narrows the phrase's hits. It only ever removes results, never adds them.
        limit : int, optional
            How many to return. Defaults to 100; above 1000 is refused.

        Returns
        -------
        list of TimeSeries

        Examples
        --------
        >>> from intellistream_datahub_sdk import TimeSeriesFilter
        >>> client.timeseries.search("discharge pressure", filter=TimeSeriesFilter(unit="bar"))
        """
    def filter(
        self,
        *,
        filter: TimeSeriesFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        unit: str | Sequence[str] | None = None,
        unit_external_id: str | Sequence[str] | None = None,
        value_type: str | Sequence[str] | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Select time series by criteria, one page at a time.

        Give the criteria either as keywords or as a prepared ``filter=``, not both. Criteria
        combine with AND; the entries of one list combine with OR. Pattern lists accept a
        single string or a list, where ``*`` and ``%`` are wildcards, ``_`` is literal, and
        matching ignores case.

        Parameters
        ----------
        filter : TimeSeriesFilter, optional
            The criteria as one object, reusable across calls. Exclusive with the
            criteria keywords below.
        id : sequence of int, optional
        external_id, name, source, unit, unit_external_id : str or sequence of str, optional
        labels : str or sequence of str, optional
            Every label listed must be present.
        metadata : mapping of str to str or None, optional
            Every key listed must be present; a ``None`` value matches the key alone.
        created_time, last_updated_time : TimeFilter, optional
            Inclusive at both ends.
        data_set_id : sequence of int, str or IdCollection, optional
            Data sets by id or external id; includes everything beneath them in the data
            set hierarchy.
        value_type : str or sequence of str, optional
            Matched exactly, ignoring case.
        limit : int, optional
            Page size. Defaults to 1000; above 10000 is refused.
        sort_by : str, optional
            One property; defaults to ``createdTime``.
        sort_order : {"asc", "desc"}, optional
            Defaults to descending.
        cursor : str, optional
            ``next_cursor`` from the previous page. It belongs to its sort: continuing it
            under another is refused.

        Returns
        -------
        Page
            A list-like page of ``TimeSeries``. Its ``next_cursor`` is ``None`` on the last
            page.

        Raises
        ------
        TypeError
            If both ``filter=`` and criteria keywords are given.

        Examples
        --------
        Every pressure series in bar, a page at a time:

        >>> page = client.timeseries.filter(name="*pressure*", unit="bar", limit=500)
        >>> series = list(page)
        >>> while page.next_cursor:
        ...     page = client.timeseries.filter(
        ...         name="*pressure*", unit="bar", limit=500, cursor=page.next_cursor
        ...     )
        ...     series.extend(page)
        """

    def insert_datapoints(self, input: builtins.list[DatapointsCollectionString]) -> builtins.list[str]:
        """Write datapoints to one or more time series.

        A datapoint whose timestamp already exists replaces the stored value, so retrying a
        write is safe. If some target series do not exist, the datapoints for the others are
        still written and the call raises.

        Parameters
        ----------
        input : list of DatapointsCollectionString
            One collection per target series.

        Returns
        -------
        list of str
            Empty on success.

        Raises
        ------
        DataHubException
            ``status_code`` 404 naming the series that do not exist; 422 for a value that
            does not parse as the series' value type.

        See Also
        --------
        timeseries.insert_from_lists : The same for one series from two parallel lists.
        timeseries.insert_datapoints_binary : The same write, compressed.

        Examples
        --------
        >>> from datetime import datetime, timezone
        >>> from intellistream_datahub_sdk import DatapointString, DatapointsCollectionString
        >>> now = datetime.now(timezone.utc)
        >>> client.timeseries.insert_datapoints([
        ...     DatapointsCollectionString(
        ...         [DatapointString(now, "4.2")], "pump_a_pressure"
        ...     )
        ... ])
        []
        """
    def insert_datapoints_binary(
        self,
        input: builtins.list[DatapointsCollectionString],
        zstd_level: int | None = None,
    ) -> builtins.list[str]:
        """Write datapoints as compressed Arrow frames.

        The same input as :meth:`timeseries.insert_datapoints`, sent to the binary endpoint.
        Much faster for large writes. Each value is checked against its series' value type
        before anything is sent.

        Parameters
        ----------
        input : list of DatapointsCollectionString
        zstd_level : {1, 3, 9}, optional
            Compression level. Defaults to 9.

        Returns
        -------
        list of str
            Empty on success.
        """
    def insert_from_lists_binary(
        self,
        timestamps: builtins.list[datetime.datetime],
        values: builtins.list[float],
        ts: int | str | TimeSeries | IdCollection,
        zstd_level: int | None = None,
    ) -> builtins.list[str]:
        """Write one series' datapoints from two parallel lists, as compressed Arrow frames.

        The binary counterpart of :meth:`timeseries.insert_from_lists`.

        Parameters
        ----------
        timestamps : list of datetime
        values : list of float
            The same length as ``timestamps``.
        ts : int, str, TimeSeries or IdCollection
            The target series.
        zstd_level : {1, 3, 9}, optional
            Compression level. Defaults to 9.

        Returns
        -------
        list of str
            Empty on success.

        Raises
        ------
        ValueError
            If ``timestamps`` and ``values`` differ in length.
        """
    def insert_from_lists(
        self,
        timestamps: builtins.list[datetime.datetime],
        values: builtins.list[float],
        ts: int | str | TimeSeries | IdCollection,
    ) -> builtins.list[str]:
        """Write one series' datapoints from two parallel lists.

        The shape a pair of DataFrame columns arrives in.

        Parameters
        ----------
        timestamps : list of datetime
        values : list of float
            The same length as ``timestamps``.
        ts : int, str, TimeSeries or IdCollection
            The target series.

        Returns
        -------
        list of str
            Empty on success.

        Examples
        --------
        >>> from datetime import datetime, timedelta, timezone
        >>> start = datetime(2026, 9, 1, tzinfo=timezone.utc)
        >>> timestamps = [start + timedelta(minutes=i) for i in range(3)]
        >>> client.timeseries.insert_from_lists(timestamps, [4.1, 4.2, 4.3], "pump_a_pressure")
        []
        """
    def retrieve_datapoints(self, input: RetrieveFilter) -> builtins.list[DatapointsCollectionDatapoints]:
        """Read one series' datapoints over a time window, raw or aggregated.

        The window includes ``start`` and excludes ``end``. Leave both out for the most
        recent ``limit`` points. When the server splits a large answer, the collection's
        ``next_cursor`` is set: pass it back as ``RetrieveFilter(cursor=...)`` for the rest.

        Parameters
        ----------
        input : RetrieveFilter
            The series, window, limit and, optionally, ``aggregates`` with a
            ``granularity``.

        Returns
        -------
        list of DatapointsCollectionDatapoints
            Call ``get_datapoints()`` on each for the ``Datapoint`` objects.

        Examples
        --------
        Hourly averages over one day:

        >>> from datetime import datetime, timezone
        >>> from intellistream_datahub_sdk import RetrieveFilter
        >>> [result] = client.timeseries.retrieve_datapoints(
        ...     RetrieveFilter(
        ...         "pump_a_pressure",
        ...         start=datetime(2026, 9, 1, tzinfo=timezone.utc),
        ...         end=datetime(2026, 9, 2, tzinfo=timezone.utc),
        ...         aggregates=["avg", "min", "max"],
        ...         granularity="1h",
        ...     )
        ... )
        >>> [(p.timestamp, p.average) for p in result.get_datapoints()][:2]
        """
    def delete_datapoints(self, input: builtins.list[DeleteFilter]) -> None:
        """Delete datapoints in a time window, keeping the series.

        This cannot be undone. Each ``DeleteFilter`` names a series and a window that
        includes ``inclusive_begin`` and excludes ``exclusive_end``; leave either end open
        to delete from the beginning or to the end, and both to empty the series.

        Parameters
        ----------
        input : list of DeleteFilter

        Examples
        --------
        >>> from datetime import datetime, timezone
        >>> from intellistream_datahub_sdk import DeleteFilter
        >>> client.timeseries.delete_datapoints([
        ...     DeleteFilter(
        ...         "pump_a_pressure",
        ...         inclusive_begin=datetime(2026, 9, 1, tzinfo=timezone.utc),
        ...         exclusive_end=datetime(2026, 9, 2, tzinfo=timezone.utc),
        ...     )
        ... ])
        """
    def retrieve_latest_datapoints(
        self, input: builtins.list[int | str | TimeSeries | IdCollection]
    ) -> builtins.list[DatapointsCollectionDatapoints]:
        """Read the most recent datapoint of each series.

        A series with no datapoints is left out of the result.

        Parameters
        ----------
        input : list of int, str, TimeSeries or IdCollection

        Returns
        -------
        list of DatapointsCollectionDatapoints
            One per series that has data, each holding its single latest datapoint.

        Examples
        --------
        >>> latest = client.timeseries.retrieve_latest_datapoints(["pump_a_pressure"])
        """


class TimeSeriesServiceAsync:
    async def list(self, limit: int | None = None) -> builtins.list[TimeSeries]: ...
    async def create(self, input: builtins.list[TimeSeries]) -> builtins.list[TimeSeries]: ...
    async def by_ids(self, input: builtins.list[int | str | TimeSeries | IdCollection]) -> builtins.list[TimeSeries]: ...
    async def delete(self, input: builtins.list[int | str | TimeSeries | IdCollection]) -> None: ...
    async def update(self, input: builtins.list[TimeSeriesUpdate]) -> builtins.list[TimeSeries]: ...
    async def search(
        self,
        query: str,
        filter: TimeSeriesFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[TimeSeries]: ...
    async def filter(
        self,
        *,
        filter: TimeSeriesFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        unit: str | Sequence[str] | None = None,
        unit_external_id: str | Sequence[str] | None = None,
        value_type: str | Sequence[str] | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    async def insert_datapoints(self, input: builtins.list[DatapointsCollectionString]) -> builtins.list[str]: ...
    async def insert_from_lists(
        self,
        timestamps: builtins.list[datetime.datetime],
        values: builtins.list[float],
        ts: int | str | TimeSeries | IdCollection,
    ) -> builtins.list[str]: ...
    async def retrieve_datapoints(
        self, input: RetrieveFilter
    ) -> builtins.list[DatapointsCollectionDatapoints]: ...
    async def delete_datapoints(self, input: builtins.list[DeleteFilter]) -> None: ...
    async def retrieve_latest_datapoints(
        self, input: builtins.list[int | str | TimeSeries | IdCollection]
    ) -> builtins.list[DatapointsCollectionDatapoints]: ...


# ====================== Events ======================

@final
class Event:
    def __new__(
        cls,
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
    ) -> Event:
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
    def related_resources(self) -> list[IdCollection]:
        """
        The resources this event is attached to, each named by ``id``, ``external_id``, or both.
        Events returned by the API carry both sides, resolved server-side.
        """
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
    def related_resource_nodes(self) -> list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """
        Fetch the resources this event references (its ``related_resources``), resolved via the
        resources service. Blocking; see [``related_resource_nodes_async``] for the awaitable variant.
        """
    async def related_resource_nodes_async(self) -> list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """
        Awaitable variant of [``related_resource_nodes``].
        """


@final
class TimeFilter:
    def __new__(
        cls,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
    ) -> TimeFilter: ...


@final
class EventFilter:
    """AND-combined criteria for ``events.filter`` (``POST /events/filter``).

    ``external_id``, ``source``, ``type``, ``sub_type`` and ``status`` are pattern lists: one
    string or a list of them, where ``*`` and ``%`` are wildcards, ``_`` is literal, and
    matching ignores case -- except that an ``external_id`` entry without a wildcard matches
    exactly, case included. So ``type=["alarm", "warning"]`` is one call. Every ``metadata`` entry must be present, and a ``None``
    value matches the key alone. ``related_resources`` keeps its plural: every entry of it must be
    attached to the event.

    ``data_set_id`` expands down the dataset hierarchy, so naming a parent covers its children.
    **``None`` and ``[]`` differ here**: ``None`` places no restriction, ``[]`` narrows to no
    datasets and matches nothing.

    There is no ``id``: events are keyed by UUID, and the field the api used to declare was typed
    as a long that nothing read. Use ``events.by_ids`` to look one up.
    """
    def __new__(
        cls,
        external_id: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        type: str | Sequence[str] | None = None,
        sub_type: str | Sequence[str] | None = None,
        status: str | Sequence[str] | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        event_time: TimeFilter | None = None,
        metadata: Mapping[str, str | None] | None = None,
        related_resources: list[IdCollection] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
    ) -> EventFilter: ...


@final
class EventIdCollection:
    """
    Event id selector exposed to Python. Events are keyed by a client-generated UUID v7, so this
    carries the ``id`` (UUID) and/or the ``external_id``. Construct with either or both:
    ``EventIdCollection(id=my_uuid)`` or ``EventIdCollection(external_id="...")``.
    """
    def __new__(
        cls,
        id: UUID | None = None,
        external_id: str | None = None,
    ) -> EventIdCollection: ...
    @property
    def id(self) -> UUID | None: ...
    @property
    def external_id(self) -> str | None: ...


@final
class EventUpdate:
    """Field-level changes for one event.

    There is deliberately no ``event_time`` and no ``external_id``: both identify an event rather
    than describe it, and the api dropped each from its update form, so sending either is a ``400``
    naming the field.

    The events table is partitioned by ``event_time``, so the mutation cannot move the row and is
    refused outright. ``externalId`` maps to the *set* of event UUIDs behind it — events sharing an
    external id are the lifecycle of one logical event — so a rename would take every sibling along,
    and an event targeted by UUID left the server without the old value to re-key with. Re-key by
    creating a new event and deleting the old one; record a corrected time the same way.
    """

    def __new__(
        cls,
        event: Event | EventIdCollection | UUID | str,
        description: FieldStr | None = None,
        type: FieldStr | None = None,
        sub_type: FieldStr | None = None,
        status: FieldStr | None = None,
        data_set_id: FieldU64 | None = None,
        metadata: MapField | None = None,
        source: FieldStr | None = None,
        related_resources: ListFieldIdCollection | None = None,
    ) -> EventUpdate: ...
    @property
    def target_id(self) -> UUID | None: ...
    @property
    def target_external_id(self) -> str | None: ...


@final
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
    """Record, find, change and delete events, and read the vocabulary of their categorical fields.

    Reached as ``client.events`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on. Reads return only events in data sets you may
    read. Events are written asynchronously, so a read made straight after a create, update
    or delete can still show the state before it.
    """
    def list(self, limit: int | None = None) -> builtins.list[Event]:
        """List events, oldest event time first.

        This returns the *oldest* ``limit`` events, not the newest: it runs
        :meth:`events.filter` with no criteria, whose default order is ``eventTime``
        ascending. For the most recent events, call :meth:`events.filter` with
        ``sort_by="eventTime", sort_order="desc"``. This is a first page and nothing more:
        there is no cursor to continue from.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of Event

        See Also
        --------
        events.filter : Select by criteria, in any order, with paging.

        Examples
        --------
        >>> earliest = client.events.list(limit=20)
        """
    def create(self, input: builtins.list[Event]) -> builtins.list[Event]:
        """Record new events.

        The batch is all-or-nothing: if any event fails validation, none is stored. The
        ``Event`` objects you pass are not modified; the returned events carry the ``id``,
        ``created_time`` and resolved ``related_resources`` the server assigned.

        ``external_id`` is not unique. Events sharing one are treated as the lifecycle of a
        single logical event, and every call that takes an external id acts on all of them.
        Sending the same event twice therefore stores it twice.

        When the client was built with ``enable_buffering=True`` and the server cannot be
        reached, or answers 401, 403, 408, 429 or 5xx, the call does not raise: the events
        are spooled to disk, the call returns an empty list, and the spool is sent ahead of
        the next ``create``.

        Parameters
        ----------
        input : list of Event
            The events to record. ``type`` and ``event_time`` are required.

        Returns
        -------
        list of Event
            The stored events. Empty when the events were buffered instead of sent.

        Raises
        ------
        DataHubException
            ``status_code`` 400 if ``type`` is blank, a ``data_set_id`` names no data set,
            or a ``related_resources`` entry names no resource or names two different ones
            by ``id`` and ``external_id``; 403 (``problem_slug`` ``"dataset-forbidden"``) if
            you may not write to the event's data set.

        Examples
        --------
        >>> from datetime import datetime, timezone
        >>> from intellistream_datahub_sdk import Event, IdCollection
        >>> [alarm] = client.events.create([
        ...     Event(
        ...         "alarm_pump_a_2026_09_24",
        ...         "alarm",
        ...         datetime(2026, 9, 24, 14, 30, tzinfo=timezone.utc),
        ...         sub_type="overpressure",
        ...         status="open",
        ...         description="Discharge pressure above 40 bar",
        ...         related_resources=[IdCollection(external_id="pump_a")],
        ...         metadata={"severity": "high"},
        ...     )
        ... ])
        >>> alarm.id
        """
    def by_ids(self, input: builtins.list[Event | EventIdCollection | UUID | str]) -> builtins.list[Event]:
        """Fetch events by id or external id.

        An external id returns every event that carries it. What is not found, or is in a
        data set you may not read, is left out of the result rather than raising.

        Parameters
        ----------
        input : list of Event, EventIdCollection, UUID or str
            A ``UUID`` is an event id and a ``str`` an external id. An ``Event`` is looked
            up by its ``id`` when it has one, otherwise by its ``external_id``. Mix freely;
            at most 10000 entries.

        Returns
        -------
        list of Event

        Raises
        ------
        DataHubException
            ``status_code`` 400 for more than 10000 entries; split the batch.

        See Also
        --------
        events.get : One event by id, or ``None``.

        Examples
        --------
        >>> history = client.events.by_ids(["alarm_pump_a_2026_09_24"])
        """
    def get(self, id: UUID) -> Event | None:
        """Fetch one event by id.

        Parameters
        ----------
        id : UUID

        Returns
        -------
        Event or None
            ``None`` if no event has this id, or it is in a data set you may not read. The
            two cases are indistinguishable by design.

        Examples
        --------
        >>> from uuid import UUID
        >>> event = client.events.get(UUID("0195f3a2-4c1b-7f9e-9c3a-1b2d4e6f8a90"))
        """
    def delete(self, input: builtins.list[Event | EventIdCollection | UUID | str]) -> None:
        """Delete events.

        This cannot be undone. An external id deletes every event that carries it. Deleting
        an event that is already gone is a no-op.

        Parameters
        ----------
        input : list of Event, EventIdCollection, UUID or str
            The events to delete. A ``UUID`` is an event id and a ``str`` an external id; an
            ``Event`` is deleted by its ``id`` when it has one, otherwise by its
            ``external_id``.

        Raises
        ------
        DataHubException
            ``status_code`` 403 (``problem_slug`` ``"dataset-forbidden"``) if you may not
            write to an event's data set.

        Examples
        --------
        >>> client.events.delete(["alarm_pump_a_2026_09_24"])
        """
    def update(self, input: builtins.list[EventUpdate]) -> builtins.list[Event]:
        """Change fields on existing events.

        Only the fields named in each ``EventUpdate`` change. ``event_time`` and
        ``external_id`` cannot be changed: to correct either, create a new event and delete
        the old one. An update addressed by external id applies to every event that carries
        it. An update whose event is not found is skipped, so compare the result with what
        you sent.

        While an update is being applied, a read of the same event can briefly return the
        previous version or both. Where the history matters, record a corrective event
        instead of changing the original.

        Parameters
        ----------
        input : list of EventUpdate

        Returns
        -------
        list of Event
            The events as they stand after the update.

        Raises
        ------
        DataHubException
            ``status_code`` 400 if ``type`` is set to null, a ``related_resources`` entry
            names no resource, or a ``data_set_id`` names no data set; 403 (``problem_slug``
            ``"dataset-forbidden"``) if you may not write to the event's current or new data
            set.

        Examples
        --------
        >>> from intellistream_datahub_sdk import EventUpdate, FieldStr, MapField
        >>> client.events.update([
        ...     EventUpdate(
        ...         "alarm_pump_a_2026_09_24",
        ...         status=FieldStr("acknowledged"),
        ...         metadata=MapField.delta(add={"acked_by": "olav"}),
        ...     )
        ... ])
        """
    def filter(
        self,
        *,
        filter: EventFilter | None = None,
        external_id: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        type: str | Sequence[str] | None = None,
        sub_type: str | Sequence[str] | None = None,
        status: str | Sequence[str] | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        event_time: TimeFilter | None = None,
        metadata: Mapping[str, str | None] | None = None,
        related_resources: Sequence[IdCollection] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
        advanced_filter: str | None = None,
    ) -> Page:
        """Select events by criteria, one page at a time.

        Give the criteria either as keywords or as a prepared ``filter=``, not both.
        ``advanced_filter`` combines with either. Criteria combine with AND; the entries of
        one list combine with OR. Pattern lists accept a single string or a list, where ``*``
        and ``%`` are wildcards, ``_`` is literal, and matching ignores case, except that an
        ``external_id`` entry without a wildcard matches exactly, case included.

        ``advanced_filter`` is a boolean expression in a PostgreSQL-flavoured language, ANDed
        with the criteria: ``AND``, ``OR``, ``NOT`` and parentheses over comparisons
        (``=``, ``!=``, ``<>``, ``<``, ``<=``, ``>``, ``>=``), ``LIKE``, ``ILIKE``, ``IN``,
        ``BETWEEN`` and ``IS NULL``. It can name ``id``, ``externalId``, ``type``,
        ``subType``, ``status``, ``source``, ``description``, ``dataSetId``, ``eventTime``,
        ``createdTime`` and ``lastUpdatedTime``, and a metadata value as
        ``metadata['key']``. Metadata values are text; compare them as numbers, booleans or
        times through ``to_number``, ``to_int``, ``to_bool``, ``to_date`` or
        ``to_timestamp``, or a ``::`` cast. ``metadata['key'] IS NULL`` means the key is
        absent. At most 4096 characters; a leading ``WHERE`` is ignored.

        Parameters
        ----------
        filter : EventFilter, optional
            The criteria as one object, reusable across calls. Exclusive with the
            criteria keywords below.
        external_id, source, type, sub_type, status : str or sequence of str, optional
        data_set_id : sequence of int, str or IdCollection, optional
            Data sets by id or external id; includes everything beneath them in the data
            set hierarchy. An empty list matches nothing.
        event_time, created_time, last_updated_time : TimeFilter, optional
            Inclusive at both ends. ``event_time`` is when the event happened;
            ``created_time`` is when it was recorded.
        metadata : mapping of str to str or None, optional
            Every key listed must be present; a ``None`` value matches the key alone.
        related_resources : sequence of IdCollection, optional
            Every resource listed must be attached to the event.
        limit : int, optional
            Page size. Defaults to 100; above 10000 is refused.
        sort_by : str, optional
            One of ``eventTime`` (the default), ``createdTime``, ``lastUpdatedTime``,
            ``externalId``, ``type``, ``subType``, ``status``, ``source`` or
            ``dataSetId``. Events without a value sort last ascending, first descending.
        sort_order : {"asc", "desc"}, optional
            Defaults to ascending.
        cursor : str, optional
            ``next_cursor`` from the previous page. It belongs to its sort: continuing it
            under another is refused.
        advanced_filter : str, optional
            A filter expression, as described above.

        Returns
        -------
        Page
            A list-like page of ``Event``. Its ``next_cursor`` is ``None`` on the last page.

        Raises
        ------
        TypeError
            If both ``filter=`` and criteria keywords are given.
        DataHubException
            ``status_code`` 400 with ``problem_slug`` ``"filter-expression"`` if
            ``advanced_filter`` does not parse; the problem's ``offset`` says where. 400
            with ``"malformed-cursor"`` for an unreadable cursor or one from another sort.

        See Also
        --------
        events.search : Find events by a phrase.

        Examples
        --------
        Open alarms and warnings on one pump in September, a page at a time:

        >>> from datetime import datetime, timezone
        >>> from intellistream_datahub_sdk import IdCollection, TimeFilter
        >>> september = TimeFilter(
        ...     start=datetime(2026, 9, 1, tzinfo=timezone.utc),
        ...     end=datetime(2026, 10, 1, tzinfo=timezone.utc),
        ... )
        >>> criteria = dict(
        ...     type=["alarm", "warning"],
        ...     status="open",
        ...     event_time=september,
        ...     related_resources=[IdCollection(external_id="pump_a")],
        ... )
        >>> page = client.events.filter(**criteria, limit=500)
        >>> events = list(page)
        >>> while page.next_cursor:
        ...     page = client.events.filter(**criteria, limit=500, cursor=page.next_cursor)
        ...     events.extend(page)

        The 50 most recent events whose ``severity`` metadata is at least 3:

        >>> client.events.filter(
        ...     advanced_filter="to_int(metadata['severity']) >= 3",
        ...     sort_by="eventTime",
        ...     sort_order="desc",
        ...     limit=50,
        ... )
        """

    def search(
        self,
        query: str,
        filter: EventFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Event]:
        """Find events whose external id, description or a metadata value contains a phrase.

        Matching is a case-insensitive substring match, not word-aware: ``"pump"`` finds
        ``"pumps"`` but not ``"pumping"``. Results are not ranked; they come newest event
        time first. For structured queries without a phrase, use :meth:`events.filter`.

        Parameters
        ----------
        query : str
            The phrase, 3--140 characters.
        filter : EventFilter, optional
            Narrows the phrase's hits. It only ever removes results, never adds them.
        limit : int, optional
            How many to return. Defaults to 100; above 1000 is refused.

        Returns
        -------
        list of Event

        Examples
        --------
        >>> from intellistream_datahub_sdk import EventFilter
        >>> client.events.search("bearing", filter=EventFilter(type="alarm", status="open"))
        """
    def count(self) -> int:
        """Count the events in the tenant.

        The count takes no criteria and, unlike every read in this service, is not narrowed
        to data sets you may read. For a count of matching events, page through
        :meth:`events.filter`.

        Returns
        -------
        int

        Examples
        --------
        >>> total = client.events.count()
        """
    def list_dimension(
        self,
        dimension: EventDimension,
        query: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[str]:
        """List the distinct values a categorical event field takes.

        Values are sorted alphabetically and drawn only from events in data sets you may
        read. They are eventually consistent with the events: a new value can take a moment
        to appear, and a value no event carries any more can linger. Good for a picker or a
        type-ahead; not proof that an event with the value exists right now.

        The eight ``list_*`` and ``search_*`` methods are shorthands for this one.

        Parameters
        ----------
        dimension : EventDimension
            ``EventDimension.TYPE``, ``SUB_TYPE``, ``STATUS`` or ``SOURCE``.
        query : str, optional
            Keep only values containing this, ignoring case. Omit to list every value.
        limit : int, optional
            How many to return. Defaults to 1000; a value outside 1--10000 is moved to the
            nearest end of that range rather than refused.

        Returns
        -------
        list of str

        Examples
        --------
        >>> from intellistream_datahub_sdk import EventDimension
        >>> client.events.list_dimension(EventDimension.STATUS, query="ack")
        """
    def list_types(self, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``type`` values of events you can read.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        events.search_types : Only the values containing a phrase.

        Examples
        --------
        >>> types = client.events.list_types()
        """
    def search_types(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``type`` values that contain a phrase, ignoring case.

        Parameters
        ----------
        query : str
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.

        Examples
        --------
        >>> client.events.search_types("alarm")
        """
    def list_sub_types(self, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``sub_type`` values of events you can read.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        events.search_sub_types : Only the values containing a phrase.
        """
    def search_sub_types(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``sub_type`` values that contain a phrase, ignoring case.

        Parameters
        ----------
        query : str
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        """
    def list_statuses(self, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``status`` values of events you can read.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        events.search_statuses : Only the values containing a phrase.
        """
    def search_statuses(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``status`` values that contain a phrase, ignoring case.

        Parameters
        ----------
        query : str
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        """
    def list_sources(self, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``source`` values of events you can read.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        events.search_sources : Only the values containing a phrase.
        """
    def search_sources(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """List the distinct ``source`` values that contain a phrase, ignoring case.

        Parameters
        ----------
        query : str
        limit : int, optional
            How many to return. Defaults to 1000; kept within 1--10000.

        Returns
        -------
        list of str
            Sorted alphabetically.

        See Also
        --------
        events.list_dimension : How these values are kept, and their consistency.
        """


class EventsServiceAsync:
    async def list(self, limit: int | None = None) -> builtins.list[Event]: ...
    async def create(self, input: builtins.list[Event]) -> builtins.list[Event]: ...
    async def by_ids(self, input: builtins.list[Event | EventIdCollection | UUID | str]) -> builtins.list[Event]: ...
    async def get(self, id: UUID) -> Event | None: ...
    async def delete(self, input: builtins.list[Event | EventIdCollection | UUID | str]) -> None: ...
    async def update(self, input: builtins.list[EventUpdate]) -> builtins.list[Event]: ...
    async def filter(
        self,
        *,
        filter: EventFilter | None = None,
        external_id: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        type: str | Sequence[str] | None = None,
        sub_type: str | Sequence[str] | None = None,
        status: str | Sequence[str] | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        event_time: TimeFilter | None = None,
        metadata: Mapping[str, str | None] | None = None,
        related_resources: Sequence[IdCollection] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
        advanced_filter: str | None = None,
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
    ) -> builtins.list[Event]: ...
    async def count(self) -> int: ...
    async def list_dimension(
        self,
        dimension: EventDimension,
        query: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[str]: ...
    async def list_types(self, limit: int | None = None) -> builtins.list[str]: ...
    async def search_types(self, query: str, limit: int | None = None) -> builtins.list[str]: ...
    async def list_sub_types(self, limit: int | None = None) -> builtins.list[str]: ...
    async def search_sub_types(self, query: str, limit: int | None = None) -> builtins.list[str]: ...
    async def list_statuses(self, limit: int | None = None) -> builtins.list[str]: ...
    async def search_statuses(self, query: str, limit: int | None = None) -> builtins.list[str]: ...
    async def list_sources(self, limit: int | None = None) -> builtins.list[str]: ...
    async def search_sources(self, query: str, limit: int | None = None) -> builtins.list[str]: ...


# ====================== Datasets ======================

@final
class Dataset:
    def __new__(
        cls,
        external_id: str,
        name: str | None = None,
        id: int | None = None,
        description: str | None = None,
        policies: list[str] | None = None,
        metadata: dict[str, str] | None = None,
        connected_data_sets: list[int] | None = None,
    ) -> Dataset: ...
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
    ) -> ResourceNetwork:
        """
        Walk the graph from this dataset and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters which
        edge types to follow (``None`` = all); ``limit`` caps the node count. Neighbour nodes are
        modelled as ``Resource``. Blocking; see [``neighbors_async``] for the awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """
        Awaitable variant of [``neighbors``].
        """
    def related_events(self, limit: int = 100) -> list[Event]:
        """
        Fetch events whose ``related_resources`` include this
        dataset (matched by graph-node id when present, else external id), via ``events.filter``.
        ``limit`` caps the results (default 100). Blocking; see [``related_events_async``].
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """
        Awaitable variant of [``related_events``].
        """


# Criteria for `datasets.filter`. Every field is optional and they AND together, so an
# argument-free DatasetFilter() places no restriction. An *empty* list or dict is likewise
# no restriction rather than "match nothing".
#
# See the class docstring below for the pattern, label and metadata rules.
@final
class DatasetFilter:
    """AND-combined criteria for ``datasets.filter``.

    ``external_id``, ``name`` and ``source`` are pattern lists: one string or a list of them,
    where ``*`` and ``%`` are wildcards, ``_`` is literal, and matching ignores case. So
    ``external_id=["sap_*"]`` replaces the retired ``external_id_prefix`` and can be combined
    with exact ids in the same list. ``labels`` must **all** be present; names are canonicalised,
    so ``"pump a"`` finds the label stored as ``PUMP_A``. Every ``metadata`` entry must be present,
    and a ``None`` value matches the key alone.

    There is no ``data_set_id``: a dataset is the thing other nodes are scoped by, and no
    ``write_protected`` / ``deactivated`` either — both were removed server-side as inert, so a
    filter carrying them looked like it was narrowing and was not.
    """
    def __new__(
        cls,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
    ) -> DatasetFilter: ...


@final
class DatasetUpdate:
    """
    A partial update for one dataset, mirroring the server's update form.

    ``dataset`` names the target — a ``Dataset``, an ``IdCollection``, an external id or a numeric id.
    Every other argument is a field wrapper and only the ones you pass are sent; anything omitted
    is left untouched.

    There is deliberately no ``policies`` or ``connected_data_sets`` here: the update endpoint does not
    accept them, whatever a ``Dataset`` can carry on create.
    """
    def __new__(
        cls,
        dataset: int | str | Dataset | IdCollection,
        external_id: FieldStr | None = None,
        name: FieldStr | None = None,
        description: FieldStr | None = None,
        metadata: MapField | None = None,
        labels: ListFieldStr | None = None,
    ) -> DatasetUpdate: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def target_external_id(self) -> str | None: ...


class DatasetsServiceSync:
    """Create, find, change and delete data sets.

    Reached as ``client.datasets`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    A data set is the unit access is granted on, so managing one is an operator action:
    :meth:`datasets.create`, :meth:`datasets.update` and :meth:`datasets.delete` need an
    all-datasets write grant, and a grant on individual data sets is not enough. The reads are
    not narrowed by grants: every caller sees every data set in the tenant.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """
    def list(self, limit: int | None = None) -> builtins.list[Dataset]:
        """List data sets, newest created first.

        This is a first page and nothing more: there is no cursor to continue from. Data sets
        are usually few, so this is often all of them; to go further, narrow the query with
        :meth:`datasets.filter` rather than raising ``limit``.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of Dataset

        See Also
        --------
        datasets.filter : Select by criteria, with paging.

        Examples
        --------
        >>> datasets = client.datasets.list()
        """
    def create(self, input: builtins.list[Dataset]) -> builtins.list[Dataset]:
        """Store new data sets.

        Each ``external_id`` must be unused in the tenant. Other nodes join a data set through
        their own ``data_set_id``.

        Parameters
        ----------
        input : list of Dataset
            The data sets to create. ``name`` defaults to the ``external_id`` when not given.

        Returns
        -------
        list of Dataset
            The stored data sets, with ``id`` and timestamps filled in by the server.

        Raises
        ------
        DataHubException
            ``status_code`` 403 (``problem_slug`` ``"dataset-forbidden"``) without an
            all-datasets write grant; 409 if an ``external_id`` is already taken, with the
            problem's ``duplicated`` member naming it.

        Examples
        --------
        >>> from intellistream_datahub_sdk import Dataset
        >>> [sap] = client.datasets.create([
        ...     Dataset(
        ...         external_id="sap_work_orders",
        ...         name="SAP work orders",
        ...         description="Work orders mirrored from SAP",
        ...     )
        ... ])
        """
    def by_ids(self, input: builtins.list[int | str | Dataset | IdCollection]) -> builtins.list[Dataset]:
        """Fetch data sets by id or external id.

        What is not found is left out of the result rather than raising, so compare the
        result with what you asked for to detect missing data sets.

        Parameters
        ----------
        input : list of int, str, Dataset or IdCollection
            Each entry is a numeric id, an external id, a ``Dataset`` or an ``IdCollection``.
            Mix freely.

        Returns
        -------
        list of Dataset

        Examples
        --------
        >>> found = client.datasets.by_ids(["sap_work_orders", 12])
        """
    def delete(self, input: builtins.list[int | str | Dataset | IdCollection]) -> None:
        """Delete data sets.

        This cannot be undone, and it does not cascade: delete or move what belongs to a data
        set, child data sets included, before deleting the data set itself. The relationships
        the data set takes part in go with it. The ids are not checked to be data sets: a node
        of another type with a matching id or external id is deleted too.

        Parameters
        ----------
        input : list of int, str, Dataset or IdCollection
            The data sets to delete, by id, external id, ``Dataset`` or ``IdCollection``.

        Raises
        ------
        DataHubException
            ``status_code`` 403 (``problem_slug`` ``"dataset-forbidden"``) without an
            all-datasets write grant. 409 while anything still belongs to the data set; with
            ``problem_slug`` ``"would-strand"``, the problem's ``blockedBy`` names the nodes
            the delete would disconnect from the graph. Nothing is deleted in either case.

        Examples
        --------
        >>> client.datasets.delete(["sap_work_orders"])
        """
    def filter(
        self,
        *,
        filter: DatasetFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Select data sets by criteria, one page at a time.

        Give the criteria either as keywords or as a prepared ``filter=``, not both. Criteria
        combine with AND; the entries of one list combine with OR. Pattern lists accept a
        single string or a list, where ``*`` and ``%`` are wildcards, ``_`` is literal, and
        matching ignores case. With no criteria at all, every data set matches.

        Parameters
        ----------
        filter : DatasetFilter, optional
            The criteria as one object, reusable across calls. Exclusive with the
            criteria keywords below.
        id : sequence of int, optional
        external_id, name, source : str or sequence of str, optional
        labels : str or sequence of str, optional
            Every label listed must be present. Names are canonicalised, so ``"pump a"``
            finds the label stored as ``PUMP_A``.
        metadata : mapping of str to str or None, optional
            Every key listed must be present; a ``None`` value matches the key alone.
        created_time, last_updated_time : TimeFilter, optional
            Inclusive at both ends.
        limit : int, optional
            Page size. Defaults to 100; above 10000 is refused.
        sort_by : str, optional
            One property; defaults to ``createdTime``.
        sort_order : {"asc", "desc"}, optional
            Defaults to descending.
        cursor : str, optional
            ``next_cursor`` from the previous page. It belongs to its sort: continuing it
            under another is refused.

        Returns
        -------
        Page
            A list-like page of ``Dataset``. Its ``next_cursor`` is ``None`` on the last
            page.

        Raises
        ------
        TypeError
            If both ``filter=`` and criteria keywords are given.

        See Also
        --------
        datasets.search : Free-text search, ranked.

        Examples
        --------
        Every SAP data set owned by plant A:

        >>> page = client.datasets.filter(external_id="sap_*", metadata={"owner": "plant_a"})
        >>> datasets = list(page)
        >>> while page.next_cursor:
        ...     page = client.datasets.filter(
        ...         external_id="sap_*", metadata={"owner": "plant_a"}, cursor=page.next_cursor
        ...     )
        ...     datasets.extend(page)
        """

    def search(
        self,
        query: str,
        filter: DatasetFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Dataset]:
        """Free-text search over name, external id and description.

        Matching is word-aware and fuzzy, and the last word also matches as a prefix, so a
        phrase typed mid-word still finds its data set. Results are ranked, best match first.
        No match is an empty list. For exact lookups use :meth:`datasets.by_ids`; for
        structured queries without a phrase, use :meth:`datasets.filter`.

        Parameters
        ----------
        query : str
            The phrase, 3--140 characters.
        filter : DatasetFilter, optional
            Narrows the phrase's hits. It only ever removes results, never adds them.
        limit : int, optional
            How many to return. Defaults to 100; above 1000 is refused.

        Returns
        -------
        list of Dataset

        Examples
        --------
        >>> from intellistream_datahub_sdk import DatasetFilter
        >>> client.datasets.search("work orders", filter=DatasetFilter(source="sap"))
        """
    def update(self, input: builtins.list[DatasetUpdate]) -> builtins.list[Dataset]:
        """Change fields on existing data sets.

        Only the fields named in each ``DatasetUpdate`` change. The batch is all-or-nothing:
        if one update fails, none is applied. A ``labels`` change cannot remove the data set's
        intrinsic ``DATASET`` label.

        Parameters
        ----------
        input : list of DatasetUpdate

        Returns
        -------
        list of Dataset
            The data sets as they stand after the update.

        Raises
        ------
        DataHubException
            ``status_code`` 403 (``problem_slug`` ``"dataset-forbidden"``) without an
            all-datasets write grant; 400 if a target data set does not exist; 409 if a new
            ``external_id`` is already taken, with the problem's ``duplicated`` member
            naming it.

        Examples
        --------
        >>> from intellistream_datahub_sdk import DatasetUpdate, FieldStr, MapField
        >>> client.datasets.update([
        ...     DatasetUpdate(
        ...         "sap_work_orders",
        ...         description=FieldStr("SAP work orders, live sync"),
        ...         metadata=MapField.delta(add={"owner": "plant_a"}),
        ...     )
        ... ])
        """
    def policies(self) -> builtins.list[Resource]:
        """List the access policies a data set can be associated with.

        Every policy in the tenant, for offering as choices for ``Dataset.policies``.

        This has been observed to come back empty while policies exist, so treat an empty
        result as inconclusive rather than as "no policies".

        Returns
        -------
        list of Resource
            One per policy.

        Examples
        --------
        >>> names = [p.external_id for p in client.datasets.policies()]
        """


class DatasetsServiceAsync:
    async def list(self, limit: int | None = None) -> builtins.list[Dataset]: ...
    async def create(self, input: builtins.list[Dataset]) -> builtins.list[Dataset]: ...
    async def by_ids(self, input: builtins.list[int | str | Dataset | IdCollection]) -> builtins.list[Dataset]: ...
    async def delete(self, input: builtins.list[int | str | Dataset | IdCollection]) -> None: ...
    async def filter(
        self,
        *,
        filter: DatasetFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
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
    ) -> builtins.list[Dataset]: ...
    async def update(self, input: builtins.list[DatasetUpdate]) -> builtins.list[Dataset]: ...
    async def policies(self) -> builtins.list[Resource]: ...


# ====================== Resources ======================

@final
class Resource:
    def __new__(
        cls,
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
    ) -> Resource: ...
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
    def geolocation(self) -> dict[str, Any] | None:
        """
        The GeoJSON geometry as a Python ``dict`` (e.g.
        ``{"type": "Point", "coordinates": [10.75, 59.91]}``), or ``None``.
        """
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
    ) -> ResourceNetwork:
        """
        Walk the graph from this resource and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters which
        edge types to follow (``None`` = all); ``limit`` caps the node count. Blocking; see
        [``neighbors_async``] for the awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """
        Awaitable variant of [``neighbors``].
        """
    def related_events(self, limit: int = 100) -> list[Event]:
        """
        Fetch events whose ``related_resources`` include this
        resource (matched by graph-node id when present, else external id), via ``events.filter``.
        ``limit`` caps the results (default 100). Blocking; see [``related_events_async``].
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """
        Awaitable variant of [``related_events``].
        """


@final
class Asset:
    """A resource that carries a geographic location.

    Assets and plain resources share a field set; the API tells them apart by the intrinsic
    "ASSET" type-label, and only an asset ever has its `geolocation` echoed back on a read.
    """

    def __new__(
        cls,
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
    ) -> Asset: ...
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


@final
class Policy:
    """An access policy, as a node.

    Sparse on every read: the API never sends a policy's `value`, `template_id` or
    `data_set_id` back, so those are always None on an object that came from the server.
    """

    def __new__(
        cls,
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
    ) -> Policy: ...
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


@final
class ResourceNetwork:
    """Connected sub-graph returned by `Resource.neighbors` (and the timeseries/dataset/
    function equivalents): the reachable `nodes`, the `edges` between them, and their
    `labels`."""
    @property
    def nodes(self) -> list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """
        The nodes in the traversed sub-graph, each as its own class (``Asset``, ``TimeSeries``,
        ``Dataset``, …). Typed but sparse — the graph carries only a subset of each node's columns.
        """
    @property
    def edges(self) -> list[EdgeProxy]: ...
    @property
    def labels(self) -> list[Label]: ...


# ====================== Relations ======================
# NOTE: `RelForm` is the request-side edge form for resource create. `RelatedNode`
# is the unified node-centric relation carried by every node type (Resource,
# TimeSeries, Function, ...). `EdgeProxy` is the full edge detail in graph responses.

@final
class EdgeProxy:
    """Server-assigned edge between two resources. The `relationship_type`
    attribute maps to the wire field `"type"`."""
    def __new__(
        cls,
        id: int | None = None,
        start: int | None = None,
        end: int | None = None,
        relationship_type: str | None = None,
        description: str | None = None,
        relationship_type_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> EdgeProxy: ...
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


@final
class RelForm:
    """Request-side edge form. Pair with a list of `Resource` and pass both to
    `ResourcesService.create()`. `relationship_type` is keyword-required."""
    def __new__(
        cls,
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
    ) -> RelForm: ...
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


@final
class GraphResult:
    """Nodes and relations returned from a graph operation."""
    @property
    def nodes(self) -> list[Asset | TimeSeries | Function | Resource | Dataset | Policy]: ...
    @property
    def relations(self) -> list[EdgeProxy]: ...




@final
class ResourceUpdate:
    """One resource's update for `resources.update`. Target the resource by a `Resource`, its
    numeric id, or its external id; every field is optional and uses the same wrappers as the
    other update APIs (`FieldStr` for scalars, `ListFieldStr` for labels, `MapField` for
    metadata). Mirrors `TimeSeriesUpdate`."""
    def __new__(
        cls,
        resource: int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy,
        external_id: FieldStr | None = None,
        name: FieldStr | None = None,
        description: FieldStr | None = None,
        data_set_id: FieldU64 | None = None,
        metadata: MapField | None = None,
        source: FieldStr | None = None,
        labels: ListFieldStr | None = None,
        geolocation: FieldGeoJson | None = None,
    ) -> ResourceUpdate: ...
    @property
    def target_id(self) -> int | None: ...
    @property
    def target_external_id(self) -> str | None: ...
    @property
    def labels(self) -> ListFieldStr | None: ...


@final
class ResourceFilter:
    """AND-combined criteria for ``resources.filter`` and the ``filter`` of ``resources.search``.

    The same arguments ``resources.filter`` takes as keywords, in an object — which is what
    ``search`` needs, since a filter passed positionally there would be indistinguishable from the
    search form.
    """
    def __new__(
        cls,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        node_type: str | Sequence[str] | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
    ) -> ResourceFilter: ...


class ResourcesServiceSync:
    """Create, find, change and delete nodes of every type, and the relationships between them.

    Reached as ``client.resources`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    This is the generic node service. Its reads span all six node types -- assets, time
    series, functions, plain resources, data sets and policies -- and return each node as its
    own class, so ``isinstance(node, TimeSeries)`` works on what comes back and every node has a
    ``node_type`` string to dispatch on. The typed services (:meth:`timeseries.filter`,
    :meth:`assets.filter`, ...) answer for one type each.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """
    def list(self, limit: int | None = None) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """List nodes of every type, newest created first.

        Only nodes in data sets you may read are returned. This is a first page and nothing
        more: there is no cursor to continue from. To go further, narrow the query with
        :meth:`resources.filter` rather than raising ``limit``.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of Asset, TimeSeries, Function, Resource, Dataset or Policy
            Each node as its own class.

        See Also
        --------
        resources.filter : Select by criteria, with paging.

        Examples
        --------
        >>> from collections import Counter
        >>> recent = client.resources.list(limit=100)
        >>> Counter(node.node_type for node in recent)
        """
    def create(
        self, nodes: builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy], relations: builtins.list[RelForm] | None = None
    ) -> GraphResult:
        """Store new nodes of any type, and optionally the relationships between them.

        Each node is created as the type of its class: an ``Asset`` becomes an asset, a
        ``TimeSeries`` a time series, and so on. A relation may name nodes created in the same
        call by external id, or nodes that already exist. The batch is all-or-nothing: if any
        node or relation is refused, nothing is created.

        Every node needs an ``external_id`` (3--256 characters, unused among the tenant's
        nodes, ignoring case) and a ``name`` (3--512 characters). A plain ``Resource`` also needs
        at least one label of its own; the other classes carry their type label already.
        Hierarchy between data sets is expressed with relations, not with ``data_set_id``.

        A ``relationship_type`` that does not exist yet is created on the fly. Relationship
        types cannot be deleted, so a misspelt one stays in the catalogue for good.

        Parameters
        ----------
        nodes : list of Asset, TimeSeries, Function, Resource, Dataset or Policy
            The nodes to create; types may be mixed.
        relations : list of RelForm, optional
            The relationships to create between them.

        Returns
        -------
        GraphResult
            ``nodes`` holds the stored nodes, each as its own class, with ``id`` filled in and
            ``related_resources`` listing the relations created in this call. ``relations``
            holds each relation as an ``EdgeProxy`` with its server-assigned ``id``.

        Raises
        ------
        DataHubException
            ``status_code`` 400 for a node or relation that fails validation, a relation
            naming a node that does not exist, or a ``Dataset`` or ``Policy`` carrying a
            ``data_set_id``; 403 when you may not write the node's data set, or when you create
            a ``Dataset`` or ``Policy`` without the grant to manage all data sets; 409 if an
            ``external_id`` is already taken, the problem's ``duplicated`` member naming it.

        See Also
        --------
        edges.create : Link nodes that already exist.

        Examples
        --------
        A site, a pump under it, and the pump's pressure series, linked in one call:

        >>> from intellistream_datahub_sdk import Asset, RelForm, Resource, TimeSeries
        >>> result = client.resources.create(
        ...     [
        ...         Resource(name="Site North", external_id="site_north", labels=["SITE"],
        ...                  is_root=True),
        ...         Asset(name="Pump A", external_id="pump_a", labels=["PUMP"]),
        ...         TimeSeries(external_id="pump_a_pressure", name="Pump A pressure",
        ...                    value_type="float", unit="bar"),
        ...     ],
        ...     [
        ...         RelForm.by_external_ids("site_north", "pump_a", "HAS_PART"),
        ...         RelForm.by_external_ids("pump_a", "pump_a_pressure", "HAS_TIMESERIES"),
        ...     ],
        ... )
        >>> [node.node_type for node in result.nodes]
        """
    def by_ids(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """Fetch nodes of any type by id or external id.

        What is not found, or lies in a data set you may not read, is left out of the result
        rather than raising, so compare the result with what you asked for to detect missing
        nodes. ``related_resources`` is empty on every node returned; call a node's
        ``neighbors`` for the graph around it.

        Parameters
        ----------
        input : list of int, str, Asset, TimeSeries, Function, Resource, Dataset or Policy
            Each entry is a numeric id, an external id, or a node object. Mix freely.

        Returns
        -------
        list of Asset, TimeSeries, Function, Resource, Dataset or Policy
            Each node as its own class.

        Examples
        --------
        >>> found = client.resources.by_ids(["site_north", "pump_a_pressure", 42])
        """
    def delete(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> None:
        """Delete nodes of any type, and every relationship they take part in.

        This cannot be undone. Identifiers that match nothing are skipped, so deleting a node
        that is already gone is a no-op. The batch is all-or-nothing: if any node is refused,
        nothing is deleted.

        A delete is refused when it would disconnect a surviving node from the graph root.
        Include the stranded nodes in the same call, or keep another path to them. The check
        reads a view of the graph that lags writes by a moment, so a node deleted immediately
        after its relationships were created can be let through and strand its neighbours;
        wait briefly after creating relationships before deleting across them.

        Parameters
        ----------
        input : list of int, str, Asset, TimeSeries, Function, Resource, Dataset or Policy
            The nodes to delete, by id, external id or node object. A node object is matched
            by its ``id`` when it has one.

        Raises
        ------
        DataHubException
            ``status_code`` 403 when you may not write a node's data set, or delete a
            ``Dataset`` or ``Policy`` without the grant to manage all data sets. 409 with
            ``problem_slug`` ``"would-strand"`` when the delete would disconnect a surviving
            node, the problem's ``blockedBy`` naming it; ``"referenced"`` when a time series is
            still bound to a subscription, ``blockedBy`` naming the subscription.

        See Also
        --------
        edges.delete : Remove one relationship and keep both nodes.

        Examples
        --------
        >>> from intellistream_datahub_sdk import DataHubException
        >>> try:
        ...     client.resources.delete(["pump_a"])
        ... except DataHubException as e:
        ...     if e.problem_slug != "would-strand":
        ...         raise
        ...     stranded = [b["externalId"] for b in e.problem["blockedBy"]]
        ...     client.resources.delete(["pump_a", *stranded])
        """
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """Free-text search over nodes of every type, by name, external id and description.

        Matching is word-aware and fuzzy -- ``"pump"`` also finds ``"pumps"`` -- and results
        are ranked, best match first. For exact lookups use :meth:`resources.by_ids`; for
        structured queries without a phrase, use :meth:`resources.filter`.

        Parameters
        ----------
        query : str
            The phrase, 3--140 characters.
        filter : ResourceFilter, optional
            Narrows the phrase's hits, for instance to one node type. It only ever removes
            results, never adds them.
        limit : int, optional
            How many to return. Defaults to 100; above 1000 is refused.

        Returns
        -------
        list of Asset, TimeSeries, Function, Resource, Dataset or Policy
            Each node as its own class.

        Examples
        --------
        >>> from intellistream_datahub_sdk import ResourceFilter
        >>> client.resources.search("feed pump", filter=ResourceFilter(node_type="asset"))
        """
    def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult:
        """Change fields on existing nodes of any type.

        Only the fields named in each ``ResourceUpdate`` change. The fields it offers are
        shared by every node type, except ``geolocation``, which is stored on assets only and
        ignored on the rest. A node's type cannot be changed: its type label stays in place
        whatever ``labels`` says. The batch is all-or-nothing.

        Parameters
        ----------
        input : list of ResourceUpdate

        Returns
        -------
        GraphResult
            ``nodes`` holds the nodes as they stand after the update, each as its own class.
            ``related_resources`` is empty on them, and ``relations`` is empty.

        Raises
        ------
        DataHubException
            ``status_code`` 400 if a target does not exist, or ``name`` or ``external_id`` is
            set to null; 409 if a new ``external_id`` is already taken, or the node changed
            under a concurrent write -- re-read it and retry.

        See Also
        --------
        timeseries.update : Also changes what only a time series has, such as ``unit``.

        Examples
        --------
        >>> from intellistream_datahub_sdk import FieldStr, ListFieldStr, MapField, ResourceUpdate
        >>> result = client.resources.update([
        ...     ResourceUpdate(
        ...         "pump_a",
        ...         description=FieldStr("Main feed pump"),
        ...         metadata=MapField.delta(add={"vendor": "Grundfos"}),
        ...         labels=ListFieldStr.delta(add=["CRITICAL"]),
        ...     )
        ... ])
        """
    def get_by_id(self, id: int) -> Asset | TimeSeries | Function | Resource | Dataset | Policy | None:
        """Fetch one node of any type by numeric id.

        Unlike :meth:`resources.by_ids`, a miss raises. A node in a data set you may not read
        is reported as missing, so a 404 does not tell you the id is free.

        Parameters
        ----------
        id : int

        Returns
        -------
        Asset, TimeSeries, Function, Resource, Dataset or Policy
            The node, as its own class.

        Raises
        ------
        DataHubException
            ``status_code`` 404 with ``problem_slug`` ``"not-found"`` if no node you may read
            has this id.

        Examples
        --------
        >>> node = client.resources.get_by_id(42)
        """
    def filter(
        self,
        filter: ResourceFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        node_type: str | Sequence[str] | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Select nodes of any type by criteria, one page at a time.

        Spans every node type unless narrowed with ``node_type``. Give the criteria either as
        keywords or as a prepared ``filter=``, not both. Criteria combine with AND; the entries
        of one list combine with OR. Pattern lists accept a single string or a list, where
        ``*`` and ``%`` are wildcards, ``_`` is literal, and matching ignores case.

        Parameters
        ----------
        filter : ResourceFilter, optional
            The criteria as one object, reusable across calls. Exclusive with the
            criteria keywords below.
        id : sequence of int, optional
        external_id, name, source : str or sequence of str, optional
        labels : str or sequence of str, optional
            Every label listed must be present.
        metadata : mapping of str to str or None, optional
            Every key listed must be present; a ``None`` value matches the key alone.
        created_time, last_updated_time : TimeFilter, optional
            Inclusive at both ends.
        node_type : str or sequence of str, optional
            Any of ``"asset"``, ``"timeseries"``, ``"function"``, ``"resource"``,
            ``"dataset"`` and ``"policy"``, ignoring case. Omitted means every type; a list
            of only unknown names matches nothing.
        is_root : bool, optional
            Match on the ``is_root`` flag. Only resources and assets can be roots; every
            other node matches ``False``.
        data_set_id : sequence of int, str or IdCollection, optional
            Data sets by id or external id; includes everything beneath them in the data
            set hierarchy. ``None`` places no restriction, but ``[]`` matches nothing.
        limit : int, optional
            Page size. Defaults to 1000; above 10000 is refused.
        sort_by : str, optional
            One of ``id``, ``externalId``, ``name``, ``source``, ``description``,
            ``createdTime``, ``lastUpdatedTime`` and ``dataSetId``; defaults to
            ``createdTime``.
        sort_order : {"asc", "desc"}, optional
            Defaults to descending.
        cursor : str, optional
            ``next_cursor`` from the previous page. It belongs to its sort: continuing it
            under another is refused.

        Returns
        -------
        Page
            A list-like page of nodes, each as its own class. Its ``next_cursor`` is ``None``
            on the last page.

        Raises
        ------
        TypeError
            If both ``filter=`` and criteria keywords are given.

        See Also
        --------
        assets.filter : The same criteria, answering with assets only.
        timeseries.filter : Adds the criteria only a time series has.

        Examples
        --------
        Pump assets and pump series in one query, split by class:

        >>> from intellistream_datahub_sdk import Asset, TimeSeries
        >>> page = client.resources.filter(
        ...     external_id="pump_*", node_type=["asset", "timeseries"], limit=500
        ... )
        >>> assets = [n for n in page if isinstance(n, Asset)]
        >>> series = [n for n in page if isinstance(n, TimeSeries)]
        """


class ResourcesServiceAsync:
    async def list(self, limit: int | None = None) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]: ...
    async def create(
        self, nodes: builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy], relations: builtins.list[RelForm] | None = None
    ) -> GraphResult: ...
    async def by_ids(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]: ...
    async def delete(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> None: ...
    async def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]: ...
    async def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult: ...
    async def get_by_id(self, id: int) -> Asset | TimeSeries | Function | Resource | Dataset | Policy | None: ...
    async def filter(
        self,
        filter: ResourceFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        node_type: str | Sequence[str] | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Async twin of ``ResourcesServiceSync.filter``."""


# ====================== Labels ======================

@final
class Label:
    """A DataHub label. `name` is the identifier you set (3–512 chars, canonicalised to
    SNAKE_UPPER_CASE server-side); `id`/`color` are usually assigned by the server. Also the
    shape returned inside a `ResourceNetwork` from `resources.fetch_related` (there
    `color`/`i18n_code` are `None`)."""
    def __new__(
        cls,
        name: str | None = None,
        id: int | None = None,
        description: str | None = None,
        color: str | None = None,
        i18n_code: str | None = None,
    ) -> Label: ...
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


@final
class LabelsServiceSync:
    """Create, list, change and delete the tenant's labels.

    Reached as ``client.labels`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    A label comes into being the first time a node is written with it, so this service is
    for pre-seeding a label's description, colour or i18n code, for renaming, and for
    cleanup. Names are canonicalised to upper snake case, so ``"pump a"`` and ``"PUMP_A"``
    are the same label. The type-labels (``ASSET``, ``TIMESERIES``, ``FUNCTION``,
    ``DATASET``, ``POLICY``) are listed like any other but cannot be renamed or deleted.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """
    def list(self) -> builtins.list[Label]:
        """List every label in the tenant.

        Labels are a small set, so this returns all of them in one call.

        Returns
        -------
        list of Label

        Examples
        --------
        >>> names = [label.name for label in client.labels.list()]
        """
    def get(self, id: int) -> Label | None:
        """Fetch one label by numeric id.

        Parameters
        ----------
        id : int
            The label's ``id``.

        Returns
        -------
        Label or None
            ``None`` if no label has this id.

        Examples
        --------
        >>> label = client.labels.get(17)
        """
    def create(self, input: builtins.list[Label]) -> builtins.list[Label]:
        """Store new labels.

        Each ``name`` is canonicalised, then must be 3--128 characters and unused in the
        tenant. A ``color`` that is not a hex colour such as ``"#3A9F2E"`` is replaced with a
        random one, and so is an omitted one.

        Parameters
        ----------
        input : list of Label
            The labels to create, each with a ``name``.

        Returns
        -------
        list of Label
            The stored labels, with ``id`` and ``color`` filled in and ``name`` in its
            canonical form.

        Raises
        ------
        ValueError
            If a ``Label`` has neither ``name`` nor ``id``. Nothing is sent.
        DataHubException
            ``status_code`` 409 (``problem_slug`` ``"duplicate"``) if a name is already
            taken; the problem's ``duplicated`` member names it. 400 for a name missing or
            outside 3--128 characters, or a ``color`` longer than 7 characters.

        Examples
        --------
        >>> from intellistream_datahub_sdk import Label
        >>> [pump] = client.labels.create([
        ...     Label(name="pump", description="Centrifugal and piston pumps", color="#3A9F2E")
        ... ])
        >>> pump.name
        'PUMP'
        """
    def update(self, input: builtins.list[Label]) -> builtins.list[Label]:
        """Change fields on existing labels.

        Each ``Label`` names its target by ``id``, or by ``name`` when it has no ``id``. Only
        the fields that are set change; a field left ``None`` keeps its value, so a field
        cannot be cleared. To rename a label, identify it by ``id`` and set the new ``name``.
        The batch is all-or-nothing.

        Parameters
        ----------
        input : list of Label

        Returns
        -------
        list of Label
            The labels as they stand after the update.

        Raises
        ------
        ValueError
            If a ``Label`` has neither ``name`` nor ``id``. Nothing is sent.
        DataHubException
            ``status_code`` 404 if a target label does not exist; 400 for renaming a
            type-label, or renaming a label to one.

        Examples
        --------
        >>> from intellistream_datahub_sdk import Label
        >>> client.labels.update([Label(name="PUMP", color="#CC11CC")])
        """
    def delete(self, input: builtins.list[Label | int | str]) -> None:
        """Delete labels.

        This cannot be undone. The batch is all-or-nothing: if any label is still carried by
        a node, nothing is deleted. Remove it from those nodes first, with a ``labels``
        delta on their update. Entries that match no label are skipped.

        Parameters
        ----------
        input : list of Label, int or str
            Each entry is a ``Label``, a numeric id, or a name. A name is canonicalised
            before matching. A ``Label`` is matched by ``id`` when it has one, otherwise by
            ``name``.

        Raises
        ------
        DataHubException
            ``status_code`` 400 if a label is still in use, with the problem's ``fields``
            naming the label and every node carrying it; 400 also for a type-label.

        Examples
        --------
        >>> client.labels.delete(["pump", 17])
        """


@final
class LabelsServiceAsync:
    async def list(self) -> builtins.list[Label]: ...
    async def get(self, id: int) -> Label | None: ...
    async def create(self, input: builtins.list[Label]) -> builtins.list[Label]: ...
    async def update(self, input: builtins.list[Label]) -> builtins.list[Label]: ...
    async def delete(self, input: builtins.list[Label | int | str]) -> None: ...


# ====================== Units ======================

@final
class Unit:
    """
    Represents a Unit in the Datahub unit system

    Parameters
    ----------
    id: int
        internal id of the unit
    external_id: str
        user provided external id of the unit
    name: str
        name of the unit ie Celcius, Newton,
    long_name: str
        long name of the unit ie Temperature_Celsius, Force_Newton,
    symbol: str
        symbol of the unit ie C, N,
    description: str
        description of the unit
    alias_names: list[str]
        alias names of the unit ie Pascal, Newton/Meter Squared,
    quantity: str
        The quantity dimension of the unit ie Temperature, Mass, Energy-seconds
    conversion: dict[str,float]
        dict of conversion factors from this unit to other units
    source: str
        source of the unit
    source_reference:
        url to the source of the unit
    """
    def __new__(
        cls,
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
    ) -> Unit: ...
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


@final
class UnitServiceSync:
    """Look up the catalogue of measurement units.

    Reached as ``client.units`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    The catalogue is managed centrally and is read-only: there is no call to create, change or
    delete a unit. A time series names its unit by the unit's ``external_id``. Every call that
    reaches the server raises ``DataHubException`` when the server refuses it; the exception
    carries the HTTP ``status_code`` and, where the server explained itself, a
    ``problem_slug`` to branch on.
    """
    def list(self) -> builtins.list[Unit]:
        """List every unit in the catalogue.

        The catalogue is small and changes rarely, so the whole of it comes back in one call and
        is safe to cache.

        Returns
        -------
        list of Unit

        Examples
        --------
        >>> units = client.units.list()
        >>> by_symbol = {u.symbol: u for u in units}
        """
    def by_ids(self, input: builtins.list[IdCollection]) -> builtins.list[Unit]:
        """Fetch units by id or external id.

        What is not found is left out of the result rather than raising, so compare the result
        with what you asked for to detect missing units. An empty ``input`` returns an empty
        list. An external id is matched exactly as stored, so ``"Pressure_Bar"`` does not find
        ``pressure_bar``; :meth:`units.by_external_ids` is lenient about case.

        Parameters
        ----------
        input : list of IdCollection
            Each entry names a unit by ``id`` or ``external_id``. A bare int or str is not
            accepted.

        Returns
        -------
        list of Unit

        Raises
        ------
        TypeError
            If an entry is not an ``IdCollection``.

        Examples
        --------
        >>> from intellistream_datahub_sdk import IdCollection
        >>> found = client.units.by_ids(
        ...     [IdCollection(external_id="pressure_bar"), IdCollection(id=9)]
        ... )
        """
    def by_external_ids(self, input: str) -> builtins.list[Unit]:
        """Fetch one unit by its external id.

        Despite the plural name this takes a single external id. The server folds it to the
        catalogue's form first -- lowercased, with spaces and punctuation turned into ``_`` --
        so ``"Pressure Bar"`` finds ``pressure_bar``. A unit that does not exist
        is an empty list, not an error.

        Parameters
        ----------
        input : str
            The external id of the unit.

        Returns
        -------
        list of Unit
            The unit, or an empty list if there is none.

        See Also
        --------
        units.by_ids : Several units at once, by id or exact external id.

        Examples
        --------
        >>> [bar] = client.units.by_external_ids("pressure_bar")
        """


@final
class UnitServiceAsync:
    async def list(self) -> builtins.list[Unit]: ...
    async def by_ids(self, input: builtins.list[IdCollection]) -> builtins.list[Unit]: ...
    async def by_external_id(self, input: str) -> builtins.list[Unit]: ...


# ====================== Files ======================

@final
class INode:
    def __new__(
        cls,
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
    ) -> INode: ...
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
    def related_resource_nodes(self) -> list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """
        Fetch the resources this file references (its ``related_resources`` ids), resolved to
        ``Resource`` objects via the resources service. (The ``related_resources`` *property* returns
        the raw ids; this resolves them.) Blocking; see [``related_resource_nodes_async``].
        """
    async def related_resource_nodes_async(self) -> list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """
        Awaitable variant of [``related_resource_nodes``].
        """


@final
class FileUpload:
    def __new__(
        cls,
        path: str,
        destination_path: str | None = None,
        external_id: str | None = None,
        name: str | None = None,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        source: str | None = None,
        data_set_id: int | None = None,
        related_resources: list[int] | None = None,
    ) -> FileUpload: ...
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


@final
class FileUpdate:
    """A partial update for one file or folder.

    Identify the node with ``external_id`` or ``id``; every other argument is optional and only
    sent when given, so an omitted field is left unchanged.
    """

    def __new__(
        cls,
        external_id: str | None = None,
        id: int | None = None,
        name: str | None = None,
        path: str | None = None,
        data_set_id: int | None = None,
        description: str | None = None,
        source: str | None = None,
        metadata: dict[str, str] | None = None,
        related_resources: list[int] | None = None,
    ) -> FileUpdate: ...
    @property
    def external_id(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


@final
class FileDownload:
    """A downloaded file's bytes plus what the server said they are."""

    @property
    def file_name(self) -> str | None: ...
    @property
    def mime_type(self) -> str | None: ...
    @property
    def content(self) -> bytes:
        """
        The file content as ``bytes``.
        """
    def __len__(self) -> int: ...


@final
class FilesServiceSync:
    """Upload, browse, change, download and delete files organised into folders.

    Reached as ``client.files`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on. Files are a per-tenant feature: where it is not
    enabled, every call raises with ``status_code`` 403 and ``problem_slug``
    ``"feature-disabled"``. A file or folder with no data set is visible to everyone; one with
    a data set only to callers who can read it. Reads treat a node you cannot see as one that
    does not exist, answering 404 or leaving it out, never 403.
    """
    def upload_file(self, file_upload: FileUpload) -> list[INode]:
        """Store a local file on the server.

        The file's bytes are streamed from disk, not read into memory first. Missing folders on
        the destination path are created. The server always turns the external id into a
        lowercase slug -- letters, digits and underscores -- so read the stored one back from
        the result rather than assuming ``file_upload.external_id``. The MIME type is detected
        from the content when the client cannot tell. Uploading never overwrites: a taken path
        or external id is refused.

        Parameters
        ----------
        file_upload : FileUpload
            The local file, where it goes (``destination_path``, default the root folder) and
            its metadata.

        Returns
        -------
        list of INode
            One element: the stored file, with ``id``, ``size`` and ``checksum`` filled in.

        Raises
        ------
        DataHubException
            ``status_code`` 409 if a file already exists at that path or with that external
            id; 403 without write access to the file's data set or to the destination
            folder's; 400 for an invalid path or unreadable metadata.

        Examples
        --------
        >>> from intellistream_datahub_sdk import FileUpload
        >>> [node] = client.files.upload_file(
        ...     FileUpload(
        ...         "pump_a_manual.pdf",
        ...         destination_path="/manuals/pumps",
        ...         description="Operating manual",
        ...     )
        ... )
        >>> node.external_id
        'pump_a_manual_pdf'
        """
    def list_root_directory(self) -> list[INode]:
        """List the files and folders directly under the root folder.

        Only the root's immediate children, not the whole tree. Use
        :meth:`files.list_directory_by_path` to descend.

        Returns
        -------
        list of INode

        See Also
        --------
        files.list_directory_by_path : The children of any folder.

        Examples
        --------
        >>> top = client.files.list_root_directory()
        """
    def delete(self, input: list[int | str | INode | FileUpload]) -> None:
        """Move files and folders to the trash.

        Deleting a folder moves everything beneath it to the trash too. Only files can be
        brought back, with :meth:`files.restore`; a deleted folder is gone for good. An entry
        that matches nothing is ignored. Deleting needs write access to the data set of every
        node removed, the folder's descendants included.

        Parameters
        ----------
        input : list of INode, FileUpload, int or str
            Each entry is a numeric id, an external id exactly as stored, an ``INode``, or the
            ``FileUpload`` a file was uploaded from, which is matched by its ``external_id``.
            ``IdCollection`` is not accepted.

        Raises
        ------
        DataHubException
            ``status_code`` 403 without write access to a node's data set.
        TypeError
            If an entry is of any other type.

        See Also
        --------
        files.list_trash : What has been deleted.
        files.restore : Bring deleted files back.

        Examples
        --------
        >>> client.files.delete(["pump_a_manual_pdf"])
        """
    def list_directory_by_path(self, path: str) -> list[INode]:
        """List the files and folders directly under a folder.

        A path that names no folder answers an empty list rather than raising.

        Parameters
        ----------
        path : str
            The folder, as an absolute path starting with ``/``, such as
            ``"/manuals/pumps"``. ``"/"`` is the root.

        Returns
        -------
        list of INode

        Raises
        ------
        DataHubException
            ``status_code`` 404 for a path that is not a valid folder path, such as one that
            climbs above the root.

        Examples
        --------
        >>> manuals = client.files.list_directory_by_path("/manuals/pumps")
        """
    def get_by_id(self, id: int) -> list[INode]:
        """Fetch the metadata of one file or folder by numeric id.

        Parameters
        ----------
        id : int

        Returns
        -------
        list of INode
            One element.

        Raises
        ------
        DataHubException
            ``status_code`` 404 if there is no such node, it is in the trash, or you cannot
            read its data set.

        Examples
        --------
        >>> [node] = client.files.get_by_id(5677892)
        """
    def get_by_external_id(self, external_id: str) -> list[INode]:
        """Fetch the metadata of one file or folder by external id.

        The external id is matched exactly as stored, which is always a lowercase slug.

        Parameters
        ----------
        external_id : str

        Returns
        -------
        list of INode
            One element.

        Raises
        ------
        DataHubException
            ``status_code`` 404 if there is no such node, it is in the trash, or you cannot
            read its data set.

        Examples
        --------
        >>> [node] = client.files.get_by_external_id("pump_a_manual_pdf")
        """
    def search(self, query: str) -> list[INode]:
        """Free-text search over file and folder names and descriptions.

        Searches the whole tree, ignoring case; the last word of ``query`` also matches as a
        prefix, so ``"operating man"`` finds a file described as ``"Operating manual"``.
        Results are ordered by name, not ranked, and at most 100 are returned. A blank query
        returns an empty list.

        Parameters
        ----------
        query : str

        Returns
        -------
        list of INode

        See Also
        --------
        files.list_directory_by_path : Walk a folder instead.

        Examples
        --------
        >>> hits = client.files.search("pump manual")
        """
    def list_trash(self) -> list[INode]:
        """List the deleted files you can read.

        Files only; deleted folders are not listed. Each keeps its ``name`` and ``path`` from
        before deletion, but its ``external_id`` is rewritten to
        ``DELETED_<checksum>_<original external id>_<epoch millis>``.

        Returns
        -------
        list of INode

        See Also
        --------
        files.restore : Bring them back.

        Examples
        --------
        >>> trashed = client.files.list_trash()
        """
    def restore(self, input: list[int | str | INode | FileUpload]) -> list[INode]:
        """Bring deleted files back to where they were.

        Each file returns to its original path under its original external id. The request is
        all-or-nothing and never overwrites: if any file cannot go back, nothing is restored.
        A file whose folder was deleted cannot return until a folder exists at that path
        again.

        Identify each file by numeric id, or by the ``INode`` from :meth:`files.list_trash`,
        which carries it. The rewritten ``DELETED_...`` external id does not find the file, so
        a str finds nothing. Entries that match no deleted file are ignored as long as one
        does.

        Parameters
        ----------
        input : list of INode, FileUpload, int or str
            The deleted files, by ``id`` or as the ``INode`` from :meth:`files.list_trash`.
            ``IdCollection`` is not accepted.

        Returns
        -------
        list of INode
            The restored files, with their original external ids.

        Raises
        ------
        DataHubException
            ``status_code`` 404 if none of the entries is a deleted file; 409 with
            ``problem_slug`` ``"restore-refused"`` if one cannot go back, the problem's
            ``reason`` saying why (``"path-taken"``, ``"external-id-taken"``,
            ``"folder-missing"``, ``"not-a-file"``); 403 without write access to a file's
            data set.
        TypeError
            If an entry is of any other type.

        Examples
        --------
        >>> trashed = client.files.list_trash()
        >>> client.files.restore([n for n in trashed if n.name == "pump_a_manual.pdf"])
        """
    def update(self, update: FileUpdate) -> list[INode]:
        """Rename, move, or change the metadata of one file or folder.

        Only the fields given in ``update`` change; there is no way to clear a field. Moving
        to a folder that does not exist creates it, and ``path="/"`` moves to the root.
        ``metadata`` and ``related_resources`` replace what is stored rather than merging with
        it. Assigning a data set to a folder also assigns it to every node beneath it that has
        none. When the ``FileUpdate`` names both an ``external_id`` and an ``id``, the
        external id is used, matched exactly as stored.

        Parameters
        ----------
        update : FileUpdate

        Returns
        -------
        list of INode
            One element: the node as it stands after the update.

        Raises
        ------
        DataHubException
            ``status_code`` 404 if the node does not exist; 409 if a file or folder already
            exists at the new path; 400 for an invalid name or path; 403 without write access
            to the node's data set, the new data set or the destination folder's.

        Examples
        --------
        >>> from intellistream_datahub_sdk import FileUpdate
        >>> client.files.update(
        ...     FileUpdate("pump_a_manual_pdf", path="/manuals/archive", description="Rev. B")
        ... )
        """
    def download(self, id: int) -> FileDownload:
        """Download a file's content into memory.

        The whole file is held in memory; for large files use :meth:`files.download_to_path`.

        Parameters
        ----------
        id : int
            The file's numeric id.

        Returns
        -------
        FileDownload
            The bytes as ``content``, with the ``file_name`` and ``mime_type`` the server
            sent.

        Raises
        ------
        DataHubException
            ``status_code`` 404 if there is no such file or you cannot read its data set.

        Examples
        --------
        >>> download = client.files.download(5677892)
        >>> data = download.content
        """
    def download_to_path(self, id: int, destination: str) -> int:
        """Download a file straight to disk, without holding it in memory.

        ``destination`` is the file to write, not a folder: it is created if missing and
        truncated if it exists. If the transfer fails part-way, the partial file is left
        behind.

        Parameters
        ----------
        id : int
            The file's numeric id.
        destination : str
            The local file path to write.

        Returns
        -------
        int
            The number of bytes written.

        Raises
        ------
        DataHubException
            ``status_code`` 404 if there is no such file or you cannot read its data set;
            also raised if ``destination`` cannot be written.

        Examples
        --------
        >>> written = client.files.download_to_path(5677892, "/tmp/pump_a_manual.pdf")
        """


@final
class FilesServiceAsync:
    async def upload_file(self, file_upload: FileUpload) -> list[INode]: ...
    async def list_root_directory(self) -> list[INode]: ...
    async def delete(self, input: list[int | str | INode | FileUpload]) -> None: ...
    async def list_directory_by_path(self, path: str) -> list[INode]: ...
    async def get_by_id(self, id: int) -> list[INode]: ...
    async def get_by_external_id(self, external_id: str) -> list[INode]: ...
    async def search(self, query: str) -> list[INode]: ...
    async def list_trash(self) -> list[INode]: ...
    async def restore(self, input: list[int | str | INode | FileUpload]) -> list[INode]: ...
    async def update(self, update: FileUpdate) -> list[INode]: ...
    async def download(self, id: int) -> FileDownload: ...
    async def download_to_path(self, id: int, destination: str) -> int: ...


# ====================== Subscriptions ======================

@final
class Subscription:
    def __new__(
        cls,
        external_id: str,
        name: str,
        timeseries: list[int | str | TimeSeries | IdCollection],
        id: int | None = None,
    ) -> Subscription: ...


@final
class SubscriptionFilter:
    def __new__(cls, timeseries: list[TimeSeries | IdCollection | int | str] | None = None) -> SubscriptionFilter: ...
    @property
    def timeseries(self) -> list[IdCollection]: ...


@final
class DataSort:
    def __new__(
        cls,
        property: list[str] | None = None,
        order: str | None = None,
    ) -> DataSort: ...
    @property
    def property(self) -> list[str]: ...
    # `property` above shadows the builtin decorator for the rest of the class body.
    @builtins.property
    def order(self) -> str | None: ...


@final
class SubscriptionFilterForm:
    def __new__(
        cls,
        filter: SubscriptionFilter | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> SubscriptionFilterForm: ...
    @property
    def filter(self) -> SubscriptionFilter: ...
    @property
    def limit(self) -> int: ...
    @property
    def sort(self) -> DataSort | None: ...


@final
class EventAction:
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


@final
class EventObject:
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


@final
class WsDatapoint:
    @property
    def timestamp(self) -> str: ...
    @property
    def value(self) -> str: ...
    def as_float(self) -> float:
        """
        Parse the value as a float. Raises ValueError if the value isn't numeric (e.g. for
        string-typed timeseries that share this delivery channel).
        """


@final
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


@final
class DataWrapperMessage:
    @property
    def event_action(self) -> EventAction: ...
    @property
    def event_object(self) -> EventObject: ...
    @property
    def tenant_id(self) -> str | None: ...
    @property
    def items(self) -> list[DataCollectionString]: ...


@final
class SubscriptionMessage:
    @property
    def subscription_external_id(self) -> str:
        """
        The subscription this message was delivered for (useful when one listener multiplexes
        several subscriptions).
        """
    @property
    def message_id(self) -> str: ...
    @property
    def payload(self) -> DataWrapperMessage: ...


@final
class SubscriptionListener:
    """
    Synchronous Python wrapper around the Rust ``SubscriptionListener``. Iterating drives the
    underlying WebSocket: ``for msg in listener:`` blocks until the next message or returns when
    the connection closes cleanly.
    """
    def __iter__(self) -> SubscriptionListener: ...
    def __next__(self) -> SubscriptionMessage: ...
    def next_message(self) -> SubscriptionMessage | None:
        """
        Wait for the next message. Returns None when the connection has been closed cleanly,
        raises on transport / deserialization errors. Equivalent to driving the iterator one
        step but without using StopIteration as the close signal.
        """
    def ack(self, message_ids: list[str]) -> None: ...
    def nack(self, message_ids: list[str]) -> None: ...
    def subscribe(self, external_ids: list[str]) -> None: ...
    def unsubscribe(self, external_ids: list[str]) -> None: ...
    def set_subscriptions(self, external_ids: list[str]) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> SubscriptionListener: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...


@final
class SubscriptionListenerAsync:
    """
    Asynchronous Python wrapper. Use ``async for msg in listener:`` on the asyncio side.
    """
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


@final
class SubscriptionsServiceSync:
    """Create, find and delete subscriptions, and listen to them for live datapoints.

    Reached as ``client.subscriptions`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    A subscription is a named, durable stream of the datapoints written to a set of time
    series. :meth:`subscriptions.listen` opens a WebSocket that delivers them. What has not been
    acknowledged survives a disconnect, so a listener that reconnects resumes where it left
    off.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on. :meth:`subscriptions.listen` is the exception: it
    raises a plain ``Exception``.
    """
    def create(self, input: builtins.list[Subscription]) -> builtins.list[Subscription]:
        """Store new subscriptions.

        Each needs an ``external_id`` unused among the tenant's subscriptions, a ``name`` and at
        least one time series, all of which must exist and be readable by you. If any
        subscription in the batch is refused, none is created. A new subscription starts from
        the datapoints written after it was created; to start one over, delete it and create
        it again.

        Parameters
        ----------
        input : list of Subscription

        Returns
        -------
        list of Subscription
            The stored subscriptions, with ``id`` and timestamps filled in by the server.

        Raises
        ------
        DataHubException
            ``status_code`` 400 if an ``external_id`` is already taken, a subscription names no
            time series, or a time series does not exist; 403 if you may not read one of the
            time series.

        See Also
        --------
        subscriptions.listen : Receive what a subscription delivers.

        Examples
        --------
        >>> from intellistream_datahub_sdk import Subscription
        >>> [sub] = client.subscriptions.create([
        ...     Subscription(
        ...         "pump_a_live", "Pump A live feed", ["pump_a_pressure", "pump_a_temperature"]
        ...     )
        ... ])
        """
    def list(self, limit: int | None = None) -> builtins.list[Subscription]:
        """List subscriptions, newest created first.

        A subscription is listed only if you may read every time series it streams. This is a
        first page and nothing more: there is no cursor to continue from. To go further, narrow
        the query with :meth:`subscriptions.filter` rather than raising ``limit``.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of Subscription

        See Also
        --------
        subscriptions.filter : Select by the time series a subscription streams.

        Examples
        --------
        >>> recent = client.subscriptions.list(limit=20)
        """
    def filter(
        self,
        form: SubscriptionFilterForm | None = None,
        timeseries: builtins.list[TimeSeries | IdCollection | int | str] | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> builtins.list[Subscription]:
        """Select subscriptions by the time series they stream.

        Give the criteria either as keywords or as a prepared ``form``, not both. A
        subscription matches if it streams at least one of the time series named, and is
        returned only if you may read every time series it streams. There is no cursor, so
        this returns one page: narrow the criteria to see past ``limit``.

        Parameters
        ----------
        form : SubscriptionFilterForm, optional
            The criteria, limit and sort as one object. Exclusive with the keywords below.
        timeseries : list of TimeSeries, IdCollection, int or str, optional
            Time series by ``TimeSeries``, ``IdCollection``, numeric id or external id. Omit it
            to match every subscription.
        limit : int, optional
            How many to return. Defaults to 100; above 10000 is refused.
        sort : DataSort, optional
            One property out of ``id``, ``externalId``, ``name``, ``createdTime`` and
            ``lastUpdatedTime``, with ``order`` ``"asc"`` or ``"desc"``. Defaults to newest
            created first.

        Returns
        -------
        list of Subscription

        Raises
        ------
        ValueError
            If both ``form`` and keywords are given.

        Examples
        --------
        Every subscription streaming the discharge pressure, oldest first:

        >>> from intellistream_datahub_sdk import DataSort
        >>> subs = client.subscriptions.filter(
        ...     timeseries=["pump_a_pressure"], sort=DataSort(["createdTime"], "asc")
        ... )
        """
    def delete(self, input: builtins.list[Subscription | IdCollection | int | str]) -> None:
        """Delete subscriptions, and whatever they have not delivered yet.

        This cannot be undone. Subscriptions that do not exist are skipped. A subscription that
        a listener is still connected to is not deleted: close every listener on it first,
        including listeners in other processes, then retry.

        Parameters
        ----------
        input : list of Subscription, IdCollection, int or str
            The subscriptions to delete, by ``Subscription``, ``IdCollection``, numeric id or
            external id.

        Raises
        ------
        DataHubException
            ``status_code`` 400 while a listener is connected to one of the subscriptions;
            nothing is deleted.

        Examples
        --------
        >>> client.subscriptions.delete(["pump_a_live"])
        """
    def listen(self, subscription_external_ids: builtins.list[str]) -> SubscriptionListener:
        """Open a WebSocket listener for one or more subscriptions.

        One listener multiplexes any number of subscriptions; each message says which one it
        came from. The list may be empty, and the set can be changed on the open listener with
        ``subscribe``, ``unsubscribe`` and ``set_subscriptions``. The listener acknowledges
        nothing by itself: call ``ack`` with the ids of the messages you have processed, and
        anything left unacknowledged is delivered again to the next listener on that
        subscription.

        A dropped connection is re-established by the listener, with a fresh token and the
        current set of subscriptions, so a short outage or a server restart does not end the
        iteration. A subscription that cannot be attached, because it does not exist or you
        may not read it, raises from the listener's next read while the other subscriptions
        keep delivering. Reads have no timeout, so close the listener when done, preferably
        with a ``with`` block, or it stays connected and blocks :meth:`subscriptions.delete`.

        Parameters
        ----------
        subscription_external_ids : list of str
            External ids of the subscriptions to listen to.

        Returns
        -------
        SubscriptionListener
            Iterate it for ``SubscriptionMessage`` objects, or call ``next_message``.

        Raises
        ------
        Exception
            If the connection cannot be opened, for example when no token can be obtained or
            the handshake is refused.

        Examples
        --------
        >>> with client.subscriptions.listen(["pump_a_live"]) as listener:
        ...     for msg in listener:
        ...         for series in msg.payload.items:
        ...             for dp in series.datapoints:
        ...                 print(series.external_id, dp.timestamp, dp.as_float())
        ...         listener.ack([msg.message_id])
        """


@final
class SubscriptionsServiceAsync:
    async def create(self, input: builtins.list[Subscription]) -> builtins.list[Subscription]: ...
    async def list(self, limit: int | None = None) -> builtins.list[Subscription]: ...
    async def filter(
        self,
        form: SubscriptionFilterForm | None = None,
        timeseries: builtins.list[TimeSeries | IdCollection | int | str] | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> builtins.list[Subscription]: ...
    async def delete(self, input: builtins.list[Subscription | IdCollection | int | str]) -> None: ...
    async def listen(self, subscription_external_ids: builtins.list[str]) -> SubscriptionListenerAsync: ...


# ====================== Functions ======================

@final
class Function:
    def __new__(
        cls,
        external_id: str,
        name: str | None = None,
    ) -> Function: ...
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
    def related_resources(self) -> list[RelatedNode]:
        """
        The nodes bound into this function (e.g. its input timeseries via PROCESSED_BY
        edges). Populated by the server on ``GET /functions``; the Python worker reads each
        entry's ``id`` and ``relationship_type == "PROCESSED_BY"`` to build its routing map.
        """
    # --- navigation (only on functions returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """
        Walk the graph from this function and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters which
        edge types to follow (``None`` = all); ``limit`` caps the node count. Neighbour nodes are
        modelled as ``Resource``. Blocking; see [``neighbors_async``] for the awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """
        Awaitable variant of [``neighbors``].
        """
    def related_events(self, limit: int = 100) -> list[Event]:
        """
        Fetch events whose ``related_resources`` include this
        function (matched by graph-node id when present, else external id), via ``events.filter``.
        ``limit`` caps the results (default 100). Blocking; see [``related_events_async``].
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """
        Awaitable variant of [``related_events``].
        """


@final
class FunctionsServiceSync:
    """Create, find, change and delete functions.

    Reached as ``client.functions`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    A function is a node in the graph like a resource, marked by its ``FUNCTION`` type label,
    and is linked to the time series it processes by relationships. The functions that
    :meth:`functions.create` and the reads return have an empty ``related_resources``; walk
    the graph with ``Function.neighbors`` to see what a function is linked to.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """
    def create(self, input: builtins.list[Function]) -> builtins.list[Function]:
        """Store new functions.

        Each needs an unused ``external_id`` and a ``name``: the server requires a name
        although ``Function`` accepts ``None``. The batch is
        all-or-nothing: if any function is refused, none is created. To link a function to
        its inputs, use :meth:`edges.create`.

        Parameters
        ----------
        input : list of Function

        Returns
        -------
        list of Function
            The stored functions, with ``id`` and timestamps filled in by the server.

        Raises
        ------
        DataHubException
            ``status_code`` 409 if an ``external_id`` is already taken; the problem's
            ``duplicated`` member names it.

        Examples
        --------
        >>> from intellistream_datahub_sdk import Function
        >>> [fn] = client.functions.create(
        ...     [Function("pump_a_anomaly_detector", name="Pump A anomaly detector")]
        ... )
        """
    def list(self, limit: int | None = None) -> builtins.list[Function]:
        """List the functions you may read, newest created first.

        This is a first page and nothing more: there is no cursor to continue from.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of Function

        Examples
        --------
        >>> recent = client.functions.list(limit=20)
        """
    def get_by_id(self, id: int) -> Function | None:
        """Fetch one function by numeric id.

        A function you may not read is reported as missing, so a 404 does not mean the id is
        unused. When you have the numeric id, prefer this to :meth:`functions.by_ids`, which
        searches a listing.

        Parameters
        ----------
        id : int
            The function's numeric id.

        Returns
        -------
        Function

        Raises
        ------
        DataHubException
            ``status_code`` 404 if there is no function with that id that you may read,
            including when the id belongs to a node of another type.

        Examples
        --------
        >>> fn = client.functions.get_by_id(5677901)
        """
    def by_ids(self, input: builtins.list[Function | IdCollection | int | str]) -> builtins.list[Function]:
        """Fetch functions by id or external id.

        What is not found is left out of the result rather than raising. The lookup is made in
        the client, over a listing of the newest 10000 functions you may read, so in a tenant
        with more than that the oldest are never found. Results come in listing order, newest
        created first, not in the order asked for. An entry naming both an id and an external
        id matches a function that has either.

        Parameters
        ----------
        input : list of Function, IdCollection, int or str
            Each entry is a ``Function``, an ``IdCollection``, a numeric id or an external id.
            Mix freely.

        Returns
        -------
        list of Function

        See Also
        --------
        functions.get_by_id : One function by numeric id, from the server directly.
        functions.by_external_id : One function by external id, raising if it is missing.

        Examples
        --------
        >>> found = client.functions.by_ids([5677901, "pump_a_anomaly_detector"])
        """
    def by_external_id(self, external_id: str) -> Function:
        """Fetch one function by external id, raising if there is none.

        Made in the client like :meth:`functions.by_ids`, with the same limit: a function
        older than the newest 10000 you may read is not found.

        Parameters
        ----------
        external_id : str

        Returns
        -------
        Function

        Raises
        ------
        DataHubException
            ``status_code`` 404 if no function you may read has that external id. The error is
            raised by the client, so ``problem`` and ``problem_slug`` are ``None``.

        Examples
        --------
        >>> fn = client.functions.by_external_id("pump_a_anomaly_detector")
        """
    def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult:
        """Change fields on existing functions.

        Only the fields named in each ``ResourceUpdate`` change. ``geolocation`` is ignored,
        being stored only on assets, and the ``FUNCTION`` type label cannot be removed.

        Parameters
        ----------
        input : list of ResourceUpdate
            One per function, naming it by ``Function``, numeric id or external id.

        Returns
        -------
        GraphResult
            ``nodes`` holds each updated function as it stands after the update, as a
            ``Function``.

        Examples
        --------
        >>> from intellistream_datahub_sdk import FieldStr, MapField, ResourceUpdate
        >>> client.functions.update([
        ...     ResourceUpdate(
        ...         "pump_a_anomaly_detector",
        ...         description=FieldStr("Flags pressure spikes on pump A"),
        ...         metadata=MapField.delta(add={"version": "2"}),
        ...     )
        ... ])
        """
    def delete(self, input: builtins.list[Function | IdCollection | int | str]) -> None:
        """Delete functions and every relationship they have.

        This cannot be undone. A delete that would leave another node disconnected from the
        graph root is refused as a whole.

        Parameters
        ----------
        input : list of Function, IdCollection, int or str
            The functions to delete, by ``Function``, ``IdCollection``, numeric id or external
            id.

        Raises
        ------
        DataHubException
            ``status_code`` 409 with ``problem_slug`` ``"would-strand"`` when the delete would
            disconnect a node, which ``problem["blockedBy"]`` names; nothing is deleted.

        Examples
        --------
        >>> client.functions.delete(["pump_a_anomaly_detector"])
        """


@final
class FunctionsServiceAsync:
    async def create(self, input: builtins.list[Function]) -> builtins.list[Function]: ...
    async def list(self, limit: int | None = None) -> builtins.list[Function]: ...
    async def get_by_id(self, id: int) -> Function | None: ...
    async def by_ids(self, input: builtins.list[Function | IdCollection | int | str]) -> builtins.list[Function]: ...
    async def by_external_id(self, external_id: str) -> Function: ...
    async def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult: ...
    async def delete(self, input: builtins.list[Function | IdCollection | int | str]) -> None: ...


# ====================== Assets ======================

@final
class AssetsServiceSync:
    """Create, find, change and delete assets.

    Reached as ``client.assets`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    An asset is a resource that can carry a ``geolocation``, and like a resource can be a
    navigation root.
    Each call runs the same server pipeline as its ``client.resources`` counterpart, so the
    same rules apply. Reads are pinned to assets and answer ``Asset`` objects rather than a
    mix of classes; :meth:`assets.update` and :meth:`assets.delete` do not check that their
    targets are assets.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """

    def create(self, input: builtins.list[Asset]) -> builtins.list[Asset]:
        """Store new assets.

        Each needs an ``external_id`` (3--256 characters, unused among the tenant's nodes,
        ignoring case) and a ``name`` (3--512 characters). The asset type label is added for
        you; labels you set are kept beside it. The batch is all-or-nothing. To create assets
        together with the relationships between them, use :meth:`resources.create`.

        Parameters
        ----------
        input : list of Asset

        Returns
        -------
        list of Asset
            The stored assets, with ``id`` and timestamps filled in by the server.

        Raises
        ------
        DataHubException
            ``status_code`` 400 for an asset that fails validation; 403 when you may not write
            its data set; 409 if an ``external_id`` is already taken, the problem's
            ``duplicated`` member naming it.

        See Also
        --------
        resources.create : Assets and their relationships in one call.

        Examples
        --------
        >>> from intellistream_datahub_sdk import Asset
        >>> [pump] = client.assets.create([
        ...     Asset(
        ...         name="Pump A",
        ...         external_id="pump_a",
        ...         labels=["PUMP"],
        ...         geolocation={"type": "Point", "coordinates": [10.75, 59.91]},
        ...     )
        ... ])
        """
    def get_by_id(self, id: int) -> Asset | None:
        """Fetch one asset by numeric id.

        Unlike :meth:`assets.by_ids`, a miss raises. A node that is not an asset, and an asset
        in a data set you may not read, are both reported as missing, so a 404 does not tell
        you the id is free.

        Parameters
        ----------
        id : int

        Returns
        -------
        Asset

        Raises
        ------
        DataHubException
            ``status_code`` 404 with ``problem_slug`` ``"not-found"`` if no asset you may
            read has this id.

        Examples
        --------
        >>> pump = client.assets.get_by_id(42)
        """
    def by_ids(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> builtins.list[Asset]:
        """Fetch assets by id or external id.

        What is not found, names a node of another type, or lies in a data set you may not
        read is left out of the result rather than raising, so compare the result with what
        you asked for to detect missing assets.

        Parameters
        ----------
        input : list of int, str, Asset, TimeSeries, Function, Resource, Dataset or Policy
            Each entry is a numeric id, an external id, or a node object. Mix freely.

        Returns
        -------
        list of Asset

        Examples
        --------
        >>> found = client.assets.by_ids(["pump_a", "pump_b", 42])
        """
    def list(self, limit: int | None = None) -> builtins.list[Asset]:
        """List assets, newest created first.

        Only assets in data sets you may read are returned. This is a first page and nothing
        more: there is no cursor to continue from. To go further, narrow the query with
        :meth:`assets.filter` rather than raising ``limit``.

        Parameters
        ----------
        limit : int, optional
            How many to return. Defaults to the server's 1000; above 10000 is refused.

        Returns
        -------
        list of Asset

        See Also
        --------
        assets.filter : Select by criteria, with paging.

        Examples
        --------
        >>> recent = client.assets.list(limit=20)
        """
    def filter(
        self,
        filter: ResourceFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Select assets by criteria, one page at a time.

        The criteria of :meth:`resources.filter`, answering with assets only. Give them either
        as keywords or as a prepared ``filter=``, not both; a ``node_type`` on a ``filter=``
        object is replaced by the asset type rather than combined with it. Criteria combine
        with AND; the entries of one list combine with OR. Pattern lists accept a single
        string or a list, where ``*`` and ``%`` are wildcards, ``_`` is literal, and matching
        ignores case.

        Parameters
        ----------
        filter : ResourceFilter, optional
            The criteria as one object, reusable across calls. Exclusive with the
            criteria keywords below.
        id : sequence of int, optional
        external_id, name, source : str or sequence of str, optional
        labels : str or sequence of str, optional
            Every label listed must be present.
        metadata : mapping of str to str or None, optional
            Every key listed must be present; a ``None`` value matches the key alone.
        created_time, last_updated_time : TimeFilter, optional
            Inclusive at both ends.
        is_root : bool, optional
            ``True`` for navigation roots only, ``False`` for the rest.
        data_set_id : sequence of int, str or IdCollection, optional
            Data sets by id or external id; includes everything beneath them in the data
            set hierarchy. ``None`` places no restriction, but ``[]`` matches nothing.
        limit : int, optional
            Page size. Defaults to 1000; above 10000 is refused.
        sort_by : str, optional
            One of ``id``, ``externalId``, ``name``, ``source``, ``description``,
            ``createdTime``, ``lastUpdatedTime`` and ``dataSetId``; defaults to
            ``createdTime``.
        sort_order : {"asc", "desc"}, optional
            Defaults to descending.
        cursor : str, optional
            ``next_cursor`` from the previous page. It belongs to its sort: continuing it
            under another is refused.

        Returns
        -------
        Page
            A list-like page of ``Asset``. Its ``next_cursor`` is ``None`` on the last page.

        Raises
        ------
        TypeError
            If both ``filter=`` and criteria keywords are given.

        Examples
        --------
        Every root asset in one data set and those beneath it, a page at a time:

        >>> page = client.assets.filter(is_root=True, data_set_id=["site_north_data"], limit=500)
        >>> roots = list(page)
        >>> while page.next_cursor:
        ...     page = client.assets.filter(
        ...         is_root=True, data_set_id=["site_north_data"], limit=500,
        ...         cursor=page.next_cursor,
        ...     )
        ...     roots.extend(page)
        """
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Asset]:
        """Free-text search over assets, by name, external id and description.

        Matching is word-aware and fuzzy -- ``"pump"`` also finds ``"pumps"`` -- and results
        are ranked, best match first. A ``node_type`` in ``filter`` is replaced by the asset
        type. For exact lookups use :meth:`assets.by_ids`; for structured queries without a
        phrase, use :meth:`assets.filter`.

        Parameters
        ----------
        query : str
            The phrase, 3--140 characters.
        filter : ResourceFilter, optional
            Narrows the phrase's hits. It only ever removes results, never adds them.
        limit : int, optional
            How many to return. Defaults to 100; above 1000 is refused.

        Returns
        -------
        list of Asset

        Examples
        --------
        >>> from intellistream_datahub_sdk import ResourceFilter
        >>> client.assets.search("feed pump", filter=ResourceFilter(labels="PUMP"))
        """
    def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult:
        """Change fields on existing assets, including their ``geolocation``.

        Only the fields named in each ``ResourceUpdate`` change. The asset type label stays
        in place whatever ``labels`` says. The batch is all-or-nothing. The targets are not
        checked to be assets: an update naming a node of another type is applied to it as
        :meth:`resources.update` would, and that node comes back as its own class.

        Parameters
        ----------
        input : list of ResourceUpdate

        Returns
        -------
        GraphResult
            ``nodes`` holds the nodes as they stand after the update, each as its own class.
            ``related_resources`` is empty on them, and ``relations`` is empty.

        Raises
        ------
        DataHubException
            ``status_code`` 400 if a target does not exist, or ``name`` or ``external_id`` is
            set to null; 409 if a new ``external_id`` is already taken, or the asset changed
            under a concurrent write -- re-read it and retry.

        Examples
        --------
        Move a pump, and clear the location of another:

        >>> from intellistream_datahub_sdk import FieldGeoJson, ResourceUpdate
        >>> result = client.assets.update([
        ...     ResourceUpdate(
        ...         "pump_a",
        ...         geolocation=FieldGeoJson({"type": "Point", "coordinates": [10.76, 59.92]}),
        ...     ),
        ...     ResourceUpdate("pump_b", geolocation=FieldGeoJson(set_null=True)),
        ... ])
        """
    def delete(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> None:
        """Delete assets, and every relationship they take part in.

        This cannot be undone. Identifiers that match nothing are skipped, so deleting an
        asset that is already gone is a no-op. The batch is all-or-nothing. The targets are
        not checked to be assets: a node of another type named here is deleted as
        :meth:`resources.delete` would delete it.

        A delete is refused when it would disconnect a surviving node from the graph root.
        Include the stranded nodes in the same call, or keep another path to them. The check
        reads a view of the graph that lags writes by a moment, so an asset deleted
        immediately after its relationships were created can be let through and strand its
        neighbours; wait briefly after creating relationships before deleting across them.

        Parameters
        ----------
        input : list of int, str, Asset, TimeSeries, Function, Resource, Dataset or Policy
            The assets to delete, by id, external id or node object. A node object is
            matched by its ``id`` when it has one.

        Raises
        ------
        DataHubException
            ``status_code`` 403 when you may not write an asset's data set. 409 with
            ``problem_slug`` ``"would-strand"`` when the delete would disconnect a surviving
            node, the problem's ``blockedBy`` naming it.

        See Also
        --------
        edges.delete : Remove one relationship and keep both nodes.

        Examples
        --------
        >>> client.assets.delete(["pump_a"])
        """


@final
class AssetsServiceAsync:
    async def create(self, input: builtins.list[Asset]) -> builtins.list[Asset]: ...
    async def get_by_id(self, id: int) -> Asset | None: ...
    async def by_ids(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> builtins.list[Asset]: ...
    async def list(self, limit: int | None = None) -> builtins.list[Asset]: ...
    async def filter(
        self,
        filter: ResourceFilter | None = None,
        id: Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        name: str | Sequence[str] | None = None,
        source: str | Sequence[str] | None = None,
        labels: str | Sequence[str] | None = None,
        metadata: Mapping[str, str | None] | None = None,
        created_time: TimeFilter | None = None,
        last_updated_time: TimeFilter | None = None,
        is_root: bool | None = None,
        data_set_id: Sequence[int | str | IdCollection] | None = None,
        limit: int | None = None,
        sort_by: str | Sequence[str] | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page: ...
    async def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Asset]: ...
    async def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult: ...
    async def delete(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> None: ...


# ====================== Edges ======================

@final
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


@final
class RelTypeForm:
    """Register a relationship type up front.

    The server uppercase-snake-cases ``name`` (``"Flows To"`` -> ``FLOWS_TO``); a name that
    normalises to nothing is rejected with status 400.
    """

    def __new__(
        cls,
        name: str,
        description: str | None = None,
        i18n_code: str | None = None,
    ) -> RelTypeForm: ...
    @property
    def name(self) -> str: ...
    @property
    def description(self) -> str | None: ...
    @property
    def i18n_code(self) -> str | None: ...


@final
class EdgesServiceSync:
    """Read, create and delete relationships between existing nodes, and manage their types.

    Reached as ``client.edges`` on a configured ``DataHubClient``, never constructed.
    ``AsyncDataHubClient`` has the same methods as coroutines.

    Relationships usually come into being together with their nodes, through
    :meth:`resources.create`. This service is for linking nodes that already exist, for reading
    or deleting a relationship on its own, and for the tenant's catalogue of relationship types.

    Every call that reaches the server raises ``DataHubException`` when the server refuses
    it; the exception carries the HTTP ``status_code`` and, where the server explained
    itself, a ``problem_slug`` to branch on.
    """
    def get(self, id: int) -> EdgeProxy | None:
        """Fetch one relationship by numeric id.

        Returns ``None`` when no relationship with that id is visible to you. That covers a
        relationship you may not read -- you need read access to the data sets of both of its
        endpoints -- so ``None`` does not mean the id is unused.

        Parameters
        ----------
        id : int
            The relationship's numeric id.

        Returns
        -------
        EdgeProxy or None

        See Also
        --------
        edges.by_ids : Several relationships, with the nodes they connect.

        Examples
        --------
        >>> edge = client.edges.get(341)
        >>> if edge is not None:
        ...     print(edge.start, edge.relationship_type, edge.end)
        """
    def by_ids(self, input: list[EdgeProxy | int]) -> GraphResult:
        """Fetch several relationships together with the nodes they connect.

        The result is a graph: ``relations`` holds the relationships and ``nodes`` the nodes at
        both ends of each, as their own types (``Asset``, ``TimeSeries``, ...), so no follow-up
        call is needed to resolve the endpoints. Ids that do not exist, and relationships with
        an endpoint you may not read, are left out rather than raising; asking only for those
        returns an empty graph.

        Parameters
        ----------
        input : list of EdgeProxy or int
            Relationships by numeric id or ``EdgeProxy``. Relationships have no external id,
            so strings are not accepted.

        Returns
        -------
        GraphResult

        Examples
        --------
        >>> graph = client.edges.by_ids([341, 342])
        >>> names = {node.id: node.external_id for node in graph.nodes}
        >>> for edge in graph.relations:
        ...     print(names[edge.start], edge.relationship_type, names[edge.end])
        """
    def create(self, input: list[RelForm]) -> list[EdgeProxy]:
        """Link nodes that already exist.

        Name each endpoint by id or external id, and the relationship by type name. A type name
        the tenant has not used before is added to the catalogue on the fly; relationship types
        cannot be deleted, so a misspelt name stays in the catalogue for good. To create nodes
        and their relationships together, use :meth:`resources.create`.

        The batch is all-or-nothing: if any relationship is refused, none is created. The same
        type between the same two nodes can exist only once. A relationship whose target is a
        data set must be of type ``BELONGS_TO``, and a time series cannot belong to a second
        data set.

        Parameters
        ----------
        input : list of RelForm
            The relationships to create, each naming its two endpoints and a
            ``relationship_type``.

        Returns
        -------
        list of EdgeProxy
            The stored relationships, with their ids filled in by the server.

        Raises
        ------
        DataHubException
            ``status_code`` 400 for an endpoint that does not exist or a relationship the rules
            above forbid; 403 without write access to the data sets of both endpoints; 409 if
            the relationship already exists.

        See Also
        --------
        resources.create : Create nodes and their relationships in one call.

        Examples
        --------
        >>> from intellistream_datahub_sdk import RelForm
        >>> [edge] = client.edges.create(
        ...     [RelForm.by_external_ids("pump_a", "valve_v9", "FLOWS_TO")]
        ... )
        """
    def delete(self, input: list[EdgeProxy | int]) -> None:
        """Delete relationships, leaving the nodes at each end in place.

        Ids that do not exist are skipped, so deleting a relationship twice is a no-op.

        A relationship that is the only route from one of its endpoints to the graph root is
        not deleted on its own: the call is refused rather than leave that node disconnected.
        Delete such a relationship together with the node it holds up, through
        :meth:`resources.delete`, or add another connecting path first. The check reads a view
        of the graph that lags writes by a moment: deleting a relationship straight after
        creating it, or straight after deleting another relationship of the same node, can
        succeed where it should have been refused and leave a node disconnected. Wait a moment
        after such a change before deleting.

        Parameters
        ----------
        input : list of EdgeProxy or int
            Relationships by numeric id or ``EdgeProxy``.

        Raises
        ------
        DataHubException
            ``status_code`` 409 with ``problem_slug`` ``"would-strand"`` when the delete would
            disconnect a node; ``problem["blockedBy"]`` names it. Nothing is deleted.

        Examples
        --------
        >>> from intellistream_datahub_sdk import DataHubException
        >>> try:
        ...     client.edges.delete([341])
        ... except DataHubException as e:
        ...     if e.problem_slug != "would-strand":
        ...         raise
        ...     stranded = [b["externalId"] for b in e.problem["blockedBy"]]
        """
    def types(self) -> list[RelationshipType]:
        """List every relationship type in the tenant's catalogue.

        Returns
        -------
        list of RelationshipType

        Examples
        --------
        >>> names = [t.name for t in client.edges.types()]
        """
    def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]:
        """Register relationship types up front.

        Not needed before :meth:`edges.create` or :meth:`resources.create`, which add a type
        the first time it is used; register one here to seed the catalogue or to give a type
        a description. Names are case-insensitive and stored in upper snake case, so
        ``"Flows To"`` is stored as ``FLOWS_TO``. Types cannot be deleted.

        The batch is all-or-nothing: one name that already exists discards the new ones beside
        it, so read :meth:`edges.types` first when some may exist.

        Parameters
        ----------
        input : list of RelTypeForm

        Returns
        -------
        list of RelationshipType
            The stored types, with their normalised names.

        Raises
        ------
        DataHubException
            ``status_code`` 409 if a name already exists; 400 for a name that normalises to
            nothing, such as one made only of symbols.

        Examples
        --------
        >>> from intellistream_datahub_sdk import RelTypeForm
        >>> [flows_to] = client.edges.create_types(
        ...     [RelTypeForm("Flows To", description="Fluid moves from start to end")]
        ... )
        """


@final
class EdgesServiceAsync:
    async def get(self, id: int) -> EdgeProxy | None: ...
    async def by_ids(self, input: list[EdgeProxy | int]) -> GraphResult: ...
    async def create(self, input: list[RelForm]) -> list[EdgeProxy]: ...
    async def delete(self, input: list[EdgeProxy | int]) -> None: ...
    async def types(self) -> list[RelationshipType]: ...
    async def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]: ...
