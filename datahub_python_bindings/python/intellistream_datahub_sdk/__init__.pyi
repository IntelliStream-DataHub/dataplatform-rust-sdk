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
            ``status_code`` 400 if a series is the start of a relationship, or is still
            bound to a subscription. Remove those first.

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
    def list(self, limit: int | None = None) -> builtins.list[Event]:
        """
        A criteria-free page of the tenant's events.

        **The oldest ``limit`` events, not the newest.** It runs the event filter with an empty body,
        whose default sort is ``eventTime`` ascending — the order the cursor pages in. The node
        listings beside it (``resources.list``, ``timeseries.list``, ``datasets.list``) really are
        newest-first; events are the one member of the family that reads the other way round. For
        "what just happened", use ``filter(sort_by="eventTime", sort_order="desc")``.

        ``limit`` defaults to the server's 1000 and may not exceed 10000. A plain list is returned
        rather than a ``Page``: there is no cursor to continue with.
        """
    def create(self, input: builtins.list[Event]) -> builtins.list[Event]: ...
    def by_ids(self, input: builtins.list[Event | EventIdCollection | UUID | str]) -> builtins.list[Event]: ...
    def get(self, id: UUID) -> Event | None:
        """
        Look up a single event by its UUID. Returns ``None`` if no such event exists.
        """
    def delete(self, input: builtins.list[Event | EventIdCollection | UUID | str]) -> None: ...
    def update(self, input: builtins.list[EventUpdate]) -> builtins.list[Event]:
        """
        Update events in place. Each ``EventUpdate`` targets one event and carries only the fields to
        change; returns the events after the update.
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
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    def search(
        self,
        query: str,
        filter: EventFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Event]:
        """
        Free-text search over event descriptions, ranked by relevance.
        """
    def count(self) -> int:
        """
        Total number of events in the tenant.
        """
    def list_dimension(
        self,
        dimension: EventDimension,
        query: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[str]:
        """
        Distinct values an event field takes in this tenant. ``query`` filters by case-insensitive
        substring; omit it to list everything. ``limit`` defaults to 1000 server-side and is clamped
        to 1..=10000. Alphabetical, and restricted to your readable datasets.
        """
    def list_types(self, limit: int | None = None) -> builtins.list[str]:
        """
        Every distinct ``type`` on events you can read.
        """
    def search_types(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """
        Distinct ``type`` values containing ``query`` (case-insensitive substring).
        """
    def list_sub_types(self, limit: int | None = None) -> builtins.list[str]:
        """
        Every distinct ``subType`` on events you can read.
        """
    def search_sub_types(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """
        Distinct ``subType`` values containing ``query`` (case-insensitive substring).
        """
    def list_statuses(self, limit: int | None = None) -> builtins.list[str]:
        """
        Every distinct ``status`` on events you can read.
        """
    def search_statuses(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """
        Distinct ``status`` values containing ``query`` (case-insensitive substring).
        """
    def list_sources(self, limit: int | None = None) -> builtins.list[str]:
        """
        Every distinct ``source`` on events you can read.
        """
    def search_sources(self, query: str, limit: int | None = None) -> builtins.list[str]:
        """
        Distinct ``source`` values containing ``query`` (case-insensitive substring).
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


# `limit` defaults to the server's 1000 and may not exceed 10000. There is no paging, so a filter
# broad enough to exceed the cap is truncated — narrow it instead.
# A partial update for one dataset. `dataset` names the target; only the fields you pass are sent,
# anything omitted is left untouched. There is deliberately no `policies` or `connected_data_sets`
# — the update endpoint does not accept them, whatever a Dataset can carry on create.
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


# `search(query, ...)`: query is 3-140 chars and Latin letters/spaces/digits only, so an external
# id with underscores is a 400 — search on words and use filter() to look up by id. Results are
# unranked.
#
# `update(...)`: there is no write_protected/deactivated — both were removed server-side as inert.
class DatasetsServiceSync:
    def list(self, limit: int | None = None) -> builtins.list[Dataset]:
        """
        Datasets in the tenant, newest first. ``limit`` defaults to the server's 1000 and may not
        exceed 10000; there is no paging, so a bigger tenant is truncated rather than paged —
        use ``filter`` to narrow instead.
        """
    def create(self, input: builtins.list[Dataset]) -> builtins.list[Dataset]: ...
    def by_ids(self, input: builtins.list[int | str | Dataset | IdCollection]) -> builtins.list[Dataset]: ...
    def delete(self, input: builtins.list[int | str | Dataset | IdCollection]) -> None: ...
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
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    def search(
        self,
        query: str,
        filter: DatasetFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Dataset]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` takes the same criteria as ``filter()`` and only ever removes hits from the
        phrase's — it cannot widen them, so omitting it returns them as found. ``limit`` caps what
        survives, defaulting to 100 and capping at 1000; the ``filter`` endpoints use 1000/10000,
        which is easy to conflate.
        """
    def update(self, input: builtins.list[DatasetUpdate]) -> builtins.list[Dataset]:
        """
        Apply partial updates, returning the datasets as they stand afterwards.

        A dataset is the unit access is granted on, so the server treats editing one as an operator
        action: this needs an all-datasets write grant and raises 403 without one, even for a
        caller who can write the dataset's contents.

        **Do not combine a ``metadata`` change with ``write_protected`` / ``deactivated`` in one update.**
        The server stores those flags as node metadata, so setting either in the same call as a
        metadata delta silently drops the delta — 200, no error, half the change lost. Send two
        updates. Their keys (``property:is_write_protected``, ``property:is_deactivated``) are also
        visible in ``Dataset.metadata``.
        """
    def policies(self) -> builtins.list[Resource]:
        """
        The access policies a dataset can be associated with, as ``Resource`` objects.

        **Known to come back empty even when policies exist** — the server answers 200 with no body
        at all. That is a server-side bug, not something these bindings can work around, so treat
        an empty result as "unknown" rather than "none".
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
    def list(self, limit: int | None = None) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """
        The first ``limit`` nodes in the tenant, newest created first — the cheap "what have I got"
        read, with no criteria and no paging.

        Spans every node type and answers each row as its own class, exactly as ``filter`` does, so
        ``isinstance(node, TimeSeries)`` works on what comes back. ``limit`` defaults to the server's
        1000 and may not exceed 10000; a ``Page`` is not returned because there is no cursor to
        continue with — narrow with ``filter`` instead of raising the number.
        """
    def create(
        self, nodes: builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy], relations: builtins.list[RelForm] | None = None
    ) -> GraphResult: ...
    def by_ids(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]: ...
    def delete(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> None: ...
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Asset | TimeSeries | Function | Resource | Dataset | Policy]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` takes the same criteria as ``filter()`` and only ever removes hits from the
        phrase's — it cannot widen them, so omitting it returns them as found. ``limit`` caps what
        survives, defaulting to 100 and capping at 1000; the ``filter`` endpoints use 1000/10000,
        which is easy to conflate.
        """
    def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult:
        """
        Update nodes in place. Each [``ResourceUpdate``] targets one node and carries only the
        fields to change; every field it can set is shared by all node types, so one update form
        covers them all.

        **The echo is typed**, like every other read here: ``.nodes`` holds each node as its own
        class, so a timeseries comes back as ``TimeSeries`` carrying its ``unit`` and an asset as
        ``Asset`` carrying its ``geo_location``. The ``labels`` reflect what the server stored, intrinsic
        type-label included.

        This used to answer with a plain ``Resource`` whatever the node's real type. The api's
        node-update refactor made the pipeline per-type and the echo followed.
        """
    def get_by_id(self, id: int) -> Asset | TimeSeries | Function | Resource | Dataset | Policy | None:
        """
        ``GET /resources/{id}`` — one resource by numeric id. Raises when it does not exist,
        unlike ``by_ids``, which silently omits what it cannot find.
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
    def list(self) -> builtins.list[Label]:
        """
        Every label in the tenant.
        """
    def get(self, id: int) -> Label | None:
        """
        A single label by numeric id, or ``None`` if it doesn't exist (the server answers an
        unknown id with 404; that is absorbed into ``None``).
        """
    def create(self, input: builtins.list[Label]) -> builtins.list[Label]:
        """
        Create labels (each needs a unique ``name``). A duplicate name raises with status 409.
        """
    def update(self, input: builtins.list[Label]) -> builtins.list[Label]:
        """
        Update labels (identify each by ``id``); only the fields you set are applied.
        """
    def delete(self, input: builtins.list[Label | int | str]) -> None:
        """
        Delete labels by ``Label``, numeric id, or name. Rejected with status 400 if a label is
        still referenced by a resource.
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
    def list(self) -> builtins.list[Unit]: ...
    def by_ids(self, input: builtins.list[IdCollection]) -> builtins.list[Unit]: ...
    def by_external_ids(self, input: str) -> builtins.list[Unit]: ...


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
    def upload_file(self, file_upload: FileUpload) -> list[INode]: ...
    def list_root_directory(self) -> list[INode]: ...
    def delete(self, input: list[INode | IdCollection | int | str]) -> None: ...
    def list_directory_by_path(self, path: str) -> list[INode]: ...
    def get_by_id(self, id: int) -> list[INode]:
        """
        Metadata for one file or folder, by numeric id.
        """
    def get_by_external_id(self, external_id: str) -> list[INode]:
        """
        Metadata for one file or folder, by external id.
        """
    def search(self, query: str) -> list[INode]:
        """
        Full-text search over file and folder names and descriptions.
        """
    def list_trash(self) -> list[INode]:
        """
        The soft-deleted files the caller can read.
        """
    def restore(self, input: list[INode | IdCollection | int | str]) -> list[INode]:
        """
        Restore soft-deleted files. Identify each by numeric id: the trashed
        ``DELETED_..._<epochMillis>`` external id does not round-trip through the server's
        lowercasing hash, so that route answers 404. See ``FileService::restore`` in the SDK.
        """
    def update(self, update: FileUpdate) -> list[INode]:
        """
        Rename, move, or edit the metadata of one file or folder.
        """
    def download(self, id: int) -> FileDownload:
        """
        Download a file's content into memory.
        """
    def download_to_path(self, id: int, destination: str) -> int:
        """
        Download a file straight to ``destination``, without holding it in memory. Returns the number
        of bytes written.
        """


@final
class FilesServiceAsync:
    async def upload_file(self, file_upload: FileUpload) -> list[INode]: ...
    async def list_root_directory(self) -> list[INode]: ...
    async def delete(self, input: list[INode | IdCollection | int | str]) -> None: ...
    async def list_directory_by_path(self, path: str) -> list[INode]: ...
    async def get_by_id(self, id: int) -> list[INode]: ...
    async def get_by_external_id(self, external_id: str) -> list[INode]: ...
    async def search(self, query: str) -> list[INode]: ...
    async def list_trash(self) -> list[INode]: ...
    async def restore(self, input: list[INode | IdCollection | int | str]) -> list[INode]: ...
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
    def create(self, input: builtins.list[Subscription]) -> builtins.list[Subscription]: ...
    def list(self, limit: int | None = None) -> builtins.list[Subscription]:
        """
        Subscriptions in the tenant, newest first. ``limit`` defaults to the server's 1000 and may
        not exceed 10000; there is no paging, so a bigger tenant is truncated rather than paged —
        use ``filter`` to narrow instead.
        """
    def filter(
        self,
        form: SubscriptionFilterForm | None = None,
        timeseries: builtins.list[TimeSeries | IdCollection | int | str] | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> builtins.list[Subscription]:
        """
        Subscriptions matching every criterion on the filter.
        """
    def delete(self, input: builtins.list[Subscription | IdCollection | int | str]) -> None: ...
    def listen(self, subscription_external_ids: builtins.list[str]) -> SubscriptionListener:
        """
        Open a WebSocket listener multiplexing the named subscriptions. The ids seed the initial
        set (may be empty — add more with .subscribe()). Returns a SubscriptionListener you can
        iterate or call .next_message() / .ack() / .subscribe() / .close() on.
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
    def create(self, input: builtins.list[Function]) -> builtins.list[Function]: ...
    def list(self, limit: int | None = None) -> builtins.list[Function]:
        """
        The first ``limit`` functions you may read, newest first. ``limit`` defaults to the server's
        1000 and may not exceed 10000; there is no paging, so a bigger catalogue is truncated.
        """
    def get_by_id(self, id: int) -> Function | None:
        """One function by numeric id; raises on 404.

        A 404 does not tell you the id is free — a function you may not read is reported as
        missing rather than forbidden. Prefer this to ``by_ids`` when you have the id: functions
        have no ``/byids`` endpoint, so ``by_ids`` pages the listing and filters client-side.
        """
    def by_ids(self, input: builtins.list[Function | IdCollection | int | str]) -> builtins.list[Function]: ...
    def by_external_id(self, external_id: str) -> Function:
        """
        Convenience for the function-worker bootstrap: returns the function with the given
        externalId, or raises if no such function exists.
        """
    def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult:
        """Update functions in place; ``geolocation`` is ignored, being asset-only.

        ``.nodes`` holds typed node objects — a function comes back as ``Function``.
        """
    def delete(self, input: builtins.list[Function | IdCollection | int | str]) -> None: ...


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
    """The typed ``/assets`` family — the ``ASSET``-labelled corner of the resource graph.

    Every call is the generic ``/resources`` pipeline with the type pinned server-side, so the
    two paths cannot drift apart on ACLs or status codes. What differs is the shape that comes
    back: ``Asset``, so ``geolocation`` and ``is_root`` are reachable without a type check.
    """

    def create(self, input: builtins.list[Asset]) -> builtins.list[Asset]:
        """Create assets. Each needs an ``external_id`` and a ``name``.

        Unlike ``resources.create``, the ``ASSET`` label need not be set by hand. Relations are
        not creatable here — use ``resources.create`` for assets and their edges in one call.
        """
    def get_by_id(self, id: int) -> Asset | None:
        """One asset by numeric id; raises on 404.

        A 404 does not tell you the id is free: a node that exists but is not an asset, and an
        asset you may not read, are both reported as missing.
        """
    def by_ids(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> builtins.list[Asset]:
        """Assets by id or external id. What cannot be found is omitted, not raised."""
    def list(self, limit: int | None = None) -> builtins.list[Asset]:
        """The first ``limit`` assets, newest created first. Defaults to 1000, caps at 10000.

        A plain list rather than a ``Page``: there is no cursor to continue with, so narrow with
        ``filter`` instead of raising the number.
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
        """Assets matching every criterion, newest first. Criteria combine with AND.

        Pass either ``filter=`` or the individual keywords, not both.

        There is no ``node_type`` keyword on purpose: this endpoint answers with assets whatever
        it is given, so the server replaces it. A ``node_type`` on a ``filter=`` object is
        discarded the same way.
        """
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[Asset]:
        """Free-text search over assets, best match first.

        ``filter`` only ever removes hits from the phrase's. ``limit`` defaults to 100 and caps
        at 1000 — ``filter`` uses 1000/10000, which is easy to conflate.
        """
    def update(self, input: builtins.list[ResourceUpdate]) -> GraphResult:
        """Update assets in place. ``geolocation`` is the field that means anything only here.

        ``.nodes`` holds typed node objects, not necessarily all assets — an update may touch
        relations whose other end is something else.
        """
    def delete(self, input: builtins.list[int | str | Asset | TimeSeries | Function | Resource | Dataset | Policy]) -> None:
        """Delete assets, and with them all their relationships.

        A delete that would disconnect a surviving node from the graph root raises 409
        ``would-strand``, naming the blockers on the exception's ``problem``.
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
    def get(self, id: int) -> EdgeProxy | None:
        """
        One relationship by numeric id, or ``None`` if no edge has that id.

        The server answers an unknown id with 404; that is absorbed into ``None`` here, matching the
        other ``get()`` methods in these bindings. Any other error still raises.
        """
    def by_ids(self, input: list[EdgeProxy | int]) -> GraphResult:
        """
        Several relationships plus the resources they connect, as a ``GraphResult`` — ``nodes`` holds
        both endpoints of each edge and ``relations`` the edges, so no follow-up call is needed.
        """
    def create(self, input: list[RelForm]) -> list[EdgeProxy]:
        """
        Link resources that already exist. To create the resources *and* their links together, use
        ``resources.create(nodes, relations)`` instead.

        All-or-nothing: if any relation in the batch fails, none are created. A relation targeting
        a dataset must use ``BELONGS_TO``; a timeseries cannot be linked to a second dataset; you
        need write access to the datasets of both endpoints. Re-creating an existing edge between
        the same two resources conflicts with status 409.
        """
    def delete(self, input: list[EdgeProxy | int]) -> None:
        """
        Delete relationships by ``EdgeProxy`` or numeric id. Deletes the link only — the resources at
        each end stay intact. Idempotent: unknown ids are silently skipped.
        """
    def types(self) -> list[RelationshipType]:
        """
        Every relationship type the tenant has defined.
        """
    def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]:
        """
        Register relationship type names up front. Names normalise to uppercase snake case.

        A name that already exists currently makes the server fail silently — it answers 200 with
        an empty body, and in a batch the valid new types are rolled back alongside the duplicate.
        Treat an empty result as "something already existed and nothing was created", and use
        ``types()`` to read the real state.
        """


@final
class EdgesServiceAsync:
    async def get(self, id: int) -> EdgeProxy | None: ...
    async def by_ids(self, input: list[EdgeProxy | int]) -> GraphResult: ...
    async def create(self, input: list[RelForm]) -> list[EdgeProxy]: ...
    async def delete(self, input: list[EdgeProxy | int]) -> None: ...
    async def types(self) -> list[RelationshipType]: ...
    async def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]: ...
