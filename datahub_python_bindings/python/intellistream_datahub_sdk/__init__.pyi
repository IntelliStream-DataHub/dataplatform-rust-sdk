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

    Behaves as a list, plus ``next_cursor``, the only way to reach the next page.

    The whole loop is::

        page = client.timeseries.filter(limit=100, sort_by="name")
        while True:
            for ts in page:
                ...
            if page.next_cursor is None:
                break
            page = client.timeseries.filter(
                limit=100, sort_by="name", cursor=page.next_cursor)

    ``next_cursor`` is ``None`` on the last page. A *full* page may still be the last, so a walk
    ends with one empty request.

    Not a ``list`` subclass, so ``isinstance(page, list)`` is ``False``; use ``page.items`` when
    something demands a real list.
    """
    @property
    def items(self) -> list[Any]:
        """The rows, as a plain list."""
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

class DataHubClient:
    """The blocking entry point: one client, one connection pool, one token, and a service
    attribute per collection.

    Build it from explicit arguments, or from the environment with ``from_env`` /
    ``from_envfile``. Everything else hangs off it::

        client = DataHubClient.from_envfile()
        series = client.timeseries.filter(name=["Pump*"], limit=100)
        client.timeseries.insert_from_lists(timestamps, values, ts="pump_1_pressure")

    Share one client rather than building one per call. Inside a running event loop, use
    ``AsyncDataHubClient``.
    """
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
        """``base_url`` is the API root. Authenticate with a ready-made ``token`` (used as-is, never
        refreshed) or the OAuth2 trio ``client_id`` / ``client_secret`` / ``token_url``. Supplying
        neither fails on the first call, with a 401.

        Buffering (off by default) spools datapoint and event ingestion to disk when the API is
        unreachable and flushes it on a later call. Enable it with ``enable_buffering=True`` or by
        setting ``buffer_retention_secs`` / ``buffer_max_bytes`` (defaults 72h / 5 GiB).
        ``buffer_dir`` defaults to ``.datahub-spool``.

        ``scope`` and ``audience`` are added to the token request when set. Against a realm using
        Keycloak Organizations, ``scope`` is required — ``organization:*``, or
        ``organization:<alias>`` for one tenant — or every call fails ``401 invalid_token``.

        An assertion source switches to the RFC 7523 ``jwt-bearer`` exchange: pass a ready-made
        ``assertion``, or ``assertion_client_id`` / ``assertion_client_secret`` /
        ``assertion_token_url`` for the SDK to fetch one. With no ``client_secret``,
        ``assertion_grant`` picks ``"client_credentials"`` (default) or ``"jwt-bearer"``. See
        ``docs/entra-federated-auth.md``.
        """
        ...
    @classmethod
    def from_env(cls) -> DataHubClient:
        """Build a client from the process environment alone.

        ``BASE_URL`` is required, plus either ``TOKEN`` or all three of ``CLIENT_ID``,
        ``CLIENT_SECRET`` and ``TOKEN_URI`` — a partial trio is accepted here and every call then
        fails with a 401. Optional: ``SCOPE``, ``AUDIENCE``, the ``ASSERTION*`` family, and
        ``ENABLE_BUFFERING``, ``BUFFER_RETENTION_SECS``, ``BUFFER_MAX_BYTES``, ``BUFFER_DIR``.
        """
    @classmethod
    def from_envfile(cls, path: str | None = None) -> DataHubClient:
        """Load a ``.env`` file, then build the client from the environment as ``from_env`` does.

        Without ``path``, searches upwards from the working directory. Variables already exported
        win over the file's — a stray ``TOKEN`` in the shell shadows the file's OAuth2 settings.
        """
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


class AsyncDataHubClient:
    """The asyncio entry point: the same services as ``DataHubClient``, with every call returning
    an awaitable. For example::

        client = AsyncDataHubClient.from_envfile()
        series = await client.timeseries.filter(name=["Pump*"], limit=100)

    Same constructor and services; the sync classes carry the per-method documentation.
    """
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

    Criteria only: paging is given on the call.

    ``external_id``, ``name``, ``source``, ``unit`` and ``unit_external_id`` are pattern
    lists — see ``PatternList``. Every ``labels`` and ``metadata`` entry must be present; a
    ``None`` metadata value matches the key alone. ``value_type`` matches exactly,
    case-insensitively.

    ``data_set_id`` covers child datasets too. **``None`` and ``[]`` differ here**: ``None``
    places no restriction, ``[]`` matches nothing. Every other list places no restriction when
    empty.

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
    """A scalar field of an update: ``FieldStr(value)`` writes it, ``FieldStr(set_null=True)``
    clears it, and leaving the field off the update leaves it untouched.
    """
    def __init__(self, value: str | None = None, set_null: bool = False) -> None:
        """Pass ``value`` to write it, or ``set_null=True`` to clear the field."""
    @property
    def value(self) -> str | None: ...
    @property
    def set_null(self) -> bool: ...


class FieldU64:
    """A scalar field of an update: ``FieldU64(value)`` writes it, ``FieldU64(set_null=True)``
    clears it, and leaving the field off the update leaves it untouched.
    """
    def __init__(self, value: int | None = None, set_null: bool = False) -> None:
        """Pass ``value`` to write it, or ``set_null=True`` to clear the field."""
    @property
    def value(self) -> int | None: ...
    @property
    def set_null(self) -> bool: ...


class FieldBool:
    """A scalar field of an update: ``FieldBool(value)`` writes it, ``FieldBool(set_null=True)``
    clears it, and leaving the field off the update leaves it untouched.
    """
    def __init__(self, value: bool | None = None, set_null: bool = False) -> None:
        """Pass ``value`` to write it, or ``set_null=True`` to clear the field."""
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


class ListFieldStr:
    """A list-valued field of an update: either replaced wholesale or edited in place.

    Build it with ``set`` (replace) or ``delta`` (add/remove). Omitting the field leaves it alone.

    For example::

        ResourceUpdate(resource="pump_1", labels=ListFieldStr.delta(add=["CRITICAL"]))

    """
    @classmethod
    def set(cls, values: list[str]) -> ListFieldStr:
        """Replace the whole list."""
    @classmethod
    def delta(cls, add: list[str] | None = None, remove: list[str] | None = None) -> ListFieldStr:
        """Add and/or remove entries, keeping the rest. Pass ``add``, ``remove``, or both."""


class ListFieldIdCollection:
    """The related-resource list of an ``EventUpdate``. Entries are ``IdCollection``s, so a
    resource can be named by id, external_id, or both; ``remove`` matches on whichever side is
    given.
    """
    @classmethod
    def set(cls, values: list[IdCollection]) -> ListFieldIdCollection:
        """Replace the whole list."""
    @classmethod
    def delta(
        cls,
        add: list[IdCollection] | None = None,
        remove: list[IdCollection] | None = None,
    ) -> ListFieldIdCollection:
        """Add and/or remove entries, keeping the rest. Pass ``add``, ``remove``, or both."""


class MapField:
    """A ``dict[str, str]`` field of an update — ``metadata``, in practice.

    ``set`` replaces every entry; ``delta`` adds or overwrites the keys in ``add`` and drops the
    keys named in ``remove``, leaving the rest alone. Omitting the field means "leave it alone".
    """
    @classmethod
    def set(cls, values: dict[str, str]) -> MapField:
        """Replace all entries."""
    @classmethod
    def delta(cls, add: dict[str, str] | None = None, remove: list[str] | None = None) -> MapField:
        """Add and/or remove entries, keeping the rest. Pass ``add``, ``remove``, or both."""


# ====================== Time series ======================

class TimeSeries:
    """A univariate series of ``(timestamp, value)`` datapoints, plus the metadata describing it.
    For example::

        ts = TimeSeries(name="Pump 1 pressure", value_type="float",
                        unit_external_id="pressure_bar")
        created = client.timeseries.create([ts])

    Parameters
    ----------
    name : str | None
        User-facing name. Give at least one of ``name`` and ``external_id``; passing neither raises
        ``ValueError``. With only ``name``, ``external_id`` becomes a snake-cased form of it.
    external_id : str | None
        3–512 characters, unique among timeseries. With only ``external_id``, ``name`` is set to the
        same string.
    value_type : {"bigint", "float", "text"}, default "bigint"
        Storage type of the values; ``"decimal"`` is an alias for ``"float"``. Fixed at creation —
        see ``ValueType``.
    metadata : dict[str, str] | None
        Free-form key/value pairs.
    description : str | None
        Free text.
    unit : str | None
        The unit as free text, e.g. ``"mW"``.
    unit_external_id : str | None
        The unit as a catalogue entry, e.g. ``"pressure_bar"``. Setting both keeps ``unit`` as
        written; the two are not reconciled.
    data_set_id : int | None
        The dataset this series belongs to.
    related_resources : list[RelatedNode] | None
        Other nodes to connect this series to; each becomes an edge on create.
    source : str | None
        Where the series came from.

    Notes
    -----
    ``id`` is assigned by the server.
    """
    def __init__(
        self,
        name: str | None = None,
        external_id: str | None = None,
        value_type: str | ValueType = "bigint",
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        data_set_id: int | None = None,
        related_resources: list[RelatedNode] | None = None,
        source: str | None = None,
    ) -> None:
        """Give at least one of ``name`` and ``external_id``; neither raises ``ValueError``.
        """
    @property
    def node_type(self) -> str:
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy")."""
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
    ) -> ResourceNetwork:
        """Walk the graph from this timeseries and return the connected sub-graph.

        ``depth`` bounds the hops (``-1``, the default, is unbounded); ``relationship_types``
        filters the edge types followed; ``limit`` caps the node count.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Events whose ``related_resources`` include this timeseries. ``limit`` caps the results.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""

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
    def from_id(cls, id: int, relationship_type: str) -> RelatedNode:
        """Name the other end of the relation by numeric id."""
    @classmethod
    def from_external_id(
        cls, external_id: str, relationship_type: str
    ) -> RelatedNode:
        """Name the other end of the relation by external id."""
    @property
    def id(self) -> int | None: ...
    @property
    def external_id(self) -> str | None: ...
    @property
    def relationship_type(self) -> str | None: ...
    @property
    def direction(self) -> str | None:
        """``"OUTBOUND"`` / ``"INBOUND"`` on read; ``None`` on input.
        """
    @property
    def edge_id(self) -> int | None: ...


class TimeSeriesUpdate:
    """A partial update for one timeseries.

    Parameters
    ----------
    ts: Timeseries
    """
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
    """One series, and the window of datapoints to remove from it, for ``delete_datapoints``.

    Both bounds are optional and the window is half-open; with neither, every datapoint of the
    series is cleared.

    Parameters
    ----------
    ts : Identifiable
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
    """The storage type of a timeseries' values, fixed when the series is created.

    ``"bigint"``, ``"float"`` or ``"text"``; ``"decimal"`` is an alias for ``"float"``. A plain
    string works wherever a ``ValueType`` is expected.
    """
    def __init__(self, value: str) -> None: ...
    def __repr__(self) -> str: ...


class Datapoint:
    """One datapoint on the way *out*, from ``DatapointsCollectionDatapoints.get_datapoints()``.

    A raw read fills ``value``; an aggregate read fills ``min``, ``max`` and ``average``.
    """
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
    """One datapoint on the way *in*, its value carried as text.

    **``timestamp`` reads back as a string of epoch milliseconds**, not a ``datetime``.
    """
    def __init__(self, ts: datetime.datetime, value: str) -> None: ...
    @classmethod
    def from_int(cls, ts: datetime.datetime, value: int) -> DatapointString:
        """Build one from an integer value, formatting it as text."""
    @classmethod
    def from_float(cls, ts: datetime.datetime, value: float) -> DatapointString:
        """Build one from a float value, formatting it as text."""
    @property
    def timestamp(self) -> str: ...
    @timestamp.setter
    def timestamp(self, value: str) -> None: ...
    @property
    def value(self) -> str: ...
    @value.setter
    def value(self, value: str) -> None: ...


class DatapointsCollectionString:
    """The write side: datapoints for one series, ready for ``timeseries.insert_datapoints``.

    Opaque once built.
    """
    def __init__(
        self,
        datapoints: list[DatapointString],
        ts: Identifiable,
    ) -> None: ...


class DatapointsCollectionDatapoints:
    """The read side: the datapoints of one series, as ``retrieve_datapoints`` and
    ``retrieve_latest_datapoints`` return them.

    **``as_dict()`` reads only ``value``**, so after an aggregate read use ``get_datapoints()``.
    """
    def get_datapoints(self) -> list[Datapoint]:
        """The datapoints as ``Datapoint`` objects.
        """
    def as_dict(self) -> dict[str, Any]:
        """The datapoints as two parallel lists, ``{"timestamps": [...], "values": [...]}``.

        **Reads only ``value``**, so it is all ``None`` after an aggregate read.
        """
    def __len__(self) -> int: ...
    @property
    def next_cursor(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


class RetrieveFilter:
    """What to read from one series: the window, how much, and whether to aggregate.

    **The window is half-open — ``start`` included, ``end`` excluded** — unlike ``TimeFilter``.

    With ``aggregates`` and a ``granularity``, datapoints carry ``min``/``max``/``average`` with
    ``value`` ``None``, each timestamped at the *start* of its bucket.

    Continue a paged read by passing the previous ``next_cursor`` as ``cursor``, with the same
    ``limit``.
    """
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
    """The blocking ``/timeseries`` surface: the series definitions, and the datapoints behind
    them.

    Reached as ``client.timeseries``.

    A read straight after a write can come back short.
    """
    def list(self, limit: int | None = None) -> list[TimeSeries]:
        """The first ``limit`` series in the tenant, newest created first. ``limit`` defaults to
        1000 and may not exceed 10000. No paging; narrow with ``filter``.
        """
    def create(self, input: list[TimeSeries]) -> list[TimeSeries]:
        """Create series definitions, returning the server's echo of them.

        Navigation such as ``neighbors()`` works on the returned objects, not on locally built ones.
        A duplicate ``external_id`` is a **409**.
        """
    def by_ids(self, input: list[Identifiable]) -> list[TimeSeries]:
        """Series by id or external id — a bare ``int`` is an id, a bare ``str`` an external id,
        and a ``TimeSeries`` or ``IdCollection`` may carry both.

        Silently omits what it cannot find; match on ``external_id``, not position.
        """
    def delete(self, input: list[Identifiable]) -> None:
        """Delete series definitions **and their datapoints**. Returns ``None``.

        Remove any subscription or edge pointing at the series first. To empty a series but keep it,
        use ``delete_datapoints``.
        """
    def update(self, input: list[TimeSeriesUpdate]) -> list[TimeSeries]:
        """Apply partial updates, returning the series as they stand afterwards.

        ``value_type`` cannot be updated.
        """
    def search(
        self,
        query: str,
        filter: TimeSeriesFilter | None = None,
        limit: int | None = None,
    ) -> list[TimeSeries]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` narrows the hits, never widens them. ``limit`` defaults to 100 and caps at 1000.
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
        """Pass either ``filter=`` or the criteria as keywords, not both. Returns a ``Page``;
        ``limit`` defaults to 1000, and above 10000 is a 400.
        """

    def insert_datapoints(self, input: list[DatapointsCollectionString]) -> list[str]:
        """Write datapoints, one ``DatapointsCollectionString`` per series.

        **Always returns an empty list**; a failure raises.

        Large batches are sent in chunks, so a failed call can leave part of the batch written.
        Re-sending is safe: a repeated ``(series, timestamp)`` replaces.

        **With buffering enabled**, a write that cannot get through — a 401/403 included — is
        spooled and looks like success.
        """
    def insert_datapoints_binary(
        self,
        input: list[DatapointsCollectionString],
        zstd_level: int | None = None,
    ) -> list[str]:
        """``POST /timeseries/data/binary``: the same collections as ``insert_datapoints``, sent as
        zstd-compressed Arrow frames. ``zstd_level`` is 1, 3 or 9 and defaults to 9.
        """
    def insert_from_lists_binary(
        self,
        timestamps: list[datetime.datetime],
        values: list[float],
        ts: Identifiable,
        zstd_level: int | None = None,
    ) -> list[str]:
        """The binary twin of ``insert_from_lists``.
        """
    def insert_from_lists(
        self,
        timestamps: list[datetime.datetime],
        values: list[float],
        ts: Identifiable,
    ) -> list[str]:
        """Write datapoints for a single series from parallel ``timestamps`` and ``values`` lists.

        Timestamps must be timezone-aware.

        **A length mismatch is not an error**: the tail of the longer list is silently dropped.

        Otherwise as ``insert_datapoints``.
        """
    def retrieve_datapoints(self, input: RetrieveFilter) -> list[DatapointsCollectionDatapoints]:
        """Read datapoints for **one** series.

        Takes one ``RetrieveFilter`` and answers with a list of at most one collection.

        **The window is half-open: ``start`` included, ``end`` excluded**, unlike ``TimeFilter``.

        With ``aggregates``, ``value`` is ``None``; use ``get_datapoints()`` rather than
        ``as_dict()``.
        """
    def delete_datapoints(self, input: list[DeleteFilter]) -> None:
        """Remove datapoints from one or more series, a window at a time. Returns ``None``.

        Takes a **list** of ``DeleteFilter``, each a series and a half-open window; both bounds
        ``None`` clears the series.

        An unknown series fails the **whole** request with a 400.

        The purge runs after the call returns, and cannot be undone.
        """
    def retrieve_latest_datapoints(
        self, input: list[Identifiable]
    ) -> list[DatapointsCollectionDatapoints]:
        """The most recent datapoint of each named series, one collection per series.

        Readable sooner after a write than a range read.
        """


class TimeSeriesServiceAsync:
    """Awaitable twin of ``TimeSeriesServiceSync``, reached as ``client.timeseries`` on an
    ``AsyncDataHubClient``.

    The binary ingest methods are sync-only.
    """
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
        """Pass either ``filter=`` or the criteria as keywords, not both. Returns a ``Page``;
        ``limit`` defaults to 1000, and above 10000 is a 400.
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
    """Something that happened, at a point in time.

    Not a node: keyed by a **UUID**, and has no ``name``.

    ``event_time`` is when it occurred; ``created_time`` is when it was recorded. ``id`` is stamped
    by ``events.create`` on the objects it *returns* — your own ``Event`` still reads
    ``id is None``.
    """
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
    """A time window for a filter criterion — ``created_time``, ``last_updated_time`` or
    ``event_time``.

    **Inclusive at both ends**, unlike ``RetrieveFilter``'s half-open datapoint window.

    Either bound may be omitted, not both. Both must be timezone-aware.
    """
    def __init__(
        self,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
    ) -> None: ...


class EventFilter:
    """AND-combined criteria for ``events.filter`` (``POST /events/filter``).

    ``external_id``, ``source``, ``type``, ``sub_type`` and ``status`` are pattern lists — see
    ``PatternList``. Every ``metadata`` and ``related_resources`` entry must be present; a
    ``None`` metadata value matches the key alone.

    ``data_set_id`` covers child datasets too. **``None`` and ``[]`` differ here**: ``None``
    places no restriction, ``[]`` matches nothing.
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
    """Names an event by ``id`` (a UUID), ``external_id``, or both.
    """
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

    There is no ``event_time`` or ``external_id``: to change either, create a new event and delete
    the old one.
    """

    def __init__(
        self,
        event: EventIdentifiable,
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

    Cheap to query, but they lag the events slightly.
    """

    TYPE: EventDimension
    SUB_TYPE: EventDimension
    STATUS: EventDimension
    SOURCE: EventDimension


class EventsServiceSync:
    """The blocking ``/events`` surface: event CRUD, filter and search, plus the vocabulary
    endpoints.

    Reached as ``client.events``.

    The ``list_*`` / ``search_*`` pairs return the values in use for the four categorical fields.
    They are cheap but lag the events slightly.
    """
    def list(self, limit: int | None = None) -> list[Event]:
        """A criteria-free page of the tenant's events.

        **The oldest ``limit`` events, not the newest.** For the newest, use
        ``filter(sort_by="eventTime", sort_order="desc")``.

        ``limit`` defaults to 1000 and may not exceed 10000. No paging.
        """
    def create(self, input: list[Event]) -> list[Event]:
        """Create events, returning the server's echo of them.

        Events without an ``id`` get a UUID v7 on the *returned* objects — **your own ``Event``
        instances still have ``id is None``**.

        ``type`` is required, 3–128 characters.

        **With buffering enabled**, a send that cannot get through is spooled and the call returns
        an empty list rather than raising.
        """
    def by_ids(self, input: list[EventIdentifiable]) -> list[Event]:
        """Events by UUID or external id — a bare ``uuid.UUID`` is an id, a bare ``str`` an
        external id, and an ``Event`` or ``EventIdCollection`` may carry either.

        Silently omits what it cannot find.

        One external id can answer with **several** events.
        """
    def get(self, id: UUID) -> Event | None:
        """Look up a single event by its UUID. Returns ``None`` if no such event exists."""
    def delete(self, input: list[EventIdentifiable]) -> None:
        """Delete events by UUID or external id. Returns ``None``.
        """
    def update(self, input: list[EventUpdate]) -> list[Event]:
        """Update events in place. Each ``EventUpdate`` targets one event and carries only the
        fields to change; returns the events after the update.
        """
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
        advanced_filter: str | None = None,
    ) -> Page:
        """Events matching every criterion, sorted by ``event_time`` **ascending** by default.

        ``advanced_filter`` takes a boolean expression, e.g.
        ``"type NOT LIKE 'pump' AND (subType = 'water' OR subType = 'gas')"``.

        Pass either ``filter=`` or the criteria as keywords, not both. Returns a ``Page``;
        ``limit`` defaults to 1000, and above 10000 is a 400.
        """

    def search(
        self,
        query: str,
        filter: EventFilter | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        """Free-text search over events, newest first."""
    def count(self) -> int:
        """Total number of events in the tenant."""
    def list_dimension(
        self,
        dimension: EventDimension,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[str]:
        """Distinct values an event field takes in this tenant. ``query`` filters by
        case-insensitive substring; omit it to list everything. ``limit`` defaults to 1000
        server-side and is clamped to 1..=10000. Alphabetical, and restricted to your readable
        datasets.
        """
    def list_types(self, limit: int | None = None) -> list[str]:
        """Every distinct ``type`` on events you can read."""
    def search_types(self, query: str, limit: int | None = None) -> list[str]:
        """Distinct ``type`` values containing ``query`` (case-insensitive substring)."""
    def list_sub_types(self, limit: int | None = None) -> list[str]:
        """Every distinct ``subType`` on events you can read."""
    def search_sub_types(self, query: str, limit: int | None = None) -> list[str]:
        """Distinct ``subType`` values containing ``query`` (case-insensitive substring)."""
    def list_statuses(self, limit: int | None = None) -> list[str]:
        """Every distinct ``status`` on events you can read."""
    def search_statuses(self, query: str, limit: int | None = None) -> list[str]:
        """Distinct ``status`` values containing ``query`` (case-insensitive substring)."""
    def list_sources(self, limit: int | None = None) -> list[str]:
        """Every distinct ``source`` on events you can read."""
    def search_sources(self, query: str, limit: int | None = None) -> list[str]:
        """Distinct ``source`` values containing ``query`` (case-insensitive substring)."""


class EventsServiceAsync:
    """Awaitable twin of ``EventsServiceSync``, reached as ``client.events`` on an
    ``AsyncDataHubClient``.
    """
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
        advanced_filter: str | None = None,
    ) -> Page:
        """Pass either ``filter=`` or the criteria as keywords, not both. Returns a ``Page``;
        ``limit`` defaults to 1000, and above 10000 is a 400.
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
    """A data set — the unit access is granted on, and what every other node is scoped by.

    A grant or a filter on a data set covers everything beneath it in the ``BELONGS_TO`` hierarchy.
    ``data_set_id`` is always ``None`` here and is dropped on create.
    """
    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        id: int | None = None,
        description: str | None = None,
        policies: list[str] | None = None,
        metadata: dict[str, str] | None = None,
        connected_data_sets: list[int] | None = None,
    ) -> None:
        """Build a data set.

        Parameters
        ----------
        external_id : str
            Required. The caller-chosen identifier.
        name : str | None
            Defaults to ``external_id``.
        id : int | None
            Server-assigned; leave unset when creating.
        description : str | None
            Free text.
        policies : list[str] | None
            Access policies this data set is associated with. Not settable through
            ``DatasetUpdate``.
        metadata : dict[str, str] | None
            Free-form key/value pairs; defaults to ``{}``. A filter criterion.
        connected_data_sets : list[int] | None
            **Input-only, and it does not build a hierarchy.** Create a ``BELONGS_TO`` edge
            ``from = parent, to = child`` instead; reversed, it silently builds nothing.
        """
    @property
    def node_type(self) -> str:
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy")."""
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
        """Walk the graph from this dataset and return the connected sub-graph.

        ``depth`` bounds the hops (``-1``, the default, is unbounded); ``relationship_types``
        filters the edge types followed; ``limit`` caps the node count.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Events whose ``related_resources`` include this dataset. ``limit`` caps the results.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""


class DatasetFilter:
    """AND-combined criteria for ``datasets.filter``.

    ``external_id``, ``name`` and ``source`` are pattern lists — see ``PatternList``. Every
    ``labels`` and ``metadata`` entry must be present; a ``None`` metadata value matches the key
    alone.
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


class DatasetUpdate:
    """A partial update for one dataset, mirroring the server's update form.

    ``dataset`` names the target by ``Dataset``, ``IdCollection``, external id or id. Omitted fields
    are left untouched.

    ``policies`` and ``connected_data_sets`` cannot be updated.
    """
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


class DatasetsServiceSync:
    """The blocking ``/datasets`` surface.

    Reached as ``client.datasets``.
    """
    def list(self, limit: int | None = None) -> list[Dataset]:
        """Datasets in the tenant, newest first. ``limit`` defaults to 1000 and may not exceed
        10000. No paging; narrow with ``filter``.
        """
    def create(self, input: list[Dataset]) -> list[Dataset]:
        """Create data sets, returning the echo with the server-assigned ``id``s.

        A ``data_set_id`` on the input is silently dropped. Build a hierarchy with an explicit
        ``BELONGS_TO`` edge; ``connected_data_sets`` does not create one.
        """
    def by_ids(self, input: list[Identifiable]) -> list[Dataset]:
        """Data sets by id or external id — a bare ``int`` is an id, a bare ``str`` an external id,
        and a ``Dataset`` or ``IdCollection`` may carry both.

        Silently omits what it cannot find rather than raising.
        """
    def delete(self, input: list[Identifiable]) -> None:
        """Delete data sets. Returns ``None``.

        **Does not cascade**, and is refused while anything still belongs to the data set.
        Delete or re-home the contents first.
        """
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
        """Pass either ``filter=`` or the criteria as keywords, not both. Returns a ``Page``;
        ``limit`` defaults to 1000, and above 10000 is a 400.
        """

    def search(
        self,
        query: str,
        filter: DatasetFilter | None = None,
        limit: int | None = None,
    ) -> list[Dataset]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` narrows the hits, never widens them. ``limit`` defaults to 100 and caps at 1000.
        """
    def update(self, input: list[DatasetUpdate]) -> list[Dataset]:
        """Apply partial updates, returning the datasets as they stand afterwards.

        Needs an all-datasets write grant (**403** without), even for a caller who can write the
        dataset's contents.

        Settable: ``external_id``, ``name``, ``description``, ``metadata`` and ``labels``. Changing
        ``external_id`` to one already taken is a **409**.
        """
    def policies(self) -> list[Resource]:
        """The access policies a dataset can be associated with, as ``Resource``s.

        **Can come back empty even when policies exist** (a server bug), so an empty result means
        "unknown".
        """


class DatasetsServiceAsync:
    """Awaitable twin of ``DatasetsServiceSync``, reached as ``client.datasets`` on an
    ``AsyncDataHubClient``.
    """
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
        """Pass either ``filter=`` or the criteria as keywords, not both. Returns a ``Page``;
        ``limit`` defaults to 1000, and above 10000 is a 400.
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
    """A plain graph node — the node type with no intrinsic type-label of its own.

    Give ``name`` or ``external_id``; the missing one is derived from the other, and passing
    neither raises ``ValueError``.

    **``geolocation`` is write-only on a plain resource** and reads back as ``None``; use an
    ``Asset`` to keep it.

    ``related_resources`` is filled only by the graph reads and the ``/resources/create`` echo;
    every flat read answers ``[]``.
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
        """This node's type as a string ("asset", "timeseries", "function", "resource",
        "dataset", "policy")."""
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
        """The GeoJSON geometry as a Python ``dict`` (e.g. ``{"type": "Point", "coordinates":
        [10.75, 59.91]}``), or ``None``.
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
        """Walk the graph from this resource and return the connected sub-graph.

        ``depth`` bounds the hops (``-1``, the default, is unbounded); ``relationship_types``
        filters the edge types followed; ``limit`` caps the node count.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Events whose ``related_resources`` include this resource. ``limit`` caps the results.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""


class Asset:
    """A resource that carries a geographic location.

    Unlike a plain ``Resource``, its ``geolocation`` survives a round trip.
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
    def nodes(self) -> list[Node]:
        """The nodes in the traversed sub-graph, each as its own class.
        """
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
    ) -> RelForm:
        """An edge between two nodes named by external id.

        Runs *from* the first *to* the second. A dataset hierarchy is ``from = parent, to = child``;
        reversed, it silently builds nothing.
        """
    @classmethod
    def by_ids(cls, from_id: int, to_id: int, relationship_type: str) -> RelForm:
        """An edge between two nodes named by numeric id, *from* the first *to* the second.
        """
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
    metadata)."""
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
    """The blocking ``/resources`` surface — the **generic node service**.

    Reached as ``client.resources``. Reads span all six node types and answer each row as its own
    class; narrow with ``node_type``.
    """
    def list(self, limit: int | None = None) -> list[Node]:
        """The first ``limit`` nodes in the tenant, newest created first.

        Each row is its own class. ``limit`` defaults to 1000 and may not exceed 10000. No paging;
        narrow with ``filter``.
        """
    def create(
        self, nodes: list[Node], relations: list[RelForm] | None = None
    ) -> GraphResult:
        """Create nodes, and optionally the edges between them, in one call.

        ``nodes`` takes any of the six node classes; ``relations`` is a list of ``RelForm``.

        Returns a ``GraphResult`` of ``.nodes`` and ``.relations``.

        - ``Dataset`` and ``Policy`` nodes need the all-datasets manage grant (**403** without
          it), and their ``data_set_id`` is silently dropped.
        - A duplicate ``external_id`` surfaces as a constraint violation, not the clean 409
          ``timeseries.create`` gives.
        - An unknown ``relationship_type`` or label is **created on the fly**, so a typo becomes a
          permanent catalogue entry.
        - A node carrying two type-labels is a 400 naming both.
        """
    def by_ids(self, input: list[ResourceIdentifiable]) -> list[Node]:
        """Nodes by id or external id, each typed as its own class.

        Accepts any node object, a bare ``int`` (id) or a bare ``str`` (external id). Silently omits
        what it cannot find.
        """
    def delete(self, input: list[ResourceIdentifiable]) -> None:
        """Delete nodes, and with them their relationships. Returns ``None``.

        **Refuses to strand a node**: **409** ``would-strand``, the blockers in
        ``problem["blockedBy"]``.

        The check lags the write: deleting right after creating can strand the node without an
        error.
        """
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> list[Node]:
        """Free-text search for ``query``, ranked by relevance.

        ``filter`` narrows the hits, never widens them. ``limit`` defaults to 100 and caps at 1000.
        """
    def update(self, input: list[ResourceUpdate]) -> GraphResult:
        """Update nodes of any type in place.

        **The echo is typed**: ``.nodes`` holds each node as its own class.
        """
    def get_by_id(self, id: int) -> Node | None:
        """``GET /resources/{id}`` — one resource by numeric id. Raises when it does not exist,
        unlike ``by_ids``, which silently omits what it cannot find.
        """
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
        ``function``, ``resource``, ``dataset``, ``policy``).

        ``external_id``, ``name`` and ``source`` are pattern lists; ``labels`` must all be present;
        a ``None`` ``metadata`` value matches the key alone. ``data_set_id`` expands down the
        dataset hierarchy, and ``None`` (no restriction) differs from ``[]`` (narrow to no datasets,
        matching nothing).
        """


class ResourcesServiceAsync:
    """Awaitable twin of ``ResourcesServiceSync``, reached as ``client.resources`` on an
    ``AsyncDataHubClient``.
    """
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
    SNAKE_UPPER_CASE server-side); `id`/`color` are usually assigned by the server."""
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
    """The blocking ``/labels`` surface — the tenant's label dictionary.

    Reached as ``client.labels``. The server creates a label on first use.
    """
    def list(self) -> list[Label]:
        """Every label in the tenant."""
    def get(self, id: int) -> Label | None:
        """A single label by numeric id, or ``None`` if it doesn't exist.
        """
    def create(self, input: list[Label]) -> list[Label]:
        """Create labels (each needs a unique ``name``). A duplicate name raises with status 409.
        """
    def update(self, input: list[Label]) -> list[Label]:
        """Update labels (identify each by ``id``); only the fields you set are applied."""
    def delete(self, input: list[LabelIdentifiable]) -> None:
        """Delete labels by ``Label``, numeric id, or name.

        Refused with **400** while any resource still carries the label, and always for an intrinsic
        type-label (``ASSET``, ``TIMESERIES``, …).
        """


class LabelsServiceAsync:
    """Awaitable twin of ``LabelsServiceSync``, reached as ``client.labels`` on an
    ``AsyncDataHubClient``.
    """
    async def list(self) -> list[Label]: ...
    async def get(self, id: int) -> Label | None: ...
    async def create(self, input: list[Label]) -> list[Label]: ...
    async def update(self, input: list[Label]) -> list[Label]: ...
    async def delete(self, input: list[LabelIdentifiable]) -> None: ...


# ====================== Units ======================

class Unit:
    """One entry of the DataHub unit catalogue — the shared vocabulary a timeseries points at
    through ``unit_external_id``.

    Read-only: the catalogue is seeded server-side.

    Parameters
    ----------
    id: int
        internal id of the unit
    external_id: str
        user provided external id of the unit, e.g. ``temperature_celsius``
    name: str
        name of the unit, e.g. Celsius, Newton
    long_name: str
        long name of the unit, e.g. Temperature_Celsius, Force_Newton
    symbol: str
        symbol of the unit, e.g. C, N
    description: str
        description of the unit
    alias_names: list[str]
        alias names of the unit, e.g. Pascal, Newton/Meter Squared
    quantity: str
        the quantity dimension of the unit, e.g. Temperature, Mass, Energy-seconds
    conversion: dict[str, float]
        conversion factors from this unit to other units
    source: str
        source of the unit
    source_reference: str
        url to the source of the unit
    """
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
    """The blocking ``/units`` surface — read-only access to the tenant's unit catalogue.

    Reached as ``client.units``.
    """
    def list(self) -> list[Unit]:
        """The whole catalogue, in one call.
        """
    def by_ids(self, input: list[IdCollection]) -> list[Unit]:
        """Units by id or external id. Missing entries are omitted rather than raising.

        **Takes ``IdCollection`` objects only**; a bare ``str`` or ``int`` is a ``TypeError``.
        """
    def by_external_ids(self, input: str) -> list[Unit]:
        """One unit by external id.

        **Singular despite the name**: one string in, a list of zero or one out.

        The awaitable twin is spelled ``by_external_id``, without the ``s``.
        """


class UnitServiceAsync:
    """Awaitable twin of ``UnitServiceSync``, reached as ``client.units`` on an
    ``AsyncDataHubClient``.
    """
    async def list(self) -> list[Unit]: ...
    async def by_ids(self, input: list[IdCollection]) -> list[Unit]: ...
    async def by_external_id(self, input: str) -> list[Unit]: ...


# ====================== Files ======================

class INode:
    """One entry in the file tree — a file or a folder.

    Separate from the resource graph; ``related_resources`` is what links the two.

    A soft-deleted file has its ``external_id`` rewritten, so ``restore`` takes the numeric id.
    """
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
    def related_resource_nodes(self) -> list[Node]:
        """Resolve ``related_resources`` to ``Resource`` objects.
        """
    async def related_resource_nodes_async(self) -> list[Node]:
        """Awaitable variant of ``related_resource_nodes``."""


class FileUpload:
    """Describes one file to upload: where to read it from locally, where to put it remotely, and
    the metadata to attach.

    **Construction reads the filesystem and panics on failure**: a bad path raises
    ``PanicException``, which ``except Exception`` does not catch.

    Leave ``mime_type`` unset to have the server detect it.
    """
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
    def from_path(cls, path: str) -> FileUpload:
        """Upload a local file, keeping its own name, into the root of the tree.
        """
    @classmethod
    def new_with_destination_path(cls, path: str, destination_path: str) -> FileUpload:
        """Upload a local file to a chosen path in the tree.
        """
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

    Identify the node with ``external_id`` or ``id``. Omitted fields are left unchanged.
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
    def content(self) -> bytes:
        """The file content as ``bytes``."""
    def __len__(self) -> int: ...


FileIdentifiable = Union[INode, IdCollection, int, str]


class FilesServiceSync:
    """The blocking ``/files`` surface — a file and folder tree of ``INode``s.

    Reached as ``client.files``.

    Deleting is a **soft** delete — the file moves to the trash, where ``list_trash`` finds it
    and ``restore`` brings it back.
    """
    def upload_file(self, file_upload: FileUpload) -> list[INode]:
        """Upload one file, described by a ``FileUpload``. Returns the created ``INode``s.

        **Local-file problems are ``PanicException``, not ``DataHubException``**, and
        ``except Exception`` does not catch them.
        """
    def list_root_directory(self) -> list[INode]:
        """The contents of the file tree's root.
        """
    def delete(self, input: list[FileIdentifiable]) -> None:
        """Move files to the trash. Returns ``None``.

        A soft delete: ``restore`` brings it back. The ``external_id`` is freed immediately, so a
        trashed file can no longer be found under it.
        """
    def list_directory_by_path(self, path: str) -> list[INode]:
        """The contents of one directory, named by its absolute path.

        **``path`` must begin with ``/``**; without it the call silently addresses the wrong route.
        """
    def get_by_id(self, id: int) -> list[INode]:
        """Metadata for one file or folder, by numeric id."""
    def get_by_external_id(self, external_id: str) -> list[INode]:
        """Metadata for one file or folder, by external id."""
    def search(self, query: str) -> list[INode]:
        """Full-text search over file and folder names and descriptions."""
    def list_trash(self) -> list[INode]:
        """The soft-deleted files the caller can read."""
    def restore(self, input: list[FileIdentifiable]) -> list[INode]:
        """Restore soft-deleted files, identified by numeric id; the trashed external id answers
        404.
        """
    def update(self, update: FileUpdate) -> list[INode]:
        """Rename, move, or edit the metadata of one file or folder."""
    def download(self, id: int) -> FileDownload:
        """Download a file's content into memory."""
    def download_to_path(self, id: int, destination: str) -> int:
        """Download a file straight to ``destination``, without holding it in memory. Returns the
        number of bytes written.
        """


class FilesServiceAsync:
    """Awaitable twin of ``FilesServiceSync``, reached as ``client.files`` on an
    ``AsyncDataHubClient``.
    """
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
    """A standing request for change notifications on a set of timeseries.

    Every referenced series must already exist, and a series bound to a subscription cannot be
    deleted.
    """
    def __init__(
        self,
        external_id: str,
        name: str,
        timeseries: list[SubscriptionTimeseriesId],
    ) -> None:
        """``timeseries`` names the series to watch, each by ``TimeSeries``, ``IdCollection``,
        numeric id or external id.
        """


SubscriptionTimeseriesId = Union[TimeSeries, IdCollection, int, str]


class SubscriptionFilter:
    """Criteria for ``subscriptions.filter`` — currently just ``timeseries``, matching
    subscriptions that watch the named series.
    """
    def __init__(self, timeseries: list[SubscriptionTimeseriesId] | None = None) -> None: ...
    @property
    def timeseries(self) -> list[IdCollection]: ...


class DataSort:
    """A sort for ``subscriptions.filter``: a ``property`` and an ``order``.

    Anything other than ``"desc"`` sorts ascending; an unknown property falls back to the default.
    """
    def __init__(
        self,
        property: list[str] | None = None,
        order: str | None = None,
    ) -> None: ...
    @property
    def property(self) -> list[str]: ...
    @property
    def order(self) -> str | None: ...


class SubscriptionFilterForm:
    """The prepared request body for ``subscriptions.filter`` — a ``SubscriptionFilter`` plus
    ``limit`` and ``sort``.

    Optional: ``filter()`` takes the same as keywords; passing both raises ``ValueError``.
    **``limit`` defaults to 100.**
    """
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
    def sort(self) -> DataSort | None: ...


class EventAction:
    """What happened to the thing a subscription message is about — created, updated, deleted."""
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


class EventObject:
    """What kind of thing a subscription message is about — a timeseries, its datapoints, and so
    on.
    """
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


class WsDatapoint:
    """One datapoint as it arrives over the subscription socket, with its value as text.

    ``as_float()`` parses it, and raises ``ValueError`` for a series whose values are not
    numeric.
    """
    @property
    def timestamp(self) -> str: ...
    @property
    def value(self) -> str: ...
    def as_float(self) -> float:
        """Parse the value as a float. Raises ``ValueError`` if the value isn't numeric.
        """


class DataCollectionString:
    """The datapoints of one series inside a subscription message, alongside the series' identity
    and the window they cover.
    """
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
    """The payload of a subscription message: what happened (``event_action``), to what kind of
    thing (``event_object``), in which tenant, and the affected ``items``.
    """
    @property
    def event_action(self) -> EventAction: ...
    @property
    def event_object(self) -> EventObject: ...
    @property
    def tenant_id(self) -> str | None: ...
    @property
    def items(self) -> list[DataCollectionString]: ...


class SubscriptionMessage:
    """One message off the subscription socket.

    Hand ``message_id`` to ``ack()`` or ``nack()``; an unacked message is redelivered.
    """
    @property
    def subscription_external_id(self) -> str:
        """The subscription this message was delivered for (useful when one listener multiplexes
        several subscriptions).
        """
    @property
    def message_id(self) -> str: ...
    @property
    def payload(self) -> DataWrapperMessage: ...


SubscriptionIdentifiable = Union[Subscription, IdCollection, int, str]


class SubscriptionListener:
    """A WebSocket listener: ``for msg in listener:`` blocks until the next message and ends when
    the connection closes.
    """
    def __iter__(self) -> SubscriptionListener: ...
    def __next__(self) -> SubscriptionMessage: ...
    def next_message(self) -> SubscriptionMessage | None:
        """Wait for the next message. Returns None when the connection has been closed cleanly,
        raises on transport / deserialization errors.
        """
    def ack(self, message_ids: list[str]) -> None:
        """Acknowledge messages, marking them done.

        An unacked message is redelivered.
        """
    def nack(self, message_ids: list[str]) -> None:
        """Negatively acknowledge messages, asking for them to be redelivered."""
    def subscribe(self, external_ids: list[str]) -> None:
        """Add subscriptions to this open connection, without reconnecting."""
    def unsubscribe(self, external_ids: list[str]) -> None:
        """Stop delivering the named subscriptions on this connection, leaving the rest running."""
    def set_subscriptions(self, external_ids: list[str]) -> None:
        """Replace the whole subscription set with ``external_ids``."""
    def close(self) -> None:
        """Close the socket. Iteration then stops and ``next_message`` returns ``None``.
        """
    def __enter__(self) -> SubscriptionListener: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...


class SubscriptionListenerAsync:
    """Asynchronous Python wrapper. Use ``async for msg in listener:`` on the asyncio side."""
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
    """The blocking ``/subscriptions`` surface, plus the WebSocket listener.

    Reached as ``client.subscriptions``. ``listen()`` opens a socket streaming the datapoints the
    subscribed series receive.

    ``list()`` defaults to 1000 rows, ``filter()`` to **100**.
    """
    def create(self, input: list[Subscription]) -> list[Subscription]:
        """Create subscriptions, returning the echo with ``id``, ``date_created`` and
        ``last_updated`` filled in.

        **Every referenced timeseries must already exist**, or it is a 400.
        """
    def list(self, limit: int | None = None) -> list[Subscription]:
        """Subscriptions in the tenant, newest first. ``limit`` defaults to 1000 and may not exceed
        10000. No paging; narrow with ``filter``.
        """
    def filter(
        self,
        form: SubscriptionFilterForm | None = None,
        timeseries: list[SubscriptionTimeseriesId] | None = None,
        limit: int | None = None,
        sort: DataSort | None = None,
    ) -> list[Subscription]:
        """Subscriptions matching every criterion on the filter."""
    def delete(self, input: list[SubscriptionIdentifiable]) -> None:
        """Delete subscriptions. Returns ``None``.
        """
    def listen(self, subscription_external_ids: list[str]) -> SubscriptionListener:
        """Open a WebSocket listener multiplexing the named subscriptions. The ids seed the initial
        set (may be empty — add more with .subscribe()). Returns a ``SubscriptionListener``.
        """


class SubscriptionsServiceAsync:
    """Awaitable twin of ``SubscriptionsServiceSync``, reached as ``client.subscriptions`` on an
    ``AsyncDataHubClient``.
    """
    async def create(self, input: list[Subscription]) -> list[Subscription]: ...
    async def list(self, limit: int | None = None) -> list[Subscription]: ...
    async def filter(
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
    """A graph node representing a computation.

    ``name`` is optional here but required by the api.
    """
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
        "dataset", "policy")."""
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
        """**Always empty**, and never sent on a write.

        Use ``neighbors()`` to read a function's edges.
        """
    # --- navigation (only on functions returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Walk the graph from this function and return the connected sub-graph.

        ``depth`` bounds the hops (``-1``, the default, is unbounded); ``relationship_types``
        filters the edge types followed; ``limit`` caps the node count.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Events whose ``related_resources`` include this function. ``limit`` caps the results.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""


FunctionIdentifiable = Union[Function, IdCollection, int, str]


class FunctionsServiceSync:
    """The blocking ``/functions`` surface.

    Reached as ``client.functions``.

    There is no function filter or search; use ``client.resources.filter(node_type=["function"])``.
    """
    def create(self, input: list[Function]) -> list[Function]:
        """Create functions, returning the echo with server-assigned ids.

        **``name`` is required** by the api, though optional on ``Function``.
        """
    def list(self, limit: int | None = None) -> list[Function]:
        """The first ``limit`` functions you may read, newest first. ``limit`` defaults to the
        server's 1000 and may not exceed 10000; there is no paging, so a bigger catalogue is
        truncated.
        """
    def get_by_id(self, id: int) -> Function | None:
        """One function by numeric id; raises on 404.

        A function you may not read is also reported as missing.
        """
    def by_ids(self, input: list[FunctionIdentifiable]) -> list[Function]:
        """Functions by id or external id, matched on either.

        **Filters a listing client-side**: one full listing per call, and a tenant holding more than
        10 000 functions **silently misses its oldest**.

        Unmatched ids are omitted. Prefer ``get_by_id`` for a numeric id.
        """
    def by_external_id(self, external_id: str) -> Function:
        """The function with the given external id; raises if there is none.
        """
    def update(self, input: list[ResourceUpdate]) -> GraphResult:
        """Update functions in place; ``geolocation`` is ignored, being asset-only.

        ``.nodes`` holds typed node objects — a function comes back as ``Function``.
        """
    def delete(self, input: list[FunctionIdentifiable]) -> None:
        """Delete functions, and with them all of their relationships. Returns ``None``.

        A delete that would strand a surviving node is refused with **409** ``problem_slug ==
        "would-strand"``, the blockers listed under ``problem["blockedBy"]``.
        """


class FunctionsServiceAsync:
    """Awaitable twin of ``FunctionsServiceSync``, reached as ``client.functions`` on an
    ``AsyncDataHubClient``.
    """
    async def create(self, input: list[Function]) -> list[Function]: ...
    async def list(self, limit: int | None = None) -> list[Function]: ...
    async def get_by_id(self, id: int) -> Function | None: ...
    async def by_ids(self, input: list[FunctionIdentifiable]) -> list[Function]: ...
    async def by_external_id(self, external_id: str) -> Function: ...
    async def update(self, input: list[ResourceUpdate]) -> GraphResult: ...
    async def delete(self, input: list[FunctionIdentifiable]) -> None: ...


# ====================== Assets ======================

class AssetsServiceSync:
    """The typed ``/assets`` family — the ``ASSET``-labelled corner of the resource graph.

    Reached as ``client.assets``. The ``/resources`` pipeline with the type pinned, answering
    ``Asset``.
    """

    def create(self, input: list[Asset]) -> list[Asset]:
        """Create assets. Each needs an ``external_id`` and a ``name``.

        Unlike ``resources.create``, the ``ASSET`` label need not be set by hand. Relations are
        not creatable here — use ``resources.create`` for assets and their edges in one call.
        """
    def get_by_id(self, id: int) -> Asset | None:
        """One asset by numeric id; raises on 404.

        A 404 does not tell you the id is free: a node that exists but is not an asset, and an
        asset you may not read, are both reported as missing.
        """
    def by_ids(self, input: list[ResourceIdentifiable]) -> list[Asset]:
        """Assets by id or external id. What cannot be found is omitted, not raised."""
    def list(self, limit: int | None = None) -> list[Asset]:
        """The first ``limit`` assets, newest created first. Defaults to 1000, caps at 10000.

        No paging; narrow with ``filter``.
        """
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
        is_root: bool | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page:
        """Assets matching every criterion, newest first. Criteria combine with AND.

        Pass either ``filter=`` or the individual keywords, not both.

        A ``node_type`` on a ``filter=`` object is ignored.
        """
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> list[Asset]:
        """Free-text search over assets, best match first.

        ``filter`` narrows the hits, never widens them. ``limit`` defaults to 100 and caps at 1000.
        """
    def update(self, input: list[ResourceUpdate]) -> GraphResult:
        """Update assets in place.

        ``.nodes`` holds typed node objects, not necessarily all assets — an update may touch
        relations whose other end is something else.
        """
    def delete(self, input: list[ResourceIdentifiable]) -> None:
        """Delete assets, and with them all their relationships.

        A delete that would disconnect a surviving node from the graph root raises 409
        ``would-strand``, naming the blockers on the exception's ``problem``.
        """


class AssetsServiceAsync:
    """Awaitable twin of ``AssetsServiceSync``, reached as ``client.assets`` on an
    ``AsyncDataHubClient``.
    """
    async def create(self, input: list[Asset]) -> list[Asset]: ...
    async def get_by_id(self, id: int) -> Asset | None: ...
    async def by_ids(self, input: list[ResourceIdentifiable]) -> list[Asset]: ...
    async def list(self, limit: int | None = None) -> list[Asset]: ...
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
        is_root: bool | None = None,
        data_set_id: Sequence[DataSetRef] | None = None,
        limit: int | None = None,
        sort_by: SortBy | None = None,
        sort_order: str | None = None,
        cursor: str | None = None,
    ) -> Page: ...
    async def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> list[Asset]: ...
    async def update(self, input: list[ResourceUpdate]) -> GraphResult: ...
    async def delete(self, input: list[ResourceIdentifiable]) -> None: ...


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
    """The blocking ``/edges`` surface — relationships between resources as first-class objects.

    Reached as ``client.edges``. For linking nodes that already exist; ``resources.create`` creates
    nodes and edges together.

    An edge can be deleted only when both endpoints stay reachable without it.
    """
    def get(self, id: int) -> EdgeProxy | None:
        """One relationship by numeric id, or ``None`` if no edge has that id.
        """
    def by_ids(self, input: list[EdgeIdentifiable]) -> GraphResult:
        """Several relationships plus both endpoints of each, as a ``GraphResult``.
        """
    def create(self, input: list[RelForm]) -> list[EdgeProxy]:
        """Link resources that already exist.

        All-or-nothing: if any relation in the batch fails, none are created. A relation
        targeting a dataset must use ``BELONGS_TO``; a timeseries cannot be linked to a second
        dataset; you need write access to the datasets of both endpoints. Re-creating an
        existing edge between the same two resources conflicts with status 409.
        """
    def delete(self, input: list[EdgeIdentifiable]) -> None:
        """Delete relationships by ``EdgeProxy`` or numeric id. Deletes the link only — the
        resources at each end stay intact. Idempotent: unknown ids are silently skipped.
        """
    def types(self) -> list[RelationshipType]:
        """Every relationship type the tenant has defined."""
    def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]:
        """Register relationship type names up front. Names normalise to uppercase snake case.

        A name that already exists makes the server answer 200 with an empty body and roll back the
        whole batch; check ``types()``.
        """


class EdgesServiceAsync:
    """Awaitable twin of ``EdgesServiceSync``, reached as ``client.edges`` on an
    ``AsyncDataHubClient``.
    """
    async def get(self, id: int) -> EdgeProxy | None: ...
    async def by_ids(self, input: list[EdgeIdentifiable]) -> GraphResult: ...
    async def create(self, input: list[RelForm]) -> list[EdgeProxy]: ...
    async def delete(self, input: list[EdgeIdentifiable]) -> None: ...
    async def types(self) -> list[RelationshipType]: ...
    async def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]: ...
