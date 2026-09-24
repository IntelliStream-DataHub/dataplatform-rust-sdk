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

    The services are ``timeseries``, ``assets``, ``resources``, ``datasets``, ``events``,
    ``files``, ``functions``, ``labels``, ``units``, ``subscriptions`` and ``edges``. Each is a
    property, cheap to read and safe to hold on to.

    One client is meant to be shared: it owns a Tokio runtime and a ``reqwest`` pool, so
    building one per call throws both away. Calling it from inside a running event loop blocks
    that loop for the duration of the request — use ``AsyncDataHubClient`` there.

    Every call raises ``DataHubException`` on an API error; the status is on ``.status_code``
    and, when the API answered with an RFC 9457 problem document, ``.problem_slug`` is what to
    branch on.
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
    def from_env(cls) -> DataHubClient:
        """Build a client from the process environment alone.

        ``BASE_URL`` is required. For credentials, set either ``TOKEN`` or all three of
        ``CLIENT_ID``, ``CLIENT_SECRET`` and ``TOKEN_URI`` — the OAuth2 client is only
        configured when the whole trio is present, so a partial set is accepted here and every
        call then fails with a 401. The optional ones are ``PROJECT_NAME``, ``SCOPE``,
        ``AUDIENCE``, the
        ``ASSERTION*`` family, and the buffering four (``ENABLE_BUFFERING``,
        ``BUFFER_RETENTION_SECS``, ``BUFFER_MAX_BYTES``, ``BUFFER_DIR``).

        Nothing is read from a ``.env`` file here — only variables already exported into the
        process. Use ``from_envfile`` to load one.
        """
    @classmethod
    def from_envfile(cls, path: str | None = None) -> DataHubClient:
        """Load a ``.env`` file, then build the client from the environment as ``from_env`` does.

        ``path`` names the file; omitted, it searches for a ``.env`` from the working directory
        upwards. Variables already exported win over the file's, which is what lets one shell
        variable override a checked-out default — a stray ``TOKEN`` in the shell will shadow the
        file's OAuth2 settings.
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

    Same constructor, same arguments, same service names — the only difference is that each
    service is the ``…ServiceAsync`` twin, so ``client.timeseries`` is a
    ``TimeSeriesServiceAsync``. Semantics, defaults and error behaviour are identical; the sync
    classes carry the detailed per-method documentation.

    Use this inside a running event loop: ``DataHubClient`` would block the loop for the length
    of each request.
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
    """A scalar field of an update: ``FieldStr(value)`` writes it, ``FieldStr(set_null=True)``
    clears it, and leaving the field off the update leaves it untouched.

    Those three states are why this wrapper exists — a bare ``None`` could not tell "don't
    touch" apart from "set to null". Most update constructors also accept a bare ``str`` and
    wrap it as a write for you; the explicit form is what you need for the clear.
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

    Those three states are why this wrapper exists — a bare ``None`` could not tell "don't
    touch" apart from "set to null". Most update constructors also accept a bare ``int`` and
    wrap it as a write for you; the explicit form is what you need for the clear.
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

    Those three states are why this wrapper exists — a bare ``None`` could not tell "don't
    touch" apart from "set to null". Most update constructors also accept a bare ``bool`` and
    wrap it as a write for you; the explicit form is what you need for the clear.
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

    An update is a *replace* (``set``) or a *delta* (``delta``), never both — the two
    constructors make the illegal mix unrepresentable, which is why there is no bare
    initializer. Leaving the field off the update entirely is the third option, and means "leave
    it alone".

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

    The object is the *definition* — name, unit, value type, where it sits in the graph. The
    datapoints live behind it and are written and read through the service:
    ``client.timeseries.insert_from_lists(...)`` and
    ``client.timeseries.retrieve_datapoints(...)``. For example::

        ts = TimeSeries(name="Pump 1 pressure", value_type="float",
                        unit_external_id="pressure_bar")
        created = client.timeseries.create([ts])

    Parameters
    ----------
    name : str | None
        User-facing name. Give at least one of ``name`` and ``external_id``; passing neither raises
        ``ValueError``. With only ``name``, ``external_id`` becomes a snake-cased form of it.
    external_id : str | None
        Caller-chosen id, 3–512 characters, unique among timeseries. With only ``external_id``,
        ``name`` is set to the same string. The same string may be reused by another entity type.
    value_type : {"bigint", "float", "text"}, default "bigint"
        Storage type of the values; ``"decimal"`` is an alias for ``"float"``. Fixed at creation —
        see ``ValueType``.
    metadata : dict[str, str] | None
        Free-form key/value pairs, and a filterable one: ``timeseries.filter(metadata=...)``
        matches on them.
    description : str | None
        Free text. Searched by ``timeseries.search``, alongside the name.
    unit : str | None
        The unit as free text, e.g. ``"mW"`` or ``"Liter/min"``. Descriptive only.
    unit_external_id : str | None
        The unit as a catalogue entry, e.g. ``"pressure_bar"`` — see ``Unit``. This is the one that
        makes a series convertible, so prefer it to ``unit`` where the catalogue has a match.
        Setting both keeps ``unit`` as written: the server does not reconcile the two.
    data_set_id : int | None
        The dataset this series belongs to. Datasets are what access is granted on, so this is
        also what decides who can read it.
    related_resources : list[RelatedNode] | None
        Other nodes to connect this series to. On create, each entry's id/external_id plus its
        ``relationship_type`` becomes an edge server-side.
    source : str | None
        Where the series came from, e.g. the name of the ingesting system. A filter criterion.

    Notes
    -----
    ``id`` is assigned by the server and is not a constructor argument; it is ``None`` until the
    series has been created.
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

        With only ``name``, the external id becomes a snake-cased form of it. With only
        ``external_id``, the name is set to the same string. ``id`` is assigned by the server and
        is not a constructor argument.
        """
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
        """Walk the graph from this timeseries and return the connected sub-graph (its ``nodes``,
        the ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in
        hops (``-1``, the default, = the whole connected component); ``relationship_types``
        filters which edge types to follow (``None`` = all); ``limit`` caps the node count.
        Neighbour nodes are typed as their own classes. Blocking; see ``neighbors_async`` for
        the awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Fetch events whose ``related_resources`` include this timeseries (matched by graph-node
        id when present, else external id), via ``events.filter``. ``limit`` caps the results
        (default 100). Blocking; see ``related_events_async``.
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
    """Python wrapper for TimeseriesUpdate, represents a request for change to a timeseries

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

    Both bounds are optional and the window is half-open, so: give both to clear the window
    between them, ``inclusive_begin`` alone to clear everything from that instant onward,
    ``exclusive_end`` alone to clear everything before it, and neither to clear every datapoint
    of the series while keeping its definition, edges and subscriptions.

    The purge is asynchronous: the call returns once the request is accepted, and a read
    straight afterwards can still see the datapoints. It cannot be undone.

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
    """Enumerator for the datatype of a timeseries.

    The storage type of a timeseries' values, fixed when the series is created.

    Three options: ``"bigint"``, ``"float"`` and ``"text"``. ``"decimal"`` is accepted as an
    alias for ``"float"`` and normalises to it, so a series created either way reads back as
    ``"float"``. Matching is case-insensitive, and anywhere a ``ValueType`` is expected a plain
    string works just as well — ``TimeSeries(external_id="p1", value_type="float")`` needs no
    wrapper.

    Note this is the *construction* catalogue. The ``value_type`` criterion on
    ``TimeSeriesFilter`` matches against the wider set the server stores (``BIGINT``, ``FLOAT``,
    ``FLOAT32``, ``NUMERIC``, ``DECIMAL32``, ``TEXT``, ``MIXED``), so a filter can name a type
    this class cannot create.
    """
    def __init__(self, value: str) -> None: ...
    def __repr__(self) -> str: ...


class Datapoint:
    """One datapoint on the way *out*, from ``DatapointsCollectionDatapoints.get_datapoints()``.

    Which fields are filled depends on the read: a raw read fills ``value``; an aggregate read
    fills ``min``, ``max`` and ``average`` and leaves ``value`` as ``None``. **``None`` means
    "the endpoint did not return it", never zero.**

    ``timestamp`` is an aware UTC ``datetime``.
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
    """One datapoint on the way *in*, as a ``(timestamp, value)`` pair with the value carried as
    text so it can stand in for any of the series value types.

    Build it from an aware ``datetime`` plus a string, or through ``from_int`` / ``from_float``.

    **The ``timestamp`` property reads back as a string of epoch milliseconds**, not the
    ``datetime`` you passed. Both properties are settable and neither is validated.
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

    Opaque once built — it carries no readable members. Use ``DatapointsCollectionDatapoints``,
    which is what the read calls hand back, when you need to get at values.

    Passing a ``TimeSeries`` as ``ts`` also carries that series' ``unit`` and
    ``unit_external_id`` along; naming it by id or external id leaves both unset.
    """
    def __init__(
        self,
        datapoints: list[DatapointString],
        ts: Identifiable,
    ) -> None: ...


class DatapointsCollectionDatapoints:
    """The read side: the datapoints of one series, as ``retrieve_datapoints`` and
    ``retrieve_latest_datapoints`` return them.

    ``get_datapoints()`` gives the ``Datapoint`` objects, ``as_dict()`` the two parallel lists
    ``{"timestamps": [...], "values": [...]}``, and ``len()`` the count. **``as_dict()`` reads
    only ``value``**, so after an aggregate read every entry in ``"values"`` is ``None`` — use
    ``get_datapoints()`` there.

    ``next_cursor`` continues a paged read, and is ``None`` when the result fit in one page.
    """
    def get_datapoints(self) -> list[Datapoint]:
        """The datapoints as ``Datapoint`` objects — the form to use after an aggregate read, where
        ``as_dict()`` cannot reach ``min``/``max``/``average``.
        """
    def as_dict(self) -> dict[str, Any]:
        """The datapoints as two parallel lists, ``{"timestamps": [...], "values": [...]}`` — the
        shape a DataFrame is built from.

        **Reads only ``value``**, so after an aggregate read every entry in ``"values"`` is
        ``None``. Use ``get_datapoints()`` there.
        """
    def __len__(self) -> int: ...
    @property
    def next_cursor(self) -> str | None: ...
    @property
    def id(self) -> int | None: ...


class RetrieveFilter:
    """What to read from one series: the window, how much, and whether to aggregate.

    ``ts`` is required and names the series. **The window is half-open — ``start`` is included,
    ``end`` is excluded** — unlike ``TimeFilter``, which is inclusive at both ends.

    Setting ``aggregates`` (e.g. ``["avg", "min", "max"]``) together with a ``granularity``
    (e.g. ``"1d"``) buckets the read: the datapoints then carry ``min``/``max``/``average`` and
    their ``value`` is ``None``, and each timestamp is the *start* of its bucket.

    Every field is read-only once constructed — build a new filter to change one. Carry
    ``next_cursor`` from the previous page into ``cursor`` to continue, passing the same
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

    Reached as ``client.timeseries``. Two halves worth keeping apart — ``create`` / ``by_ids`` /
    ``list`` / ``filter`` / ``search`` / ``update`` / ``delete`` operate on the *definitions*,
    while ``insert_datapoints`` / ``insert_from_lists`` / ``retrieve_datapoints`` /
    ``retrieve_latest_datapoints`` / ``delete_datapoints`` operate on the values.

    Datapoints land in ClickHouse and settle after the call returns, so a read straight after a
    write can come back short. Poll rather than assert once.
    """
    def list(self, limit: int | None = None) -> list[TimeSeries]:
        """The first ``limit`` series in the tenant, newest created first. ``limit`` defaults to
        the server's 1000 and may not exceed 10000; there is no paging, so a bigger tenant is
        truncated rather than paged — use ``filter`` to narrow instead.
        """
    def create(self, input: list[TimeSeries]) -> list[TimeSeries]:
        """Create series definitions, returning the server's echo of them.

        The echoed objects carry a client, so ``neighbors()`` and the other navigation methods
        work on them — locally built ones raise instead. A duplicate ``external_id`` is a
        **409**.

        This creates the definition only; the datapoints go in separately with
        ``insert_from_lists`` or ``insert_datapoints``.
        """
    def by_ids(self, input: list[Identifiable]) -> list[TimeSeries]:
        """Series by id or external id — a bare ``int`` is an id, a bare ``str`` an external id,
        and a ``TimeSeries`` or ``IdCollection`` may carry both.

        Batch lookups answer with the subset that was found, so a shorter list back is the
        normal way an unknown id is reported. Match on ``external_id`` rather than on position.
        """
    def delete(self, input: list[Identifiable]) -> None:
        """Delete series definitions **and their datapoints**. Returns ``None``.

        The definition is gone when the call returns; the datapoints are purged afterwards.
        Nothing can read them in between, because every read resolves the series first.

        Remove any subscription or edge pointing at the series first — the api refuses to strand
        one. To empty a series but keep its definition, edges and subscriptions, use
        ``delete_datapoints`` with both bounds left as ``None``.
        """
    def update(self, input: list[TimeSeriesUpdate]) -> list[TimeSeries]:
        """Apply partial updates, returning the series as they stand afterwards.

        Each ``TimeSeriesUpdate`` names one series and carries only the fields to change;
        anything it leaves out is untouched.

        **There is no ``value_type`` on the update form.** A series' storage type is fixed at
        creation — re-typing it would invalidate the datapoints already stored. Create a new
        series and re-ingest instead.
        """
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

    def insert_datapoints(self, input: list[DatapointsCollectionString]) -> list[str]:
        """Write datapoints, one ``DatapointsCollectionString`` per series.

        **Returns an empty list, always** — the api answers a successful write with 204 and no
        body, and a buffered write with 202 and no body. So the return value tells you nothing;
        what tells you the write failed is the exception.

        Large batches are cut into chunks of at most 100 000 datapoints and sent concurrently. A
        chunk that is refused raises, and the chunks after it are not sent — so a failed call
        can leave part of the batch written. Re-sending is safe: datapoints are keyed by
        ``(series, timestamp)`` and a repeat replaces rather than duplicates.

        **With buffering enabled** (see ``DataHubClient``), a write that cannot get through
        spools to disk and returns normally. Nothing in the return value distinguishes that from
        a confirmed write, and 401/403 are buffered too, so an expired credential also looks
        like success here.
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
        """The binary twin of ``insert_from_lists``: parallel timestamp and value sequences for one
        series, which is the shape a DataFrame column pair arrives in.
        """
    def insert_from_lists(
        self,
        timestamps: list[datetime.datetime],
        values: list[float],
        ts: Identifiable,
    ) -> list[str]:
        """Write datapoints for a single series from parallel ``timestamps`` and ``values`` lists —
        the shape a DataFrame column pair arrives in.

        ``ts`` names the series by external id, numeric id, or a ``TimeSeries``. Timestamps must
        be timezone-aware; a naive one raises ``TypeError``.

        **The two lists are zipped, and a length mismatch is not an error** — the tail of the
        longer one is silently dropped. Check ``len(timestamps) == len(values)`` yourself. (The
        binary twin, ``insert_from_lists_binary``, does raise.)

        Otherwise identical to ``insert_datapoints``, including the empty return and the
        buffering behaviour.
        """
    def retrieve_datapoints(self, input: RetrieveFilter) -> list[DatapointsCollectionDatapoints]:
        """Read datapoints for **one** series.

        Takes a single ``RetrieveFilter``, not a list, and answers with a list holding at most
        one collection — so the idiom is ``client.timeseries.retrieve_datapoints(rf)[0]``, and
        an unmatched read can give you an empty list.

        **The window is half-open: ``start`` is included, ``end`` is excluded.** This is the
        opposite of ``TimeFilter``, which backs ``created_time`` / ``last_updated_time`` /
        ``event_time`` and is inclusive at both ends. Two different idioms in one SDK,
        deliberately, so a window written for one is wrong for the other.

        Asking for ``aggregates`` changes what comes back: the datapoints then carry ``min``,
        ``max`` and ``average``, and ``value`` is ``None``. ``as_dict()`` reads only ``value``,
        so use ``get_datapoints()`` for an aggregate read.

        Page with the collection's ``next_cursor``, fed back as the next ``RetrieveFilter``'s
        ``cursor``; it is ``None`` when the result fit in one page.
        """
    def delete_datapoints(self, input: list[DeleteFilter]) -> None:
        """Remove datapoints from one or more series, a window at a time. Returns ``None``.

        Takes a **list** of ``DeleteFilter`` — note the asymmetry with ``retrieve_datapoints``,
        which takes one. Each filter names a series and a half-open window; leaving both bounds
        ``None`` clears every datapoint of that series while keeping its definition, edges and
        subscriptions, which is how a bad backfill is undone.

        An item naming a series that does not exist fails the **whole** request with a 400.

        **Accepted is not done, and none of it can be undone.** The call returns once the
        request is accepted; the purge runs afterwards, so a read straight after can still see
        the rows.
        """
    def retrieve_latest_datapoints(
        self, input: list[Identifiable]
    ) -> list[DatapointsCollectionDatapoints]:
        """The most recent datapoint of each named series, one collection per series.

        Series are named by external id, numeric id, ``TimeSeries`` or ``IdCollection``. This
        becomes readable sooner after a write than a range read does, which makes it the usual
        way to check whether an ingest landed at all.
        """


class TimeSeriesServiceAsync:
    """Awaitable twin of ``TimeSeriesServiceSync``, reached as ``client.timeseries`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``TimeSeriesServiceSync`` carries the per-method documentation.

    Note ``insert_datapoints_binary`` and ``insert_from_lists_binary`` are sync-only; there is
    no awaitable binary ingest path yet.
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
    """Something that happened, at a point in time.

    Not a node: events are keyed by a **UUID** rather than a numeric id, have no ``name``, and
    are stored in their own partitioned table.

    ``event_time`` is when the thing *occurred* — the source or sensor time — as distinct from
    the server-set ``created_time``, which is when it was recorded. ``id`` is a client-generated
    UUID v7, stamped by ``events.create`` on the objects it returns, so a locally-built
    ``Event`` still reads ``id is None`` after a successful create: take the id off the result.

    ``related_resources`` holds *selectors* (``IdCollection``), naming a resource by id,
    external id or both — not edges. ``related_resource_nodes()`` resolves them to node objects.
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

    **Inclusive at both ends**: a row landing exactly on ``end`` is returned. To exclude the
    upper endpoint, subtract a millisecond, the resolution these columns are stored at. Note
    this is the opposite of ``RetrieveFilter``'s datapoint window, which is half-open.

    ``TimeFilter(start, end)`` bounds both sides, ``TimeFilter(start=...)`` alone is "from then
    on", ``TimeFilter(end=...)`` alone is "up to then". Both ``None`` raises ``ValueError``, as
    does a ``start`` after the ``end``. Both must be timezone-aware.
    """
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
    """Event id selector exposed to Python. Events are keyed by a client-generated UUID v7, so this
    carries the ``id`` (UUID) and/or the ``external_id``. Construct with either or both:
    ``EventIdCollection(id=my_uuid)`` or ``EventIdCollection(external_id="...")``.
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

    There is deliberately no ``event_time`` and no ``external_id``: both identify an event rather
    than describe it, and the api dropped each from its update form, so sending either is a ``400``
    naming the field.

    The events table is partitioned by ``event_time``, so the mutation cannot move the row and is
    refused outright. ``externalId`` maps to the *set* of event UUIDs behind it — events sharing an
    external id are the lifecycle of one logical event — so a rename would take every sibling along,
    and an event targeted by UUID left the server without the old value to re-key with. Re-key by
    creating a new event and deleting the old one; record a corrected time the same way.
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

    Served from dimension tables the write path maintains, so they are cheap enough for a
    typeahead but *eventually consistent* with the events themselves.
    """

    TYPE: EventDimension
    SUB_TYPE: EventDimension
    STATUS: EventDimension
    SOURCE: EventDimension


class EventsServiceSync:
    """The blocking ``/events`` surface: event CRUD, filter and search, plus the vocabulary
    endpoints.

    Reached as ``client.events``. Events are not nodes — they are keyed by UUID rather than a
    numeric id, have no ``name``, and carry an ``event_time`` saying when the thing happened, as
    distinct from the server-set ``created_time`` saying when it was recorded.

    The ``list_*`` / ``search_*`` pairs answer "what values does this tenant actually use" for
    the four categorical fields, and are what a filter dropdown is built from. They read small
    server-side tables rather than scanning events, so they are cheap but lag the events
    slightly.

    Events live in ClickHouse and settle after the call returns, so poll rather than assert
    once.
    """
    def list(self, limit: int | None = None) -> list[Event]:
        """A criteria-free page of the tenant's events.

        **The oldest ``limit`` events, not the newest.** It runs the event filter with an empty
        body, whose default sort is ``eventTime`` ascending — the order the cursor pages in. The
        node listings beside it (``resources.list``, ``timeseries.list``, ``datasets.list``)
        really are newest-first; events are the one member of the family that reads the other
        way round. For "what just happened", use ``filter(sort_by="eventTime",
        sort_order="desc")``.

        ``limit`` defaults to the server's 1000 and may not exceed 10000. A plain list is
        returned rather than a ``Page``: there is no cursor to continue with.
        """
    def create(self, input: list[Event]) -> list[Event]:
        """Create events, returning the server's echo of them.

        Any event without an ``id`` is stamped with a client-generated UUID v7 before the first
        send, so a retry collapses onto the same row instead of duplicating. The stamp lands on
        the returned objects — **your own ``Event`` instances still have ``id is None``**, so
        read the id off the result.

        ``type`` is required and must be 3–128 non-blank characters; a blank one is a 400 naming
        the offending index.

        **With buffering enabled**, a send that cannot get through spools to disk and the call
        returns an **empty list** rather than raising. An empty result therefore means
        "buffered", not "nothing was created".
        """
    def by_ids(self, input: list[EventIdentifiable]) -> list[Event]:
        """Events by UUID or external id — a bare ``uuid.UUID`` is an id, a bare ``str`` an
        external id, and an ``Event`` or ``EventIdCollection`` may carry either.

        Silently omits what it cannot find, so a shorter list back is how an unknown id is
        reported. Use ``get(uuid)`` when you want a single event and ``None`` for a miss.

        One external id can answer with **several** events: an external id names the set of
        UUIDs behind it, which together are the lifecycle of one logical event.
        """
    def get(self, id: UUID) -> Event | None:
        """Look up a single event by its UUID. Returns ``None`` if no such event exists."""
    def delete(self, input: list[EventIdentifiable]) -> None:
        """Delete events by UUID or external id. Returns ``None``.

        Also the second half of a re-key: an event's ``external_id`` and ``event_time`` cannot
        be updated, so correcting either means creating a replacement and deleting the original.
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
        """Pass either ``filter=`` or the individual criteria keywords; passing both is a
        ``TypeError``. Paging is always given here rather than on the filter, so one filter can be
        reused across calls.
        """

    def search(
        self,
        query: str,
        filter: EventFilter | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        """Free-text search over event descriptions, ranked by relevance."""
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

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``EventsServiceSync`` carries the per-method documentation.
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
    """A data set — the unit access is granted on, and what every other node is scoped by.

    Granting someone a data set grants them everything beneath it in the ``BELONGS_TO``
    hierarchy, and naming one in a filter's ``data_set_id`` matches its children too. That is
    also why a data set cannot itself belong to one: ``data_set_id`` is always ``None`` here and
    is dropped on create.
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
            Defaults to ``external_id``. Note this runs the opposite way from ``TimeSeries`` and
            ``Resource``, where a missing ``external_id`` is derived from the name.
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
            **Input-only, and it does not build a hierarchy.** The api never populates it on a
            read, so it is empty on everything the server returns. To make one data set a child
            of another, create the edge explicitly — and mind the direction: the row is stored
            ``from = parent, to = child``, even though the relationship is named ``BELONGS_TO``.
            Reversing it produces no hierarchy and no error.
        """
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
        """Walk the graph from this dataset and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters
        which edge types to follow (``None`` = all); ``limit`` caps the node count. Neighbour
        nodes are typed as their own classes. Blocking; see ``neighbors_async`` for the
        awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Fetch events whose ``related_resources`` include this dataset (matched by graph-node id
        when present, else external id), via ``events.filter``. ``limit`` caps the results
        (default 100). Blocking; see ``related_events_async``.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""


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


class DatasetUpdate:
    """A partial update for one dataset, mirroring the server's update form.

    ``dataset`` names the target — a ``Dataset``, an ``IdCollection``, an external id or a
    numeric id. Every other argument is a field wrapper and only the ones you pass are sent;
    anything omitted is left untouched.

    There is deliberately no ``policies`` or ``connected_data_sets`` here: the update endpoint
    does not accept them, whatever a ``Dataset`` can carry on create.
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

    Reached as ``client.datasets``. A data set is the unit access is granted on, and what every
    other node is scoped by — so it sits above the things that belong to it, both for
    permissions and for deletion.

    Naming a data set in a filter covers everything beneath it in the ``BELONGS_TO`` hierarchy,
    the same expansion its ACL grant applies.
    """
    def list(self, limit: int | None = None) -> list[Dataset]:
        """Datasets in the tenant, newest first. ``limit`` defaults to the server's 1000 and may
        not exceed 10000; there is no paging, so a bigger tenant is truncated rather than paged
        — use ``filter`` to narrow instead.
        """
    def create(self, input: list[Dataset]) -> list[Dataset]:
        """Create data sets, returning the echo with the server-assigned ``id``s.

        A ``data_set_id`` set on the input is silently dropped: a data set inside a data set
        would orphan its ACL grant. Build the hierarchy with an explicit ``BELONGS_TO`` edge
        instead — ``connected_data_sets`` does not create one.
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
    def update(self, input: list[DatasetUpdate]) -> list[Dataset]:
        """Apply partial updates, returning the datasets as they stand afterwards.

        A dataset is the unit access is granted on, so the server treats editing one as an
        operator action: this needs an all-datasets write grant and raises 403 without one, even
        for a caller who can write the dataset's contents.

        Settable: ``external_id``, ``name``, ``description``, ``metadata`` and ``labels``. There
        is no ``policies`` or ``connected_data_sets`` — the endpoint does not accept them,
        whatever a ``Dataset`` can carry on create — and no ``write_protected`` /
        ``deactivated``, both removed server-side as inert. Changing ``external_id`` to one
        already taken is a **409**.
        """
    def policies(self) -> list[Resource]:
        """The access policies a dataset can be associated with, as ``Resource``s.

        **Known to come back empty even when policies exist** — the server answers 200 with no
        body at all. That is a server-side bug, not something these bindings can work around, so
        treat an empty result as "unknown" rather than "none".
        """


class DatasetsServiceAsync:
    """Awaitable twin of ``DatasetsServiceSync``, reached as ``client.datasets`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``DatasetsServiceSync`` carries the per-method documentation.
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
    """A plain graph node — the node type with no intrinsic type-label of its own.

    Give ``name`` or ``external_id``; the missing one is derived from the other, and passing
    neither raises ``ValueError``.

    **``geolocation`` is write-only on a plain resource**: the server accepts it and never
    echoes it, so it reads back as ``None``. Assets do carry theirs — create the node with the
    ``ASSET`` label, or use ``client.assets``, if the geometry needs to survive a round trip.

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
        """Walk the graph from this resource and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters
        which edge types to follow (``None`` = all); ``limit`` caps the node count. Blocking;
        see ``neighbors_async`` for the awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Fetch events whose ``related_resources`` include this resource (matched by graph-node id
        when present, else external id), via ``events.filter``. ``limit`` caps the results
        (default 100). Blocking; see ``related_events_async``.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""


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
    def nodes(self) -> list[Node]:
        """The nodes in the traversed sub-graph, each as its own class (``Asset``, ``TimeSeries``,
        ``Dataset``, …), carrying what a flat read carries. Type-specific fields stay optional:
        a node written before a field was projected reports it absent, which is not a default.
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

        Direction matters and is not implied by the type name: the edge runs *from* the first
        argument *to* the second. A dataset hierarchy is stored ``from = parent, to = child``
        under ``BELONGS_TO``; reversing it produces no hierarchy and no error.
        """
    @classmethod
    def by_ids(cls, from_id: int, to_id: int, relationship_type: str) -> RelForm:
        """An edge between two nodes named by numeric id. Runs *from* the first *to* the second —
        see ``by_external_ids`` on why the direction matters.
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
    """The blocking ``/resources`` surface — the **generic node service**.

    Reached as ``client.resources``. Unlike the typed services beside it, every read here spans
    all six node types (asset, timeseries, function, resource, data set, policy) and answers
    each row as its own class, so ``isinstance(node, TimeSeries)`` works on what comes back and
    an object from ``resources.filter()`` behaves exactly like one from ``timeseries.by_ids()``.
    Narrow with ``node_type`` when you want only some of them.

    This is also the only service that creates nodes **and** the edges between them in one call.
    Edges between resources that already exist go through ``client.edges`` instead.
    """
    def list(self, limit: int | None = None) -> list[Node]:
        """The first ``limit`` nodes in the tenant, newest created first — the cheap "what have I
        got" read, with no criteria and no paging.

        Spans every node type and answers each row as its own class, exactly as ``filter`` does,
        so ``isinstance(node, TimeSeries)`` works on what comes back. ``limit`` defaults to the
        server's 1000 and may not exceed 10000; a ``Page`` is not returned because there is no
        cursor to continue with — narrow with ``filter`` instead of raising the number.
        """
    def create(
        self, nodes: list[Node], relations: list[RelForm] | None = None
    ) -> GraphResult:
        """Create nodes, and optionally the edges between them, in one call.

        ``nodes`` takes any of the six node classes; each is dispatched server-side by its own
        type-labels. ``relations`` is a list of ``RelForm``, creating edges among the nodes
        being created; omit it for nodes alone.

        Returns a ``GraphResult``: ``.nodes`` typed per row, ``.relations`` the created edges
        with their server-assigned ids. This is one of the only two paths that populate a node's
        ``related_resources`` — flat reads always answer ``[]``.

        Things worth knowing:

        - ``Dataset`` and ``Policy`` nodes need the all-datasets manage grant (**403** without
          it), and their ``data_set_id`` is silently dropped.
        - A duplicate ``external_id`` surfaces as a constraint violation, not the clean 409
          ``timeseries.create`` gives.
        - An unknown ``relationship_type``, or an unknown entry in ``labels``, is **created on the
          fly** rather than rejected. Convenient, but a typo becomes a permanent catalogue entry,
          and relationship types cannot be deleted.
        - A node carrying two type-labels is a 400 naming both.
        """
    def by_ids(self, input: list[ResourceIdentifiable]) -> list[Node]:
        """Nodes by id or external id, each typed as its own class.

        Accepts any node object, a bare ``int`` (id) or a bare ``str`` (external id). Silently
        omits what it cannot find — contrast ``get_by_id``, which raises on a miss.

        ``related_resources`` is empty on this path, as on every flat read.
        """
    def delete(self, input: list[ResourceIdentifiable]) -> None:
        """Delete nodes, and with them their relationships. Returns ``None``.

        **Refuses to strand a node.** Deleting something that is another node's only route to
        the graph root answers **409** with ``problem_slug == "would-strand"``, naming the
        blockers in ``problem["blockedBy"]``. Include them in the same delete, or keep a
        connecting path.

        The check reads the graph projection, which lags the write — so deleting very soon after
        creating gets the *wrong answer* rather than an error: the refusal does not fire and the
        node is stranded. Leave a moment between the two.
        """
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
    def update(self, input: list[ResourceUpdate]) -> GraphResult:
        """Update nodes in place. Each ``ResourceUpdate`` targets one node and carries only the
        fields to change; every field it can set is shared by all node types, so one update form
        covers them all.

        **The echo is typed**, like every other read here: ``.nodes`` holds each node as its own
        class, so a timeseries comes back as ``TimeSeries`` carrying its ``unit`` and an asset
        as ``Asset`` carrying its ``geo_location``. The ``labels`` reflect what the server
        stored, intrinsic type-label included.

        This used to answer with a plain ``Resource`` whatever the node's real type. The api's
        node-update refactor made the pipeline per-type and the echo followed.
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
        ``function``, ``resource``, ``dataset``, ``policy``); every node carries its type as a
        label so you can tell what came back.

        ``external_id``, ``name`` and ``source`` are pattern lists; ``labels`` must all be
        present; a ``None`` ``metadata`` value matches the key alone. ``data_set_id`` expands
        down the dataset hierarchy, and ``None`` (no restriction) differs from ``[]``
        (narrow to no datasets, matching nothing). See ``TimeSeriesFilter`` for the sort and
        cursor rules.
        """


class ResourcesServiceAsync:
    """Awaitable twin of ``ResourcesServiceSync``, reached as ``client.resources`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``ResourcesServiceSync`` carries the per-method documentation.
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
    """The blocking ``/labels`` surface — the tenant's label dictionary.

    Reached as ``client.labels``. A label is a dictionary row rather than an entity: the server
    creates one on first use, so tagging a resource with a new name needs no seeding here, and a
    label cannot be deleted while anything still carries it.
    """
    def list(self) -> list[Label]:
        """Every label in the tenant."""
    def get(self, id: int) -> Label | None:
        """A single label by numeric id, or ``None`` if it doesn't exist (the server answers an
        unknown id with 404; that is absorbed into ``None``).
        """
    def create(self, input: list[Label]) -> list[Label]:
        """Create labels (each needs a unique ``name``). A duplicate name raises with status 409.
        """
    def update(self, input: list[Label]) -> list[Label]:
        """Update labels (identify each by ``id``); only the fields you set are applied."""
    def delete(self, input: list[LabelIdentifiable]) -> None:
        """Delete labels by ``Label``, numeric id, or name.

        Refused with **400** while any resource still carries the label — drop it from those
        resources first, with ``resources.update`` and ``labels.remove``. The problem's
        ``fields`` name the label and the node still holding it. An intrinsic type-label
        (``ASSET``, ``TIMESERIES``, …) is refused the same way: those are reserved, attached or
        not.
        """


class LabelsServiceAsync:
    """Awaitable twin of ``LabelsServiceSync``, reached as ``client.labels`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``LabelsServiceSync`` carries the per-method documentation.
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

    Referencing a catalogue entry rather than typing free text into ``TimeSeries.unit`` is what
    makes a series' unit comparable and convertible: ``conversion`` carries the factors to the
    other units of the same ``quantity``. Units are read-only here — the catalogue is seeded
    server-side, so this class is what ``units.list()`` and ``units.by_ids()`` hand back.

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

    Reached as ``client.units``. The catalogue is seeded server-side, so there is no create,
    update or delete here. A unit's ``external_id`` is the stable handle you put in
    ``TimeSeries.unit_external_id``.
    """
    def list(self) -> list[Unit]:
        """The whole catalogue, in one call.

        No ``limit``, no filter, no paging — this is the only way to enumerate units, and the
        way to find the ``external_id`` for a unit you want to reference.
        """
    def by_ids(self, input: list[IdCollection]) -> list[Unit]:
        """Units by id or external id. Missing entries are omitted rather than raising.

        **Takes ``IdCollection`` objects only** — unlike every other ``by_ids`` in these
        bindings, a bare ``str`` or ``int`` is a ``TypeError``. Write
        ``units.by_ids([IdCollection(external_id="pressure_bar")])``.
        """
    def by_external_ids(self, input: str) -> list[Unit]:
        """One unit by external id.

        **Singular despite the name** — it takes one string, not a list, and answers with a list
        of zero or one. A unit that does not exist is an empty list rather than an exception.

        The awaitable twin is spelled ``by_external_id``, without the ``s``.
        """


class UnitServiceAsync:
    """Awaitable twin of ``UnitServiceSync``, reached as ``client.units`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``UnitServiceSync`` carries the per-method documentation.
    """
    async def list(self) -> list[Unit]: ...
    async def by_ids(self, input: list[IdCollection]) -> list[Unit]: ...
    async def by_external_id(self, input: str) -> list[Unit]: ...


# ====================== Files ======================

class INode:
    """One entry in the file tree — a file or a folder.

    The file hierarchy is separate from the resource graph: ``INode``s have their own ids, their
    own ``path``, and their own ``security_categories``. What ties the two together is
    ``related_resources``, whose ids ``related_resource_nodes()`` resolves to node objects.

    A soft-deleted file keeps its row but has its ``external_id`` rewritten to
    ``DELETED_<checksum>_<id>_<epochMillis>``, which is why ``restore`` wants the numeric id.
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
        """Fetch the resources this file references (its ``related_resources`` ids), resolved to
        ``Resource`` objects via the resources service. (The ``related_resources`` *property*
        returns the raw ids; this resolves them.) Blocking; see
        ``related_resource_nodes_async``.
        """
    async def related_resource_nodes_async(self) -> list[Node]:
        """Awaitable variant of ``related_resource_nodes``."""


class FileUpload:
    """Describes one file to upload: where to read it from locally, where to put it remotely, and
    the metadata to attach.

    ``from_path`` is the short form for a file that should keep its own name;
    ``new_with_destination_path`` places it somewhere else in the tree.

    **Construction touches the filesystem and panics on failure** — a missing path, a
    non-regular file or an unreadable name raises ``PanicException``, which derives from
    ``BaseException`` and so is not caught by ``except Exception``. Check the path first.

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

        Touches the filesystem: a missing or unreadable path raises ``PanicException``, which
        ``except Exception`` does not catch.
        """
    @classmethod
    def new_with_destination_path(cls, path: str, destination_path: str) -> FileUpload:
        """Upload a local file to a chosen path in the tree, rather than to the root under its own
        name. Same filesystem caveat as ``from_path``.
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
    def content(self) -> bytes:
        """The file content as ``bytes``."""
    def __len__(self) -> int: ...


FileIdentifiable = Union[INode, IdCollection, int, str]


class FilesServiceSync:
    """The blocking ``/files`` surface — a file and folder tree of ``INode``s.

    Reached as ``client.files``. Separate from the resource graph: files have their own
    hierarchy, their own ids, and their own ``security_categories``. What ties the two together
    is an ``INode``'s ``related_resources``.

    Deleting is a **soft** delete — the file moves to the trash, where ``list_trash`` finds it
    and ``restore`` brings it back.
    """
    def upload_file(self, file_upload: FileUpload) -> list[INode]:
        """Upload one file, described by a ``FileUpload``. Returns the created ``INode``s.

        The content is the raw ``PUT`` body — streamed, so a large file is not held in memory —
        and every piece of metadata rides in ``X-Datahub-*`` headers. Leaving ``mime_type``
        unset lets the server detect it.

        **Local-file problems are ``PanicException``, not ``DataHubException``.** A path that
        does not exist, is not a regular file, or cannot be opened raises a panic that derives
        from ``BaseException``, so a plain ``except Exception`` will not catch it. Check the
        path before calling.
        """
    def list_root_directory(self) -> list[INode]:
        """The contents of the file tree's root. Takes no arguments — there is no limit and no
        cursor on this endpoint.
        """
    def delete(self, input: list[FileIdentifiable]) -> None:
        """Move files to the trash. Returns ``None``.

        **A soft delete**, not a purge: the node stays visible through ``list_trash`` and can be
        brought back with ``restore``. Its ``external_id`` is rewritten to
        ``DELETED_<checksum>_<id>_<epochMillis>`` on the way, so the original external id is
        free again immediately — and a trashed file can no longer be found under it.
        """
    def list_directory_by_path(self, path: str) -> list[INode]:
        """The contents of one directory, named by its absolute path.

        **``path`` must begin with ``/``** — it is appended to the route as given, so
        ``"/images"`` works and ``"images"`` silently addresses the wrong route. ``""`` is the
        root, the same as ``list_root_directory``.
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
        """Restore soft-deleted files. Identify each by numeric id: the trashed
        ``DELETED_..._<epochMillis>`` external id does not round-trip through the server's
        lowercasing hash, so that route answers 404. See ``FileService::restore`` in the SDK.
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

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``FilesServiceSync`` carries the per-method documentation.
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

    Create one with ``subscriptions.create``, then open a WebSocket for it with
    ``subscriptions.listen``. ``id`` is assigned by the server and is read-only.
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

    Anything other than ``"desc"`` sorts ascending, and an unrecognised property falls back to
    the default rather than raising.
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

    Optional: ``filter()`` takes the same things as keywords. Passing both a ``form`` and any of
    the keywords raises ``ValueError``. **Its ``limit`` defaults to 100**, where ``list()``
    leaves the server's 1000.
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
        """Parse the value as a float. Raises ValueError if the value isn't numeric (e.g. for
        string-typed timeseries that share this delivery channel).
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

    ``payload`` is the content; ``message_id`` is what you hand to ``ack()`` or ``nack()``. An
    unacked message is redelivered to the next listener on the same subscription, so acking is
    what marks it done.
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
    """Synchronous Python wrapper around the Rust ``SubscriptionListener``. Iterating drives the
    underlying WebSocket: ``for msg in listener:`` blocks until the next message or returns when
    the connection closes cleanly.
    """
    def __iter__(self) -> SubscriptionListener: ...
    def __next__(self) -> SubscriptionMessage: ...
    def next_message(self) -> SubscriptionMessage | None:
        """Wait for the next message. Returns None when the connection has been closed cleanly,
        raises on transport / deserialization errors. Equivalent to driving the iterator one
        step but without using StopIteration as the close signal.
        """
    def ack(self, message_ids: list[str]) -> None:
        """Acknowledge messages, marking them done.

        An unacked message is redelivered to the next listener on the same subscription, so
        acking is what stops it coming back.
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

        The listener is also a context manager, which closes on exit.
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

    Reached as ``client.subscriptions``. A subscription binds a set of timeseries to a fan-out
    topic; ``listen()`` then opens a socket that streams the datapoints those series receive.

    Note the two reads default differently: ``list()`` leaves the server's 1000, while
    ``filter()`` with no ``limit`` caps at **100**.
    """
    def create(self, input: list[Subscription]) -> list[Subscription]:
        """Create subscriptions, returning the echo with ``id``, ``date_created`` and
        ``last_updated`` filled in.

        **Every referenced timeseries must already exist** — a subscription naming one that does
        not is a 400.
        """
    def list(self, limit: int | None = None) -> list[Subscription]:
        """Subscriptions in the tenant, newest first. ``limit`` defaults to the server's 1000 and
        may not exceed 10000; there is no paging, so a bigger tenant is truncated rather than
        paged — use ``filter`` to narrow instead.
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

        Also what unblocks a timeseries delete: a series still bound to a subscription cannot be
        deleted, so drop the subscription first.
        """
    def listen(self, subscription_external_ids: list[str]) -> SubscriptionListener:
        """Open a WebSocket listener multiplexing the named subscriptions. The ids seed the initial
        set (may be empty — add more with .subscribe()). Returns a SubscriptionListener you can
        iterate or call .next_message() / .ack() / .subscribe() / .close() on.
        """


class SubscriptionsServiceAsync:
    """Awaitable twin of ``SubscriptionsServiceSync``, reached as ``client.subscriptions`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``SubscriptionsServiceSync`` carries the per-method documentation.
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
    """A graph node representing a computation — a plain node distinguished only by its
    ``FUNCTION`` type-label, with no fields of its own.

    ``name`` is optional on this constructor but required by the api, so a create without one is
    a 400. ``related_resources`` is always empty on anything the functions service returns; use
    ``neighbors()`` to read a function's edges.
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
        """**Always empty.** Declared by the shared node base, but the api maps a function through
        a transformer that never joins its edges in — so ``list``, ``get_by_id`` and even the
        ``create`` echo all answer ``[]``. It is not sent on a write either.

        To read a function's edges, use ``neighbors()`` or the ``edges`` service.
        """
    # --- navigation (only on functions returned by the API; raises otherwise) ---
    def neighbors(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Walk the graph from this function and return the connected sub-graph (its ``nodes``, the
        ``edges`` between them, and their ``labels``). ``depth`` bounds the traversal in hops
        (``-1``, the default, = the whole connected component); ``relationship_types`` filters
        which edge types to follow (``None`` = all); ``limit`` caps the node count. Neighbour
        nodes are typed as their own classes. Blocking; see ``neighbors_async`` for the
        awaitable variant.
        """
    async def neighbors_async(
        self,
        depth: int = -1,
        relationship_types: list[str] | None = None,
        limit: int = 5000,
    ) -> ResourceNetwork:
        """Awaitable variant of ``neighbors``."""
    def related_events(self, limit: int = 100) -> list[Event]:
        """Fetch events whose ``related_resources`` include this function (matched by graph-node id
        when present, else external id), via ``events.filter``. ``limit`` caps the results
        (default 100). Blocking; see ``related_events_async``.
        """
    async def related_events_async(self, limit: int = 100) -> list[Event]:
        """Awaitable variant of ``related_events``."""


FunctionIdentifiable = Union[Function, IdCollection, int, str]


class FunctionsServiceSync:
    """The blocking ``/functions`` surface.

    Reached as ``client.functions``. A function is a plain graph node distinguished only by its
    ``FUNCTION`` type-label; it carries no fields of its own.

    **The api serves no ``/byids``, ``/filter`` or ``/search`` for functions** — the only node
    type missing all three. ``by_ids`` works around that client-side (see there), and there is
    no function search at all: reach them through
    ``client.resources.filter(node_type=["function"])`` when you need criteria.
    """
    def create(self, input: list[Function]) -> list[Function]:
        """Create functions, returning the echo with server-assigned ids.

        **``name`` is optional here but non-null on the api**, so omitting it is a 400. Setting
        ``related_resources`` locally has no effect either — the field is never sent.
        """
    def list(self, limit: int | None = None) -> list[Function]:
        """The first ``limit`` functions you may read, newest first. ``limit`` defaults to the
        server's 1000 and may not exceed 10000; there is no paging, so a bigger catalogue is
        truncated.
        """
    def get_by_id(self, id: int) -> Function | None:
        """One function by numeric id; raises on 404.

        A 404 does not tell you the id is free — a function you may not read is reported as
        missing rather than forbidden. Prefer this to ``by_ids`` when you have the id: functions
        have no ``/byids`` endpoint, so ``by_ids`` pages the listing and filters client-side.
        """
    def by_ids(self, input: list[FunctionIdentifiable]) -> list[Function]:
        """Functions by id or external id, matched on either.

        **There is no ``/byids`` endpoint behind this.** It fetches a listing of up to 10 000
        functions and filters it in the client, so it costs one full listing per call whatever
        the size of ``input``, and **a tenant holding more than 10 000 functions silently misses
        its oldest** — an existing function past the cap comes back simply absent, with no
        error.

        Unmatched ids are omitted rather than raising. Prefer ``get_by_id`` when you have a
        numeric id: that one is a real endpoint.
        """
    def by_external_id(self, external_id: str) -> Function:
        """Convenience for the function-worker bootstrap: returns the function with the given
        externalId, or raises if no such function exists.
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

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``FunctionsServiceSync`` carries the per-method documentation.
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

    Every call is the generic ``/resources`` pipeline with the type pinned server-side, so the
    two paths cannot drift apart on ACLs or status codes. What differs is the shape that comes
    back: ``Asset``, so ``geolocation`` and ``is_root`` are reachable without a type check.
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

        A plain list rather than a ``Page``: there is no cursor to continue with, so narrow with
        ``filter`` instead of raising the number.
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

        There is no ``node_type`` keyword on purpose: this endpoint answers with assets whatever
        it is given, so the server replaces it. A ``node_type`` on a ``filter=`` object is
        discarded the same way.
        """
    def search(
        self,
        query: str,
        filter: ResourceFilter | None = None,
        limit: int | None = None,
    ) -> list[Asset]:
        """Free-text search over assets, best match first.

        ``filter`` only ever removes hits from the phrase's. ``limit`` defaults to 100 and caps
        at 1000 — ``filter`` uses 1000/10000, which is easy to conflate.
        """
    def update(self, input: list[ResourceUpdate]) -> GraphResult:
        """Update assets in place. ``geolocation`` is the field that means anything only here.

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

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``AssetsServiceSync`` carries the per-method documentation.
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

    Reached as ``client.edges``. Edges normally come into being through
    ``resources.create(nodes, relations)``; this service is for linking resources that **already
    exist**, for reading one edge back, and for the relationship-type catalogue.

    An edge is separately deletable only when both of its endpoints stay reachable without it —
    otherwise it goes away with the resources.
    """
    def get(self, id: int) -> EdgeProxy | None:
        """One relationship by numeric id, or ``None`` if no edge has that id.

        The server answers an unknown id with 404; that is absorbed into ``None`` here, matching
        the other ``get()`` methods in these bindings. Any other error still raises.
        """
    def by_ids(self, input: list[EdgeIdentifiable]) -> GraphResult:
        """Several relationships plus the resources they connect, as a ``GraphResult`` — ``nodes``
        holds both endpoints of each edge and ``relations`` the edges, so no follow-up call is
        needed.
        """
    def create(self, input: list[RelForm]) -> list[EdgeProxy]:
        """Link resources that already exist. To create the resources *and* their links together,
        use ``resources.create(nodes, relations)`` instead.

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

        A name that already exists currently makes the server fail silently — it answers 200
        with an empty body, and in a batch the valid new types are rolled back alongside the
        duplicate. Treat an empty result as "something already existed and nothing was created",
        and use ``types()`` to read the real state.
        """


class EdgesServiceAsync:
    """Awaitable twin of ``EdgesServiceSync``, reached as ``client.edges`` on an
    ``AsyncDataHubClient``.

    Same methods, same arguments, same semantics — each returns an awaitable instead of
    blocking. ``EdgesServiceSync`` carries the per-method documentation.
    """
    async def get(self, id: int) -> EdgeProxy | None: ...
    async def by_ids(self, input: list[EdgeIdentifiable]) -> GraphResult: ...
    async def create(self, input: list[RelForm]) -> list[EdgeProxy]: ...
    async def delete(self, input: list[EdgeIdentifiable]) -> None: ...
    async def types(self) -> list[RelationshipType]: ...
    async def create_types(self, input: list[RelTypeForm]) -> list[RelationshipType]: ...
