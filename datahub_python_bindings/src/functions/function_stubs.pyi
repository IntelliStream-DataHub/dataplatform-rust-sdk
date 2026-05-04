import datetime
from typing import Any, Optional, Union


class IdCollection:
    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> Optional[str]: ...


FunctionId = Union["Function", IdCollection, str, int]


class Function:
    """
    A DataHub function. Binds a model template (model_name) to a configuration map; once
    attached to one or more timeseries via PROCESSED_BY edges the server auto-provisions a
    system-managed Subscription per binding, which the function's external worker listens
    on with the Python SDK.

    Attributes
    ----------
    id : int | None
        Server-assigned numeric id (None until the function has been created).
    external_id : str
        User-supplied identifier; must be unique per function.
    name : str | None
        Human-readable display name.
    model_name : str
        Stable identifier of the model template (e.g. ``forecast-ema``,
        ``forecast-linear``, ``anomaly-detection``). Determines which handler the worker
        dispatches to.
    config : dict[str, Any]
        Merged configuration: server-applied template defaults plus any user overrides.
    labels : list[str]
        Always contains the canonical ``FUNCTION`` label.
    metadata : dict[str, str]
        Free-form metadata attached on create.
    """
    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> str: ...
    @property
    def name(self) -> Optional[str]: ...
    @property
    def model_name(self) -> str: ...
    @property
    def config(self) -> dict[str, Any]: ...
    @property
    def labels(self) -> list[str]: ...
    @property
    def metadata(self) -> dict[str, str]: ...
    @property
    def created_time(self) -> Optional[datetime.datetime]: ...
    @property
    def last_updated_time(self) -> Optional[datetime.datetime]: ...

    def __init__(
        self,
        external_id: str,
        model_name: str,
        name: Optional[str] = None,
        config: Optional[dict[str, Any]] = None,
    ) -> None: ...


class FunctionsServiceSync:
    """Synchronous CRUD for functions, plus convenience lookups for the worker bootstrap."""
    def create(self, input: list[Function]) -> list[Function]: ...
    def list(self) -> list[Function]: ...
    def by_ids(self, input: list[FunctionId]) -> list[Function]: ...
    def by_external_id(self, external_id: str) -> Function:
        """Return the function with the given externalId. Raises if no such function exists."""
        ...
    def delete(self, input: list[FunctionId]) -> None: ...


class FunctionsServiceAsync:
    """Asyncio variant of FunctionsServiceSync."""
    async def create(self, input: list[Function]) -> list[Function]: ...
    async def list(self) -> list[Function]: ...
    async def by_ids(self, input: list[FunctionId]) -> list[Function]: ...
    async def by_external_id(self, external_id: str) -> Function: ...
    async def delete(self, input: list[FunctionId]) -> None: ...
