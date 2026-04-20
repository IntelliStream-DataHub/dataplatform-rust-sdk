
#from .timeseries import Timeseries
#from .async_client import AsyncClient
#from .sync_client import SyncClient
#from .units import Unit
from .datahub_sdk import *
#from .timeseries import TimeSeries as TimeSeries
from .timeseries import TimeseriesUpdate as TimeseriesUpdate
from .timeseries import DeleteFilter as DeleteFilter
from .timeseries import RetrieveFilter as RetrieveFilter
from .units import Unit
from .events import Event,EventId,EventFilter,BasicEventFilter



class TimeSeries:
    @property
    def id(self) -> int: ...
    @property
    def external_id(self) -> str: ...

