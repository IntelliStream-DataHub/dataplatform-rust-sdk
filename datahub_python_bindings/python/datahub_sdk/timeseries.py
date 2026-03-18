"""from datahub_python_sdk.datahub_python_sdk import PyUnit

from ._rust_interface import PyTimeSeries
class Timeseries:
    _ts: PyTimeSeries
    def __init__(self,
                 id: int | None,
                 external_id: str ,
                 name: str| None,
                 metadata: dict| None = None,
                 description: str|None= None,
                 unit: str|PyUnit|None = None,
                 unit_external_id: str|None = None,
                 security_categories: list[int]|None = None,
                 data_set_id: str|None = None,
                 value_type: str ="BIGINT"):

        if not unit and not unit_external_id:
            raise ValueError("Either unit or unit_external_id must be provided")

        elif isinstance(PyUnit,unit) and unit.external_id:
            unit_external_id = unit.external_id
            unit = unit.name
        elif unit_external_id and not unit:
            unit = unit_external_id
        elif not unit_external_id and unit:
            unit_external_id = unit

        self._ts = PyTimeSeries(
            id = id,
            external_id = external_id,
            name = name,
            metadata = metadata,
            description = description,
            unit = unit,
            unit_external_id = unit_external_id,
            security_categories = security_categories,
            data_set_id = data_set_id,
            value_type = value_type
        )
    @classmethod
    def from_pyts(cls,py_ts:PyTimeSeries):
        ts = cls.__new__(cls)
        ts._ts = py_ts
        return ts
        """