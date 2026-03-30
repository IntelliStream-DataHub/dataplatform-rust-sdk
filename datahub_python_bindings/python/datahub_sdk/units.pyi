"""from ._rust_interface import PyUnit
class Unit:
    _unit: PyUnit
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
            conversion: dict[str, int],
            source: str,
            source_reference: str
    ):
        self._unit = PyUnit(
            id=id,
            external_id= external_id,
            name= name,
            long_name= long_name,
            symbol= symbol,
            description= description,
            alias_names= alias_names,
            quantity= quantity,
            conversion= conversion,
            source= source,
            source_reference= source_reference,
        )"""


class Unit:
    pass