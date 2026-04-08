from typing import Optional, Union, Sequence

from datahub_sdk import IdCollection


class Unit:
    id: Optional[int]
    external_id: Optional[str]
    name: Optional[str]
    long_name: Optional[str]
    symbol: Optional[str]
    description: Optional[str]
    alias_names: Optional[list[str]]
    quantity: Optional[str]
    conversion: Optional[dict[str, int]]
    source: Optional[str]
    source_reference: Optional[str]
    
    def __init__(
            self, 
            external_id: Optional[str],
            name: Optional[str],
            long_name: Optional[str],
            symbol: Optional[str],
            description: Optional[str],
            alias_names: Optional[list[str]],
            quantity: Optional[str],
            conversion: Optional[dict[str, int]],
            source: Optional[str],
            source_reference: Optional[str]
                 ): ...

class UnitsServiceAsync:
    def list(self,): ...
    def by_ids(self,input: Sequence[Union[Unit,IdCollection,str,int]]): ...
    def by_external_id(self,input: Sequence[str]): ...


class UnitsServiceAsync:
    def list(self,): ...
    def by_ids(self,input: Sequence[Union[Unit,IdCollection,str,int]]): ...
    def by_external_id(self,input: Sequence[str]): ...