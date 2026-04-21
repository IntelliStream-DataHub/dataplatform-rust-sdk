from datetime import datetime
from typing import Optional, Sequence, Union

from datahub_sdk import IdCollection


class Resource:
    """
    Represents a resources object in the data hub.

    Attributes
    ----------
    id : int or None
        The internal ID of the resource.
    external_id : str
        The external ID of the resource.
    name : str
        The name of the resources.
    metadata : dict[str, str] or None
        Metadata associated with the resource.
    description : str or None
        A description of the resource.
    is_root: bool
        indicates if the resource is a root resource
    data_set_id: int or None
        the dataset the resoruce belongs to
    source: str or None,
        the source of the dataset
    labels: list[str]
        list of labels for the resoruce

    relations: None
        Todo implement this so it actually works
        should be a EdgeProxy
    geolocation: dict[str,flaot] or None
        works as a place holder for geoJson datat that will be implemented later.
    created_time : datetime.datetime or None
        The timestamp when the resources first saved to the database
    last_updated_time : datetime.datetime or None
        The timestamp when the resources was last updated.
    """

    @property
    def id(self) -> Optional[int]: ...
    @property
    def external_id(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def metadata(self)-> Optional[dict[str,str]]: ...
    @property
    def description(self)-> Optional[str]: ...
    @property
    def policies(self)-> Optional[Sequence[str]]: ...
    @property
    def connected_data_sets(self)-> Optional[Sequence[int]]: ...
    @property
    def created_time(self)-> Optional[datetime.datetime]: ...
    @property
    def last_updated_time(self)-> Optional[datetime.datetime]: ...
    def __init__(
            self,
            name: Optional[str],
            external_id: Optional[str],
            id: Optional[int],
            metadata: Optional[dict[str,str]],
            description: Optional[str],
            is_root: Optional[bool],
            data_set_id: Optional[int],
            source: Optional[int],
            labels: Optional[Sequence[str]],
            relations: Optional[Sequence[str]], # todo implement edgeproxy for this
            geolocation: Optional[dict[str,float]], # todo implement GEOJSON, not prio atm
            created_time: Optional[datetime.datetime],
            last_updated_time: Optional[datetime.datetime],
            ):
        """
        Initialize a Resource object.

        Parameters
        ----------
        name : str or None
            The name of the resources.
        external_id : str or None
            The external ID of the resources.

        metadata : dict[str, str] or None
            Metadata associated with the resources.
        description : str or None
            A description of the resources.
        is_root : bool
            is this a root node in a graph component?
        data_set_id : int
            dataset id this resource belongs to
        source: str
            source of this resoruce
        labels: Sequence[str] or None
            labels for the resource
        relations: Sequence[str] or None # todo make into EdgeProxy
            relations to other entities
        geolocation: None
            not implemented yet
        created_time: datetime.datetime or None
            timestamp of when resource was first created
        last_updated_time: datetime.datetime or None
            timestamp of when resource was last updated
        """
        ...



class ResourcesServiceAsync:
    """
    Asynchronous Resource service for CRD operations for resources.

    Methods
    -------
    create
        Save resources to the DataHub Data platform.
    delete
        Delete Resource from the DataHub Data platform.
    by_ids
        Retrieve resources by their identifiers from the DataHub Data platform.
    """

    def create(self, input: Sequence[Resource]) -> list[Resource]:
        """
        Save Resources to the DataHub Data platform.

        parameters
        ----------
        input:
            List of Resources to be created

        Returns
        -------
        List of resources created
        """
    def delete(self, input: Sequence[Union[Resource, IdCollection, str, int]]) -> None:
        """
        Delete Resources from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Resource | IdCollection | str | int]
            Accepts a list of Resources, IdCollection, external ids, or internal ids.


        Returns
        -------
        None
        """

    def by_ids(self, input: Sequence[Union[Resource, IdCollection, str, int]]) -> list[Resource]:
        """
        Retrieve Resources from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Resource | IdCollection | str | int]
            Accepts a list of Resources, IdCollection, external ids, or internal ids.

        Returns
        -------
        list[Resources]
            List of Resources with the given ids.
        """

class ResourcesServiceSync:
    """
    Synchronous Resource service for CRD operations for resources.


    Methods
    -------
    create
        Save resources to the DataHub Data platform.
    delete
        Delete Resource from the DataHub Data platform.
    by_ids
        Retrieve resources by their identifiers from the DataHub Data platform.
    """

    def create(self, input: Sequence[Resource]) -> list[Resource]:
        """
        Save Resources to the DataHub Data platform.

        parameters
        ----------
        input:
            List of Resources to be created
        Returns
        -------
        List of resources created
        """
    def delete(self, input: Sequence[Union[Resource, IdCollection, str, int]]) -> None:
        """
        Delete resources from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Resource | IdCollection | str | int]
            Accepts a list of Resources, IdCollection, external ids, or internal ids.


        Returns
        -------
        None
        """

    def by_ids(self, input: Sequence[Union[Resource, IdCollection, str, int]]) -> list[Resource]:
        """
        Retrieve resources from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Resource | IdCollection | str | int]
            Accepts a list of Resources, IdCollection, external ids, or internal ids.

        Returns
        -------
        list[Resources]
            List of Resources with the given ids.
        """