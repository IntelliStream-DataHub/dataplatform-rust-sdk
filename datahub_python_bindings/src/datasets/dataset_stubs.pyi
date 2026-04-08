from datetime import datetime
from typing import Optional, Sequence, Union

from datahub_sdk import IdCollection


class Dataset:
    """
    Represents a datasets object in the data hub.

    Attributes
    ----------
    id : int or None
        The internal ID of the datasets.
    external_id : str
        The external ID of the datasets.
    name : str
        The name of the datasets.
    metadata : dict[str, str] or None
        Metadata associated with the datasets.
    description : str or None
        A description of the datasets.
    policies : sequence[str] or None
        list of external_ids for the policies in effect on this datasets
    connected_data_sets: sequence[int] or None
        list of ids of the datasets connected to this datasets
    created_time : datetime.datetime or None
        The timestamp when the datasets first saved to the database
    last_updated_time : datetime.datetime or None
        The timestamp when the datasets was last updated.
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
    def __init__(self,
                 name: Optional[str],
                 external_id: Optional[str],
                 value_type: Optional[str],
                 metadata: Optional[dict[str,str]],
                 description: Optional[str],
                 policies: Optional[Sequence[str]],
                 connected_data_sets: Optional[Sequence[int]],
                 ):
        """
        Initialize a Dataset object.

        Parameters
        ----------
        name : str or None
            The name of the datasets.
        external_id : str or None
            The external ID of the datasets.
        value_type : str or None
            The type of values stored in the datasets.
        metadata : dict[str, str] or None
            Metadata associated with the datasets.
        description : str or None
            A description of the datasets.
        policies: Sequence[str] or None
            policies enforced on the datasets
        connected_data_sets
            other datasets connected to this datasets
        """
        ...


class DatasetsServiceAsync:
    """
    Asynchronous Dataset service for CRD operations for datasets.

    Methods
    -------
    create
        Save datasets to the DataHub Data platform.
    list
        list all datasets
    delete
        Delete Dataset from the DataHub Data platform.
    by_ids
        Retrieve datasets by their identifiers from the DataHub Data platform.
    """

    def create(self, input: Sequence[Dataset]) -> list[Dataset]:
        """
        Save Datasets to the DataHub Data platform.

        parameters
        ----------
        input:
            List of Datasets to be created

        Returns
        -------
        List of datasets created
        """
    def delete(self, input: Sequence[Union[Dataset, IdCollection, str, int]]) -> None:
        """
        Delete Datasets from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Dataset | IdCollection | str | int]
            Accepts a list of Datasets, IdCollection, external ids, or internal ids.


        Returns
        -------
        None
        """

    def by_ids(self, input: Sequence[Union[Dataset, IdCollection, str, int]]) -> list[Dataset]:
        """
        Retrieve Datasets from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Dataset | IdCollection | str | int]
            Accepts a list of Datasets, IdCollection, external ids, or internal ids.

        Returns
        -------
        list[Datasets]
            List of Datasets with the given ids.
        """

class DatasetsServiceSync:
    """
    Synchronous Dataset service for CRD operations for datasets.


    Methods
    -------
    create
        Save datasets to the DataHub Data platform.
    list
        list all datasets
    delete
        Delete Dataset from the DataHub Data platform.
    by_ids
        Retrieve datasets by their identifiers from the DataHub Data platform.
    """

    def create(self, input: Sequence[Dataset]) -> list[Dataset]:
        """
        Save Datasets to the DataHub Data platform.

        parameters
        ----------
        input:
            List of Datasets to be created
        Returns
        -------
        List of datasets created
        """
    def delete(self, input: Sequence[Union[Dataset, IdCollection, str, int]]) -> None:
        """
        Delete datasets from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Dataset | IdCollection | str | int]
            Accepts a list of Datasets, IdCollection, external ids, or internal ids.


        Returns
        -------
        None
        """

    def by_ids(self, input: Sequence[Union[Dataset, IdCollection, str, int]]) -> list[Dataset]:
        """
        Retrieve datasets from the DataHub Data platform.

        Parameters
        ----------
        input: Sequence[Dataset | IdCollection | str | int]
            Accepts a list of Datasets, IdCollection, external ids, or internal ids.

        Returns
        -------
        list[Datasets]
            List of Datasets with the given ids.
        """