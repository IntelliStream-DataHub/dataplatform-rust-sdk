"""
File management stub definitions for DataHub Python bindings.

This module provides type stubs for file upload, file node representations,
and synchronous/asynchronous file service operations.
"""
from datetime import datetime
from typing import Optional, Sequence, Mapping, Union


class FileUpload:
    """
    Represents a file to be uploaded to DataHub.

    Attributes
    ----------
    external_id : str
        Unique external identifier for the file.
    file_path : str
        Local file system path to the file.
    destination_path : Optional[str]
        Destination path in DataHub where the file will be stored.
    name : str
        Display name of the file.
    metadata : Optional[Mapping[str, str]]
        Key-value pairs of metadata associated with the file.
    description : Optional[str]
        Description of the file.
    source : Optional[str]
        Source system or origin of the file.
    data_set_id : Optional[int]
        ID of the dataset this file belongs to.
    mime_type : Optional[str]
        MIME type of the file.
    related_resources : Optional[Sequence[int]]
        IDs of related resources.
    source_date_created : Optional[datetime]
        Creation date in the source system.
    source_last_updated : Optional[datetime]
        Last update date in the source system.
    """
    external_id: str
    file_path: str
    destination_path: Optional[str]
    name: str
    metadata: Optional[Mapping[str,str]]
    description: Optional[str]
    source: Optional[str]
    data_set_id: Optional[int]
    mime_type: Optional[str]
    related_resources: Optional[Sequence[int]]
    source_date_created: Optional[datetime]
    source_last_updated: Optional[datetime]


    def __init__(
            self,
            path: str,
            destination_path: Optional[str],
            external_id: Optional[str],
            name: Optional[str],
            metadata: Optional[Mapping[str,str]],
            description: Optional[str],
            source: Optional[str],
            data_set_id: Optional[int],
            related_resources: Optional[Sequence[int]], ):
        """
        Initialize a FileUpload instance.

        Parameters
        ----------
        path : str
            Local file system path to the file.
        destination_path : Optional[str]
            Destination path in DataHub.
        external_id : Optional[str]
            Unique external identifier.
        name : Optional[str]
            Display name of the file.
        metadata : Optional[Mapping[str, str]]
            Metadata key-value pairs.
        description : Optional[str]
            Description of the file.
        source : Optional[str]
            Source system or origin.
        data_set_id : Optional[int]
            Dataset ID.
        related_resources : Optional[Sequence[int]]
            Related resource IDs.
        """
        ...

    @classmethod
    def from_path(cls, path: str):
        """
        Create a FileUpload instance from a file path.

        Parameters
        ----------
        path : str
            Local file system path to the file.

        Returns
        -------
        FileUpload
            New FileUpload instance.
        """
        ...

    @classmethod
    def new_with_destination_path(
        cls,
        path: str,
        destination_path: str,
    ):
        """
        Create a FileUpload instance with a specified destination path.

        Parameters
        ----------
        path : str
            Local file system path to the file.
        destination_path : str
            Destination path in DataHub.

        Returns
        -------
        FileUpload
            New FileUpload instance with destination path.
        """
        ...


class INode:
    """
    Represents a file or directory node in DataHub.

    Attributes
    ----------
    id : Optional[int]
        Unique internal identifier.
    name : str
        Name of the file or directory.
    description : Optional[str]
        Description of the node.
    external_id : str
        Unique external identifier.
    path : str
        Full path to the node in DataHub.
    size : int
        Size of the file in bytes.
    checksum : Optional[str]
        Checksum of the file content.
    source : Optional[str]
        Source system or origin.
    type : Optional[str]
        Type of the node.
    mime_type : Optional[str]
        MIME type of the file.
    source_date_created : Optional[datetime]
        Creation date in the source system.
    source_last_updated : Optional[datetime]
        Last update date in the source system.
    date_created : datetime
        Creation date in DataHub.
    last_updated : datetime
        Last update date in DataHub.
    parent_id : Optional[int]
        ID of the parent directory.
    parent_external_id : Optional[str]
        External ID of the parent directory.
    data_set_id : Optional[int]
        ID of the dataset this node belongs to.
    metadata : Optional[Mapping[str, str]]
        Key-value pairs of metadata.
    related_resources : Optional[Sequence[int]]
        IDs of related resources.
    security_categories : Optional[Sequence[int]]
        IDs of security categories.
    """
    id: Optional[int]
    name: str
    description: Optional[str]
    external_id: str
    path: str
    size: int
    checksum: Optional[str]
    source: Optional[str]
    type: Optional[str]
    mime_type: Optional[str]
    source_date_created: Optional[datetime]
    source_last_updated: Optional[datetime]
    date_created: datetime
    last_updated: datetime
    parent_id: Optional[int]
    parent_external_id: Optional[str]
    data_set_id: Optional[int]
    metadata: Optional[Mapping[str,str]]
    related_resources: Optional[Sequence[int]]
    security_categories: Optional[Sequence[int]]

class PyFilesServiceSync:
    """
    Synchronous file service for DataHub operations.

    Provides methods for uploading, listing, and deleting files in DataHub.
    """

    def upload_file(self, file_upload: FileUpload) -> FileUpload:
        """
        Upload a file to DataHub synchronously.

        Parameters
        ----------
        file_upload : FileUpload
            File upload configuration.

        Returns
        -------
        FileUpload
            Updated file upload information after successful upload.
        """
        ...

    def list_root_directory(self) -> Sequence[INode]:
        """
        List all nodes in the root directory.

        Returns
        -------
        Sequence[INode]
            List of nodes in the root directory.
        """
        ...

    def delete(self, input: Sequence[Union[INode, FileUpload, str, int]]) -> None:
        """
        Delete files or directories from DataHub.

        Parameters
        ----------
        input : Sequence[Union[INode, FileUpload, str, int]]
            List of items to delete. Can be INode objects, FileUpload objects,
            external IDs (str), or internal IDs (int).

        Returns
        -------
        None
        """
        ...

    def list_directory_by_path(self, path: str) -> Sequence[INode]:
        """
        List all nodes in a directory specified by path.

        Parameters
        ----------
        path : str
            Path to the directory.

        Returns
        -------
        Sequence[INode]
            List of nodes in the specified directory.
        """
        ...


class PyFilesServiceAsync:
    """
    Asynchronous file service for DataHub operations.

    Provides async methods for uploading, listing, and deleting files in DataHub.
    """

    def upload_file(self, file_upload: FileUpload) -> FileUpload:
        """
        Upload a file to DataHub asynchronously.

        Parameters
        ----------
        file_upload : FileUpload
            File upload configuration.

        Returns
        -------
        FileUpload
            Updated file upload information after successful upload.
        """
        ...

    def list_root_directory(self) -> Sequence[INode]:
        """
        List all nodes in the root directory asynchronously.

        Returns
        -------
        Sequence[INode]
            List of nodes in the root directory.
        """
        ...

    def delete(self, input: Sequence[Union[INode, FileUpload, str, int]]) -> None:
        """
        Delete files or directories from DataHub asynchronously.

        Parameters
        ----------
        input : Sequence[Union[INode, FileUpload, str, int]]
            List of items to delete. Can be INode objects, FileUpload objects,
            external IDs (str), or internal IDs (int).

        Returns
        -------
        None
        """
        ...

    def list_directory_by_path(self, path: str) -> Sequence[INode]:
        """
        List all nodes in a directory specified by path asynchronously.

        Parameters
        ----------
        path : str
            Path to the directory.

        Returns
        -------
        Sequence[INode]
            List of nodes in the specified directory.
        """
        ...
