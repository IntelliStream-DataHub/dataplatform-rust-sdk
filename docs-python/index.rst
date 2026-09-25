Python client reference
=======================

Everything is reached through a configured client:

.. code-block:: python

   from intellistream_datahub_sdk import DataHubClient

   client = DataHubClient.from_env()
   client.datasets.list(limit=10)

Services
--------

.. include:: services/list.inc

Everything else
---------------

- :doc:`clients` — where every call starts.
- :doc:`entities` — what the services create, read and return.
- :doc:`datapoints` — values on a series, and the requests that read and delete them.
- :doc:`filters-and-identifiers` — criteria for ``filter`` and ``search``, and ways of naming an entity.
- :doc:`updates` — partial updates, and the field wrappers they are built from.
- :doc:`graph` — relationships between resources.
- :doc:`files` — uploading and downloading content.
- :doc:`subscriptions` — listening for changes as they happen.

Async
-----

``AsyncDataHubClient`` has the same services and methods, each one a coroutine to ``await``.
Where the two differ, the method says so.

.. code-block:: python

   from intellistream_datahub_sdk import AsyncDataHubClient

   client = AsyncDataHubClient.from_env()
   await client.datasets.list(limit=10)

Ingesting, querying, subscriptions and the industry walkthroughs live in the
`main documentation <https://intellistream.ai/sdk-documentation/>`_. This is the reference for
what the client exposes.

.. toctree::
   :hidden:

   clients
   services/index
   entities
   datapoints
   filters-and-identifiers
   updates
   graph
   files
   subscriptions
