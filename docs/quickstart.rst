Quick Start
===========

Using the CLI
-------------

Upload RPMs and Artifacts
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pulp-tool upload \
     --build-id my-build-123 \
     --namespace my-namespace \
     --parent-package my-package \
     --rpm-path /path/to/rpms \
     --sbom-path /path/to/sbom.json \
     --config ~/.config/pulp/cli.toml

Download Artifacts
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pulp-tool transfer \
     --artifact-location /path/to/artifacts.json \
     --cert-path /path/to/cert.pem \
     --key-path /path/to/key.pem \
     --config ~/.config/pulp/cli.toml

Using the Python API
--------------------

Basic Usage
~~~~~~~~~~~

.. code-block:: python

   from pulp_tool import PulpClient, PulpHelper

   # Create a client from configuration file
   client = PulpClient.create_from_config_file(path="~/.config/pulp/cli.toml")

   try:
       # Use the helper for high-level operations
       helper = PulpHelper(client)
       repositories = helper.setup_repositories("my-build-123")

       # Upload content
       artifact_href = client.upload_content(
           "/path/to/package.rpm",
           {"build_id": "my-build-123", "arch": "x86_64"},
           file_type="rpm",
           arch="x86_64"
       )
   finally:
       client.close()

See :doc:`api/index` for complete API reference.
