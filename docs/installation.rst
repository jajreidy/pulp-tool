Installation
============

Installation from Source
-------------------------

.. code-block:: bash

   git clone https://github.com/konflux/pulp-tool.git
   cd pulp-tool
   pip install -e .

Development Installation
------------------------

For development, install with dev dependencies:

.. code-block:: bash

   git clone https://github.com/konflux/pulp-tool.git
   cd pulp-tool
   pip install -e ".[dev]"

Requirements
------------

- Python 3.12 or higher
- pip

Optional Dependencies
---------------------

- Development dependencies: See ``pyproject.toml`` for full list
- Pre-commit hooks: Install with ``pip install pre-commit && pre-commit install``
