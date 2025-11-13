Overview
========

pulp-tool provides a comprehensive, modern Python client for interacting with Pulp API to manage RPM repositories, file repositories, and content uploads with OAuth2 authentication.

Key Features
------------

- **Type-safe operations**: Built with Pydantic for data validation and type safety
- **Modern HTTP client**: Uses httpx for async-capable HTTP operations
- **CLI interface**: Intuitive Click-based command-line interface
- **Modular architecture**: Clean separation of concerns with mixin pattern
- **Comprehensive testing**: High test coverage with unit and integration tests

Architecture
------------

pulp-tool uses a modular architecture:

- **PulpClient**: Main client class composed of specialized mixins
- **Mixins**: ContentManagerMixin, ContentQueryMixin, RepositoryManagerMixin, TaskManagerMixin
- **Helpers**: PulpHelper for high-level workflow operations
- **Models**: Pydantic models for type-safe data handling

See :doc:`architecture` for detailed architecture documentation.
