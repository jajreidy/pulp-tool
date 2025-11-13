# Architecture Documentation

## Overview

pulp-tool is designed with a modular architecture that emphasizes separation of concerns, type safety, and maintainability.

## Core Architecture

### Mixin Pattern

The `PulpClient` class uses Python's mixin pattern to compose functionality from specialized mixins:

- **ContentManagerMixin**: Handles content upload and creation operations
- **ContentQueryMixin**: Provides content search and filtering capabilities
- **RepositoryManagerMixin**: Manages repositories and distributions
- **TaskManagerMixin**: Handles asynchronous task monitoring and management

This design allows:
- Clean separation of concerns
- Easy testing of individual components
- Flexible composition of functionality
- Protocol-based interfaces for type safety

### Module Organization

```
pulp_tool/
├── api/              # API client implementations
│   ├── pulp_client.py          # Main client (composed of mixins)
│   ├── auth.py                 # OAuth2 authentication
│   ├── content_manager.py      # Content upload operations
│   ├── content_query.py        # Content search operations
│   ├── distribution_client.py # Distribution downloads
│   ├── repository_manager.py  # Repository operations
│   └── task_manager.py         # Task monitoring
├── models/           # Pydantic data models
│   ├── artifacts.py            # Artifact models
│   ├── context.py              # Context models
│   ├── pulp_api.py             # API response models
│   ├── repository.py           # Repository models
│   └── results.py              # Result models
├── utils/            # Utility functions
│   ├── error_handling.py       # Error handling utilities
│   ├── pulp_helper.py          # High-level helper class
│   ├── validation.py           # Validation functions
│   └── ...                     # Other utilities
├── cli.py            # CLI entry point
├── transfer.py        # Transfer operations
└── upload.py         # Upload operations
```

## Design Patterns

### Protocol Mixins

Mixins use `typing.Protocol` to define interfaces, allowing for:
- Type checking without inheritance
- Flexible implementation
- Clear API contracts

Example:
```python
@runtime_checkable
class ContentQueryMixin(Protocol):
    """Protocol for content query operations."""
    session: httpx.Client
    timeout: int
    # ... method definitions
```

### Context Managers

Resources are managed using context managers:

```python
with PulpClient.create_from_config_file() as client:
    # Use client
    pass
# Client automatically closed
```

### Pydantic Models

All data structures use Pydantic models for:
- Type validation
- IDE autocomplete
- Runtime validation
- Serialization/deserialization

## Data Flow

### Upload Flow

1. CLI parses arguments and creates `UploadContext`
2. `PulpHelper.setup_repositories()` creates/gets repositories
3. `PulpHelper.process_uploads()` orchestrates uploads:
   - Processes architectures in parallel
   - Uploads RPMs, logs, SBOMs
   - Collects results
4. Results are gathered and uploaded as JSON

### Transfer Flow

1. CLI parses arguments and creates `TransferContext`
2. Artifact metadata is loaded and validated
3. Artifacts are downloaded concurrently
4. Optionally, artifacts are re-uploaded to destination Pulp
5. Transfer report is generated

## Authentication Flow

1. OAuth2 credentials loaded from config file
2. `OAuth2ClientCredentialsAuth` handles token management:
   - Retrieves access token
   - Refreshes proactively before expiration
   - Handles 401 errors with automatic retry
3. Token added to all requests via httpx Auth interface

## Error Handling

- Standardized error handling via `with_error_handling` decorator
- HTTP errors handled with context-specific messages
- Generic errors logged with tracebacks
- Graceful degradation where possible

## Testing Strategy

- **Unit tests**: Test individual functions and classes
- **Integration tests**: Test end-to-end workflows
- **Mocking**: External dependencies (HTTP, file system) are mocked
- **Fixtures**: Common test data and setup in `conftest.py`

## Performance Considerations

- Concurrent operations using ThreadPoolExecutor
- Async operations for repository setup
- Connection pooling via httpx
- Request caching for GET operations
- Exponential backoff for task polling

## Future Improvements

- More extensive async/await usage
- Performance benchmarking suite
- Metrics and telemetry
- Enhanced error recovery
