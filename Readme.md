# Data Scraper Project - Testing Guide

This guide covers the testing strategy for the Data Scraper project, including how to run unit tests, integration tests, and end-to-end tests.

## Test Coverage Overview

The current test coverage is:
- Unit Tests: 86% overall coverage
  - extractors.py: 94%
  - loaders.py: 96%
  - transformers.py: 100%
  - validators.py: 100%
  - pipeline.py: 0% (now covered by integration tests)

## Test Structure

The tests are organized into three main categories:

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test how components work together
3. **End-to-End Tests**: Test complete workflows from extraction to loading

## Running the Tests

### Prerequisites

Make sure you have the required dependencies installed:

```bash
pip install pytest pytest-cov
```

### Running Unit Tests

To run the unit tests with coverage reporting:

```bash
pytest tests/unit --cov=datascraper
```

### Running Integration Tests

To run the integration tests:

```bash
pytest tests/integration --cov=datascraper --cov-append
```

### Running End-to-End Tests

To run the end-to-end tests:

```bash
pytest tests/e2e --cov=datascraper --cov-append
```

### Running All Tests

To run all tests and generate a combined coverage report:

```bash
pytest tests/unit tests/integration tests/e2e --cov=datascraper --cov-report=html
```

This will generate an HTML coverage report in the `htmlcov` directory.

## Test Directory Structure

```
tests/
├── unit/                  # Unit tests for individual components
│   ├── test_extractors.py
│   ├── test_transformers.py
│   ├── test_loaders.py
│   └── test_validators.py
├── integration/           # Integration tests for component interactions
│   └── test_pipeline.py
└── e2e/                   # End-to-end tests for complete workflows
    └── test_workflows.py
```

## Continuous Integration

The project uses GitHub Actions for continuous integration. The workflow automatically:

1. Runs unit tests, integration tests, and end-to-end tests
2. Generates coverage reports
3. Uploads coverage to Codecov
4. Performs code quality checks (lint, formatting)

## Adding New Tests

When adding new features:

1. Start with unit tests for the individual components
2. Add integration tests to verify component interactions
3. Update end-to-end tests to include the new functionality in complete workflows

## Mocking External Dependencies

For tests that involve external dependencies (websites, APIs), use the unittest.mock library:

```python
from unittest.mock import patch, MagicMock

@patch('requests.get')
def test_function(mock_get):
    # Configure mock
    mock_response = MagicMock()
    mock_response.text = "<html>Mock HTML</html>"
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    
    # Your test code here
```

## Test Fixtures

Common test fixtures are defined in the `setUp` and `tearDown` methods of each test class. For fixtures that need to be shared across multiple test modules, consider using pytest fixtures in a `conftest.py` file.
