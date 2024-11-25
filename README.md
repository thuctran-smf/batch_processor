# Record Batch Processor

A Python library for efficiently splitting large arrays of records into optimally sized batches for systems with specific size and count constraints. Particularly useful for preparing data for streaming services like AWS Kinesis.

## Features

- Efficient batch splitting based on size and count constraints.
- Clean, type-safe implementation using dataclasses and type hints.
- Comprehensive error handling and input validation.
- Detailed logging with both file and console output.
- Extensive unit test.
- Automated CI/CD pipeline with GitHub Actions
- Zero external dependencies beyond Python standard library

## Batch requirements

Strict limitations on batch size and record counts are often applied when ingesting large amounts of data to streaming services. This library helps optimize data transmission by automatically splitting input records into appropriate batches while respecting the following constraints:

- Maximum individual record size: 1 MB
- Maximum batch size: 5 MB
- Maximum records per batch: 500

## Project Structure

```
batch-processor/
├── .github/
│   └── workflows/
│       └── common-ci.yml           # CI pipeline configuration for core functionality
│       └── common-skip.yml           # CI pipeline configuration for documentation
├── src/                     # Source code
│   ├── __init__.py
│   ├── batch_processor.py   # Core processing logic
│   ├── batch_constraints.py # Constraint definitions
│   └── batch_metrics.py     # Metrics collection
├── tests/                   # Test suite
│   ├── __init__.py
│   └── test_batch_processor.py
├── demo.py                  # Usage examples
├── mypy.ini                # Type checking configuration
└── README.md
```

## Core Components

### BatchConstraints

Immutable configuration using dataclass:

```python
from src.batch_constraints import BatchConstraints

# Default constraints
constraints = BatchConstraints(
    max_record_size_bytes=1_000_000,  # 1 MB
    max_batch_size_bytes=5_000_000,   # 5 MB
    max_records_per_batch=500
)
```

### BatchMetrics

Metrics collection using dataclass:

```python
metrics = processor.get_metrics()
print(metrics)
# Output: {
#     'records_processed': 1000,
#     'records_discarded': 5,
#     'batches_created': 10,
#     'total_bytes_processed': 4500000
# }
```

### BatchProcessor

Main processing class with separation of concerns:

```python
from src.batch_processor import BatchProcessor

# Initialize with default constraints
processor = BatchProcessor()

# Process records into batches
records = ["record1", "record2", "record3"]
for batch in processor.create_batches(records):
    print(f"Processing batch of {len(batch)} records")
```

## Error Handling

The code includes error handling with clear error messages:

```python
# Type validation
try:
    processor.create_batches(["valid", None, "invalid"])
except TypeError as e:
    print(f"Error: {e}")  # Error: Record must be string, got NoneType

# Size validation
try:
    huge_record = "x" * 2_000_000  # 2MB record
    processor.is_valid_record(huge_record)  # Returns False, logs warning
except Exception as e:
    print(f"Error: {e}")
```

## Logging

Structured logging with different levels for file and console:

```python
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_processor.log'),
        logging.StreamHandler()
    ]
)
```

## Testing

Comprehensive test suite covering core functionality, edge cases, and error conditions:

```python
# Run tests locally
python -m unittest tests/test_batch_processor.py

# Key areas tested:
# - Record validation
# - Batch constraints
# - Error handling
# - Order preservation
# - Metrics collection
# - Edge cases
```

## Continuous Integration

The project uses GitHub Actions with a dual-workflow system for efficient CI/CD:

### Common CI

Main workflow that runs when Python code is modified:

```yaml
name: Common CI

on:
  push:
    branches: [main]
    paths:
      - 'src/**'
      - 'tests/**'
      - 'demo.py'
      - 'requirements.txt'
      - '.github/workflows/common-ci.yml'
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.12"]

    steps:
    - Type checking with mypy for src/ and demo.py
    - Unit tests with pytest
    - Dependency caching for faster builds
```

### Skip Workflow

Complementary workflow that automatically succeeds for documentation changes:

```yaml
name: Common Skip

on:
  pull_request:
    paths:
      - "**/*.md"
      - "docs/**"
      - ".gitignore"
      - "README.md"

jobs:
  test:
    if: false
    runs-on: ubuntu-latest
    steps:
      - run: echo "Skipping tests"
```

The CI system handles changes as follows:

- Code changes trigger full test suite through Common CI
- Documentation changes trigger Common Skip, which succeeds automatically
- Both workflows satisfy branch protection rules
- No unnecessary test runs for documentation updates

## Demo

Run the demonstration script to see clean code in action:

```bash
python demo.py
```

The demo shows:

- Clear initialization
- Proper error handling
- Metrics collection
- Logging configuration
- Batch processing with realistic data

## Development Guidelines

The code follows these principles:

1. Type safety with mypy
2. Clear documentation and docstrings
3. Comprehensive error handling
4. Separation of concerns
5. Immutable configuration
6. Consistent logging
7. Thorough testing
8. Automated validation through CI

## Type Checking

Verify type safety:

```bash
mypy --config-file mypy.ini src/
```

## Suggestions for Future Development

1. Package Structure Enhancement
   - Convert to proper Python package
   - Add pyproject.toml for modern package management
   - Structure for PyPI distribution

2. Performance Optimization
   - Memory usage optimization for large datasets
   - Batch size optimization based on metrics
   - Async processing support

3. Feature Enhancements
   - Custom validation rules
   - Additional metric collectors
   - Integration with monitoring systems
   - Support for different record types

4. CI/CD Enhancements
   - Add code coverage reporting
   - Include linting checks
   - Automated version management
   - Release automation

## Support

If you encounter issues or have questions, please file an issue on the GitHub repository.
