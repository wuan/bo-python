# AGENTS.md - Blitzortung Python Project Guidelines

## Environment & Requirements

**Python**: 3.11-3.13. Uses modern features like `zoneinfo.ZoneInfo` for timezone handling.

**Dependencies**: Poetry-managed. Key dev tools: pytest 8.x, pytest-cov, pytest-benchmark, testcontainers (PostgreSQL).

## Build, Lint & Test Commands

```bash
# Install dependencies
poetry install

# Run all tests with coverage
poetry run pytest --cov blitzortung --cov-report xml --cov-report term --junitxml=junit.xml tests

# Run a single test file or test
poetry run pytest tests/test_base.py
poetry run pytest tests/test_base.py::PointTest::test_get_coordinate_components

# Run linter (pylint)
poetry run pylint blitzortung

# Build package
poetry build

# Run pre-commit hooks
pre-commit run --all-files
```

## Testing Notes

**Framework**: Pytest-based with unittest.TestCase; uses pytest fixtures in conftest.py for reusable setup.

**Database tests** (tests/db/): Use testcontainers with PostgreSQL/PostGIS. Requires Docker/Podman running. Module-scoped fixtures auto-start containers.

**Assertions**: unittest assertions, assertpy, and pytest assertions. Mock available via `mock` package.

## Code Style Guidelines

**Imports**: Order by stdlib, third-party, local. No star imports. Use `from . import` for relative imports.

**Formatting**: Max 120 chars/line (pylint.rc), 4-space indentation, UTF-8 header `# -*- coding: utf8 -*-` in all files.

**Types**: Type hints used in modern code (especially tests/conftest.py). Use type annotations for clarity in new code and docstrings.

**Naming**: Classes `CamelCase`, functions/methods/variables `snake_case`. Min 3 chars (min-similarity-lines=4). Good names: `i,j,k,ex,Run,_`. Avoid: `foo,bar,baz,toto,tutu,tata`.

**Functions/Methods**: Max 5 args, max 15 locals, max 6 returns, max 12 branches, max 50 statements per function.

**Classes**: Min 2 public methods, max 20 public methods, max 7 attributes, max 7 parent classes.

**Error Handling**: Use base `Error` exception class from `blitzortung.base`. Catch specific exceptions; avoid bare `except:`.

**Docstrings**: Required except for `__*__` methods. Use Apache 2.0 header in all source files.

**Pre-commit**: Runs gitleaks, trailing whitespace fixer, and pylint.
