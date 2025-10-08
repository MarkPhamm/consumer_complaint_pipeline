# CI/CD Configuration

## Format Check Workflow

This repository includes an automated format check workflow that runs on every pull request and push to main.

### What it checks

- **Black**: Python code formatting (line length: 100)
- **isort**: Import statement sorting

### Local Development

#### Install formatting tools

```bash
pip install -r requirements-dev.txt
```

#### Check formatting (without fixing)

```bash
./scripts/format_check.sh
```

Or manually:

```bash
black --check .
isort --check-only .
```

#### Auto-fix formatting

```bash
./scripts/format_fix.sh
```

Or manually:

```bash
black .
isort .
```

### Configuration

Formatting configuration is defined in `pyproject.toml`:

- Line length: 100 characters
- Target: Python 3.11+
- isort profile: black (for compatibility)

### CI Workflow

The workflow runs automatically on:

- Pull requests to any branch
- Pushes to the main branch

If formatting issues are found, the workflow will fail with details about which files need formatting.
