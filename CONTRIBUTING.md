# Contributing to ProfitScout Engine

We welcome contributions! Please follow these guidelines to ensure code quality and consistency.

## Development Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd profitscout-engine
    ```

2.  **Create a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    pip install black mypy pytest pytest-cov pre-commit types-requests types-python-dateutil
    ```

4.  **Install pre-commit hooks:**
    This step is critical to ensure your code meets our standards before committing.
    ```bash
    pre-commit install
    ```

## Development Workflow

1.  **Create a branch:** Always work on a new branch for your features or fixes.
    ```bash
    git checkout -b feature/my-new-feature
    ```

2.  **Code Standards:**
    *   **Formatting:** We use [Black](https://github.com/psf/black).
    *   **Type Checking:** We use [mypy](http://mypy-lang.org/).
    *   **Testing:** We use [pytest](https://docs.pytest.org/).

3.  **Running Tests:**
    Run the full test suite with coverage:
    ```bash
    pytest --cov=. --cov-report=term-missing
    ```

4.  **Committing:**
    When you commit, `pre-commit` will automatically run:
    *   `black` to format your code.
    *   `mypy` to check types.
    *   `pytest` to run tests.
    
    If any hook fails, fix the issue and add the changes before committing again.

## Pull Requests

*   Ensure your CI passes on GitHub Actions.
*   Keep your changes focused and atomic.
*   Add tests for any new functionality.
