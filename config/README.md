# Shared tool configuration

| File | Used by |
|------|---------|
| `pylintrc` | `make lint`, pre-commit `pylint` (`--rcfile=config/pylintrc`) |
| `yamllint.yml` | pre-commit and CI (`yamllint -c config/yamllint.yml`) |
| `codespell-ignore-words.txt` | pre-commit and CI `codespell --ignore-words=…` |

Ruff, mypy, and pytest settings live in [`pyproject.toml`](../pyproject.toml).
