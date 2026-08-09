"""Kirakira service launcher.

Recommended usage::

    uv run python main.py
    uv run python main.py setup
    uv run python main.py init
    uv run python main.py gateway
"""

from bootstrap.main import main


if __name__ == "__main__":
    main()
