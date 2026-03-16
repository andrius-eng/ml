"""Ensure the environment is running on Python 3.11+.

Useful to validate your `uv`/venv before running the rest of the demo scripts.
"""

import sys


def main() -> None:
    if sys.version_info < (3, 11):
        sys.exit(
            "ERROR: Python 3.11 or newer is required. "
            f"Found: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        )

    print(f"Python is {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} — good to go!")


if __name__ == "__main__":
    main()
