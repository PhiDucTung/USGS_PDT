"""Compatibility entrypoint for the Gold fill utility."""

from gold.scripts.fill_gold import *  # noqa: F401,F403


if __name__ == "__main__":
    from gold.scripts.fill_gold import main

    raise SystemExit(main())
