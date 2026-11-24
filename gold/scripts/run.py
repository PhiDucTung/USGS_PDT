"""Run Gold building and validation commands from any working directory."""

from __future__ import annotations

import os
from pathlib import Path
import sys


COMMANDS = {"gold", "validate-gold", "build-gold"}
ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        choices = ", ".join(sorted(COMMANDS))
        print(f"Usage: python gold/scripts/run.py <{choices}> [options]", file=sys.stderr)
        return 2

    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    from usgs_pipeline.cli import main as pipeline_main

    return pipeline_main()


if __name__ == "__main__":
    raise SystemExit(main())
