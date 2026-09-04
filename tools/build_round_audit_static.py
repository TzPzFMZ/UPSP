#!/usr/bin/env python
"""Build static JS projection for UPSP OS/audit/round.html."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = REPO_ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from data.round_audit_static import write_static_projection  # noqa: E402
from paths import AUDIT_DIR, STM_CTX_ROUND_DIR  # noqa: E402


def default_round_dir() -> Path:
    return Path(STM_CTX_ROUND_DIR)


def default_audit_dir() -> Path:
    return Path(AUDIT_DIR)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build UPSP round audit static projection")
    parser.add_argument("--round-dir", type=Path, default=default_round_dir())
    parser.add_argument("--audit-dir", type=Path, default=default_audit_dir())
    args = parser.parse_args(argv)

    index = write_static_projection(args.round_dir, args.audit_dir)
    print(f"Generated {len(index['rounds'])} round audit projection(s)")
    print(f"Index: {args.audit_dir / 'round-index.js'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
