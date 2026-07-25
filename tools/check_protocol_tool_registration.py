#!/usr/bin/env python3
"""Read-only protocol tool registration chain gate."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path


def _repo_root() -> Path:
    try:
        import subprocess

        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            text=True,
            capture_output=True,
            check=True,
        )
        return Path(result.stdout.strip()).resolve()
    except Exception:
        return Path(__file__).resolve().parents[1]


def main() -> int:
    root = _repo_root()
    sys.path.insert(0, str(root / "tools"))
    import audit_upsp_consistency as audit

    findings = audit.run_protocol_tool_registration_audit(root)
    summary = {key: 0 for key in ("P0", "P1", "P2")}
    for finding in findings:
        if finding.severity in summary:
            summary[finding.severity] += 1
    payload = {
        "ok": summary["P0"] == 0 and summary["P1"] == 0,
        "summary": summary,
        "matrix": [
            asdict(row)
            for row in audit.build_protocol_tool_registration_matrix(root)
        ],
        "findings": [asdict(finding) for finding in findings],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
