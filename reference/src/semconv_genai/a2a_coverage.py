"""Reduce A2A reference reports until the shared runner recognizes A2A spans."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def _attributes(span: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for attribute in span.get("attributes", []):
        if not isinstance(attribute, dict):
            continue
        name = attribute.get("name")
        if not isinstance(name, str):
            continue
        result = attribute.get("live_check_result", {})
        findings = result.get("all_advice", []) if isinstance(result, dict) else []
        if any(finding.get("id") == "type_mismatch" for finding in findings if isinstance(finding, dict)):
            continue
        names.add(name)
    return names


def reduce_reports(report_dir: Path) -> dict[str, object]:
    """Return the declared A2A client attributes observed in Weaver reports."""
    attributes: set[str] = set()
    for path in sorted(report_dir.rglob("*.json")):
        report = json.loads(path.read_text(encoding="utf-8"))
        for sample in report.get("samples", []):
            if not isinstance(sample, dict):
                continue
            span = sample.get("span")
            if not isinstance(span, dict) or span.get("kind") != "client":
                continue
            names = _attributes(span)
            if "a2a.method.name" in names:
                attributes.update(names)
    return {"spans": {"a2a.client": sorted(attributes)}, "events": {}, "metrics": {}}


def main() -> None:
    print(json.dumps(reduce_reports(Path(sys.argv[1])), indent=2))


if __name__ == "__main__":
    main()
