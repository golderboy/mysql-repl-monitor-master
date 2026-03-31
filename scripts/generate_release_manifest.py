#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

DEFAULT_INCLUDE = ["app", "deploy", "scripts", "requirements.txt", ".env.example"]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def collect_files(root: Path, include: list[str]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for name in include:
        target = root / name
        if not target.exists():
            continue
        if target.is_file():
            rel = target.relative_to(root).as_posix()
            items.append({"path": rel, "size": target.stat().st_size, "sha256": sha256_file(target)})
            continue
        for path in sorted(p for p in target.rglob('*') if p.is_file()):
            rel = path.relative_to(root).as_posix()
            items.append({"path": rel, "size": path.stat().st_size, "sha256": sha256_file(path)})
    return items


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate release manifest for patchable baseline")
    parser.add_argument("--release", required=True, help="Release label such as R3.1.0")
    parser.add_argument("--output", required=True, help="Output JSON path")
    parser.add_argument("--include", nargs='*', default=DEFAULT_INCLUDE)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    files = collect_files(root, args.include)
    payload = {
        "release": args.release,
        "project_root": str(root),
        "file_count": len(files),
        "include": args.include,
        "files": files,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print({"status": "ok", "release": args.release, "output": str(out), "file_count": len(files)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
