"""Patch migrated Airflow DAG Param defaults for jar_file_uris before Composer deploy."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_JAR_PARAM_DEFAULT_PATTERN = re.compile(
    r'((?:["\'])jar_file_uris(?:["\'])\s*:\s*Param\s*\(\s*default\s*=\s*)\[[\s\S]*?\](\s*,)',
    re.MULTILINE,
)


def _python_list_literal(uris: list[str]) -> str:
    return "[" + ", ".join(repr(uri) for uri in uris) + "]"


def patch_jar_file_uris_default(source: str, uris: list[str]) -> tuple[str, bool]:
    """Replace ``Param(default=[...])`` for ``jar_file_uris`` in migrated DAG source."""
    if not uris:
        raise ValueError("jar_file_uris must contain at least one URI")
    replacement = _python_list_literal(uris)
    updated, count = _JAR_PARAM_DEFAULT_PATTERN.subn(
        rf"\1{replacement}\2",
        source,
        count=1,
    )
    return updated, count > 0


def patch_migrated_dag(path: Path, uris: list[str]) -> bool:
    updated, changed = patch_jar_file_uris_default(path.read_text(encoding="utf-8"), uris)
    if changed:
        path.write_text(updated, encoding="utf-8")
    return changed


def patch_dags_dir(dags_dir: Path, uris: list[str]) -> list[Path]:
    updated: list[Path] = []
    for path in sorted(dags_dir.rglob("*_migrated.py")):
        if patch_migrated_dag(path, uris):
            updated.append(path)
    return updated


def parse_uris(raw: str) -> list[str]:
    uris = [part.strip() for part in raw.split(",") if part.strip()]
    if not uris:
        raise ValueError("No JAR URIs provided")
    return uris


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inject jar_file_uris Param defaults into migrated Airflow DAG files.",
    )
    parser.add_argument("dags_dir", type=Path, help="Directory containing migrated DAG modules")
    parser.add_argument(
        "jar_file_uris",
        help="Comma-separated GCS URIs (for example gs://bucket/jars/app.jar)",
    )
    args = parser.parse_args(argv)
    if not args.dags_dir.is_dir():
        print(f"Error: {args.dags_dir} is not a directory", file=sys.stderr)
        return 1
    try:
        uris = parse_uris(args.jar_file_uris)
        updated = patch_dags_dir(args.dags_dir, uris)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    if not updated:
        print(f"No *_migrated.py files with jar_file_uris Param found under {args.dags_dir}")
        return 0
    for path in updated:
        print(f"Updated {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
