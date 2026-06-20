"""Patch ``jar_file_uris`` Param defaults in migrated Airflow DAG files at deploy time."""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys
from pathlib import Path


def resolve_jar_file_uris_from_env(
    *,
    jar_file_uris: str | None = None,
    gcs_bucket: str | None = None,
    gcs_jar_prefix: str | None = None,
    versionless_jar_name: str | None = None,
) -> list[str] | None:
    """Resolve JAR URIs from explicit env values or ``gs://<bucket>/<prefix>/<jar>``."""
    explicit = (jar_file_uris if jar_file_uris is not None else os.environ.get("JAR_FILE_URIS", "")).strip()
    if explicit:
        if explicit.startswith("["):
            parsed = json.loads(explicit)
            if not isinstance(parsed, list):
                raise ValueError("JAR_FILE_URIS JSON must be an array of strings")
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [part.strip() for part in explicit.split(",") if part.strip()]

    bucket = (gcs_bucket if gcs_bucket is not None else os.environ.get("GCS_BUCKET", "")).strip()
    prefix = (gcs_jar_prefix if gcs_jar_prefix is not None else os.environ.get("GCS_JAR_PREFIX", "jars")).strip()
    jar_name = (
        versionless_jar_name
        if versionless_jar_name is not None
        else os.environ.get("VERSIONLESS_JAR_NAME", "")
    ).strip()
    if bucket and jar_name:
        bucket = bucket.removeprefix("gs://").rstrip("/")
        prefix = prefix.strip("/")
        path = f"{prefix}/{jar_name}" if prefix else jar_name
        return [f"gs://{bucket}/{path}"]
    return None


def _jar_uris_list_expr(jar_uris: list[str]) -> ast.List:
    return ast.List(elts=[ast.Constant(value=uri) for uri in jar_uris], ctx=ast.Load())


def _set_param_default(param_call: ast.Call, jar_uris: list[str]) -> bool:
    new_default = _jar_uris_list_expr(jar_uris)
    if param_call.args:
        param_call.args[0] = new_default
        return True
    for keyword in param_call.keywords:
        if keyword.arg == "default":
            keyword.value = new_default
            return True
    param_call.keywords.insert(0, ast.keyword(arg="default", value=new_default))
    return True


def _is_param_call(node: ast.expr) -> bool:
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Param"


def _is_dag_call(node: ast.expr) -> bool:
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "DAG"


def _patch_params_dict(params_dict: ast.Dict, jar_uris: list[str]) -> bool:
    changed = False
    for key, value in zip(params_dict.keys, params_dict.values):
        if key is None or value is None:
            continue
        if not isinstance(key, ast.Constant) or key.value != "jar_file_uris":
            continue
        if not _is_param_call(value):
            continue
        changed = _set_param_default(value, jar_uris) or changed
    return changed


def patch_dag_source(source: str, jar_uris: list[str]) -> tuple[str, bool]:
    """Return updated source and whether ``jar_file_uris`` was patched."""
    tree = ast.parse(source)
    changed = False
    for node in ast.walk(tree):
        if not _is_dag_call(node):
            continue
        for keyword in node.keywords:
            if keyword.arg != "params" or not isinstance(keyword.value, ast.Dict):
                continue
            if _patch_params_dict(keyword.value, jar_uris):
                changed = True
    if not changed:
        return source, False
    ast.fix_missing_locations(tree)
    return ast.unparse(tree) + "\n", True


def patch_dag_file(path: Path, jar_uris: list[str]) -> bool:
    """Patch one DAG file in place. Returns True when updated."""
    source = path.read_text(encoding="utf-8")
    if "jar_file_uris" not in source:
        return False
    updated, changed = patch_dag_source(source, jar_uris)
    if not changed:
        return False
    path.write_text(updated, encoding="utf-8")
    return True


def patch_dags_tree(dags_dir: Path, jar_uris: list[str]) -> list[Path]:
    """Patch all ``*.py`` DAG files under ``dags_dir`` that declare ``jar_file_uris``."""
    patched: list[Path] = []
    if not dags_dir.is_dir():
        return patched
    for path in sorted(dags_dir.rglob("*.py")):
        if patch_dag_file(path, jar_uris):
            patched.append(path)
    return patched


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Patch jar_file_uris defaults in migrated DAG files")
    parser.add_argument(
        "--dags-dir",
        default=os.environ.get("DAGS_DIR", "dags"),
        help="Directory containing migrated DAG Python files",
    )
    args = parser.parse_args(argv)

    jar_uris = resolve_jar_file_uris_from_env()
    if not jar_uris:
        print(
            "No JAR URIs configured (set JAR_FILE_URIS or GCS_BUCKET + VERSIONLESS_JAR_NAME); skipping.",
            file=sys.stderr,
        )
        return 0

    dags_dir = Path(args.dags_dir)
    patched = patch_dags_tree(dags_dir, jar_uris)
    if not patched:
        print(f"No jar_file_uris defaults updated under {dags_dir}/")
        return 0

    print(f"Patched jar_file_uris in {len(patched)} file(s):")
    for path in patched:
        print(f"  - {path}")
    print("Resolved URIs:", ", ".join(jar_uris))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
