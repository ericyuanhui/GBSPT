#!/usr/bin/env python3
"""Execute LDBC SF01 schema + copy scripts and report write performance."""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple

import ladybug as lb


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SCHEMA = BASE_DIR / "ldbc_schema.cypher"
DEFAULT_COPY = BASE_DIR / "ldbc_copy.cypher"
DEFAULT_DB_PATH = BASE_DIR / "ldbc_sf01.lbug"
DEFAULT_RESULT = BASE_DIR / "perf_benchmark_result"
DEFAULT_ASSUMED_ROWS_PER_FILE = 50_000_000
DEFAULT_BUFFER_POOL_SIZE = 48 * 1024**3


def split_cypher_statements(file_path: Path) -> List[str]:
    text = file_path.read_text(encoding="utf-8")
    cleaned_lines: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if "--" in line:
            line = line.split("--", 1)[0]
        cleaned_lines.append(line)
    joined = "\n".join(cleaned_lines)
    return [stmt.strip() for stmt in joined.split(";") if stmt.strip()]


def parse_table_kinds(schema_file: Path) -> Dict[str, str]:
    kinds: Dict[str, str] = {}
    for stmt in split_cypher_statements(schema_file):
        node_match = re.match(r"^create\s+node\s+table\s+([A-Za-z_][A-Za-z0-9_]*)", stmt, re.IGNORECASE)
        if node_match:
            kinds[node_match.group(1)] = "node"
            continue
        rel_match = re.match(r"^create\s+rel\s+table\s+([A-Za-z_][A-Za-z0-9_]*)", stmt, re.IGNORECASE)
        if rel_match:
            kinds[rel_match.group(1)] = "rel"
    return kinds


def parse_copy_statement_parts(stmt: str) -> Tuple[str, str, str]:
    match = re.match(
        r"^copy\s+([A-Za-z_][A-Za-z0-9_]*)\s+from\s+[\"']([^\"']+)[\"']\s*(.*)$",
        stmt,
        re.IGNORECASE,
    )
    if not match:
        raise ValueError(f"Unsupported COPY statement: {stmt}")
    table = match.group(1)
    csv_token = match.group(2)
    tail = match.group(3).strip()
    return table, csv_token, tail


def shard_sort_key(path: Path) -> Tuple[int, int, str]:
    m = re.search(r"_(\d+)_(\d+)$", path.stem)
    if not m:
        return (0, 0, path.name)
    return (int(m.group(1)), int(m.group(2)), path.name)


def resolve_csv_paths(csv_root: Path, csv_token: str) -> List[Path]:
    token_path = Path(csv_token)
    resolved = token_path.resolve() if token_path.is_absolute() else (csv_root / token_path).resolve()

    # If token has shard suffix like *_0_0.csv, expand to all shards in same directory.
    m = re.search(r"^(.*)_\d+_\d+$", resolved.stem)
    if m:
        stem_prefix = m.group(1)
        parent = resolved.parent
        suffix = re.escape(resolved.suffix)
        shard_regex = re.compile(rf"^{re.escape(stem_prefix)}_\d+_\d+{suffix}$", re.IGNORECASE)
        candidates = [
            p.resolve()
            for p in parent.glob(f"*{resolved.suffix}")
            if p.is_file() and shard_regex.fullmatch(p.name)
        ]
        if candidates:
            return sorted(candidates, key=shard_sort_key)

    if resolved.exists() and resolved.is_file():
        return [resolved]

    raise FileNotFoundError(f"Cannot resolve CSV token={csv_token} under csv_root={csv_root}")


def build_copy_sql(table: str, csv_path: Path, tail: str) -> str:
    normalized = f"COPY {table} FROM \"{csv_path}\""
    if tail:
        normalized += f" {tail}"
    if not normalized.endswith(";"):
        normalized += ";"
    return normalized


def format_rows_per_sec(rows: int, elapsed: float) -> str:
    safe_elapsed = elapsed if elapsed > 0 else 1e-9
    return f"{rows / safe_elapsed:.2f} rows/s"


def append_result(result_file: Path, lines: List[str]) -> None:
    result_file.parent.mkdir(parents=True, exist_ok=True)
    with result_file.open("a", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def remove_existing_db(db_path: Path) -> None:
    if db_path.exists():
        if db_path.is_file():
            db_path.unlink()
            print(f"[head] removed existing db file: {db_path}")
        else:
            raise RuntimeError(f"db path exists but is not a file: {db_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run LDBC SF01 COPY benchmark for Ladybug")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="schema cypher file")
    parser.add_argument("--copy", type=Path, default=DEFAULT_COPY, help="copy cypher file")
    parser.add_argument(
        "--csv-root",
        type=Path,
        default=BASE_DIR,
        help="CSV root directory; script scans only static/ and dynamic/ beneath it",
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="output Ladybug db path")
    parser.add_argument("--result-file", type=Path, default=DEFAULT_RESULT, help="result output file")
    parser.add_argument(
        "--assumed-rows",
        type=int,
        default=DEFAULT_ASSUMED_ROWS_PER_FILE,
        help="assumed rows per copied CSV file; no CSV row counting is performed",
    )
    parser.add_argument(
        "--buffer-pool-size",
        type=int,
        default=DEFAULT_BUFFER_POOL_SIZE,
        help="Ladybug buffer pool size in bytes; default is 48 GiB",
    )
    args = parser.parse_args()

    schema_file = args.schema.resolve()
    copy_file = args.copy.resolve()
    csv_root = args.csv_root.resolve()
    db_path = args.db_path.resolve()
    result_file = args.result_file.resolve()

    head_line = (
        f"[head] schema={schema_file} copy={copy_file} csv_root={csv_root} db={db_path} "
        f"assumed_rows={args.assumed_rows} buffer_pool_size={args.buffer_pool_size}"
    )
    print(head_line)
    append_result(result_file, [head_line])

    table_kinds = parse_table_kinds(schema_file)
    schema_statements = split_cypher_statements(schema_file)
    copy_statements = split_cypher_statements(copy_file)
    remove_existing_db(db_path)

    db = lb.Database(str(db_path), buffer_pool_size=args.buffer_pool_size, max_db_size=1 << 43)
    conn = lb.Connection(db)

    node_time = 0.0
    rel_time = 0.0
    node_rows = 0
    rel_rows = 0

    try:
        for stmt in schema_statements:
            conn.execute(stmt + ";")

        copy_jobs: List[Tuple[str, str, str]] = []
        for stmt in copy_statements:
            table, csv_token, tail = parse_copy_statement_parts(stmt)
            copy_jobs.append((table, csv_token, tail))

        ordered_jobs = [
            ("node", [job for job in copy_jobs if table_kinds.get(job[0], "unknown") == "node"]),
            ("rel", [job for job in copy_jobs if table_kinds.get(job[0], "unknown") == "rel"]),
            ("unknown", [job for job in copy_jobs if table_kinds.get(job[0], "unknown") == "unknown"]),
        ]

        expanded_jobs: List[Tuple[str, str, str, Path]] = []
        for phase_kind, jobs in ordered_jobs:
            if not jobs:
                continue
            print(f"[phase] resolving {phase_kind} COPY statements: {len(jobs)}")
            for table, csv_token, tail in jobs:
                for csv_path in resolve_csv_paths(csv_root, csv_token):
                    expanded_jobs.append((phase_kind, table, tail, csv_path))

        batch_idx = 0
        for phase_kind, table, tail, csv_path in expanded_jobs:
            batch_idx += 1
            sql = build_copy_sql(table, csv_path, tail)
            kind = table_kinds.get(table, "unknown")
            rows = args.assumed_rows

            start = time.perf_counter()
            conn.execute(sql)
            elapsed = time.perf_counter() - start
            rate = format_rows_per_sec(rows, elapsed)

            line = (
                f"[copy] kind={kind} table={table} batch={batch_idx} file={csv_path.name} rows={rows} "
                f"time={elapsed:.8f}s {rate}"
            )
            print(line)
            append_result(result_file, [line])

            if kind == "node":
                node_time += elapsed
                node_rows += rows
            elif kind == "rel":
                rel_time += elapsed
                rel_rows += rows

    finally:
        conn.close()

    summary_lines = [
        (
            f"[summary] node_time_batch_sum={node_time:.8f}s rows={node_rows} "
            f"rate={format_rows_per_sec(node_rows, node_time)}"
        ),
        (
            f"[summary] rel_time_batch_sum={rel_time:.8f}s rows={rel_rows} "
            f"rate={format_rows_per_sec(rel_rows, rel_time)}"
        ),
    ]

    for line in summary_lines:
        print(line)
    append_result(result_file, summary_lines)


if __name__ == "__main__":
    main()
