#!/usr/bin/env python3
"""Execute SAM schema + copy scripts and report write performance."""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import ladybug as lb


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SCHEMA = BASE_DIR / "SI_SAM_schema.cypher"
DEFAULT_COPY = BASE_DIR / "sam_copy.cypher"
DEFAULT_CSV_ROOT = BASE_DIR / "CsvBasic"
DEFAULT_DB_PATH = BASE_DIR / "sam_sf.lbug"
DEFAULT_RESULT = BASE_DIR / "perf_benchmark_result"
DEFAULT_ASSUMED_ROWS_PER_FILE = 50_000_000
DEFAULT_BUFFER_POOL_SIZE = 32 * 1024**3
DEFAULT_MAX_DB_SIZE = 1 << 43
DEFAULT_MAX_NUM_THREADS = 32
DEFAULT_START_DAY = 150


@dataclass(frozen=True)
class CopyJob:
    table: str
    csv_token: str
    tail: str
    kind: str
    order: int


@dataclass(frozen=True)
class ExpandedCopyJob:
    table: str
    tail: str
    kind: str
    csv_path: Path
    day: int
    shard: int
    order: int


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
        node_match = re.match(r"^create\s+node\s+table\s+`?([A-Za-z_][A-Za-z0-9_]*)`?", stmt, re.IGNORECASE)
        if node_match:
            kinds[node_match.group(1)] = "node"
            continue
        rel_match = re.match(r"^create\s+rel\s+table\s+`?([A-Za-z_][A-Za-z0-9_]*)`?", stmt, re.IGNORECASE)
        if rel_match:
            kinds[rel_match.group(1)] = "rel"
    return kinds


def parse_copy_statement_parts(stmt: str) -> Tuple[str, str, str]:
    match = re.match(
        r"^copy\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s+from\s+[\"']([^\"']+)[\"']\s*(.*)$",
        stmt,
        re.IGNORECASE,
    )
    if not match:
        raise ValueError(f"Unsupported COPY statement: {stmt}")
    table = match.group(1)
    csv_token = match.group(2)
    tail = match.group(3).strip()
    return table, csv_token, tail


def shard_parts(path: Path) -> Tuple[Optional[str], int, int]:
    m = re.search(r"^(.*)_(\d+)_(\d+)$", path.stem)
    if not m:
        return None, 0, 0
    return m.group(1), int(m.group(2)), int(m.group(3))


def shard_sort_key(path: Path) -> Tuple[int, int, str]:
    _prefix, day, shard = shard_parts(path)
    return day, shard, path.name


def table_file_prefix(csv_token: str) -> str:
    token_path = Path(csv_token)
    stem = token_path.stem
    m = re.search(r"^(.*)_\d+_\d+$", stem)
    return m.group(1) if m else stem


def resolve_csv_paths(csv_root: Path, csv_token: str, phase: str) -> List[Path]:
    token_path = Path(csv_token)
    prefix = table_file_prefix(csv_token)
    suffix = token_path.suffix or ".csv"
    shard_regex = re.compile(rf"^{re.escape(prefix)}_\d+_\d+{re.escape(suffix)}$", re.IGNORECASE)

    search_dirs: List[Path] = []
    if token_path.is_absolute():
        search_dirs.append(token_path.parent)
    elif token_path.parent != Path("."):
        search_dirs.append((csv_root / token_path.parent).resolve())
    else:
        search_dirs.append((csv_root / phase).resolve())

    candidates: List[Path] = []
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        candidates.extend(
            p.resolve()
            for p in search_dir.glob(f"{prefix}_*{suffix}")
            if p.is_file() and shard_regex.fullmatch(p.name)
        )

    return sorted(candidates, key=shard_sort_key)


def build_copy_sql(table: str, csv_path: Path, tail: str, kind: str) -> str:
    normalized = f"COPY `{table}` FROM \"{csv_path}\""
    effective_tail = tail
    # Generated LDBC CSVs include a header row for both node and rel tables.
    # Preserve explicit header options and add header=true only when it was not specified.
    if "header" not in tail.lower():
        effective_tail = f"(header=true) {tail}".strip()
    if effective_tail:
        normalized += f" {effective_tail}"
    if not normalized.endswith(";"):
        normalized += ";"
    return normalized


def format_rows_per_sec(rows: int, elapsed: float) -> str:
    safe_elapsed = elapsed if elapsed > 0 else 1e-9
    return f"{rows / safe_elapsed:.2f} rows/s"


def count_csv_rows(csv_path: Path) -> int:
    newline_count = 0
    with csv_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            newline_count += chunk.count(b"\n")
    return max(0, newline_count - 1)


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


def open_database(
    db_path: Path,
    buffer_pool_size: int,
    max_num_threads: int,
) -> Tuple[lb.Database, lb.Connection]:
    db = lb.Database(
        str(db_path),
        buffer_pool_size=buffer_pool_size,
        max_num_threads=max_num_threads,
        max_db_size=DEFAULT_MAX_DB_SIZE,
    )
    return db, lb.Connection(db, num_threads=max_num_threads)


def close_database(conn: lb.Connection | None, db: lb.Database | None) -> None:
    if conn is not None:
        conn.close()
    if db is not None:
        db.close()


def close_and_reopen(
    conn: lb.Connection | None,
    db: lb.Database | None,
    db_path: Path,
    buffer_pool_size: int,
    max_num_threads: int,
    result_file: Path,
    reason: str,
    reopen: bool,
) -> Tuple[lb.Database | None, lb.Connection | None, float]:
    start = time.perf_counter()
    close_database(conn, db)
    if not reopen:
        elapsed = time.perf_counter() - start
        line = f"[close] reason={reason} time={elapsed:.8f}s"
        print(line)
        append_result(result_file, [line])
        return None, None, elapsed

    next_db, next_conn = open_database(db_path, buffer_pool_size, max_num_threads)
    elapsed = time.perf_counter() - start
    line = f"[reopen] reason={reason} time={elapsed:.8f}s"
    print(line)
    append_result(result_file, [line])
    return next_db, next_conn, elapsed


def order_jobs_for_copy(jobs: Iterable[ExpandedCopyJob]) -> List[ExpandedCopyJob]:
    kind_rank = {"node": 0, "rel": 1}
    return sorted(jobs, key=lambda job: (kind_rank.get(job.kind, 2), job.order, job.shard, job.csv_path.name))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SAM COPY benchmark for Ladybug")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="schema cypher file")
    parser.add_argument("--copy", type=Path, default=DEFAULT_COPY, help="copy cypher file")
    parser.add_argument(
        "--csv-root",
        type=Path,
        default=DEFAULT_CSV_ROOT,
        help="CSV root directory; script scans static/ and dynamic/ beneath it",
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="output Ladybug db path")
    parser.add_argument("--result-file", type=Path, default=DEFAULT_RESULT, help="result output file")
    parser.add_argument(
        "--assumed-rows",
        type=int,
        default=DEFAULT_ASSUMED_ROWS_PER_FILE,
        help="rows per copied CSV file when --row-count-mode=assumed",
    )
    parser.add_argument(
        "--row-count-mode",
        choices=("actual", "assumed"),
        default="actual",
        help="use actual CSV line counts, or the legacy assumed rows per file",
    )
    parser.add_argument(
        "--buffer-pool-size",
        type=int,
        default=DEFAULT_BUFFER_POOL_SIZE,
        help="Ladybug buffer pool size in bytes; default is 32 GiB",
    )
    parser.add_argument(
        "--max-num-threads",
        type=int,
        default=DEFAULT_MAX_NUM_THREADS,
        help="maximum number of execution threads; default is 4",
    )
    parser.add_argument(
        "--start-day",
        type=int,
        default=DEFAULT_START_DAY,
        help="only import dynamic CSV shards with day >= this value; default resumes from day 150",
    )
    args = parser.parse_args()

    schema_file = args.schema.resolve()
    copy_file = args.copy.resolve()
    csv_root = args.csv_root.resolve()
    db_path = args.db_path.resolve()
    result_file = args.result_file.resolve()

    head_line = (
        f"[head] schema={schema_file} copy={copy_file} csv_root={csv_root} db={db_path} "
        f"assumed_rows={args.assumed_rows} buffer_pool_size={args.buffer_pool_size} "
        f"max_num_threads={args.max_num_threads} start_day={args.start_day}"
    )
    print(head_line)
    append_result(result_file, [head_line])

    table_kinds = parse_table_kinds(schema_file)
    schema_statements = split_cypher_statements(schema_file)
    copy_statements = split_cypher_statements(copy_file)

    # Resume import into an existing database, so keep the old DB file.
    remove_existing_db(db_path)
    db: lb.Database | None = None
    conn: lb.Connection | None = None

    node_time = 0.0
    rel_time = 0.0
    reopen_time = 0.0
    node_rows = 0
    rel_rows = 0
    skipped_copy_statements = 0
    batch_idx = 0
    row_count_cache: Dict[Path, int] = {}

    def rows_for_file(csv_path: Path) -> int:
        if args.row_count_mode == "assumed":
            return args.assumed_rows
        cached = row_count_cache.get(csv_path)
        if cached is not None:
            return cached
        rows = count_csv_rows(csv_path)
        row_count_cache[csv_path] = rows
        return rows

    def run_copy_group(label: str, jobs: List[ExpandedCopyJob]) -> Tuple[int, int, float, float]:
        nonlocal batch_idx, node_rows, rel_rows, node_time, rel_time

        group_node_rows = 0
        group_rel_rows = 0
        group_node_time = 0.0
        group_rel_time = 0.0

        for job in jobs:
            batch_idx += 1
            sql = build_copy_sql(job.table, job.csv_path, job.tail, job.kind)
            rows = rows_for_file(job.csv_path)

            start = time.perf_counter()
            try:
                conn.execute(sql)
            except Exception as exc:
                error_line = (
                    f"[error] group={label} kind={job.kind} table={job.table} "
                    f"file={job.csv_path.name} sql={sql} error={exc}"
                )
                print(error_line)
                append_result(result_file, [error_line])
                raise
            elapsed = time.perf_counter() - start
            rate = format_rows_per_sec(rows, elapsed)

            line = (
                f"[copy] group={label} kind={job.kind} table={job.table} batch={batch_idx} "
                f"file={job.csv_path.name} rows={rows} time={elapsed:.8f}s {rate}"
            )
            print(line)
            append_result(result_file, [line])

            if job.kind == "node":
                node_time += elapsed
                node_rows += rows
                group_node_time += elapsed
                group_node_rows += rows
            elif job.kind == "rel":
                rel_time += elapsed
                rel_rows += rows
                group_rel_time += elapsed
                group_rel_rows += rows

        return group_node_rows, group_rel_rows, group_node_time, group_rel_time

    try:
        db, conn = open_database(db_path, args.buffer_pool_size, args.max_num_threads)

        # Resume import into an existing database, so skip DDL execution here.
        for stmt in schema_statements:
            sql = stmt + ";"
            try:
                conn.execute(sql)
            except Exception as exc:
                error_line = f"[error] phase=schema sql={sql} error={exc}"
                print(error_line)
                append_result(result_file, [error_line])
                raise

        copy_jobs: List[CopyJob] = []
        for order, stmt in enumerate(copy_statements):
            table, csv_token, tail = parse_copy_statement_parts(stmt)
            kind = table_kinds.get(table, "unknown")
            copy_jobs.append(CopyJob(table=table, csv_token=csv_token, tail=tail, kind=kind, order=order))

        static_jobs: List[ExpandedCopyJob] = []
        dynamic_jobs_by_day: Dict[int, List[ExpandedCopyJob]] = {}

        print("[phase] resolving static and dynamic COPY files")
        for job in copy_jobs:
            paths_by_phase = {
                "static": resolve_csv_paths(csv_root, job.csv_token, "static"),
                "dynamic": resolve_csv_paths(csv_root, job.csv_token, "dynamic"),
            }
            if not paths_by_phase["static"] and not paths_by_phase["dynamic"]:
                skipped_copy_statements += 1
                line = (
                    f"[skip] kind={job.kind} table={job.table} token={job.csv_token} "
                    f"reason=csv_not_found under={csv_root}/{{static,dynamic}}"
                )
                print(line)
                append_result(result_file, [line])
                continue

            for phase, csv_paths in paths_by_phase.items():
                for csv_path in csv_paths:
                    _prefix, day, shard = shard_parts(csv_path)
                    if phase == "dynamic" and day < args.start_day:
                        continue
                    expanded = ExpandedCopyJob(
                        table=job.table,
                        tail=job.tail,
                        kind=job.kind,
                        csv_path=csv_path,
                        day=day,
                        shard=shard,
                        order=job.order,
                    )
                    if phase == "dynamic":
                        dynamic_jobs_by_day.setdefault(day, []).append(expanded)

        static_jobs = order_jobs_for_copy(static_jobs)
        # Resume import from dynamic day 150+, so skip static CSV batches.
        # static_jobs = []
        dynamic_days = sorted(dynamic_jobs_by_day)
        total_groups = (1 if static_jobs else 0) + len(dynamic_days)
        # total_groups = len(dynamic_days)

        if static_jobs:
            rows_n, rows_r, time_n, time_r = run_copy_group("static", static_jobs)
            static_line = (
                f"[static-summary] node_rows={rows_n} node_time={time_n:.8f}s "
                f"node_rate={format_rows_per_sec(rows_n, time_n)} rel_rows={rows_r} "
                f"rel_time={time_r:.8f}s rel_rate={format_rows_per_sec(rows_r, time_r)}"
            )
            print(static_line)
            append_result(result_file, [static_line])

            reopen = len(dynamic_days) > 0
            db, conn, elapsed = close_and_reopen(
                conn,
                db,
                db_path,
                args.buffer_pool_size,
                args.max_num_threads,
                result_file,
                reason="after_static",
                reopen=reopen,
            )
            if reopen:
                reopen_time += elapsed

        for day_index, day in enumerate(dynamic_days):
            day_jobs = order_jobs_for_copy(dynamic_jobs_by_day[day])
            rows_n, rows_r, time_n, time_r = run_copy_group(f"day={day}", day_jobs)
            day_line = (
                f"[day-summary] day={day} node_rows={rows_n} node_time={time_n:.8f}s "
                f"node_rate={format_rows_per_sec(rows_n, time_n)} rel_rows={rows_r} "
                f"rel_time={time_r:.8f}s rel_rate={format_rows_per_sec(rows_r, time_r)}"
            )
            print(day_line)
            append_result(result_file, [day_line])

            reopen = day_index + 1 < len(dynamic_days)
            db, conn, elapsed = close_and_reopen(
                conn,
                db,
                db_path,
                args.buffer_pool_size,
                args.max_num_threads,
                result_file,
                reason=f"after_day_{day}",
                reopen=reopen,
            )
            if reopen:
                reopen_time += elapsed

        if total_groups == 0:
            close_database(conn, db)
            conn = None
            db = None

    finally:
        close_database(conn, db)

    summary_lines = [
        (
            f"[summary] node_time_batch_sum={node_time:.8f}s rows={node_rows} "
            f"rate={format_rows_per_sec(node_rows, node_time)}"
        ),
        (
            f"[summary] rel_time_batch_sum={rel_time:.8f}s rows={rel_rows} "
            f"rate={format_rows_per_sec(rel_rows, rel_time)}"
        ),
        f"[summary] reopen_time_sum={reopen_time:.8f}s",
        f"[summary] skipped_copy_statements={skipped_copy_statements}",
    ]

    for line in summary_lines:
        print(line)
    append_result(result_file, summary_lines)


if __name__ == "__main__":
    main()
