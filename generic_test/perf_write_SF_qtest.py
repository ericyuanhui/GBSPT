#!/usr/bin/env python3
"""Execute schema + copy scripts and fallback to UNWIND+MERGE on COPY failure."""

from __future__ import annotations

import argparse
import csv
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import ladybug as lb


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SCHEMA = BASE_DIR / "ldbc_schema.cypher"
DEFAULT_COPY = BASE_DIR / "ldbc_copy.cypher"
DEFAULT_DB_PATH = BASE_DIR / "ldbc_sf01.lbug"
DEFAULT_RESULT = BASE_DIR / "perf_benchmark_result"
DEFAULT_UNWIND_BATCH_SIZE = 10_000
DEFAULT_STATIC_REOPEN_ROWS = 10_000_000


@dataclass(frozen=True)
class NodeTableSchema:
    name: str
    columns: List[Tuple[str, str]]


@dataclass(frozen=True)
class RelTableSchema:
    name: str
    src: str
    dst: str
    columns: List[Tuple[str, str]]


@dataclass(frozen=True)
class ExpandedCopyJob:
    phase: str
    table: str
    tail: str
    csv_path: Path
    day: int
    shard: int


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


def parse_schema_tables(schema_file: Path) -> Tuple[Dict[str, NodeTableSchema], Dict[str, RelTableSchema]]:
    node_tables: Dict[str, NodeTableSchema] = {}
    rel_tables: Dict[str, RelTableSchema] = {}

    for stmt in split_cypher_statements(schema_file):
        node_match = re.match(
            r"^create\s+node\s+table\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s*\((.*)\)$",
            stmt,
            re.IGNORECASE | re.DOTALL,
        )
        if node_match:
            table = node_match.group(1)
            body = node_match.group(2)
            columns: List[Tuple[str, str]] = []
            for raw_part in body.split(","):
                part = raw_part.strip()
                col_match = re.match(r"^`?([^`]+)`?\s+([A-Z0-9]+)$", part, re.IGNORECASE)
                if not col_match:
                    continue
                col_name = col_match.group(1)
                if col_name.upper() == "PRIMARY":
                    continue
                columns.append((col_name, col_match.group(2).upper()))
            node_tables[table] = NodeTableSchema(name=table, columns=columns)
            continue

        rel_match = re.match(
            r"^create\s+rel\s+table\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s*\((.*)\)$",
            stmt,
            re.IGNORECASE | re.DOTALL,
        )
        if rel_match:
            table = rel_match.group(1)
            body = rel_match.group(2)
            endpoints = re.search(r"FROM\s+`([^`]+)`\s+TO\s+`([^`]+)`", body, re.IGNORECASE)
            if not endpoints:
                raise ValueError(f"Unsupported REL TABLE statement: {stmt}")
            columns: List[Tuple[str, str]] = []
            body_without_endpoints = re.sub(
                r"FROM\s+`[^`]+`\s+TO\s+`[^`]+`\s*,?",
                "",
                body,
                count=1,
                flags=re.IGNORECASE,
            )
            for raw_part in body_without_endpoints.split(","):
                part = raw_part.strip()
                if not part or part.upper() in {"MANY_ONE", "ONE_MANY", "MANY_MANY", "ONE_ONE"}:
                    continue
                col_match = re.match(r"^`?([^`]+)`?\s+([A-Z0-9]+)$", part, re.IGNORECASE)
                if not col_match:
                    continue
                columns.append((col_match.group(1), col_match.group(2).upper()))
            rel_tables[table] = RelTableSchema(
                name=table,
                src=endpoints.group(1),
                dst=endpoints.group(2),
                columns=columns,
            )

    return node_tables, rel_tables


def parse_table_kinds(
    node_tables: Dict[str, NodeTableSchema],
    rel_tables: Dict[str, RelTableSchema],
) -> Dict[str, str]:
    kinds: Dict[str, str] = {}
    for table in node_tables:
        kinds[table] = "node"
    for table in rel_tables:
        kinds[table] = "rel"
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


def shard_sort_key(path: Path) -> Tuple[int, int, str]:
    phase_rank = 0
    parts = [part.lower() for part in path.parts]
    if "dynamic" in parts:
        phase_rank = 1
    m = re.search(r"_(\d+)_(\d+)$", path.stem)
    if not m:
        return (phase_rank, 0, 0, path.name)
    return (phase_rank, int(m.group(1)), int(m.group(2)), path.name)


def shard_parts(path: Path) -> Tuple[int, int]:
    m = re.search(r"_(\d+)_(\d+)$", path.stem)
    if not m:
        return 0, 0
    return int(m.group(1)), int(m.group(2))


def resolve_csv_paths(csv_root: Path, csv_token: str) -> List[Path]:
    token_path = Path(csv_token)
    resolved = token_path.resolve() if token_path.is_absolute() else (csv_root / token_path).resolve()

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

    return []


def build_copy_sql(table: str, csv_path: Path, tail: str) -> str:
    normalized = f"COPY `{table}` FROM \"{csv_path}\""
    effective_tail = tail
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
    buffer_pool_size: int | None,
    max_num_threads: int | None,
    max_db_size: int | None,
) -> Tuple[lb.Database, lb.Connection]:
    db_kwargs = {}
    if buffer_pool_size is not None:
        db_kwargs["buffer_pool_size"] = buffer_pool_size
    if max_num_threads is not None:
        db_kwargs["max_num_threads"] = max_num_threads
    if max_db_size is not None:
        db_kwargs["max_db_size"] = max_db_size

    db = lb.Database(str(db_path), **db_kwargs)

    conn_kwargs = {}
    if max_num_threads is not None:
        conn_kwargs["num_threads"] = max_num_threads
    return db, lb.Connection(db, **conn_kwargs)


def close_database(conn: lb.Connection | None, db: lb.Database | None) -> None:
    if conn is not None:
        conn.close()
    if db is not None:
        db.close()


def close_and_reopen(
    conn: lb.Connection | None,
    db: lb.Database | None,
    db_path: Path,
    buffer_pool_size: int | None,
    max_num_threads: int | None,
    max_db_size: int | None,
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

    next_db, next_conn = open_database(db_path, buffer_pool_size, max_num_threads, max_db_size)
    elapsed = time.perf_counter() - start
    line = f"[reopen] reason={reason} time={elapsed:.8f}s"
    print(line)
    append_result(result_file, [line])
    return next_db, next_conn, elapsed


def is_csv_file_empty(csv_path: Path) -> bool:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return True
        if not header:
            return True
        return next(reader, None) is None


def escape_single_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def cast_value(raw: str, field_type: str):
    t = field_type.upper()
    if t == "INT64":
        return int(raw)
    if t == "DOUBLE":
        return float(raw)
    if t == "BOOL":
        return raw.lower() == "true"
    return raw


def cypher_literal(value, field_type: str) -> str:
    t = field_type.upper()
    if t in {"INT64", "DOUBLE"}:
        return str(value)
    if t == "BOOL":
        return "true" if bool(value) else "false"
    return f"'{escape_single_quote(str(value))}'"


def build_cypher_map_literal(columns: Sequence[Tuple[str, str]], row: Dict[str, str]) -> str:
    parts: List[str] = []
    for col_name, col_type in columns:
        raw_value = row.get(col_name, "")
        value = cast_value(raw_value, col_type)
        parts.append(f"{col_name}: {cypher_literal(value, col_type)}")
    return "{" + ", ".join(parts) + "}"


def chunked(rows: Sequence[Dict[str, str]], size: int) -> Iterable[List[Dict[str, str]]]:
    for idx in range(0, len(rows), size):
        yield list(rows[idx : idx + size])


def load_csv_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def build_unwind_merge_node_stmt(table: str, columns: Sequence[Tuple[str, str]], rows: Sequence[Dict[str, str]]) -> str:
    if not columns:
        raise ValueError(f"Node table {table} has no columns in schema")
    payload = "[" + ", ".join(build_cypher_map_literal(columns, row) for row in rows) + "]"
    id_field = columns[0][0]
    set_clauses = [f"n.{col_name} = row.{col_name}" for col_name, _ in columns if col_name != id_field]
    stmt = f"UNWIND {payload} AS row MERGE (n:`{table}` {{{id_field}: row.{id_field}}})"
    if set_clauses:
        stmt += " SET " + ", ".join(set_clauses)
    return stmt + ";"


def build_unwind_merge_rel_stmt(
    rel_table: str,
    rel_schema: RelTableSchema,
    rows: Sequence[Dict[str, str]],
) -> str:
    payload_columns = [("from", "STRING"), ("to", "STRING"), *rel_schema.columns]
    normalized_rows: List[Dict[str, str]] = []
    for row in rows:
        normalized: Dict[str, str] = {
            "from": row["from"],
            "to": row["to"],
        }
        for col_name, _col_type in rel_schema.columns:
            normalized[col_name] = row.get(col_name, "")
        normalized_rows.append(normalized)

    payload = "[" + ", ".join(build_cypher_map_literal(payload_columns, row) for row in normalized_rows) + "]"
    stmt = (
        f"UNWIND {payload} AS row "
        f"MATCH (src:`{rel_schema.src}` {{id: row.from}}), (dst:`{rel_schema.dst}` {{id: row.to}}) "
        f"MERGE (src)-[r:`{rel_table}`]->(dst)"
    )
    if rel_schema.columns:
        sets = ", ".join(f"r.{col_name} = row.{col_name}" for col_name, _ in rel_schema.columns)
        stmt += f" SET {sets}"
    return stmt + ";"


def fallback_unwind_merge(
    conn: lb.Connection,
    table: str,
    kind: str,
    csv_path: Path,
    node_tables: Dict[str, NodeTableSchema],
    rel_tables: Dict[str, RelTableSchema],
    result_file: Path,
    unwind_batch_size: int,
) -> int:
    rows = load_csv_rows(csv_path)
    if not rows:
        line = f"[fallback-skip] kind={kind} table={table} file={csv_path.name} reason=no_data_rows"
        print(line)
        append_result(result_file, [line])
        return 0

    total_written = 0
    total_chunks = (len(rows) + unwind_batch_size - 1) // unwind_batch_size
    for chunk_idx, chunk_rows in enumerate(chunked(rows, unwind_batch_size), start=1):
        if kind == "node":
            table_schema = node_tables.get(table)
            if table_schema is None:
                raise ValueError(f"Node table schema not found for fallback: {table}")
            sql = build_unwind_merge_node_stmt(table, table_schema.columns, chunk_rows)
        elif kind == "rel":
            rel_schema = rel_tables.get(table)
            if rel_schema is None:
                raise ValueError(f"Rel table schema not found for fallback: {table}")
            sql = build_unwind_merge_rel_stmt(table, rel_schema, chunk_rows)
        else:
            raise ValueError(f"Unsupported fallback kind for table {table}: {kind}")

        sql_line = (
            f"[fallback-sql] kind={kind} table={table} file={csv_path.name} "
            f"chunk={chunk_idx}/{total_chunks} rows={len(chunk_rows)} sql={sql}"
        )
        print(sql_line)
        append_result(result_file, [sql_line])

        start = time.perf_counter()
        conn.execute(sql)
        elapsed = time.perf_counter() - start
        total_written += len(chunk_rows)
        line = (
            f"[fallback-copy] kind={kind} table={table} file={csv_path.name} "
            f"chunk={chunk_idx}/{total_chunks} rows={len(chunk_rows)} "
            f"time={elapsed:.8f}s {format_rows_per_sec(len(chunk_rows), elapsed)}"
        )
        print(line)
        append_result(result_file, [line])

    return total_written


def build_static_groups(
    jobs: Sequence[ExpandedCopyJob],
    row_count_cache: Dict[Path, int],
    threshold_rows: int,
) -> List[Tuple[str, List[ExpandedCopyJob]]]:
    if not jobs:
        return []
    groups: List[Tuple[str, List[ExpandedCopyJob]]] = []
    current: List[ExpandedCopyJob] = []
    current_rows = 0
    group_idx = 1
    for job in jobs:
        job_rows = row_count_cache[job.csv_path]
        if current and current_rows + job_rows > threshold_rows:
            groups.append((f"static_group={group_idx}", current))
            group_idx += 1
            current = []
            current_rows = 0
        current.append(job)
        current_rows += job_rows
    if current:
        groups.append((f"static_group={group_idx}", current))
    return groups


def build_dynamic_groups(jobs: Sequence[ExpandedCopyJob]) -> List[Tuple[str, List[ExpandedCopyJob]]]:
    grouped: Dict[int, List[ExpandedCopyJob]] = {}
    for job in jobs:
        grouped.setdefault(job.day, []).append(job)
    return [(f"day={day}", grouped[day]) for day in sorted(grouped)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run COPY benchmark for Ladybug with UNWIND fallback")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="schema cypher file")
    parser.add_argument("--copy", type=Path, default=DEFAULT_COPY, help="copy cypher file")
    parser.add_argument(
        "--csv-root",
        type=Path,
        default=BASE_DIR,
        help="CSV root directory; script scans static/ and dynamic/ beneath it",
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="output Ladybug db path")
    parser.add_argument("--result-file", type=Path, default=DEFAULT_RESULT, help="result output file")
    parser.add_argument(
        "--buffer-pool-size",
        type=int,
        default=None,
        help="Ladybug buffer pool size in bytes; omit to use Ladybug default",
    )
    parser.add_argument(
        "--max-db-size",
        type=int,
        default=None,
        help="Ladybug max database size in bytes; omit to use Ladybug default",
    )
    parser.add_argument(
        "--max-num-threads",
        type=int,
        default=None,
        help="maximum number of execution threads; omit to use Ladybug default",
    )
    parser.add_argument(
        "--unwind-batch-size",
        type=int,
        default=DEFAULT_UNWIND_BATCH_SIZE,
        help="rows per fallback UNWIND+MERGE statement when COPY fails; default is 10000",
    )
    parser.add_argument(
        "--static-reopen-rows",
        type=int,
        default=DEFAULT_STATIC_REOPEN_ROWS,
        help="reopen after this many planned static rows; default is 10000000",
    )
    args = parser.parse_args()

    if args.unwind_batch_size <= 0:
        raise ValueError("--unwind-batch-size must be > 0")
    if args.static_reopen_rows <= 0:
        raise ValueError("--static-reopen-rows must be > 0")

    schema_file = args.schema.resolve()
    copy_file = args.copy.resolve()
    csv_root = args.csv_root.resolve()
    db_path = args.db_path.resolve()
    result_file = args.result_file.resolve()

    head_line = (
        f"[head] schema={schema_file} copy={copy_file} csv_root={csv_root} db={db_path} "
        f"buffer_pool_size={args.buffer_pool_size if args.buffer_pool_size is not None else 'ladybug_default'} "
        f"max_db_size={args.max_db_size if args.max_db_size is not None else 'ladybug_default'} "
        f"max_num_threads={args.max_num_threads if args.max_num_threads is not None else 'ladybug_default'} "
        f"unwind_batch_size={args.unwind_batch_size} "
        f"static_reopen_rows={args.static_reopen_rows}"
    )
    print(head_line)
    append_result(result_file, [head_line])

    node_tables, rel_tables = parse_schema_tables(schema_file)
    table_kinds = parse_table_kinds(node_tables, rel_tables)
    schema_statements = split_cypher_statements(schema_file)
    copy_statements = split_cypher_statements(copy_file)
    #remove_existing_db(db_path)

    db: lb.Database | None = None
    conn: lb.Connection | None = None

    node_time = 0.0
    rel_time = 0.0
    reopen_time = 0.0
    node_rows = 0
    rel_rows = 0
    skipped_copy_statements = 0
    skipped_empty_files = 0
    fallback_files = 0
    fallback_rows = 0
    row_count_cache: Dict[Path, int] = {}

    try:
        db, conn = open_database(db_path, args.buffer_pool_size, args.max_num_threads, args.max_db_size)
        for stmt in schema_statements:
            try:
                conn.execute(stmt + ";")
            except Exception as exc:
                line = f"[schema-skip] stmt={stmt} reason={exc}"
                print(line)
                append_result(result_file, [line])

        copy_jobs: List[Tuple[str, str, str]] = []
        for stmt in copy_statements:
            table, csv_token, tail = parse_copy_statement_parts(stmt)
            copy_jobs.append((table, csv_token, tail))

        ordered_jobs = [
            ("static", [job for job in copy_jobs if "/static/" in f"/{job[1].replace('\\', '/')}/" or job[1].startswith("static/")]),
            ("dynamic", [job for job in copy_jobs if "/dynamic/" in f"/{job[1].replace('\\', '/')}/" or job[1].startswith("dynamic/")]),
            (
                "other",
                [
                    job
                    for job in copy_jobs
                    if not ("/static/" in f"/{job[1].replace('\\', '/')}/" or job[1].startswith("static/"))
                    and not ("/dynamic/" in f"/{job[1].replace('\\', '/')}/" or job[1].startswith("dynamic/"))
                ],
            ),
        ]

        expanded_jobs: List[ExpandedCopyJob] = []
        for phase_name, jobs in ordered_jobs:
            if not jobs:
                continue
            print(f"[phase] resolving {phase_name} COPY statements: {len(jobs)}")
            for table, csv_token, tail in jobs:
                csv_paths = resolve_csv_paths(csv_root, csv_token)
                if not csv_paths:
                    skipped_copy_statements += 1
                    line = (
                        f"[skip] phase={phase_name} kind={table_kinds.get(table, 'unknown')} table={table} "
                        f"token={csv_token} reason=csv_not_found under={csv_root}"
                    )
                    print(line)
                    append_result(result_file, [line])
                    continue
                for csv_path in csv_paths:
                    if is_csv_file_empty(csv_path):
                        skipped_empty_files += 1
                        line = (
                            f"[skip] phase={phase_name} kind={table_kinds.get(table, 'unknown')} table={table} "
                            f"file={csv_path.name} reason=csv_empty"
                        )
                        print(line)
                        append_result(result_file, [line])
                        continue
                    row_count_cache[csv_path] = count_csv_rows(csv_path)
                    day, shard = shard_parts(csv_path)
                    expanded_jobs.append(
                        ExpandedCopyJob(
                            phase=phase_name,
                            table=table,
                            tail=tail,
                            csv_path=csv_path,
                            day=day,
                            shard=shard,
                        )
                    )

        static_jobs = sorted(
            [job for job in expanded_jobs if job.phase == "static"],
            key=lambda job: (job.table, job.shard, job.csv_path.name),
        )
        dynamic_jobs = sorted(
            [job for job in expanded_jobs if job.phase == "dynamic"],
            key=lambda job: (job.day, job.table, job.shard, job.csv_path.name),
        )
        other_jobs = sorted(
            [job for job in expanded_jobs if job.phase == "other"],
            key=lambda job: (job.table, job.day, job.shard, job.csv_path.name),
        )

        execution_groups: List[Tuple[str, List[ExpandedCopyJob]]] = []
        execution_groups.extend(build_static_groups(static_jobs, row_count_cache, args.static_reopen_rows))
        execution_groups.extend(build_dynamic_groups(dynamic_jobs))
        if other_jobs:
            execution_groups.append(("other", other_jobs))

        batch_idx = 0
        total_groups = len(execution_groups)
        for group_index, (group_label, group_jobs) in enumerate(execution_groups, start=1):
            group_line = f"[group] label={group_label} files={len(group_jobs)}"
            print(group_line)
            append_result(result_file, [group_line])

            for job in group_jobs:
                batch_idx += 1
                sql = build_copy_sql(job.table, job.csv_path, job.tail)
                kind = table_kinds.get(job.table, "unknown")
                rows = row_count_cache[job.csv_path]

                start = time.perf_counter()
                try:
                    conn.execute(sql)
                    elapsed = time.perf_counter() - start
                    rate = format_rows_per_sec(rows, elapsed)
                    line = (
                        f"[copy] phase={job.phase} group={group_label} kind={kind} table={job.table} "
                        f"batch={batch_idx} file={job.csv_path.name} rows={rows} time={elapsed:.8f}s {rate}"
                    )
                    print(line)
                    append_result(result_file, [line])

                    if kind == "node":
                        node_time += elapsed
                        node_rows += rows
                    elif kind == "rel":
                        rel_time += elapsed
                        rel_rows += rows
                except Exception as exc:
                    elapsed = time.perf_counter() - start
                    fail_line = (
                        f"[copy-failed] phase={job.phase} group={group_label} kind={kind} table={job.table} "
                        f"batch={batch_idx} file={job.csv_path.name} time={elapsed:.8f}s error={exc}"
                    )
                    print(fail_line)
                    append_result(result_file, [fail_line])

                    fallback_written = fallback_unwind_merge(
                        conn=conn,
                        table=job.table,
                        kind=kind,
                        csv_path=job.csv_path,
                        node_tables=node_tables,
                        rel_tables=rel_tables,
                        result_file=result_file,
                        unwind_batch_size=args.unwind_batch_size,
                    )
                    fallback_files += 1
                    fallback_rows += fallback_written
                    if kind == "node":
                        node_rows += fallback_written
                    elif kind == "rel":
                        rel_rows += fallback_written

            reopen = group_index < total_groups
            db, conn, elapsed = close_and_reopen(
                conn=conn,
                db=db,
                db_path=db_path,
                buffer_pool_size=args.buffer_pool_size,
                max_num_threads=args.max_num_threads,
                max_db_size=args.max_db_size,
                result_file=result_file,
                reason=f"after_{group_label}",
                reopen=reopen,
            )
            if reopen:
                reopen_time += elapsed

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
        f"[summary] skipped_empty_files={skipped_empty_files}",
        f"[summary] fallback_files={fallback_files}",
        f"[summary] fallback_rows={fallback_rows}",
    ]

    for line in summary_lines:
        print(line)
    append_result(result_file, summary_lines)


if __name__ == "__main__":
    main()
