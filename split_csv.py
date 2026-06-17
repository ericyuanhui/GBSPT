#!/usr/bin/env python3
"""Split node or relationship CSV shard groups into fixed-size row chunks.

The script uses a schema file to classify CSV shard groups as node or relationship data.
For each selected group, existing source shards are renamed to ``bak_*.csv`` first, then
new shards are written with the original LDBC-style ``_<major>_<minor>.csv`` suffixes.

Example:
    comment_hasCreator_person_0_0.csv
    comment_hasCreator_person_0_1.csv
    ...

becomes backups:
    bak_comment_hasCreator_person_0_0.csv
    bak_comment_hasCreator_person_0_1.csv
    ...

and is rewritten as continuous chunks:
    comment_hasCreator_person_0_0.csv
    comment_hasCreator_person_0_1.csv
    ...
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SCHEMA = SCRIPT_DIR / "ldbc_schema.cypher"
DEFAULT_ROWS_PER_FILE = 10_000_000
DEFAULT_CPU_RATIO = 0.8
DEFAULT_MEMORY_RATIO = 0.75
DEFAULT_WORKER_MEMORY_BYTES = 16 * 1024**2


@dataclass(frozen=True)
class TableInfo:
    name: str
    kind: str
    from_table: str | None = None
    to_table: str | None = None


@dataclass(frozen=True)
class CSVShard:
    path: Path
    prefix: str
    major: int
    minor: int


@dataclass(frozen=True)
class CSVGroup:
    prefix: str
    kind: str
    shards: Tuple[CSVShard, ...]


class MemoryGate:
    def __init__(self, total_bytes: int, per_worker_bytes: int):
        self.total_bytes = max(total_bytes, per_worker_bytes)
        self.per_worker_bytes = per_worker_bytes
        self.available_bytes = self.total_bytes
        self.condition = threading.Condition()

    def acquire(self) -> None:
        with self.condition:
            while self.available_bytes < self.per_worker_bytes:
                self.condition.wait()
            self.available_bytes -= self.per_worker_bytes

    def release(self) -> None:
        with self.condition:
            self.available_bytes += self.per_worker_bytes
            self.condition.notify()


IDENT = r"(?:`([^`]+)`|([A-Za-z_][A-Za-z0-9_]*))"


def normalize_name(name: str) -> str:
    return name.replace("`", "").lower()


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


def first_identifier(match: re.Match[str], group_a: int, group_b: int) -> str:
    return match.group(group_a) or match.group(group_b)


def parse_schema(schema_file: Path) -> Dict[str, TableInfo]:
    tables: Dict[str, TableInfo] = {}
    node_re = re.compile(rf"^create\s+node\s+table\s+{IDENT}", re.IGNORECASE)
    rel_re = re.compile(
        rf"^create\s+rel\s+table\s+{IDENT}\s*\(\s*from\s+{IDENT}\s+to\s+{IDENT}",
        re.IGNORECASE,
    )
    for stmt in split_cypher_statements(schema_file):
        node_match = node_re.match(stmt)
        if node_match:
            name = first_identifier(node_match, 1, 2)
            tables[normalize_name(name)] = TableInfo(name=name, kind="node")
            continue
        rel_match = rel_re.match(stmt)
        if rel_match:
            name = first_identifier(rel_match, 1, 2)
            from_table = first_identifier(rel_match, 3, 4)
            to_table = first_identifier(rel_match, 5, 6)
            tables[normalize_name(name)] = TableInfo(
                name=name, kind="rel", from_table=from_table, to_table=to_table
            )
    return tables


def iter_csv_files(csv_root: Path) -> Iterable[Path]:
    yield from sorted(csv_root.rglob("*.csv"))


def parse_csv_shard(path: Path) -> CSVShard | None:
    if path.name.startswith("bak_"):
        return None
    match = re.match(r"^(.*)_([0-9]+)_([0-9]+)$", path.stem)
    if not match:
        return None
    return CSVShard(
        path=path,
        prefix=match.group(1),
        major=int(match.group(2)),
        minor=int(match.group(3)),
    )


def candidate_prefixes(table: TableInfo) -> List[str]:
    table_name = normalize_name(table.name)
    candidates = [table_name]
    if table.kind == "rel" and table.from_table and table.to_table:
        from_name = normalize_name(table.from_table)
        to_name = normalize_name(table.to_table)
        candidates.extend(
            [
                f"{table_name}_{to_name}",
                f"{from_name}_{table_name}",
                f"{from_name}_{table_name}_{to_name}",
            ]
        )
    return sorted(set(candidates), key=len, reverse=True)


def classify_prefix(prefix: str, tables: Dict[str, TableInfo]) -> str | None:
    normalized = normalize_name(prefix)
    for table in sorted(tables.values(), key=lambda t: len(t.name), reverse=True):
        for candidate in candidate_prefixes(table):
            if normalized == candidate or normalized.startswith(candidate + "_"):
                return table.kind
    return None


def discover_groups(csv_root: Path, schema_file: Path, kind_to_split: str) -> List[CSVGroup]:
    tables = parse_schema(schema_file)
    groups: Dict[str, List[CSVShard]] = {}
    for csv_path in iter_csv_files(csv_root):
        shard = parse_csv_shard(csv_path)
        if shard is None:
            continue
        kind = classify_prefix(shard.prefix, tables)
        if kind != kind_to_split:
            continue
        groups.setdefault(str(csv_path.parent / shard.prefix), []).append(shard)

    result: List[CSVGroup] = []
    for shards in groups.values():
        ordered = tuple(sorted(shards, key=lambda s: (s.major, s.minor)))
        if ordered:
            result.append(CSVGroup(prefix=ordered[0].prefix, kind=kind_to_split, shards=ordered))
    return sorted(result, key=lambda g: str(g.shards[0].path))


def backup_path(path: Path) -> Path:
    return path.with_name("bak_" + path.name)


def output_path_for_chunk(first_shard: CSVShard, chunk_idx: int) -> Path:
    return first_shard.path.with_name(f"{first_shard.prefix}_{first_shard.major}_{first_shard.minor + chunk_idx}.csv")


def rename_sources_to_backups(shards: Sequence[CSVShard], dry_run: bool, overwrite_backup: bool) -> List[Path]:
    backups = [backup_path(shard.path) for shard in shards]
    for original, backup in zip((s.path for s in shards), backups):
        if backup.exists() and not overwrite_backup:
            raise FileExistsError(f"Backup exists, pass --overwrite-backup to replace: {backup}")
        if dry_run:
            print(f"[dry-run] rename {original} -> {backup}")
        else:
            if backup.exists():
                backup.unlink()
            original.rename(backup)
    return backups


def open_output(path: Path, header: str, dry_run: bool):
    if dry_run:
        print(f"[dry-run] would create {path}")
        return None
    out = path.open("w", encoding="utf-8", newline="")
    out.write(header)
    return out


def split_group(group: CSVGroup, rows_per_file: int, dry_run: bool, overwrite_backup: bool) -> str:
    first_shard = group.shards[0]
    backups = rename_sources_to_backups(group.shards, dry_run, overwrite_backup)

    header = ""
    chunk_idx = 0
    rows_in_chunk = 0
    chunks_written = 0
    total_rows = 0
    sources_done = 0
    out = None

    try:
        for backup_idx, backup in enumerate(backups):
            with backup.open("r", encoding="utf-8", newline="") as src:
                file_header = src.readline()
                if file_header == "":
                    sources_done += 1
                    continue
                if backup_idx == 0:
                    header = file_header
                elif file_header != header:
                    print(f"[warn] header differs from first shard: {backup}")

                for line in src:
                    if out is None:
                        out_path = output_path_for_chunk(first_shard, chunk_idx)
                        out = open_output(out_path, header, dry_run)
                        chunks_written += 1
                    if out is not None:
                        out.write(line)
                    rows_in_chunk += 1
                    total_rows += 1
                    if rows_in_chunk >= rows_per_file:
                        if out is not None:
                            out.close()
                        out = None
                        rows_in_chunk = 0
                        chunk_idx += 1

            sources_done += 1
            print(
                f"[source-done] prefix={group.prefix} backup={backup} "
                f"sources_done={sources_done}/{len(backups)} rows={total_rows} chunks={chunks_written}",
                flush=True,
            )
    finally:
        if out is not None:
            out.close()

    return (
        f"[split] kind={group.kind} prefix={group.prefix} sources={len(group.shards)} "
        f"rows={total_rows} rows_per_file={rows_per_file} chunks={chunks_written} "
        f"first_source={group.shards[0].path}"
    )


def read_mem_total_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return 1 << 60
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    return 1 << 60


def default_workers() -> int:
    cpus = os.cpu_count() or 1
    return max(1, int(cpus * DEFAULT_CPU_RATIO))


def main() -> None:
    parser = argparse.ArgumentParser(description="Split node or rel CSV shard groups.")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="schema cypher file")
    parser.add_argument("--kind", choices=("node", "rel"), required=True, help="split node or rel CSV groups")
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS_PER_FILE,
        help="rows per output CSV file, excluding header",
    )
    parser.add_argument("--csv-root", type=Path, default=None, help="CSV root to scan; defaults to schema parent")
    parser.add_argument("--workers", type=int, default=0, help="parallel group workers; default is 80%% of CPU cores")
    parser.add_argument("--dry-run", action="store_true", help="print planned changes only")
    parser.add_argument("--overwrite-backup", action="store_true", help="replace existing bak_*.csv files")
    args = parser.parse_args()

    if args.rows <= 0:
        raise ValueError("rows must be positive")

    schema_file = args.schema.resolve()
    csv_root = args.csv_root.resolve() if args.csv_root is not None else schema_file.parent.resolve()
    workers_by_cpu = args.workers if args.workers > 0 else default_workers()
    mem_budget = int(read_mem_total_bytes() * DEFAULT_MEMORY_RATIO)
    workers_by_mem = max(1, mem_budget // DEFAULT_WORKER_MEMORY_BYTES)
    workers = max(1, min(workers_by_cpu, workers_by_mem))

    print(
        f"[head] schema={schema_file} csv_root={csv_root} kind={args.kind} rows={args.rows} "
        f"workers={workers} cpu_workers={workers_by_cpu} memory_budget_bytes={mem_budget}"
    )

    groups = discover_groups(csv_root, schema_file, args.kind)
    if not groups:
        print(f"[done] no {args.kind} CSV shard groups found")
        return

    gate = MemoryGate(mem_budget, DEFAULT_WORKER_MEMORY_BYTES)

    def run_group(group: CSVGroup) -> str:
        gate.acquire()
        try:
            return split_group(group, args.rows, args.dry_run, args.overwrite_backup)
        finally:
            gate.release()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(run_group, group) for group in groups]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

    print(f"[done] groups={len(groups)}")


if __name__ == "__main__":
    main()
