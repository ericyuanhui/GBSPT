#!/usr/bin/env python3
import argparse
import bisect
import csv
import datetime as dt
import math
import random
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


NODE_RE = re.compile(r"^CREATE\s+NODE\s+TABLE\s+`([^`]+)`\s*\((.*)\)$", re.IGNORECASE | re.DOTALL)
REL_RE = re.compile(r"^CREATE\s+REL\s+TABLE\s+`([^`]+)`\s*\((.*)\)$", re.IGNORECASE | re.DOTALL)
COL_RE = re.compile(r"`([^`]+)`\s+([A-Z0-9]+)")
FROM_TO_RE = re.compile(r"FROM\s+`([^`]+)`\s+TO\s+`([^`]+)`", re.IGNORECASE)
CARDINALITY_RE = re.compile(r"\b(MANY_ONE|ONE_MANY|MANY_MANY|ONE_ONE)\b", re.IGNORECASE)

DEFAULT_STATIC_BATCH_SIZE = 100_000


@dataclass
class NodeTable:
    name: str
    columns: List[Tuple[str, str]]


@dataclass
class RelTable:
    name: str
    src: str
    dst: str
    rel_type: str
    columns: List[Tuple[str, str]]


@dataclass
class RelConstraintState:
    src_unique: bool
    dst_unique: bool
    src_to_dst: Dict[int, int] = field(default_factory=dict)
    dst_to_src: Dict[int, int] = field(default_factory=dict)
    seen_pairs: set[Tuple[int, int]] = field(default_factory=set)


@dataclass
class RangePool:
    start: int
    count: int


class IdRegistry:
    def __init__(self) -> None:
        self._all_cache: Dict[str, Tuple[Tuple[RangePool, ...], Tuple[int, ...]]] = {}
        self._today_cache: Dict[str, Tuple[Tuple[RangePool, ...], Tuple[int, ...]]] = {}
        self._ranges: Dict[str, List[Tuple[int, int]]] = {}
        self._lock = threading.Lock()

    def add(self, table: str, start: int, count: int, day: Optional[int]) -> int:
        if count <= 0:
            return start
        with self._lock:
            existing = self._ranges.setdefault(table, [])
            actual_start = start
            if existing:
                actual_start = max(actual_start, max(existing_end for _existing_start, existing_end in existing))
            end = actual_start + count
            existing.append((actual_start, end))
            all_pools, all_prefix = self._all_cache.get(table, ((), ()))
            all_total = all_prefix[-1] if all_prefix else 0
            new_pool = RangePool(actual_start, count)
            self._all_cache[table] = (all_pools + (new_pool,), all_prefix + (all_total + count,))
            if day is not None:
                day_key = f"{table}@{day}"
                day_pools, day_prefix = self._today_cache.get(day_key, ((), ()))
                day_total = day_prefix[-1] if day_prefix else 0
                self._today_cache[day_key] = (day_pools + (new_pool,), day_prefix + (day_total + count,))
            return actual_start

    def sample(self, table: str, rng: random.Random, day: Optional[int] = None) -> Optional[int]:
        entry = None
        if day is not None:
            entry = self._today_cache.get(f"{table}@{day}")
        if not entry:
            entry = self._all_cache.get(table)
        if not entry:
            return None

        pools, prefix = entry
        total = prefix[-1]
        pick = rng.randrange(total)
        idx = bisect.bisect_right(prefix, pick)
        prev_total = prefix[idx - 1] if idx > 0 else 0
        selected = pools[idx]
        return selected.start + (pick - prev_total)

    def count(self, table: str, day: Optional[int] = None) -> int:
        entry = None
        if day is not None:
            entry = self._today_cache.get(f"{table}@{day}")
        if not entry:
            entry = self._all_cache.get(table)
        if not entry:
            return 0
        _pools, prefix = entry
        return prefix[-1] if prefix else 0


def split_sql_statements(schema_path: Path) -> List[str]:
    text = schema_path.read_text(encoding="utf-8")
    cleaned_lines: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("--"):
            continue
        if "--" in raw:
            raw = raw.split("--", 1)[0]
        cleaned_lines.append(raw.strip())
    joined = "\n".join(cleaned_lines)
    return [stmt.strip() for stmt in joined.split(";") if stmt.strip()]


def parse_schema(schema_path: Path) -> Tuple[Dict[str, NodeTable], Dict[str, RelTable]]:
    nodes: Dict[str, NodeTable] = {}
    rels: Dict[str, RelTable] = {}

    for stmt in split_sql_statements(schema_path):
        m_node = NODE_RE.match(stmt)
        if m_node:
            name = m_node.group(1)
            body = m_node.group(2)
            cols: List[Tuple[str, str]] = []
            for cm in COL_RE.finditer(body):
                col, ctype = cm.group(1), cm.group(2)
                if col.upper() == "PRIMARY":
                    continue
                cols.append((col, ctype))
            nodes[name] = NodeTable(name=name, columns=cols)
            continue

        m_rel = REL_RE.match(stmt)
        if m_rel:
            name = m_rel.group(1)
            body = m_rel.group(2)
            from_to = FROM_TO_RE.search(body)
            cardinality = CARDINALITY_RE.search(body)
            if not from_to or not cardinality:
                raise ValueError(f"Unsupported REL TABLE statement: {stmt}")
            src, dst = from_to.group(1), from_to.group(2)
            property_body = FROM_TO_RE.sub("", body, count=1).replace(cardinality.group(1), "")
            cols: List[Tuple[str, str]] = []
            for cm in COL_RE.finditer(property_body):
                cols.append((cm.group(1), cm.group(2)))
            rels[name] = RelTable(
                name=name,
                src=src,
                dst=dst,
                rel_type=cardinality.group(1).upper(),
                columns=cols,
            )

    return nodes, rels


def weighted_split(target: int, weights: Dict[str, int], keys: List[str]) -> Dict[str, int]:
    if target <= 0 or not keys:
        return {k: 0 for k in keys}

    positive_sum = sum(max(weights.get(k, 0), 0) for k in keys)
    if positive_sum <= 0:
        base = target // len(keys)
        result = {k: base for k in keys}
        for i in range(target - base * len(keys)):
            result[keys[i % len(keys)]] += 1
        return result

    raw = {k: target * max(weights.get(k, 0), 0) / positive_sum for k in keys}
    floored = {k: int(math.floor(v)) for k, v in raw.items()}
    remaining = target - sum(floored.values())
    if remaining > 0:
        ranked = sorted(keys, key=lambda k: raw[k] - floored[k], reverse=True)
        for i in range(remaining):
            floored[ranked[i % len(ranked)]] += 1
    return floored


def derive_node_weights(node_tables: Dict[str, NodeTable], rel_tables: Dict[str, RelTable], static_tables: set[str]) -> Dict[str, int]:
    weights: Dict[str, int] = {}
    for name in node_tables:
        if name in static_tables:
            continue
        degree = 0
        for rel in rel_tables.values():
            if rel.src == name:
                degree += 2
            if rel.dst == name:
                degree += 2
        weights[name] = max(1, degree)
    return weights


def derive_rel_weights(rel_tables: Dict[str, RelTable]) -> Dict[str, int]:
    weights: Dict[str, int] = {}
    for name, rel in rel_tables.items():
        card_bonus = {
            "MANY_MANY": 4,
            "MANY_ONE": 3,
            "ONE_MANY": 3,
            "ONE_ONE": 2,
        }.get(rel.rel_type, 2)
        weights[name] = max(1, card_bonus + len(rel.columns))
    return weights


def build_rel_constraint_state(rel_type: str) -> RelConstraintState:
    t = rel_type.strip().upper()
    if t == "MANY_ONE":
        return RelConstraintState(src_unique=True, dst_unique=False)
    if t == "ONE_MANY":
        return RelConstraintState(src_unique=False, dst_unique=True)
    if t == "ONE_ONE":
        return RelConstraintState(src_unique=True, dst_unique=True)
    return RelConstraintState(src_unique=False, dst_unique=False)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def shard_ranges(total: int, batch_size: int) -> List[Tuple[int, int, int]]:
    if total <= 0:
        return []
    out: List[Tuple[int, int, int]] = []
    shard = 0
    start = 0
    while start < total:
        size = min(batch_size, total - start)
        out.append((shard, start, size))
        start += size
        shard += 1
    return out


def value_for_type(col: str, ctype: str, idx: int, rng: random.Random) -> str:
    if col == "id":
        return str(idx)
    if ctype == "INT64":
        lowered = col.lower()
        if lowered.endswith("_ts") or lowered.endswith("time"):
            base = 1_735_689_600
            return str(base + (idx % 31_536_000))
        return str(idx)
    if ctype == "DOUBLE":
        return f"{(idx % 1000) + rng.random():.4f}"
    if ctype == "BOOL":
        return "true" if idx % 2 == 0 else "false"
    if ctype == "DATE":
        date_value = dt.date(2026, 1, 1) + dt.timedelta(days=(idx % 365))
        return date_value.isoformat()
    if ctype == "STRING":
        lowered = col.lower()
        if "ip" in lowered:
            return f"10.{(idx // 65536) % 255}.{(idx // 256) % 255}.{idx % 254 + 1}"
        if "mac" in lowered:
            return "02:%02x:%02x:%02x:%02x:%02x" % (
                (idx >> 32) & 0xFF,
                (idx >> 24) & 0xFF,
                (idx >> 16) & 0xFF,
                (idx >> 8) & 0xFF,
                idx & 0xFF,
            )
        if "email" in lowered:
            return f"user{idx}@example.com"
    return f"{col}_{idx}"


def write_node_shard(
    out_file: Path,
    table: NodeTable,
    global_start: int,
    count: int,
    table_name: str,
    seed: int,
) -> None:
    rng = random.Random(seed)
    with out_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([col for col, _ctype in table.columns])
        for i in range(count):
            gidx = global_start + i
            row: List[str] = []
            for col, ctype in table.columns:
                if col == "id":
                    row.append(f"{table_name}_{gidx}")
                else:
                    row.append(value_for_type(col, ctype, gidx, rng))
            writer.writerow(row)
    print(f"[node-file] table={table_name} file={out_file.name} rows={count}")


def write_rel_shard(
    out_file: Path,
    rel: RelTable,
    count: int,
    day: int,
    registry: IdRegistry,
    seed: int,
    state: RelConstraintState,
) -> int:
    rng = random.Random(seed)
    src_count = registry.count(rel.src, day=day)
    dst_count = registry.count(rel.dst, day=day)
    if src_count == 0:
        src_count = registry.count(rel.src, day=None)
    if dst_count == 0:
        dst_count = registry.count(rel.dst, day=None)

    allows_duplicate_pairs = rel.rel_type == "MANY_MANY"
    target_rows = count
    max_unique_rows = src_count * dst_count if src_count and dst_count else 0
    if state.src_unique:
        max_unique_rows = min(max_unique_rows, src_count)
    if state.dst_unique:
        max_unique_rows = min(max_unique_rows, dst_count)
    if not allows_duplicate_pairs:
        target_rows = min(count, max(0, max_unique_rows - len(state.seen_pairs)))

    def sample_unseen_endpoint(table: str, used: Dict[int, int], max_tries: int = 256) -> Optional[int]:
        table_count = registry.count(table, day=day)
        if table_count == 0:
            table_count = registry.count(table, day=None)
        if len(used) >= table_count:
            return None
        for _ in range(max_tries):
            idx = registry.sample(table, rng, day=day)
            if idx is None:
                idx = registry.sample(table, rng, day=None)
            if idx is not None and idx not in used:
                return idx
        return None

    def next_pair(max_tries: int = 512) -> Optional[Tuple[int, int]]:
        for _ in range(max_tries):
            src_idx: Optional[int] = None
            dst_idx: Optional[int] = None

            if not state.src_unique and not state.dst_unique:
                src_idx = registry.sample(rel.src, rng, day=day)
                dst_idx = registry.sample(rel.dst, rng, day=day)
                if src_idx is None:
                    src_idx = registry.sample(rel.src, rng, day=None)
                if dst_idx is None:
                    dst_idx = registry.sample(rel.dst, rng, day=None)
            elif state.src_unique and not state.dst_unique:
                src_idx = sample_unseen_endpoint(rel.src, state.src_to_dst)
                if src_idx is not None:
                    dst_idx = registry.sample(rel.dst, rng, day=day)
                    if dst_idx is None:
                        dst_idx = registry.sample(rel.dst, rng, day=None)
            elif not state.src_unique and state.dst_unique:
                dst_idx = sample_unseen_endpoint(rel.dst, state.dst_to_src)
                if dst_idx is not None:
                    src_idx = registry.sample(rel.src, rng, day=day)
                    if src_idx is None:
                        src_idx = registry.sample(rel.src, rng, day=None)
            else:
                src_idx = sample_unseen_endpoint(rel.src, state.src_to_dst)
                dst_idx = sample_unseen_endpoint(rel.dst, state.dst_to_src)

            if src_idx is None or dst_idx is None:
                continue

            pair = (src_idx, dst_idx)
            if not allows_duplicate_pairs and pair in state.seen_pairs:
                continue
            if state.src_unique and src_idx in state.src_to_dst:
                continue
            if state.dst_unique and dst_idx in state.dst_to_src:
                continue

            if not allows_duplicate_pairs:
                state.seen_pairs.add(pair)
            if state.src_unique:
                state.src_to_dst[src_idx] = dst_idx
            if state.dst_unique:
                state.dst_to_src[dst_idx] = src_idx
            return pair
        return None

    with out_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["from", "to"] + [col for col, _ctype in rel.columns])
        rows_written = 0
        while rows_written < target_rows:
            pair = next_pair()
            if pair is None:
                break
            src_idx, dst_idx = pair
            rel_idx = rows_written + seed
            row = [f"{rel.src}_{src_idx}", f"{rel.dst}_{dst_idx}"]
            for col, ctype in rel.columns:
                row.append(value_for_type(col, ctype, rel_idx, rng))
            writer.writerow(row)
            rows_written += 1
    print(f"[rel-file] table={rel.name} file={out_file.name} rows={rows_written}")
    return rows_written


def compute_daily_budget(
    remaining_node: int,
    remaining_rel: int,
    daily_total: int,
) -> Tuple[int, int]:
    total_remaining = remaining_node + remaining_rel
    if total_remaining <= 0:
        return 0, 0
    today_total = min(daily_total, total_remaining)
    if remaining_node == 0:
        return 0, today_total
    if remaining_rel == 0:
        return today_total, 0
    node_share = today_total * remaining_node / total_remaining
    day_nodes = min(remaining_node, int(round(node_share)))
    day_rels = min(remaining_rel, today_total - day_nodes)

    unused = today_total - day_nodes - day_rels
    if unused > 0:
        take_nodes = min(unused, remaining_node - day_nodes)
        day_nodes += take_nodes
        unused -= take_nodes
    if unused > 0:
        day_rels += min(unused, remaining_rel - day_rels)
    return day_nodes, day_rels


def parse_name_list(raw: str) -> List[str]:
    if not raw.strip():
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def generate_static(
    static_dir: Path,
    node_tables: Dict[str, NodeTable],
    static_tables: List[str],
    static_table_rows: int,
    static_batch_size: int,
    registry: IdRegistry,
    seed: int,
) -> None:
    ensure_dir(static_dir)
    for table_name in static_tables:
        table = node_tables.get(table_name)
        if table is None:
            raise ValueError(f"Static table not found in schema: {table_name}")
        num_files = len(shard_ranges(static_table_rows, static_batch_size))
        print(f"[static-table] table={table_name} rows={static_table_rows} files={num_files}")
        actual_start = registry.add(table_name, 0, static_table_rows, day=None)
        for shard, offset, size in shard_ranges(static_table_rows, static_batch_size):
            out_file = static_dir / f"{table_name}_0_{shard}.csv"
            write_node_shard(out_file, table, actual_start + offset, size, table_name, seed + shard)


def generate_dynamic(
    dynamic_dir: Path,
    node_tables: Dict[str, NodeTable],
    rel_tables: Dict[str, RelTable],
    static_tables: set[str],
    dynamic_node_total: int,
    dynamic_rel_total: int,
    daily_total: int,
    dynamic_batch_size: int,
    registry: IdRegistry,
    seed: int,
) -> Tuple[int, int]:
    ensure_dir(dynamic_dir)

    dynamic_node_names = [name for name in node_tables if name not in static_tables]
    dynamic_rel_names = list(rel_tables.keys())
    rel_states = {name: build_rel_constraint_state(rel.rel_type) for name, rel in rel_tables.items()}

    node_weights = derive_node_weights(node_tables, rel_tables, static_tables)
    rel_weights = derive_rel_weights(rel_tables)

    remaining_node = dynamic_node_total
    remaining_rel = dynamic_rel_total
    node_cursor = {name: registry.count(name, day=None) for name in dynamic_node_names}
    day = 0
    written_nodes = 0
    written_rels = 0

    while remaining_node > 0 or remaining_rel > 0:
        day_nodes, day_rels = compute_daily_budget(remaining_node, remaining_rel, daily_total)
        node_plan = weighted_split(day_nodes, node_weights, dynamic_node_names)
        rel_plan = weighted_split(day_rels, rel_weights, dynamic_rel_names)
        active_node_tables = sum(1 for count in node_plan.values() if count > 0)
        active_rel_tables = sum(1 for count in rel_plan.values() if count > 0)
        print(
            f"[dynamic-day] day={day} node_rows={day_nodes} rel_rows={day_rels} "
            f"node_tables={active_node_tables} rel_tables={active_rel_tables}"
        )

        for table_name in dynamic_node_names:
            count = node_plan.get(table_name, 0)
            if count <= 0:
                continue
            table = node_tables[table_name]
            requested_start = node_cursor[table_name]
            actual_start = registry.add(table_name, requested_start, count, day=day)
            node_cursor[table_name] = actual_start + count
            for shard, offset, size in shard_ranges(count, dynamic_batch_size):
                out_file = dynamic_dir / f"{table_name}_{day}_{shard}.csv"
                write_node_shard(
                    out_file,
                    table,
                    actual_start + offset,
                    size,
                    table_name,
                    seed + 10_000 + day * 1000 + shard,
                )
            written_nodes += count
            remaining_node -= count

        for rel_name in dynamic_rel_names:
            rel = rel_tables[rel_name]
            state = rel_states[rel_name]
            count = rel_plan.get(rel_name, 0)
            if count <= 0:
                continue
            for shard, _offset, size in shard_ranges(count, dynamic_batch_size):
                out_file = dynamic_dir / f"{rel_name}_{day}_{shard}.csv"
                written = write_rel_shard(
                    out_file,
                    rel,
                    size,
                    day,
                    registry,
                    seed + 20_000 + day * 1000 + shard,
                    state,
                )
                written_rels += written
                remaining_rel -= written
        print(
            f"[dynamic-day-done] day={day} remaining_node_rows={remaining_node} "
            f"remaining_rel_rows={remaining_rel}"
        )
        day += 1

        if day_nodes == 0 and day_rels == 0:
            break

    return written_nodes, written_rels


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate generic static/dynamic CSV files from a Ladybug schema.")
    parser.add_argument("--schema", required=True, help="Path to schema cypher file")
    parser.add_argument("--out", default="CsvBasic", help="Output root directory")
    parser.add_argument("--dynamic-node-total", type=int, required=True, help="Total dynamic node rows to generate")
    parser.add_argument("--dynamic-rel-total", type=int, required=True, help="Total dynamic rel rows to generate")
    parser.add_argument("--daily-total", type=int, required=True, help="Maximum total dynamic rows to generate per day")
    parser.add_argument("--dynamic-batch-size", type=int, required=True, help="Rows per dynamic output file")
    parser.add_argument(
        "--static-tables",
        default="",
        help="Comma-separated static node table names, for example User,Teacher,Student",
    )
    parser.add_argument(
        "--static-table-rows",
        type=int,
        default=0,
        help="Rows to generate for each static table",
    )
    parser.add_argument(
        "--static-batch-size",
        type=int,
        default=DEFAULT_STATIC_BATCH_SIZE,
        help="Rows per static output file; default is 100000",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    if args.dynamic_node_total < 0 or args.dynamic_rel_total < 0:
        raise ValueError("--dynamic-node-total and --dynamic-rel-total must be >= 0")
    if args.daily_total <= 0:
        raise ValueError("--daily-total must be > 0")
    if args.dynamic_batch_size <= 0:
        raise ValueError("--dynamic-batch-size must be > 0")
    if args.static_table_rows < 0:
        raise ValueError("--static-table-rows must be >= 0")
    if args.static_batch_size <= 0:
        raise ValueError("--static-batch-size must be > 0")

    random.seed(args.seed)

    schema_path = Path(args.schema)
    out_root = Path(args.out)
    static_dir = out_root / "static"
    dynamic_dir = out_root / "dynamic"
    static_tables = parse_name_list(args.static_tables)
    static_table_set = set(static_tables)

    node_tables, rel_tables = parse_schema(schema_path)
    registry = IdRegistry()

    ensure_dir(out_root)
    ensure_dir(static_dir)
    ensure_dir(dynamic_dir)
    print(
        f"[start] schema={schema_path} out={out_root} dynamic_node_total={args.dynamic_node_total} "
        f"dynamic_rel_total={args.dynamic_rel_total} daily_total={args.daily_total} "
        f"dynamic_batch_size={args.dynamic_batch_size} static_tables={','.join(static_tables) or '-'} "
        f"static_table_rows={args.static_table_rows} static_batch_size={args.static_batch_size}"
    )

    if static_tables and args.static_table_rows > 0:
        generate_static(
            static_dir=static_dir,
            node_tables=node_tables,
            static_tables=static_tables,
            static_table_rows=args.static_table_rows,
            static_batch_size=args.static_batch_size,
            registry=registry,
            seed=args.seed,
        )

    written_nodes, written_rels = generate_dynamic(
        dynamic_dir=dynamic_dir,
        node_tables=node_tables,
        rel_tables=rel_tables,
        static_tables=static_table_set,
        dynamic_node_total=args.dynamic_node_total,
        dynamic_rel_total=args.dynamic_rel_total,
        daily_total=args.daily_total,
        dynamic_batch_size=args.dynamic_batch_size,
        registry=registry,
        seed=args.seed,
    )

    print(f"Done. dynamic_node_rows={written_nodes} dynamic_rel_rows={written_rels}")
    print(f"Output static: {static_dir}")
    print(f"Output dynamic: {dynamic_dir}")


if __name__ == "__main__":
    main()
