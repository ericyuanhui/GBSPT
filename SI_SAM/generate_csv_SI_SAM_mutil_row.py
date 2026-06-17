#!/usr/bin/env python3
import argparse
import bisect
import csv
import datetime as dt
import math
import os
import random
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple


NODE_RE = re.compile(r"^CREATE NODE TABLE `([^`]+)` \((.*)\);$")
REL_RE = re.compile(r"^CREATE REL TABLE `([^`]+)` \(FROM `([^`]+)` TO `([^`]+)`,\s*([^\)]+)\);$")
COL_RE = re.compile(r"`([^`]+)`\s+([A-Z0-9]+)")

# 人员/组织相关节点固定为原始 CSV 数量，不参与 dynamic 按比例放大。
FIXED_STATIC_NODES = {
    "User",
    "Group",
    "Teacher",
    "Student",
    "Post",
    "Label",
}

# 每天 dynamic 批次固定 1000w 行，其中边是点的 3 倍。
DYNAMIC_DAILY_TOTAL = 10_000_000
DYNAMIC_DAILY_NODE = DYNAMIC_DAILY_TOTAL // 4
DYNAMIC_DAILY_REL = DYNAMIC_DAILY_TOTAL - DYNAMIC_DAILY_NODE


@dataclass
class NodeTable:
    name: str
    columns: List[Tuple[str, str]]  # (name, type)


@dataclass
class RelTable:
    name: str
    src: str
    dst: str
    rel_type: str


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
            self._all_cache[table] = (
                all_pools + (new_pool,),
                all_prefix + (all_total + count,),
            )
            if day is not None:
                day_key = f"{table}@{day}"
                day_pools, day_prefix = self._today_cache.get(day_key, ((), ()))
                day_total = day_prefix[-1] if day_prefix else 0
                self._today_cache[day_key] = (
                    day_pools + (new_pool,),
                    day_prefix + (day_total + count,),
                )
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


class ProgressReporter:
    def __init__(self, total_files: int, interval_sec: int = 10) -> None:
        self.total_files = total_files
        self.interval_sec = max(1, interval_sec)
        self.completed_files = 0
        self._recent_done: Deque[str] = deque(maxlen=5)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._start_ts = time.time()

    def start(self) -> None:
        self._thread.start()

    def mark_done(self, file_path: Path) -> None:
        with self._lock:
            self.completed_files += 1
            self._recent_done.append(file_path.as_posix())

    def elapsed_sec(self) -> float:
        return time.time() - self._start_ts

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=1)
        self.print_snapshot(prefix="progress-final")

    def print_snapshot(self, prefix: str = "progress") -> None:
        with self._lock:
            done = self.completed_files
            total = self.total_files
            remaining = max(0, total - done)
            recent = list(self._recent_done)
        elapsed = self.elapsed_sec()
        recent_text = ", ".join(recent) if recent else "-"
        print(
            f"[{prefix}] elapsed={elapsed:.1f}s done={done}/{total} remaining={remaining} recent=[{recent_text}]"
        )

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_sec):
            self.print_snapshot()


def get_total_memory_bytes() -> int:
    # Linux: 优先使用 sysconf 获取物理内存。
    if hasattr(os, "sysconf"):
        page_size = os.sysconf("SC_PAGE_SIZE")
        phys_pages = os.sysconf("SC_PHYS_PAGES")
        if isinstance(page_size, int) and isinstance(phys_pages, int) and page_size > 0 and phys_pages > 0:
            return int(page_size * phys_pages)
    return 8 * 1024**3


def parse_schema(schema_path: Path) -> Tuple[Dict[str, NodeTable], Dict[str, RelTable]]:
    nodes: Dict[str, NodeTable] = {}
    rels: Dict[str, RelTable] = {}

    with schema_path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            m_node = NODE_RE.match(line)
            if m_node:
                name = m_node.group(1)
                body = m_node.group(2)
                cols: List[Tuple[str, str]] = []
                for cm in COL_RE.finditer(body):
                    col, ctype = cm.group(1), cm.group(2)
                    if col == "PRIMARY":
                        continue
                    cols.append((col, ctype))
                nodes[name] = NodeTable(name=name, columns=cols)
                continue

            m_rel = REL_RE.match(line)
            if m_rel:
                name, src, dst, rtype = m_rel.group(1), m_rel.group(2), m_rel.group(3), m_rel.group(4)
                rels[name] = RelTable(name=name, src=src, dst=dst, rel_type=rtype)

    return nodes, rels


def read_counts(csv_path: Path) -> Dict[str, Tuple[str, int]]:
    counts: Dict[str, Tuple[str, int]] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("ladybug表名") or "").strip().strip('"')
            t = (row.get("表类型") or "").strip().strip('"')
            c = (row.get("行数") or "0").strip().strip('"')
            if not name:
                continue
            try:
                counts[name] = (t, int(c))
            except ValueError:
                counts[name] = (t, 0)
    return counts


def weighted_split(target: int, weights: Dict[str, int], keys: List[str]) -> Dict[str, int]:
    if target <= 0 or not keys:
        return {k: 0 for k in keys}

    pos_sum = sum(max(weights.get(k, 0), 0) for k in keys)
    if pos_sum <= 0:
        base = target // len(keys)
        res = {k: base for k in keys}
        for i in range(target - base * len(keys)):
            res[keys[i % len(keys)]] += 1
        return res

    raw = {k: target * max(weights.get(k, 0), 0) / pos_sum for k in keys}
    floored = {k: int(math.floor(v)) for k, v in raw.items()}
    remain = target - sum(floored.values())
    if remain > 0:
        frac = sorted(keys, key=lambda k: raw[k] - floored[k], reverse=True)
        for i in range(remain):
            floored[frac[i % len(frac)]] += 1
    return floored


def value_for_type(col: str, ctype: str, idx: int, rng: random.Random) -> str:
    if col == "id":
        return str(idx)
    if ctype == "INT64":
        return str(idx)
    if ctype == "DOUBLE":
        return f"{(idx % 1000) + rng.random():.4f}"
    if ctype == "BOOL":
        return "true" if (idx % 2 == 0) else "false"
    if ctype == "DATE":
        d = dt.date(2026, 1, 1) + dt.timedelta(days=(idx % 365))
        return d.isoformat()
    return f"{col}_{idx}"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def build_rel_constraint_state(rel_type: str) -> RelConstraintState:
    t = rel_type.strip().upper()
    if t == "MANY_ONE":
        return RelConstraintState(src_unique=True, dst_unique=False)
    if t == "ONE_MANY":
        return RelConstraintState(src_unique=False, dst_unique=True)
    if t == "ONE_ONE":
        return RelConstraintState(src_unique=True, dst_unique=True)
    return RelConstraintState(src_unique=False, dst_unique=False)


def estimate_total_output_files(
    node_tables: Dict[str, NodeTable],
    rel_tables: Dict[str, RelTable],
    counts: Dict[str, Tuple[str, int]],
    batch_size: int,
    target_node_total: int,
    target_rel_total: int,
) -> int:
    total = 0

    for t in sorted(FIXED_STATIC_NODES):
        if t not in node_tables:
            continue
        c = counts.get(t, ("NODE", 0))[1]
        total += len(shard_ranges(c, batch_size))

    for _rn, rt in rel_tables.items():
        if rt.src not in FIXED_STATIC_NODES or rt.dst not in FIXED_STATIC_NODES:
            continue
        c = counts.get(rt.name, ("REL", 0))[1]
        total += len(shard_ranges(c, batch_size))

    dynamic_node_names = [n for n in node_tables.keys() if n not in FIXED_STATIC_NODES]
    dynamic_rel_names = [n for n in rel_tables.keys()]
    node_weights = {n: counts.get(n, ("NODE", 0))[1] for n in dynamic_node_names}
    rel_weights = {n: counts.get(n, ("REL", 0))[1] for n in dynamic_rel_names}

    generated_node = 0
    generated_rel = 0
    while generated_node < target_node_total or generated_rel < target_rel_total:
        remain_node = max(0, target_node_total - generated_node)
        remain_rel = max(0, target_rel_total - generated_rel)
        if remain_node == 0 and remain_rel == 0:
            break

        day_nodes = min(DYNAMIC_DAILY_NODE, remain_node)
        day_rels = min(DYNAMIC_DAILY_REL, remain_rel)
        node_plan = weighted_split(day_nodes, node_weights, dynamic_node_names)
        rel_plan = weighted_split(day_rels, rel_weights, dynamic_rel_names)

        for n in dynamic_node_names:
            total += len(shard_ranges(node_plan.get(n, 0), batch_size))
        for r in dynamic_rel_names:
            total += len(shard_ranges(rel_plan.get(r, 0), batch_size))

        generated_node += day_nodes
        generated_rel += day_rels

    return total


def estimate_writer_chunk_rows(rows_in_memory: int, batch_size: int, workers: int) -> int:
    # 将可用行预算按并发 writer 均摊，并限制上限避免单次缓冲过大。
    target = rows_in_memory // max(1, workers * 8)
    return max(1000, min(batch_size, 50_000, target if target > 0 else 1000))


def shard_ranges(total: int, batch_size: int) -> List[Tuple[int, int, int]]:
    if total <= 0:
        return []
    out = []
    shard = 0
    start = 0
    while start < total:
        size = min(batch_size, total - start)
        out.append((shard, start, size))
        start += size
        shard += 1
    return out


def write_node_shard(
    out_file: Path,
    table: NodeTable,
    global_start: int,
    count: int,
    table_name: str,
    seed: int,
    chunk_rows: int,
    progress: Optional[ProgressReporter] = None,
) -> None:
    rng = random.Random(seed)
    with out_file.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([c for c, _ in table.columns])
        buffered_rows: List[List[str]] = []
        for i in range(count):
            gidx = global_start + i
            row = []
            for col, ctype in table.columns:
                if col == "id":
                    row.append(f"{table_name}_{gidx}")
                else:
                    row.append(value_for_type(col, ctype, gidx, rng))
            buffered_rows.append(row)
            if len(buffered_rows) >= chunk_rows:
                w.writerows(buffered_rows)
                buffered_rows.clear()
        if buffered_rows:
            w.writerows(buffered_rows)
    if progress is not None:
        progress.mark_done(out_file)


def write_rel_shard(
    out_file: Path,
    rel: RelTable,
    count: int,
    day: int,
    registry: IdRegistry,
    seed: int,
    state: RelConstraintState,
    chunk_rows: int,
    progress: Optional[ProgressReporter] = None,
) -> None:
    rng = random.Random(seed)
    allows_duplicate_pairs = not state.src_unique and not state.dst_unique
    src_count = registry.count(rel.src, day=day)
    dst_count = registry.count(rel.dst, day=day)
    target_rows = count
    if not allows_duplicate_pairs:
        max_unique_rows = src_count * dst_count
        if state.src_unique:
            max_unique_rows = min(max_unique_rows, src_count)
        if state.dst_unique:
            max_unique_rows = min(max_unique_rows, dst_count)
        target_rows = min(count, max_unique_rows - len(state.seen_pairs))

    def sample_unseen_endpoint(table: str, used: Dict[int, int], max_tries: int = 256) -> Optional[int]:
        if len(used) >= registry.count(table, day=day):
            return None
        for _ in range(max_tries):
            idx = registry.sample(table, rng, day=day)
            if idx is not None and idx not in used:
                return idx
        return None

    def next_unique_pair(max_tries: int = 512) -> Optional[Tuple[int, int]]:
        for _ in range(max_tries):
            src_idx: Optional[int] = None
            dst_idx: Optional[int] = None

            # MANY_MANY: 双端随机采样，但过滤重复 pair。
            if not state.src_unique and not state.dst_unique:
                src_idx = registry.sample(rel.src, rng, day=day)
                dst_idx = registry.sample(rel.dst, rng, day=day)

            # MANY_ONE: 每个 src 仅建立一次映射，避免重复 pair。
            elif state.src_unique and not state.dst_unique:
                src_idx = sample_unseen_endpoint(rel.src, state.src_to_dst)
                if src_idx is not None:
                    dst_idx = registry.sample(rel.dst, rng, day=day)

            # ONE_MANY: 每个 dst 仅建立一次映射，避免重复 pair。
            elif not state.src_unique and state.dst_unique:
                dst_idx = sample_unseen_endpoint(rel.dst, state.dst_to_src)
                if dst_idx is not None:
                    src_idx = registry.sample(rel.src, rng, day=day)

            # ONE_ONE: 两端都必须是未使用过的端点。
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
        w = csv.writer(f)
        w.writerow(["from", "to"])
        buffered_rows: List[List[str]] = []
        rows_written = 0
        while rows_written < target_rows:
            pair = next_unique_pair()
            if pair is None:
                if target_rows < count:
                    print(
                        f"[warn] rel={rel.name} requested_rows={count} written_rows={rows_written} "
                        f"reason=unique_pair_space_exhausted"
                    )
                elif rows_written < count:
                    print(
                        f"[warn] rel={rel.name} requested_rows={count} written_rows={rows_written} "
                        f"reason=unable_to_sample_more_unique_pairs"
                    )
                break
            src_idx, dst_idx = pair
            buffered_rows.append([f"{rel.src}_{src_idx}", f"{rel.dst}_{dst_idx}"])
            rows_written += 1
            if len(buffered_rows) >= chunk_rows:
                w.writerows(buffered_rows)
                buffered_rows.clear()
        if buffered_rows:
            w.writerows(buffered_rows)
    if progress is not None:
        progress.mark_done(out_file)


def write_rel_table_shards_with_batch(
    out_dir: Path,
    rel_name: str,
    rel: RelTable,
    count: int,
    day: int,
    batch_size: int,
    registry: IdRegistry,
    seed_base: int,
    state: RelConstraintState,
    chunk_rows: int,
    progress: Optional[ProgressReporter] = None,
) -> None:
    for shard, _offset, size in shard_ranges(count, batch_size):
        out = out_dir / f"{rel_name}_{day}_{shard}.csv"
        write_rel_shard(
            out_file=out,
            rel=rel,
            count=size,
            day=day,
            registry=registry,
            seed=seed_base + shard,
            state=state,
            chunk_rows=chunk_rows,
            progress=progress,
        )


def generate_static(
    static_dir: Path,
    node_tables: Dict[str, NodeTable],
    rel_tables: Dict[str, RelTable],
    rel_states: Dict[str, RelConstraintState],
    counts: Dict[str, Tuple[str, int]],
    batch_size: int,
    workers: int,
    registry: IdRegistry,
    chunk_rows: int,
    progress: Optional[ProgressReporter] = None,
) -> None:
    ensure_dir(static_dir)

    futures = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # 固定节点表：行数与 ladybug_table_counts.csv 保持一致。
        for t in sorted(FIXED_STATIC_NODES):
            nt = node_tables.get(t)
            if nt is None:
                continue
            c = counts.get(t, ("NODE", 0))[1]
            static_start = registry.add(t, 0, c, day=None)
            for shard, start, size in shard_ranges(c, batch_size):
                out = static_dir / f"{t}_0_{shard}.csv"
                futures.append(
                    pool.submit(
                        write_node_shard,
                        out,
                        nt,
                        static_start + start,
                        size,
                        t,
                        1000 + shard,
                        chunk_rows,
                        progress,
                    )
                )

        # 两端都在固定节点集合的关系表，同样保持原始行数，输出到 static。
        for rn, rt in rel_tables.items():
            if rt.src not in FIXED_STATIC_NODES or rt.dst not in FIXED_STATIC_NODES:
                continue
            c = counts.get(rn, ("REL", 0))[1]
            state = rel_states[rn]
            if c <= 0:
                continue
            futures.append(
                pool.submit(
                    write_rel_table_shards_with_batch,
                    static_dir,
                    rn,
                    rt,
                    c,
                    0,
                    batch_size,
                    registry,
                    2000,
                    state,
                    chunk_rows,
                    progress,
                )
            )

        for fu in as_completed(futures):
            fu.result()


def generate_dynamic(
    dynamic_dir: Path,
    node_tables: Dict[str, NodeTable],
    rel_tables: Dict[str, RelTable],
    rel_states: Dict[str, RelConstraintState],
    counts: Dict[str, Tuple[str, int]],
    batch_size: int,
    workers: int,
    registry: IdRegistry,
    target_node_total: int,
    target_rel_total: int,
    rows_in_memory: int,
    chunk_rows: int,
    progress: Optional[ProgressReporter] = None,
) -> None:
    ensure_dir(dynamic_dir)

    dynamic_node_names = [n for n in node_tables.keys() if n not in FIXED_STATIC_NODES]
    dynamic_rel_names = [n for n in rel_tables.keys()]

    node_weights = {n: counts.get(n, ("NODE", 0))[1] for n in dynamic_node_names}
    rel_weights = {n: counts.get(n, ("REL", 0))[1] for n in dynamic_rel_names}

    generated_node = 0
    generated_rel = 0
    day = 0
    global_node_cursor: Dict[str, int] = {n: 0 for n in dynamic_node_names}

    while generated_node < target_node_total or generated_rel < target_rel_total:
        remain_node = max(0, target_node_total - generated_node)
        remain_rel = max(0, target_rel_total - generated_rel)
        if remain_node == 0 and remain_rel == 0:
            break

        day_nodes = min(DYNAMIC_DAILY_NODE, remain_node)
        day_rels = min(DYNAMIC_DAILY_REL, remain_rel)

        node_plan = weighted_split(day_nodes, node_weights, dynamic_node_names)
        rel_plan = weighted_split(day_rels, rel_weights, dynamic_rel_names)

        # 内存预算用于限制一次提交过多大分片任务（粗粒度控制）。
        max_pending_rows = max(rows_in_memory, batch_size)
        pending_rows = 0
        pending = []

        with ThreadPoolExecutor(max_workers=workers) as pool:
            # 先生成节点，再为关系提供可引用的 ID 范围。
            for n in dynamic_node_names:
                count = node_plan.get(n, 0)
                if count <= 0:
                    continue
                nt = node_tables[n]
                requested_start = global_node_cursor[n]
                actual_start = registry.add(n, requested_start, count, day=day)
                global_node_cursor[n] = actual_start + count

                for shard, offset, size in shard_ranges(count, batch_size):
                    out = dynamic_dir / f"{n}_{day}_{shard}.csv"
                    pending_rows += size
                    pending.append(
                        pool.submit(
                            write_node_shard,
                            out,
                            nt,
                            actual_start + offset,
                            size,
                            n,
                            10_000 + day * 1000 + shard,
                            chunk_rows,
                            progress,
                        )
                    )
                    if pending_rows >= max_pending_rows:
                        for fu in as_completed(pending):
                            fu.result()
                        pending.clear()
                        pending_rows = 0

            if pending:
                for fu in as_completed(pending):
                    fu.result()
                pending.clear()

            # 关系生成，保证方向性与端点类型符合 schema。
            rel_futures = []
            for r in dynamic_rel_names:
                rt = rel_tables[r]
                state = rel_states[r]
                count = rel_plan.get(r, 0)
                if count <= 0:
                    continue

                rel_futures.append(
                    pool.submit(
                        write_rel_table_shards_with_batch,
                        dynamic_dir,
                        r,
                        rt,
                        count,
                        day,
                        batch_size,
                        registry,
                        20_000 + day * 1000,
                        state,
                        chunk_rows,
                        progress,
                    )
                )

            for fu in as_completed(rel_futures):
                fu.result()

        generated_node += day_nodes
        generated_rel += day_rels
        day += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate CsvBasic static/dynamic CSV files from schema and count ratios.")
    parser.add_argument("--schema", default="SI_SAM_schema.cypher", help="Path to schema cypher file")
    parser.add_argument("--counts", default="ladybug_table_counts.csv", help="Path to table count CSV")
    parser.add_argument("--out", default="CsvBasic", help="Output root directory")
    parser.add_argument("--batch-size", type=int, required=True, help="Rows per output shard file")
    parser.add_argument("--node-total", type=int, required=True, help="Target generated dynamic node rows")
    parser.add_argument("--rel-total", type=int, required=True, help="Target generated dynamic rel rows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--memory-limit-gb",
        type=float,
        default=0.0,
        help="Optional hard memory budget cap in GB (0 means use 70%% of detected RAM)",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError("--batch-size must be > 0")
    if args.node_total < 0 or args.rel_total < 0:
        raise ValueError("--node-total and --rel-total must be >= 0")
    if args.memory_limit_gb < 0:
        raise ValueError("--memory-limit-gb must be >= 0")

    random.seed(args.seed)

    schema_path = Path(args.schema)
    counts_path = Path(args.counts)
    out_root = Path(args.out)
    static_dir = out_root / "static"
    dynamic_dir = out_root / "dynamic"

    nodes, rels = parse_schema(schema_path)
    rel_states = {name: build_rel_constraint_state(rt.rel_type) for name, rt in rels.items()}
    counts = read_counts(counts_path)

    cpu_total = os.cpu_count() or 4
    workers = max(1, int(cpu_total * 0.8))

    total_mem = get_total_memory_bytes()
    mem_budget = int(total_mem * 0.7)
    if args.memory_limit_gb > 0:
        user_cap = int(args.memory_limit_gb * 1024**3)
        mem_budget = min(mem_budget, user_cap)
    # 估算平均 512 字节/行来限制并发任务累计行数，避免内存过冲。
    rows_in_memory = max(args.batch_size, mem_budget // 512)
    chunk_rows = estimate_writer_chunk_rows(rows_in_memory, args.batch_size, workers)

    registry = IdRegistry()

    ensure_dir(out_root)
    ensure_dir(static_dir)
    ensure_dir(dynamic_dir)

    total_files = estimate_total_output_files(
        node_tables=nodes,
        rel_tables=rels,
        counts=counts,
        batch_size=args.batch_size,
        target_node_total=args.node_total,
        target_rel_total=args.rel_total,
    )
    progress = ProgressReporter(total_files=total_files, interval_sec=10)
    progress.start()

    try:
        generate_static(
            static_dir=static_dir,
            node_tables=nodes,
            rel_tables=rels,
            rel_states=rel_states,
            counts=counts,
            batch_size=args.batch_size,
            workers=workers,
            registry=registry,
            chunk_rows=chunk_rows,
            progress=progress,
        )

        generate_dynamic(
            dynamic_dir=dynamic_dir,
            node_tables=nodes,
            rel_tables=rels,
            rel_states=rel_states,
            counts=counts,
            batch_size=args.batch_size,
            workers=workers,
            registry=registry,
            target_node_total=args.node_total,
            target_rel_total=args.rel_total,
            rows_in_memory=rows_in_memory,
            chunk_rows=chunk_rows,
            progress=progress,
        )
    finally:
        progress.stop()

    print(f"Done. workers={workers} (~80% CPU), mem_budget={mem_budget} bytes, chunk_rows={chunk_rows}")
    print(f"Output static: {static_dir}")
    print(f"Output dynamic: {dynamic_dir}")


if __name__ == "__main__":
    main()
