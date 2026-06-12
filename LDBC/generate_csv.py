#!/usr/bin/env python3
"""Generate LDBC-style CSV files from node/rel totals with sharding and concurrency.

This script infers table distributions from existing CsvBasic data, then scales up to
the requested total nodes and relationships.
"""

from __future__ import annotations

import argparse
import csv
import math
import multiprocessing as mp
import os
import random
import re
import string
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE = BASE_DIR / "CsvBasic"


NODE_HEADERS: Dict[str, List[str]] = {
    "Comment": ["id", "creationDate", "locationIP", "browserUsed", "content", "length"],
    "Forum": ["id", "title", "creationDate"],
    "Organisation": ["id", "type", "name", "url"],
    "Person": [
        "id",
        "firstName",
        "lastName",
        "gender",
        "birthday",
        "creationDate",
        "locationIP",
        "browserUsed",
    ],
    "Place": ["id", "name", "url", "type"],
    "Post": [
        "id",
        "imageFile",
        "creationDate",
        "locationIP",
        "browserUsed",
        "language",
        "content",
        "length",
    ],
    "Tag": ["id", "name", "url"],
    "TagClass": ["id", "name", "url"],
}


REL_HEADERS: Dict[str, List[str]] = {
    "Comment_hasCreator": ["Comment.id", "Person.id"],
    "Comment_hasTag": ["Comment.id", "Tag.id"],
    "Comment_isLocatedIn": ["Comment.id", "Place.id"],
    "replyOf_Comment": ["Comment.id", "Comment.id"],
    "replyOf_Post": ["Comment.id", "Post.id"],
    "containerOf": ["Forum.id", "Post.id"],
    "hasMember": ["Forum.id", "Person.id", "joinDate"],
    "hasModerator": ["Forum.id", "Person.id"],
    "Forum_hasTag": ["Forum.id", "Tag.id"],
    "Organisation_isLocatedIn": ["Organisation.id", "Place.id"],
    "hasInterest": ["Person.id", "Tag.id"],
    "Person_isLocatedIn": ["Person.id", "Place.id"],
    "knows": ["Person.id", "Person.id", "creationDate"],
    "likes_Comment": ["Person.id", "Comment.id", "creationDate"],
    "likes_Post": ["Person.id", "Post.id", "creationDate"],
    "studyAt": ["Person.id", "Organisation.id", "classYear"],
    "workAt": ["Person.id", "Organisation.id", "workFrom"],
    "isPartOf": ["Place.id", "Place.id"],
    "Post_hasCreator": ["Post.id", "Person.id"],
    "Post_hasTag": ["Post.id", "Tag.id"],
    "Post_isLocatedIn": ["Post.id", "Place.id"],
    "hasType": ["Tag.id", "TagClass.id"],
    "isSubclassOf": ["TagClass.id", "TagClass.id"],
}


COPY_NODE_TO_FILE: Dict[str, str] = {
    "Comment": "dynamic/comment_0_0.csv",
    "Forum": "dynamic/forum_0_0.csv",
    "Organisation": "static/organisation_0_0.csv",
    "Person": "dynamic/person_0_0.csv",
    "Place": "static/place_0_0.csv",
    "Post": "dynamic/post_0_0.csv",
    "Tag": "static/tag_0_0.csv",
    "TagClass": "static/tagclass_0_0.csv",
}


COPY_REL_TO_FILE: Dict[str, str] = {
    "Comment_hasCreator": "dynamic/comment_hasCreator_person_0_0.csv",
    "Comment_hasTag": "dynamic/comment_hasTag_tag_0_0.csv",
    "Comment_isLocatedIn": "dynamic/comment_isLocatedIn_place_0_0.csv",
    "replyOf_Comment": "dynamic/comment_replyOf_comment_0_0.csv",
    "replyOf_Post": "dynamic/comment_replyOf_post_0_0.csv",
    "containerOf": "dynamic/forum_containerOf_post_0_0.csv",
    "hasMember": "dynamic/forum_hasMember_person_0_0.csv",
    "hasModerator": "dynamic/forum_hasModerator_person_0_0.csv",
    "Forum_hasTag": "dynamic/forum_hasTag_tag_0_0.csv",
    "Organisation_isLocatedIn": "static/organisation_isLocatedIn_place_0_0.csv",
    "hasInterest": "dynamic/person_hasInterest_tag_0_0.csv",
    "Person_isLocatedIn": "dynamic/person_isLocatedIn_place_0_0.csv",
    "knows": "dynamic/person_knows_person_0_0.csv",
    "likes_Comment": "dynamic/person_likes_comment_0_0.csv",
    "likes_Post": "dynamic/person_likes_post_0_0.csv",
    "studyAt": "dynamic/person_studyAt_organisation_0_0.csv",
    "workAt": "dynamic/person_workAt_organisation_0_0.csv",
    "isPartOf": "static/place_isPartOf_place_0_0.csv",
    "Post_hasCreator": "dynamic/post_hasCreator_person_0_0.csv",
    "Post_hasTag": "dynamic/post_hasTag_tag_0_0.csv",
    "Post_isLocatedIn": "dynamic/post_isLocatedIn_place_0_0.csv",
    "hasType": "static/tag_hasType_tagclass_0_0.csv",
    "isSubclassOf": "static/tagclass_isSubclassOf_tagclass_0_0.csv",
}


NODE_MIN_BASE = {
    "Comment": 1,
    "Forum": 1,
    "Organisation": 1,
    "Person": 1,
    "Place": 2,
    "Post": 1,
    "Tag": 2,
    "TagClass": 2,
}


@dataclass
class TableTask:
    kind: str
    table: str
    file_path: str
    header: List[str]
    count: int
    shard_rows: int
    id_start: int
    id_ranges: Dict[str, Tuple[int, int]]
    seed: int
    extra_of: str = ""


def split_sql_statements(sql_file: Path) -> List[str]:
    text = sql_file.read_text(encoding="utf-8")
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        if "--" in line:
            line = line.split("--", 1)[0]
        lines.append(line)
    return [x.strip() for x in "\n".join(lines).split(";") if x.strip()]


def parse_schema(schema_file: Path) -> Tuple[List[str], List[str]]:
    nodes: List[str] = []
    rels: List[str] = []
    for stmt in split_sql_statements(schema_file):
        n = re.match(r"^create\s+node\s+table\s+([A-Za-z_][A-Za-z0-9_]*)", stmt, re.I)
        if n:
            nodes.append(n.group(1))
            continue
        r = re.match(r"^create\s+rel\s+table\s+([A-Za-z_][A-Za-z0-9_]*)", stmt, re.I)
        if r:
            rels.append(r.group(1))
    return nodes, rels


def parse_copy(copy_file: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for stmt in split_sql_statements(copy_file):
        m = re.match(
            r"^copy\s+([A-Za-z_][A-Za-z0-9_]*)\s+from\s+['\"]([^'\"]+)['\"]",
            stmt,
            re.I,
        )
        if m:
            mapping[m.group(1)] = m.group(2)
    return mapping


def count_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return max(0, sum(1 for _ in f) - 1)


def allocate_by_ratio(total: int, weights: Dict[str, int], minimums: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    keys = sorted(weights.keys())
    minimums = minimums or {}
    out = {k: minimums.get(k, 0) for k in keys}
    base_sum = sum(out.values())
    if total <= base_sum:
        if total < base_sum:
            # Keep mandatory uniqueness constraints if requested total is too small.
            pass
        return out

    remain = total - base_sum
    total_w = sum(weights[k] for k in keys)
    if total_w == 0:
        step = remain // max(1, len(keys))
        for k in keys:
            out[k] += step
        for k in keys[: remain % max(1, len(keys))]:
            out[k] += 1
        return out

    frac: List[Tuple[float, str]] = []
    assigned = 0
    for k in keys:
        exact = remain * (weights[k] / total_w)
        whole = int(math.floor(exact))
        out[k] += whole
        assigned += whole
        frac.append((exact - whole, k))
    frac.sort(reverse=True)
    left = remain - assigned
    for i in range(left):
        out[frac[i][1]] += 1
    return out


def derive_reference_distribution(csv_root: Path, node_tables: List[str], rel_tables: List[str],
    copy_map: Dict[str, str]) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_weights: Dict[str, int] = {}
    rel_weights: Dict[str, int] = {}

    for t in node_tables:
        rel_path = copy_map.get(t, COPY_NODE_TO_FILE.get(t))
        if not rel_path:
            continue
        p = csv_root / rel_path
        node_weights[t] = count_rows(p) if p.exists() else 0

    for t in rel_tables:
        rel_path = copy_map.get(t, COPY_REL_TO_FILE.get(t))
        if not rel_path:
            continue
        p = csv_root / rel_path
        rel_weights[t] = count_rows(p) if p.exists() else 0

    return node_weights, rel_weights


def build_node_ranges(node_counts: Dict[str, int]) -> Dict[str, Tuple[int, int]]:
    ranges: Dict[str, Tuple[int, int]] = {}
    cur = 1
    for t in sorted(node_counts.keys()):
        n = node_counts[t]
        ranges[t] = (cur, cur + n - 1)
        cur += n
    return ranges


def to_shard_name(base_rel_path: str, shard_idx: int) -> str:
    p = Path(base_rel_path)
    stem = p.stem
    stem = re.sub(r"_0_0$", f"_0_{shard_idx}", stem)
    return str(p.with_name(stem + p.suffix))


def safe_choice(rng: random.Random, lo: int, hi: int) -> int:
    if hi < lo:
        return lo
    return rng.randint(lo, hi)


def fake_date(rng: random.Random) -> str:
    y = rng.randint(2008, 2025)
    m = rng.randint(1, 12)
    d = rng.randint(1, 28)
    hh = rng.randint(0, 23)
    mm = rng.randint(0, 59)
    ss = rng.randint(0, 59)
    return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mm:02d}:{ss:02d}.000+0000"


def fake_ipv4(rng: random.Random) -> str:
    return f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"


def fake_text(rng: random.Random, min_len: int = 12, max_len: int = 80) -> str:
    n = rng.randint(min_len, max_len)
    chars = string.ascii_letters + string.digits + " "
    return "".join(rng.choice(chars) for _ in range(n)).strip() or "x"


def random_node_id(rng: random.Random, node_type: str, id_ranges: Dict[str, Tuple[int, int]]) -> int:
    lo, hi = id_ranges[node_type]
    return safe_choice(rng, lo, hi)


def node_row(table: str, row_id: int, rng: random.Random) -> List[str]:
    if table == "Comment":
        content = fake_text(rng, 8, 128)
        return [str(row_id), fake_date(rng), fake_ipv4(rng), "Firefox", content, str(len(content))]
    if table == "Forum":
        return [str(row_id), f"Forum_{row_id}", fake_date(rng)]
    if table == "Organisation":
        typ = "university" if row_id % 2 == 0 else "company"
        return [str(row_id), typ, f"Org_{row_id}", f"http://org/{row_id}"]
    if table == "Person":
        gender = "male" if row_id % 2 == 0 else "female"
        return [
            str(row_id),
            f"First{row_id % 100000}",
            f"Last{(row_id * 7) % 100000}",
            gender,
            f"{rng.randint(1950,2008):04d}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
            fake_date(rng),
            fake_ipv4(rng),
            "Chrome",
        ]
    if table == "Place":
        typ = ["city", "country", "continent"][row_id % 3]
        return [str(row_id), f"Place_{row_id}", f"http://place/{row_id}", typ]
    if table == "Post":
        content = fake_text(rng, 16, 256)
        return [
            str(row_id),
            f"img_{row_id}.jpg",
            fake_date(rng),
            fake_ipv4(rng),
            "Safari",
            ["en", "zh", "de", "fr"][row_id % 4],
            content,
            str(len(content)),
        ]
    if table == "Tag":
        return [str(row_id), f"Tag_{row_id}", f"http://tag/{row_id}"]
    if table == "TagClass":
        return [str(row_id), f"TagClass_{row_id}", f"http://tagclass/{row_id}"]
    raise ValueError(f"Unsupported node table: {table}")


def rel_row(table: str, rng: random.Random, id_ranges: Dict[str, Tuple[int, int]]) -> List[str]:
    if table == "Comment_hasCreator":
        return [str(random_node_id(rng, "Comment", id_ranges)), str(random_node_id(rng, "Person", id_ranges))]
    if table == "Comment_hasTag":
        return [str(random_node_id(rng, "Comment", id_ranges)), str(random_node_id(rng, "Tag", id_ranges))]
    if table == "Comment_isLocatedIn":
        return [str(random_node_id(rng, "Comment", id_ranges)), str(random_node_id(rng, "Place", id_ranges))]
    if table == "replyOf_Comment":
        return [str(random_node_id(rng, "Comment", id_ranges)), str(random_node_id(rng, "Comment", id_ranges))]
    if table == "replyOf_Post":
        return [str(random_node_id(rng, "Comment", id_ranges)), str(random_node_id(rng, "Post", id_ranges))]
    if table == "containerOf":
        return [str(random_node_id(rng, "Forum", id_ranges)), str(random_node_id(rng, "Post", id_ranges))]
    if table == "hasMember":
        return [str(random_node_id(rng, "Forum", id_ranges)), str(random_node_id(rng, "Person", id_ranges)), fake_date(rng)]
    if table == "hasModerator":
        return [str(random_node_id(rng, "Forum", id_ranges)), str(random_node_id(rng, "Person", id_ranges))]
    if table == "Forum_hasTag":
        return [str(random_node_id(rng, "Forum", id_ranges)), str(random_node_id(rng, "Tag", id_ranges))]
    if table == "Organisation_isLocatedIn":
        return [str(random_node_id(rng, "Organisation", id_ranges)), str(random_node_id(rng, "Place", id_ranges))]
    if table == "hasInterest":
        return [str(random_node_id(rng, "Person", id_ranges)), str(random_node_id(rng, "Tag", id_ranges))]
    if table == "Person_isLocatedIn":
        return [str(random_node_id(rng, "Person", id_ranges)), str(random_node_id(rng, "Place", id_ranges))]
    if table == "knows":
        return [str(random_node_id(rng, "Person", id_ranges)), str(random_node_id(rng, "Person", id_ranges)), fake_date(rng)]
    if table == "likes_Comment":
        return [str(random_node_id(rng, "Person", id_ranges)), str(random_node_id(rng, "Comment", id_ranges)), fake_date(rng)]
    if table == "likes_Post":
        return [str(random_node_id(rng, "Person", id_ranges)), str(random_node_id(rng, "Post", id_ranges)), fake_date(rng)]
    if table == "studyAt":
        return [
            str(random_node_id(rng, "Person", id_ranges)),
            str(random_node_id(rng, "Organisation", id_ranges)),
            str(rng.randint(1980, 2026)),
        ]
    if table == "workAt":
        return [
            str(random_node_id(rng, "Person", id_ranges)),
            str(random_node_id(rng, "Organisation", id_ranges)),
            str(rng.randint(1980, 2026)),
        ]
    if table == "isPartOf":
        return [str(random_node_id(rng, "Place", id_ranges)), str(random_node_id(rng, "Place", id_ranges))]
    if table == "Post_hasCreator":
        return [str(random_node_id(rng, "Post", id_ranges)), str(random_node_id(rng, "Person", id_ranges))]
    if table == "Post_hasTag":
        return [str(random_node_id(rng, "Post", id_ranges)), str(random_node_id(rng, "Tag", id_ranges))]
    if table == "Post_isLocatedIn":
        return [str(random_node_id(rng, "Post", id_ranges)), str(random_node_id(rng, "Place", id_ranges))]
    if table == "hasType":
        return [str(random_node_id(rng, "Tag", id_ranges)), str(random_node_id(rng, "TagClass", id_ranges))]
    if table == "isSubclassOf":
        return [str(random_node_id(rng, "TagClass", id_ranges)), str(random_node_id(rng, "TagClass", id_ranges))]
    raise ValueError(f"Unsupported rel table: {table}")


def write_shard(task: TableTask, shard_idx: int, start: int, end: int, out_dir: Path) -> int:
    if end <= start:
        return 0
    shard_path = out_dir / to_shard_name(task.file_path, shard_idx)
    shard_path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(task.seed + shard_idx * 1000003 + start)
    rows = end - start

    with shard_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(task.header)
        if task.kind == "node":
            for i in range(rows):
                rid = task.id_start + start + i
                writer.writerow(node_row(task.table, rid, rng))
        else:
            for _ in range(rows):
                writer.writerow(rel_row(task.table, rng, task.id_ranges))
    return rows


def worker_run(args: Tuple[TableTask, int, int, int, str]) -> Tuple[str, str, int, float]:
    task, shard_idx, start, end, out_dir = args
    t0 = time.perf_counter()
    n = write_shard(task, shard_idx, start, end, Path(out_dir))
    dt = time.perf_counter() - t0
    return task.kind, task.table, n, dt


def build_shard_jobs(task: TableTask, out_dir: Path) -> List[Tuple[TableTask, int, int, int, str]]:
    jobs: List[Tuple[TableTask, int, int, int, str]] = []
    if task.count <= 0:
        return jobs
    num_shards = max(1, math.ceil(task.count / task.shard_rows))
    for idx in range(num_shards):
        start = idx * task.shard_rows
        end = min(task.count, start + task.shard_rows)
        jobs.append((task, idx, start, end, str(out_dir)))
    return jobs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate LDBC CSV datasets at scale")
    p.add_argument("--total-nodes", type=int, required=True, help="Total number of node rows to generate")
    p.add_argument("--total-rels", type=int, required=True, help="Total number of rel rows to generate")
    p.add_argument(
        "--source-root",
        type=Path,
        default=DEFAULT_SOURCE,
        help="Reference CsvBasic root used to infer table ratios",
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=BASE_DIR / "CsvGenerated",
        help="Output root containing static/ and dynamic/",
    )
    p.add_argument(
        "--shard-rows",
        type=int,
        default=50_000_000,
        help="Rows per shard file, e.g. 50000000 -> *_0_0.csv ... *_0_N.csv",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Concurrent worker processes. 0 means floor(CPU*0.8)",
    )
    p.add_argument(
        "--max-memory-ratio",
        type=float,
        default=0.8,
        help="Target max memory usage ratio for generator workers (default 0.8)",
    )
    p.add_argument(
        "--estimated-worker-mem-mb",
        type=int,
        default=128,
        help="Estimated per-worker RSS in MB used to cap worker count by memory",
    )
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    return p.parse_args()


def get_mem_available_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return 0
    with meminfo.open("r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1]) * 1024
    return 0


def resolve_workers(user_workers: int, max_memory_ratio: float, estimated_worker_mem_mb: int) -> Tuple[int, int, int]:
    cpu = os.cpu_count() or 1
    cpu_based = user_workers if user_workers > 0 else max(1, int(cpu * 0.8))
    mem_avail = get_mem_available_bytes()
    if mem_avail <= 0:
        return cpu_based, cpu_based, 0
    est_worker_mem = max(1, estimated_worker_mem_mb) * 1024 * 1024
    mem_based = max(1, int((mem_avail * max(0.01, min(max_memory_ratio, 0.99))) // est_worker_mem))
    return max(1, min(cpu_based, mem_based)), cpu_based, mem_based


def main() -> None:
    args = parse_args()
    if args.total_nodes <= 0 or args.total_rels <= 0:
        raise ValueError("--total-nodes and --total-rels must be positive")
    if args.shard_rows <= 0:
        raise ValueError("--shard-rows must be positive")
    if args.estimated_worker_mem_mb <= 0:
        raise ValueError("--estimated-worker-mem-mb must be positive")

    schema_file = BASE_DIR / "ldbc_schema.cypher"
    copy_file = BASE_DIR / "ldbc_copy.cypher"
    node_tables, rel_tables = parse_schema(schema_file)
    copy_map = parse_copy(copy_file)

    node_weights, rel_weights = derive_reference_distribution(
        args.source_root, node_tables, rel_tables, copy_map
    )

    for t in node_tables:
        node_weights.setdefault(t, 1)
    for t in rel_tables:
        rel_weights.setdefault(t, 1)

    node_counts = allocate_by_ratio(args.total_nodes, node_weights, NODE_MIN_BASE)
    rel_counts = allocate_by_ratio(args.total_rels, rel_weights)
    node_ranges = build_node_ranges(node_counts)

    out_root = args.output_root.resolve()
    (out_root / "static").mkdir(parents=True, exist_ok=True)
    (out_root / "dynamic").mkdir(parents=True, exist_ok=True)

    workers, cpu_based_workers, mem_based_workers = resolve_workers(
        args.workers, args.max_memory_ratio, args.estimated_worker_mem_mb
    )
    cpu = os.cpu_count() or 1

    tasks: List[TableTask] = []
    id_cur: Dict[str, int] = {k: node_ranges[k][0] for k in node_ranges}

    for t in node_tables:
        base_rel_path = copy_map.get(t, COPY_NODE_TO_FILE[t])
        tasks.append(
            TableTask(
                kind="node",
                table=t,
                file_path=base_rel_path,
                header=NODE_HEADERS[t],
                count=node_counts[t],
                shard_rows=args.shard_rows,
                id_start=id_cur[t],
                id_ranges=node_ranges,
                seed=args.seed,
            )
        )

    for t in rel_tables:
        base_rel_path = copy_map.get(t, COPY_REL_TO_FILE[t])
        tasks.append(
            TableTask(
                kind="rel",
                table=t,
                file_path=base_rel_path,
                header=REL_HEADERS[t],
                count=rel_counts[t],
                shard_rows=args.shard_rows,
                id_start=0,
                id_ranges=node_ranges,
                seed=args.seed + 997,
            )
        )

    jobs: List[Tuple[TableTask, int, int, int, str]] = []
    for task in tasks:
        jobs.extend(build_shard_jobs(task, out_root))

    print(f"[plan] total_nodes={args.total_nodes} total_rels={args.total_rels} shard_rows={args.shard_rows}")
    print(f"[plan] workers={workers} cpu={cpu} (target CPU usage ~80%)")
    if mem_based_workers > 0:
        print(
            f"[plan] memory_guard max_ratio={args.max_memory_ratio:.2f} "
            f"estimated_worker_mem_mb={args.estimated_worker_mem_mb} "
            f"cpu_based_workers={cpu_based_workers} mem_based_workers={mem_based_workers}"
        )
    print(f"[plan] node_tables={len(node_tables)} rel_tables={len(rel_tables)} shard_jobs={len(jobs)}")
    print("[note] writer uses streaming rows per process, avoids large in-memory buffers")

    stats = {
        "node_rows": 0,
        "node_time": 0.0,
        "rel_rows": 0,
        "rel_time": 0.0,
    }

    t_total = time.perf_counter()
    progress_interval_sec = 10.0
    next_progress_ts = t_total + progress_interval_sec
    with mp.Pool(processes=workers, maxtasksperchild=1) as pool:
        for kind, table, rows, dt in pool.imap_unordered(worker_run, jobs, chunksize=1):
            if kind == "node":
                stats["node_rows"] += rows
                stats["node_time"] += dt
            else:
                stats["rel_rows"] += rows
                stats["rel_time"] += dt

            now = time.perf_counter()
            if now >= next_progress_ts:
                elapsed = max(now - t_total, 1e-9)
                total_rows = stats["node_rows"] + stats["rel_rows"]
                print(
                    "[progress] "
                    f"elapsed={elapsed:.1f}s "
                    f"node_rows={stats['node_rows']} "
                    f"rel_rows={stats['rel_rows']} "
                    f"total_rows={total_rows} "
                    f"rows_per_wall_sec={total_rows / elapsed:.2f}"
                )
                next_progress_ts = now + progress_interval_sec

    total_wall = time.perf_counter() - t_total

    node_rate = stats["node_rows"] / max(stats["node_time"], 1e-9)
    rel_rate = stats["rel_rows"] / max(stats["rel_time"], 1e-9)
    node_wall_rate = stats["node_rows"] / max(total_wall, 1e-9)
    rel_wall_rate = stats["rel_rows"] / max(total_wall, 1e-9)

    print("[summary]")
    print(f"  node_rows={stats['node_rows']} cpu_time={stats['node_time']:.3f}s rows_per_cpu_sec={node_rate:.2f}")
    print(f"  rel_rows={stats['rel_rows']} cpu_time={stats['rel_time']:.3f}s rows_per_cpu_sec={rel_rate:.2f}")
    print(f"  wall_time={total_wall:.3f}s")
    print(f"  node_rows_per_wall_sec={node_wall_rate:.2f}")
    print(f"  rel_rows_per_wall_sec={rel_wall_rate:.2f}")
    print(f"  output_root={out_root}")

    # Print per-table allocation for reproducibility.
    print("[alloc] nodes")
    for k in sorted(node_counts):
        print(f"  {k}: {node_counts[k]}")
    print("[alloc] rels")
    for k in sorted(rel_counts):
        print(f"  {k}: {rel_counts[k]}")


if __name__ == "__main__":
    main()
