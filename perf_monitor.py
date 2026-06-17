#!/usr/bin/env python3
"""Periodically monitor a process and append resource usage to perf_monitor.

Sample fields:
- ts: Local timestamp when the sample was collected.
- pid: Monitored process id.
- name: Process command name from /proc/<pid>/comm.
- cpu: Process CPU usage over the previous sampling interval. 100% means one full CPU core.
- vm: Virtual memory size from VmSize, including reserved mmap address space.
- rss: Resident set size from VmRSS, the process memory currently resident in RAM.
- hwm: Peak resident set size from VmHWM since the process started.
- rss_stat: Resident set size calculated from /proc/<pid>/stat RSS pages.
- read_bytes: Cumulative bytes actually fetched from storage by this process, from /proc/<pid>/io.
- write_bytes: Cumulative bytes actually written to storage by this process, from /proc/<pid>/io.
- syscr: Cumulative number of read-like syscalls issued by this process.
- syscw: Cumulative number of write-like syscalls issued by this process.
- interval_s: Seconds between this sample and the previous sample.
- read_Bps: Storage read throughput attributed to this process during the previous interval.
- write_Bps: Storage write throughput attributed to this process during the previous interval.
- read_MiBps: Same as read_Bps, converted to MiB/s.
- write_MiBps: Same as write_Bps, converted to MiB/s.
- read_ops_s: Read-like syscall rate during the previous interval. This is a per-process
  approximation, not block-device IOPS.
- write_ops_s: Write-like syscall rate during the previous interval. This is a per-process
  approximation, not block-device IOPS.

Notes:
- Ladybug's BufferManager uses mmap for address-space management. Reserved mmap space increases
  virtual memory, but it is not storage I/O by itself.
- /proc/<pid>/io read_bytes/write_bytes count bytes that the kernel attributes to this process as
  actual storage I/O. Memory served from page cache may not increase read_bytes.
- For true device-level IOPS, queue depth, await, and utilization, use system tools such as iostat.
  This script reports per-process counters and interval rates derived from /proc/<pid>/io.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Dict, NamedTuple, Tuple


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = BASE_DIR / "perf_monitor"
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


class PreviousSample(NamedTuple):
    timestamp: float
    proc_ticks: int
    total_cpu_ticks: int
    read_bytes: int
    write_bytes: int
    syscr: int
    syscw: int


def read_key_value_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values


def read_proc_stat(pid: int) -> Tuple[int, int, int]:
    text = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    end_comm = text.rfind(")")
    if end_comm == -1:
        raise RuntimeError(f"Cannot parse /proc/{pid}/stat")
    fields = text[end_comm + 2 :].split()
    utime = int(fields[11])
    stime = int(fields[12])
    rss_pages = int(fields[21])
    return utime, stime, rss_pages


def read_total_cpu_ticks() -> int:
    with Path("/proc/stat").open("r", encoding="utf-8") as f:
        first = f.readline().split()
    return sum(int(value) for value in first[1:])


def read_process_name(pid: int) -> str:
    try:
        return (Path("/proc") / str(pid) / "comm").read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def process_is_running(pid: int) -> bool:
    status_path = Path("/proc") / str(pid) / "status"
    if not status_path.exists():
        return False
    try:
        status = read_key_value_file(status_path)
    except FileNotFoundError:
        return False
    state = status.get("State", "")
    return not state.startswith("Z")


def format_bytes(num_bytes: int) -> str:
    return f"{num_bytes / 1024 / 1024 / 1024:.3f}GiB"


def sample(pid: int, previous: PreviousSample | None) -> Tuple[str, PreviousSample]:
    proc_dir = Path("/proc") / str(pid)
    now = time.monotonic()
    status = read_key_value_file(proc_dir / "status")
    io_values = read_key_value_file(proc_dir / "io")
    utime, stime, rss_pages = read_proc_stat(pid)
    total_cpu_ticks = read_total_cpu_ticks()
    proc_ticks = utime + stime

    vm_size_kb = int(status.get("VmSize", "0 kB").split()[0])
    vm_rss_kb = int(status.get("VmRSS", "0 kB").split()[0])
    vm_hwm_kb = int(status.get("VmHWM", "0 kB").split()[0])
    rss_from_stat = rss_pages * PAGE_SIZE

    read_bytes = int(io_values.get("read_bytes", "0"))
    write_bytes = int(io_values.get("write_bytes", "0"))
    syscr = int(io_values.get("syscr", "0"))
    syscw = int(io_values.get("syscw", "0"))

    if previous is None:
        interval_s = 0.0
        cpu_percent = 0.0
        read_bps = 0.0
        write_bps = 0.0
        read_ops_s = 0.0
        write_ops_s = 0.0
    else:
        interval_s = max(now - previous.timestamp, 1e-9)
        proc_delta = proc_ticks - previous.proc_ticks
        total_delta = total_cpu_ticks - previous.total_cpu_ticks
        cpu_percent = (proc_delta / total_delta * os.cpu_count() * 100.0) if total_delta > 0 else 0.0
        read_bps = max(read_bytes - previous.read_bytes, 0) / interval_s
        write_bps = max(write_bytes - previous.write_bytes, 0) / interval_s
        read_ops_s = max(syscr - previous.syscr, 0) / interval_s
        write_ops_s = max(syscw - previous.syscw, 0) / interval_s

    line = (
        f"[sample] ts={time.strftime('%Y-%m-%d %H:%M:%S')} pid={pid} "
        f"name={read_process_name(pid)} cpu={cpu_percent:.2f}% "
        f"vm={format_bytes(vm_size_kb * 1024)} rss={format_bytes(vm_rss_kb * 1024)} "
        f"hwm={format_bytes(vm_hwm_kb * 1024)} rss_stat={format_bytes(rss_from_stat)} "
        f"read_bytes={format_bytes(read_bytes)} write_bytes={format_bytes(write_bytes)} "
        f"syscr={syscr} syscw={syscw} interval_s={interval_s:.3f} "
        f"read_Bps={read_bps:.2f} write_Bps={write_bps:.2f} "
        f"read_MiBps={read_bps / 1024 / 1024:.3f} write_MiBps={write_bps / 1024 / 1024:.3f} "
        f"read_ops_s={read_ops_s:.2f} write_ops_s={write_ops_s:.2f}"
    )
    current = PreviousSample(
        timestamp=now,
        proc_ticks=proc_ticks,
        total_cpu_ticks=total_cpu_ticks,
        read_bytes=read_bytes,
        write_bytes=write_bytes,
        syscr=syscr,
        syscw=syscw,
    )
    return line, current


def append_line(output_file: Path, line: str) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor VM/RSS/CPU/IO usage for a process")
    parser.add_argument("--pid", type=int, required=True, help="process id to monitor")
    parser.add_argument("--interval", type=float, default=60.0, help="sample interval in seconds")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="output file path")
    args = parser.parse_args()

    if args.interval <= 0:
        raise ValueError("--interval must be greater than 0")

    output_file = args.output.resolve()
    start_line = (
        f"[head] ts={time.strftime('%Y-%m-%d %H:%M:%S')} pid={args.pid} "
        f"interval={args.interval}s output={output_file}"
    )
    print(start_line, flush=True)
    append_line(output_file, start_line)

    previous: PreviousSample | None = None
    while process_is_running(args.pid):
        try:
            line, previous = sample(args.pid, previous)
        except FileNotFoundError:
            break
        print(line, flush=True)
        append_line(output_file, line)
        time.sleep(args.interval)

    end_line = f"[end] ts={time.strftime('%Y-%m-%d %H:%M:%S')} pid={args.pid} process_exited=true"
    print(end_line, flush=True)
    append_line(output_file, end_line)


if __name__ == "__main__":
    main()
