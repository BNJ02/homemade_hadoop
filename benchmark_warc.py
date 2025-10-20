#!/usr/bin/env python3
"""
Benchmark runner to launch WARC-based MapReduce jobs on varying worker groups.

The script reproduces the orchestration logic of run_cluster.sh but focuses on:
  * enforcing WARC inputs (10 splits by default, same file naming pattern),
  * running successive experiments with 1, 3, 5, then 10 active machines,
  * collecting timings and computing Amdahl-law speedups.

Usage example (defaults mirror run_cluster.sh):
    python benchmark_warc.py \
        --master tp-4b01-10 \
        --host-pool tp-4b01-11,tp-4b01-12,...,tp-4b01-20
"""

from __future__ import annotations

import argparse
import datetime
import os
import platform
import posixpath
import random
import shlex
import shutil
import subprocess
import sys
import time
import json
from dataclasses import dataclass, field
from typing import Iterable, List, Sequence

# Load default host pool from JSON file
hosts = []
with open('hosts.json', 'r') as f:
    hosts = json.load(f)
DEFAULT_HOST_POOL = hosts

DEFAULT_SSH_OPTIONS = [
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "ConnectTimeout=5",
]


@dataclass
class RunResult:
    machine_count: int
    worker_layout: List[str]
    run_iteration: int = 1
    map_max_lines: int | None = None
    elapsed_seconds: float | None = None
    speedup: float | None = None
    serial_fraction: float | None = None
    status: str = "pending"
    notes: List[str] = field(default_factory=list)

    def record_failure(self, message: str) -> None:
        self.status = "failed"
        self.notes.append(message)

    def record_success(self, elapsed: float) -> None:
        self.status = "ok"
        self.elapsed_seconds = elapsed


def remote_path_expr(remote_root: str, name: str) -> str:
    combined = posixpath.join(remote_root, name)
    if combined.startswith("~"):
        return f"$HOME{combined[1:]}"
    if combined.startswith("$"):
        return combined
    return combined


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch MapReduce WARC splits on varying worker counts and measure speedup.",
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("MASTER", "tp-4b01-10"),
        help="Master hostname (default: %(default)s or $MASTER)",
    )
    parser.add_argument(
        "--host-pool",
        default=",".join(DEFAULT_HOST_POOL),
        help="Comma-separated list of worker hostnames (default: 10 tp-4b01-XX nodes)",
    )
    parser.add_argument(
        "--machine-counts",
        default="1,3,5,10",
        help="Comma-separated counts of distinct machines to activate (default: %(default)s)",
    )
    parser.add_argument(
        "--total-workers",
        type=int,
        default=10,
        help="Total number of worker processes / splits to launch (default: %(default)s)",
    )
    parser.add_argument(
        "--warc-dir",
        default="/cal/commoncrawl",
        help="Directory containing WARC files (default: %(default)s)",
    )
    parser.add_argument(
        "--warc-template",
        default="CC-MAIN-20230320083513-20230320113513-{index:05d}.warc.wet",
        help="Filename template for WARC splits (default: %(default)s)",
    )
    parser.add_argument(
        "--warc-offset",
        type=int,
        default=0,
        help="Starting index for WARC filenames (default: %(default)s)",
    )
    parser.add_argument(
        "--control-port",
        type=int,
        default=5374,
        help="Control port used by the master (default: %(default)s)",
    )
    parser.add_argument(
        "--shuffle-port-base",
        type=int,
        default=6200,
        help="Base port for shuffle sockets (default: %(default)s)",
    )
    parser.add_argument(
        "--remote-python",
        default="python3",
        help="Python executable to invoke remotely (default: %(default)s)",
    )
    parser.add_argument(
        "--remote-root",
        default="~",
        help="Directory on remote hosts containing serveur.py/client.py (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=900,
        help="Per-run timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--sleep-after-master",
        type=float,
        default=2.0,
        help="Seconds to wait after starting the master before spawning workers",
    )
    parser.add_argument(
        "--results-csv",
        help="Optional path to write results as CSV (appended if file exists)",
    )
    parser.add_argument(
        "--map-max-lines",
        type=int,
        help="Optional limit of input lines per worker during the map stage",
    )
    parser.add_argument(
        "--map-lines-lists",
        help=(
            "Optional list describing max lines per machine count (format "
            "\"N1:V1,V2;N2:V3\"), used when --map-max-lines is not set"
        ),
    )
    parser.add_argument(
        "--map-lines-all",
        help=(
            "Optional comma-separated limits applied to every machine count "
            "when --map-max-lines is not set (overridden by --map-lines-lists)"
        ),
    )
    parser.add_argument(
        "--runs-per-count",
        type=int,
        default=1,
        help="Number of repetitions to execute for each machine count (default: 1)",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Do not copy client.py/serveur.py to remote hosts before launching runs",
    )
    parser.add_argument(
        "--ssh-args",
        default="",
        help=(
            "Extra options appended to default SSH flags "
            "(BatchMode=yes, StrictHostKeyChecking=accept-new, ConnectTimeout=5)"
        ),
    )
    parser.add_argument(
        "--ssh-user",
        default=os.environ.get("SSH_USER") or os.environ.get("USER") or os.environ.get("USERNAME"),
        help="Username for SSH connections (default: $SSH_USER else $USER/$USERNAME)",
    )
    parser.add_argument(
        "--ssh-key",
        help="Private key passed to ssh via -i (optional)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the orchestration steps without executing any ssh command",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show ssh command invocations and progress details",
    )
    return parser.parse_args(argv)


def parse_csv_list(raw: str) -> List[str]:
    items = [part.strip() for part in raw.split(",")]
    return [item for item in items if item]


def parse_line_limits_list(raw: str | None) -> List[int]:
    if not raw:
        return []
    values: List[int] = []
    for value in raw.split(","):
        value = value.strip()
        if not value:
            continue
        try:
            parsed = int(value)
        except ValueError as exc:
            raise ValueError(f"Invalid line limit '{value}' in --map-lines-all") from exc
        if parsed <= 0:
            raise ValueError("--map-lines-all values must be > 0")
        values.append(parsed)
    if not values:
        raise ValueError("No valid values supplied to --map-lines-all")
    return values


def parse_map_lines_config(raw: str | None) -> dict[int, List[int]]:
    if not raw:
        return {}
    mapping: dict[int, List[int]] = {}
    entries = [entry.strip() for entry in raw.split(";") if entry.strip()]
    if not entries:
        raise ValueError("Invalid --map-lines-lists format (empty entries)")
    for entry in entries:
        if ":" not in entry:
            raise ValueError(
                f"Invalid --map-lines-lists fragment '{entry}' (expected COUNT:V1,V2)"
            )
        count_part, values_part = entry.split(":", 1)
        try:
            count = int(count_part.strip())
        except ValueError as exc:
            raise ValueError(
                f"Invalid machine count '{count_part}' in --map-lines-lists"
            ) from exc
        if count < 1:
            raise ValueError("--map-lines-lists machine counts must be >= 1")
        values_raw = [v.strip() for v in values_part.split(",") if v.strip()]
        if not values_raw:
            raise ValueError(
                f"No values provided for machine count {count} in --map-lines-lists"
            )
        values: List[int] = []
        for value in values_raw:
            try:
                parsed = int(value)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid line limit '{value}' for machine count {count}"
                ) from exc
            if parsed <= 0:
                raise ValueError("--map-lines-lists values must be > 0")
            values.append(parsed)
        mapping[count] = values
    return mapping


def is_host_reachable(host: str, timeout: float = 1.5) -> bool:
    if not host:
        return False
    if shutil.which("ping") is None:
        return True
    system = platform.system().lower()
    if system == "windows":
        command = ["ping", "-n", "1", "-w", str(int(timeout * 1000)), host]
    else:
        command = ["ping", "-c", "1", "-W", str(int(timeout)), host]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout + 1.0,
            check=False,
        )
        return completed.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def filter_reachable_hosts(hosts: Sequence[str]) -> tuple[List[str], List[str]]:
    if shutil.which("ping") is None:
        return list(hosts), []
    reachable: List[str] = []
    unreachable: List[str] = []
    for host in hosts:
        if is_host_reachable(host):
            reachable.append(host)
        else:
            unreachable.append(host)
    return reachable, unreachable


def generate_warc_paths(
    total: int,
    warc_dir: str,
    template: str,
    offset: int,
) -> List[str]:
    warc_paths: List[str] = []
    for i in range(total):
        filename = template.format(index=i + offset)
        warc_paths.append(posixpath.join(warc_dir, filename))
    return warc_paths


def build_worker_layout(pool: Sequence[str], machine_count: int, total_workers: int) -> List[str]:
    if machine_count < 1:
        raise ValueError("machine_count must be >= 1")
    if machine_count > len(pool):
        raise ValueError(f"Requested {machine_count} machines but only {len(pool)} provided")
    selected = list(pool[:machine_count])
    layout: List[str] = []
    for i in range(total_workers):
        layout.append(selected[i % machine_count])
    return layout


class SSHRunner:
    def __init__(
        self,
        extra_args: Sequence[str],
        *,
        user: str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        self.extra_args = list(extra_args)
        self.user = user
        self.dry_run = dry_run
        self.verbose = verbose

    def _format_host(self, host: str) -> str:
        if "@" in host:
            return host
        if self.user:
            return f"{self.user}@{host}"
        return host

    def run(
        self,
        host: str,
        command: str,
        *,
        check: bool = True,
        capture: bool = False,
        log: bool | None = None,
    ) -> subprocess.CompletedProcess[str]:
        target = self._format_host(host)
        ssh_cmd = ["ssh", target, *self.extra_args, command]
        if log is None:
            should_log = self.dry_run or self.verbose
        else:
            should_log = log or self.dry_run
        if should_log:
            print(f"[ssh] {' '.join(shlex.quote(part) for part in ssh_cmd)}")
        if self.dry_run:
            return subprocess.CompletedProcess(ssh_cmd, 0, "", "")
        return subprocess.run(
            ssh_cmd,
            text=True,
            check=check,
            capture_output=capture,
        )


def quotecmd(parts: Iterable[str]) -> str:
    return shlex.join(list(parts))


def build_remote_launcher(
    python_bin: str,
    remote_root: str,
    script_name: str,
    extra_args: Sequence[str],
    log_path: str,
) -> str:
    script_expr = remote_path_expr(remote_root, script_name)
    log_expr = remote_path_expr(remote_root, log_path)
    args_str = " ".join(shlex.quote(arg) for arg in extra_args)
    python_expr = shlex.quote(python_bin)
    launch = f"nohup {python_expr} {script_expr}"
    if args_str:
        launch += f" {args_str}"
    launch += f" > {log_expr} 2>&1 &"
    return f"bash -lc {shlex.quote(launch)}"


def sync_remote_code(
    hosts: Iterable[str],
    *,
    user: str | None,
    remote_root: str,
    ssh_options: Sequence[str],
    dry_run: bool,
    verbose: bool,
) -> None:
    unique_hosts = sorted({host.split("@")[-1] for host in hosts})
    if not unique_hosts:
        return
    remote_dir = remote_root
    if not remote_dir.endswith("/"):
        remote_dir = f"{remote_dir}/"
    files_to_copy = ["serveur.py", "client.py"]
    # All hosts are expected to have the same remote_root structure (NFS : Network File System)
    # Take a random host to display the scp command, real random selection is needed
    host = random.choice(unique_hosts)
    target = host if user is None or "@" in host else f"{user}@{host}"
    for local_path in files_to_copy:
        scp_cmd = ["scp", *ssh_options, local_path, f"{target}:{remote_dir}"]
        if dry_run or verbose:
            print(f"[scp] {' '.join(shlex.quote(part) for part in scp_cmd)}")
        if dry_run:
            continue
        subprocess.run(scp_cmd, check=True)


def kill_remote_processes(runner: SSHRunner, hosts: Iterable[str]) -> None:
    unique_hosts = sorted(set(hosts))
    for host in unique_hosts:
        try:
            runner.run(
                host,
                "bash -lc 'pkill -f \"python3.*serveur.py\" 2>/dev/null || true; "
                "pkill -f \"python3.*client.py\" 2>/dev/null || true'",
                check=False,
            )
        except subprocess.CalledProcessError:
            pass


def wait_for_completion(
    runner: SSHRunner,
    master: str,
    worker_hosts: Sequence[str],
    timeout: int,
    remote_root: str,
    poll_interval: float = 2.0,
) -> bool:
    master_log_expr = remote_path_expr(remote_root, "mapreduce_master.log")
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        sentinel_cmd = (
            "bash -lc "
            f"\"test -f {master_log_expr} && grep -q 'Final wordcount' {master_log_expr}\""
        )
        sentinel_result = runner.run(master, sentinel_cmd, check=False, log=False)
        if sentinel_result.returncode == 0:
            return True
        time.sleep(poll_interval)
    return False


def compute_speedups(results: List[RunResult]) -> None:
    grouped: dict[int | None, List[RunResult]] = {}
    for result in results:
        grouped.setdefault(result.map_max_lines, []).append(result)

    for map_lines, group in grouped.items():
        ok_results = [
            res for res in group if res.status == "ok" and res.elapsed_seconds
        ]
        if not ok_results:
            continue
        baseline_result = min(
            ok_results, key=lambda res: (res.machine_count, res.elapsed_seconds)
        )
        baseline = baseline_result.elapsed_seconds
        if not baseline:
            continue
        serial_estimates: List[float] = []
        for result in ok_results:
            result.speedup = baseline / result.elapsed_seconds
            if result.machine_count > 1:
                ratio = result.elapsed_seconds / baseline
                n = result.machine_count
                numerator = ratio - (1.0 / n)
                denominator = 1.0 - (1.0 / n)
                if denominator > 0:
                    serial = max(0.0, min(1.0, numerator / denominator))
                    result.serial_fraction = serial
                    serial_estimates.append(serial)
        if serial_estimates:
            avg_serial = sum(serial_estimates) / len(serial_estimates)
            for result in ok_results:
                predicted = 1.0 / (
                    avg_serial + (1.0 - avg_serial) / result.machine_count
                )
                note = (
                    f"Predicted speedup (Amdahl, f={avg_serial:.3f}): "
                    f"{predicted:.3f}"
                )
                result.notes.append(note)


def write_csv(results: List[RunResult], path: str) -> None:
    header = (
        "machine_count,run_iteration,map_max_lines,"
        "elapsed_seconds,speedup,serial_fraction,status,notes\n"
    )
    line_parts = []
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="ascii") as handle:
        if not file_exists:
            handle.write(header)
        for result in results:
            notes = "|".join(result.notes)
            line_parts = [
                str(result.machine_count),
                str(result.run_iteration),
                str(result.map_max_lines) if result.map_max_lines is not None else "",
                f"{result.elapsed_seconds:.3f}" if result.elapsed_seconds else "",
                f"{result.speedup:.3f}" if result.speedup else "",
                f"{result.serial_fraction:.3f}" if result.serial_fraction else "",
                result.status,
                shlex.quote(notes),
            ]
            handle.write(",".join(line_parts) + "\n")


def launch_benchmark(args: argparse.Namespace) -> List[RunResult]:
    if args.runs_per_count < 1:
        raise ValueError("--runs-per-count must be greater than or equal to 1")
    try:
        map_lines_mapping = parse_map_lines_config(args.map_lines_lists)
    except ValueError as exc:
        raise ValueError(f"Invalid --map-lines-lists configuration: {exc}") from exc
    try:
        map_lines_all = parse_line_limits_list(args.map_lines_all)
    except ValueError as exc:
        raise ValueError(f"Invalid --map-lines-all configuration: {exc}") from exc
    host_pool = parse_csv_list(args.host_pool)
    reachable_hosts, unreachable_hosts = filter_reachable_hosts(host_pool)
    if unreachable_hosts:
        print(
            "[host-check] Removing unreachable hosts: "
            + ", ".join(unreachable_hosts),
            file=sys.stderr,
        )
    if not reachable_hosts:
        raise RuntimeError(
            "No reachable workers detected in host pool after filtering."
        )
    host_pool = reachable_hosts
    if not is_host_reachable(args.master):
        print(
            f"[host-check] Warning: master host {args.master} did not respond to ping.",
            file=sys.stderr,
        )
    machine_counts = [int(value) for value in parse_csv_list(args.machine_counts)]
    warc_paths = generate_warc_paths(
        args.total_workers,
        args.warc_dir,
        args.warc_template,
        args.warc_offset,
    )
    extra_args = list(DEFAULT_SSH_OPTIONS)
    if args.ssh_key:
        extra_args.extend(["-i", args.ssh_key])
    if args.ssh_args:
        extra_args.extend(shlex.split(args.ssh_args))
    runner = SSHRunner(
        extra_args=extra_args,
        user=args.ssh_user,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    all_hosts_for_sync = [args.master, *host_pool]
    if not args.skip_sync:
        try:
            sync_remote_code(
                all_hosts_for_sync,
                user=args.ssh_user,
                remote_root=args.remote_root,
                ssh_options=extra_args,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
        except subprocess.CalledProcessError as exc:
            print(f"Failed to sync code to remote hosts: {exc}", file=sys.stderr)
            return [RunResult(machine_count=0, worker_layout=[], status="failed", notes=[str(exc)])]

    results: List[RunResult] = []
    for count in machine_counts:
        try:
            layout = build_worker_layout(host_pool, count, args.total_workers)
        except ValueError as exc:
            result = RunResult(machine_count=count, worker_layout=[], status="failed")
            result.notes.append(str(exc))
            results.append(result)
            continue

        if args.map_max_lines is not None:
            line_limits = [args.map_max_lines]
        else:
            if count in map_lines_mapping:
                line_limits = list(map_lines_mapping[count])
            elif map_lines_all:
                line_limits = list(map_lines_all)
            else:
                line_limits = [None]

        for line_limit in line_limits:
            for iteration in range(args.runs_per_count):
                result = RunResult(
                    machine_count=count,
                    worker_layout=list(layout),
                    run_iteration=iteration + 1,
                    map_max_lines=line_limit,
                )
                results.append(result)

                iteration_suffix = ""
                if args.runs_per_count > 1:
                    iteration_suffix = f" (run {iteration + 1}/{args.runs_per_count})"
                max_lines_label = (
                    str(line_limit) if line_limit is not None else "None"
                )

                print(
                    f"\n=== Run with {count} machine(s){iteration_suffix} for "
                    f"{args.total_workers} WARC splits ==="
                )
                print(f"Master: {args.master}")
                print(f"Worker layout: {layout}")
                print(f"WARC files: {warc_paths}")
                print(f"Map max lines: {max_lines_label}")

                kill_hosts = set(layout)
                kill_hosts.add(args.master)
                kill_remote_processes(runner, kill_hosts)

                master_cmd = build_remote_launcher(
                    args.remote_python,
                    args.remote_root,
                    "serveur.py",
                    [
                        "--host",
                        "0.0.0.0",
                        "--port",
                        str(args.control_port),
                        "--num-workers",
                        str(args.total_workers),
                    ],
                    "mapreduce_master.log",
                )
                try:
                    runner.run(args.master, master_cmd)
                except subprocess.CalledProcessError as exc:
                    result.record_failure(f"Failed to start master: {exc}")
                    continue

                if args.sleep_after_master:
                    time.sleep(args.sleep_after_master)

                host_args = layout
                worker_failures = False
                for index, host in enumerate(layout, start=1):
                    extra = [
                        str(index),
                        *host_args,
                        "--master-host",
                        args.master,
                        "--control-port",
                        str(args.control_port),
                        "--shuffle-port-base",
                        str(args.shuffle_port_base),
                        "--split-id",
                        warc_paths[index - 1],
                    ]
                    if line_limit is not None:
                        extra.extend(["--max-lines", str(line_limit)])
                    worker_cmd = build_remote_launcher(
                        args.remote_python,
                        args.remote_root,
                        "client.py",
                        extra,
                        f"mapreduce_worker_{index}.log",
                    )
                    try:
                        runner.run(host, worker_cmd)
                    except subprocess.CalledProcessError as exc:
                        worker_failures = True
                        result.notes.append(f"Worker {index} launch failed on {host}: {exc}")

                if worker_failures:
                    result.record_failure("One or more workers failed to launch")
                    continue

                start = time.monotonic()
                if runner.dry_run:
                    result.status = "skipped"
                    result.notes.append("Dry-run mode: commands printed but not executed")
                    print("Dry-run: skipping remote execution and timing for this configuration.")
                    continue

                completed = wait_for_completion(
                    runner,
                    args.master,
                    layout,
                    args.timeout,
                    args.remote_root,
                )
                elapsed = time.monotonic() - start

                if completed:
                    result.record_success(elapsed)
                    print(f"Run completed in {elapsed:.2f}s")
                else:
                    result.record_failure(f"Timeout after {elapsed:.2f}s (limit {args.timeout}s)")

                kill_remote_processes(runner, kill_hosts)

    compute_speedups(results)
    return results


def format_results(results: List[RunResult]) -> None:
    print("\n=== Benchmark summary ===")
    for result in results:
        line = (
            f"{result.machine_count} machine(s): status={result.status}"
        )
        if result.map_max_lines is not None:
            line += f", map_max_lines={result.map_max_lines}"
        else:
            line += ", map_max_lines=None"
        if result.elapsed_seconds is not None:
            line += f", elapsed={result.elapsed_seconds:.2f}s"
        if result.speedup is not None:
            line += f", speedup={result.speedup:.3f}"
        if result.serial_fraction is not None:
            line += f", f_serial={result.serial_fraction:.3f}"
        print(line)
        for note in result.notes:
            print(f"  - {note}")


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    try:
        results = launch_benchmark(args)
    except KeyboardInterrupt:
        print("\nInterrupted by user, aborting.")
        return 130
    format_results(results)
    if args.results_csv:
        args_results_csv = "".join([args.results_csv, datetime.datetime.now().strftime("_%Y-%m-%d_%H-%M-%S"), ".csv"])
        write_csv(results, args_results_csv)
        print(f"\nResults appended to {args_results_csv}")
    failures = [res for res in results if res.status != "ok"]
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
