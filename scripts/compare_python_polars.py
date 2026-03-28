#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gc
import os
import resource
import sys
import tempfile
import time
from pathlib import Path


def sanitize_sys_path() -> None:
    script_path = Path(__file__).resolve()
    repo_root = script_path.parent.parent.resolve()
    script_dir = script_path.parent.resolve()
    cwd = Path.cwd().resolve()

    sanitized: list[str] = []
    for entry in sys.path:
        if not entry:
            continue
        try:
            resolved = Path(entry).resolve()
        except OSError:
            sanitized.append(entry)
            continue
        if resolved in {repo_root, script_dir, cwd}:
            continue
        sanitized.append(entry)
    sys.path[:] = sanitized


sanitize_sys_path()

import polars as pl


def frame_data(n: int) -> dict[str, list[object]]:
    departments = ["Engineering", "Marketing", "Sales", "Finance"]
    return {
        "id": list(range(1, n + 1)),
        "name": [f"user_{i:05d}" for i in range(1, n + 1)],
        "age": [20 + (i % 30) for i in range(n)],
        "salary": [50000 + (i % 17) * 1250 for i in range(n)],
        "department": [departments[i % len(departments)] for i in range(n)],
    }


def join_frame_data(n: int) -> tuple[dict[str, list[object]], dict[str, list[object]]]:
    left = {
        "id": list(range(1, n + 1)),
        "department": [f"dept_{i % 16:02d}" for i in range(n)],
        "salary": [50000 + (i % 17) * 1250 for i in range(n)],
    }
    right = {
        "id": list(range(1, n + 1)),
        "level": [f"L{i % 6}" for i in range(n)],
        "bonus": [1000 + (i % 9) * 100 for i in range(n)],
    }
    return left, right


def ensure_fixtures(fixture_dir: Path, sizes: list[int]) -> None:
    fixture_dir.mkdir(parents=True, exist_ok=True)
    for n in sizes:
        df = pl.DataFrame(frame_data(n))
        df.write_csv(fixture_dir / f"polars_compare_{n}.csv")
        df.write_parquet(fixture_dir / f"polars_compare_{n}.parquet")


def loops_for_size(n: int) -> int:
    if n <= 4000:
        return 20
    if n <= 16000:
        return 8
    if n <= 100000:
        return 5
    if n <= 1000000:
        return 3
    return 2


def maxrss_mib() -> float:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return value / (1024 * 1024)
    return value / 1024


def run_case(label: str, loops: int, fn) -> float:
    gc.collect()
    warm = fn()
    if hasattr(warm, "__len__") and len(warm) == 0:
        raise RuntimeError(f"empty result for {label}")
    start = time.perf_counter()
    for _ in range(loops):
        out = fn()
        if hasattr(out, "__len__") and len(out) == 0:
            raise RuntimeError(f"empty result for {label}")
    elapsed = time.perf_counter() - start
    us_per_op = (elapsed / loops) * 1e6
    print(f"{label}: {us_per_op:.0f} us/op")
    return us_per_op


def build_cases(
    n: int, fixture_dir: Path
) -> dict[str, tuple[str, callable]]:
    csv_path = fixture_dir / f"polars_compare_{n}.csv"
    parquet_path = fixture_dir / f"polars_compare_{n}.parquet"
    base_data = frame_data(n)
    left_data, right_data = join_frame_data(n)

    def base_df() -> pl.DataFrame:
        return pl.DataFrame(base_data)

    def join_frames() -> tuple[pl.DataFrame, pl.DataFrame]:
        return pl.DataFrame(left_data), pl.DataFrame(right_data)

    return {
        "query_collect_like": (
            "  QueryCollectLike(df.filter.select.to_dicts)",
            lambda: base_df().filter(pl.col("age") > 30)
            .select(["name", "salary", "department"])
            .to_dicts(),
        ),
        "to_dicts": ("  ToDicts", lambda: base_df().to_dicts()),
        "to_maps": ("  ToDicts", lambda: base_df().to_dicts()),
        "scan_csv": (
            "  ScanCSV(filter+select+collect+to_dicts)",
            lambda: pl.scan_csv(csv_path)
            .filter(pl.col("age") > 30)
            .select(["name", "salary", "department"])
            .collect()
            .to_dicts(),
        ),
        "scan_parquet": (
            "  ScanParquet(filter+select+collect+to_dicts)",
            lambda: pl.scan_parquet(parquet_path)
            .filter(pl.col("age") > 30)
            .select(["name", "salary", "department"])
            .collect()
            .to_dicts(),
        ),
        "group_by_agg": (
            "  GroupByAgg(to_dicts)",
            lambda: base_df().group_by("department")
            .agg(
                [
                    pl.col("salary").sum().alias("salary_sum"),
                    pl.col("id").count().alias("employee_count"),
                    pl.col("age").mean().alias("avg_age"),
                ]
            )
            .to_dicts(),
        ),
        "join_inner": (
            "  JoinInner(select.to_dicts)",
            lambda: _join_to_dicts(join_frames),
        ),
        "sink_ndjson_file": (
            "  SinkNDJSONFile",
            lambda: _sink_ndjson_size(base_df(), n),
        ),
    }


def _join_to_dicts(join_frames_factory) -> list[dict[str, object]]:
    left, right = join_frames_factory()
    return left.join(right, on="id", how="inner").select(["id", "salary", "bonus"]).to_dicts()


def _sink_ndjson_size(df: pl.DataFrame, n: int) -> int:
    with tempfile.TemporaryDirectory() as td:
        sink_path = Path(td) / f"bench-{n}.jsonl"
        df.lazy().filter(pl.col("age") > 30).select(["id", "name", "department"]).sink_ndjson(
            sink_path
        )
        return os.path.getsize(sink_path)


def benchmark(fixture_dir: Path, sizes: list[int]) -> None:
    print(f"python-polars: {pl.__version__}")
    print(f"fixture_dir: {fixture_dir}")
    print()

    for n in sizes:
        loops = loops_for_size(n)
        cases = build_cases(n, fixture_dir)
        order = [
            "query_collect_like",
            "to_dicts",
            "scan_csv",
            "scan_parquet",
            "group_by_agg",
            "join_inner",
            "sink_ndjson_file",
        ]

        print(f"Rows{n}")
        for name in order:
            label, fn = cases[name]
            run_case(label, loops, fn)
        print()


def benchmark_single_case(fixture_dir: Path, n: int, case_name: str) -> None:
    loops = loops_for_size(n)
    cases = build_cases(n, fixture_dir)
    if case_name not in cases:
        raise ValueError(f"unknown case: {case_name}")
    label, fn = cases[case_name]
    us_per_op = run_case(label, loops, fn)
    print(
        f"RESULT case={case_name} size={n} us_per_op={us_per_op:.0f} maxrss_mib={maxrss_mib():.1f}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Python Polars materialization-heavy paths with this repo's Go bridge benchmarks."
    )
    parser.add_argument(
        "--fixture-dir",
        default="/tmp/polars-go-compare",
        help="Directory used for shared CSV/Parquet fixtures.",
    )
    parser.add_argument(
        "--sizes",
        default="4000,16000",
        help="Comma-separated row counts to benchmark.",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Only generate shared CSV/Parquet fixtures and exit.",
    )
    parser.add_argument(
        "--case",
        default="",
        help="Run only one case in this process and print a machine-readable RESULT line.",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=0,
        help="Single row count used together with --case.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sizes = [int(part) for part in args.sizes.split(",") if part]
    fixture_dir = Path(args.fixture_dir).expanduser().resolve()
    ensure_fixtures(fixture_dir, sizes)
    if args.prepare_only:
        print(f"prepared fixtures in {fixture_dir}")
        return
    if args.case:
        if args.size <= 0:
            raise ValueError("--size is required when --case is set")
        benchmark_single_case(fixture_dir, args.size, args.case)
        return
    benchmark(fixture_dir, sizes)


if __name__ == "__main__":
    main()
