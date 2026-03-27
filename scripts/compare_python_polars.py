#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gc
import os
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


def join_frames(n: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    left = pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "department": [f"dept_{i % 16:02d}" for i in range(n)],
            "salary": [50000 + (i % 17) * 1250 for i in range(n)],
        }
    )
    right = pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "level": [f"L{i % 6}" for i in range(n)],
            "bonus": [1000 + (i % 9) * 100 for i in range(n)],
        }
    )
    return left, right


def ensure_fixtures(fixture_dir: Path, sizes: list[int]) -> None:
    fixture_dir.mkdir(parents=True, exist_ok=True)
    for n in sizes:
        df = pl.DataFrame(frame_data(n))
        df.write_csv(fixture_dir / f"polars_compare_{n}.csv")
        df.write_parquet(fixture_dir / f"polars_compare_{n}.parquet")


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


def benchmark(fixture_dir: Path, sizes: list[int]) -> None:
    print(f"python-polars: {pl.__version__}")
    print(f"fixture_dir: {fixture_dir}")
    print()

    for n in sizes:
        loops = 20 if n == 4000 else 8
        df = pl.DataFrame(frame_data(n))
        left, right = join_frames(n)
        csv_path = fixture_dir / f"polars_compare_{n}.csv"
        parquet_path = fixture_dir / f"polars_compare_{n}.parquet"

        print(f"Rows{n}")
        run_case(
            "  QueryCollectLike(df.filter.select.to_dicts)",
            loops,
            lambda: df.filter(pl.col("age") > 30)
            .select(["name", "salary", "department"])
            .to_dicts(),
        )
        run_case("  ToDicts", loops, lambda: df.to_dicts())
        run_case(
            "  ScanCSV(filter+select+collect+to_dicts)",
            loops,
            lambda: pl.scan_csv(csv_path)
            .filter(pl.col("age") > 30)
            .select(["name", "salary", "department"])
            .collect()
            .to_dicts(),
        )
        run_case(
            "  ScanParquet(filter+select+collect+to_dicts)",
            loops,
            lambda: pl.scan_parquet(parquet_path)
            .filter(pl.col("age") > 30)
            .select(["name", "salary", "department"])
            .collect()
            .to_dicts(),
        )
        run_case(
            "  GroupByAgg(to_dicts)",
            loops,
            lambda: df.group_by("department")
            .agg(
                [
                    pl.col("salary").sum().alias("salary_sum"),
                    pl.col("id").count().alias("employee_count"),
                    pl.col("age").mean().alias("avg_age"),
                ]
            )
            .to_dicts(),
        )
        run_case(
            "  JoinInner(select.to_dicts)",
            loops,
            lambda: left.join(right, on="id", how="inner")
            .select(["id", "salary", "bonus"])
            .to_dicts(),
        )
        with tempfile.TemporaryDirectory() as td:
            sink_path = Path(td) / f"bench-{n}.jsonl"
            run_case(
                "  SinkNDJSONFile",
                loops,
                lambda: (
                    df.lazy()
                    .filter(pl.col("age") > 30)
                    .select(["id", "name", "department"])
                    .sink_ndjson(sink_path),
                    os.path.getsize(sink_path),
                )[1],
            )
        print()


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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sizes = [int(part) for part in args.sizes.split(",") if part]
    fixture_dir = Path(args.fixture_dir).expanduser().resolve()
    ensure_fixtures(fixture_dir, sizes)
    if args.prepare_only:
        print(f"prepared fixtures in {fixture_dir}")
        return
    benchmark(fixture_dir, sizes)


if __name__ == "__main__":
    main()
