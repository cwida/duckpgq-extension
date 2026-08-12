#!/usr/bin/env python3
import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_ROOT = REPO_ROOT / "data" / "ldbc-pathfinding" / "results"


def read_trials(results_root, run_label):
    trials = defaultdict(list)
    pattern = "graphalytics_*/summary_*modesql_match_cache_*.csv"
    for path in results_root.glob(pattern):
        with path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        if not rows or rows[0].get("benchmark_run_label") != run_label:
            continue
        trials[rows[0]["scale_factor"]].append(rows)
    return trials


def mean(values):
    return statistics.mean(values) if values else None


def stdev(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


def optional_int(value):
    return None if value in (None, "", "NULL") else int(value)


def summarize_dataset(dataset, trials):
    cold = [float(trial[0]["query_s"]) for trial in trials]
    warm = [float(row["query_s"]) for trial in trials for row in trial if row["mode"] == "sql_match_warm"]
    q10 = [float(trial[9]["amortized_query_s"]) for trial in trials if len(trial) >= 10]
    first = trials[0][0]
    checks = [row.get("graphalytics_reference_match") == "1" for trial in trials for row in trial]
    return {
        "dataset": dataset,
        "scale": first.get("dataset_metadata_graphalytics_scale", ""),
        "vertex_count": int(first["dataset_metadata_person_rows"]),
        "directed_edge_count": int(first["dataset_metadata_person_knows_person_rows"]),
        "pair_count": int(first["pair_count"]),
        "reachable_count": int(first["reachable_count"]),
        "total_len": optional_int(first["total_len"]),
        "min_len": optional_int(first["min_len"]),
        "max_len": optional_int(first["max_len"]),
        "trial_count": len(trials),
        "warm_query_count": len(warm),
        "cold_mean_s": mean(cold),
        "cold_stdev_s": stdev(cold),
        "warm_mean_s": mean(warm),
        "warm_stdev_s": stdev(warm),
        "q10_amortized_mean_s": mean(q10),
        "reuse_speedup_q10": mean(cold) / mean(q10) if q10 else None,
        "all_reference_checks_passed": all(checks),
    }


def summarize_run(results_root, run_label):
    trials = read_trials(results_root, run_label)
    return [summarize_dataset(dataset, dataset_trials) for dataset, dataset_trials in sorted(trials.items())]


def format_optional(value):
    return "" if value is None else f"{value:.6f}"


def write_csv(path, rows):
    fields = list(rows[0])
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def comparison_rows(current, baseline):
    baseline_by_dataset = {row["dataset"]: row for row in baseline}
    rows = []
    for row in current:
        old = baseline_by_dataset.get(row["dataset"])
        if old is None:
            continue
        result = {"dataset": row["dataset"]}
        for name in ("cold_mean_s", "warm_mean_s", "q10_amortized_mean_s"):
            current_value = row[name]
            baseline_value = old[name]
            result[f"count_only_{name}"] = format_optional(baseline_value)
            result[f"path_length_{name}"] = format_optional(current_value)
            result[f"ratio_{name}"] = format_optional(current_value / baseline_value)
        rows.append(result)
    return rows


def diagnostic_rows(results_root, run_label):
    rows = []
    for dataset, trials in sorted(read_trials(results_root, run_label).items()):
        cold = next(row for row in trials[0] if row["mode"] == "sql_match_cold")
        query_s = float(cold["query_s"])
        phases = {
            "pair_analysis_s": float(cold.get("pair_analysis_s") or 0),
            "csr_build_s": float(cold.get("local_csr_forward_s") or 0),
            "bfs_s": float(cold.get("bfs_s") or 0),
            "scatter_s": float(cold.get("scatter_s") or 0),
        }
        measured_s = sum(phases.values())
        rows.append(
            {
                "dataset": dataset,
                "query_s": f"{query_s:.6f}",
                **{name: f"{value:.6f}" for name, value in phases.items()},
                "other_wall_s": f"{max(0.0, query_s - measured_s):.6f}",
                "csr_share": f"{phases['csr_build_s'] / query_s:.6f}",
                "aggregation_cpu_s": cold.get("aggregation_cpu_s", ""),
                "reference_match": cold.get("graphalytics_reference_match", ""),
            }
        )
    return rows


def write_markdown(path, current, comparison, failed):
    comparison_by_dataset = {row["dataset"]: row for row in comparison}
    lines = [
        "# Graphalytics path-length validated SQL MATCH sweep",
        "",
        "| Dataset | Scale | Vertices | Directed adjacency rows | Cold mean | Warm mean | Q10 mean | Q10 reuse | Reference |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in current:
        lines.append(
            f"| {row['dataset']} | {row['scale']} | {row['vertex_count']:,} | "
            f"{row['directed_edge_count']:,} | {row['cold_mean_s']:.3f}s | {row['warm_mean_s']:.3f}s | "
            f"{row['q10_amortized_mean_s']:.3f}s | {row['reuse_speedup_q10']:.2f}x | "
            f"{'pass' if row['all_reference_checks_passed'] else 'FAIL'} |"
        )
    lines.extend(["", "## Count-only comparison", ""])
    lines.extend([
        "| Dataset | Cold ratio | Warm ratio | Q10 ratio |",
        "|---|---:|---:|---:|",
    ])
    for row in current:
        comparison_row = comparison_by_dataset.get(row["dataset"])
        if comparison_row is None:
            continue
        lines.append(
            f"| {row['dataset']} | {float(comparison_row['ratio_cold_mean_s']):.3f}x | "
            f"{float(comparison_row['ratio_warm_mean_s']):.3f}x | "
            f"{float(comparison_row['ratio_q10_amortized_mean_s']):.3f}x |"
        )
    if failed:
        lines.extend(["", "## Capacity limits", ""])
        lines.extend(f"- `{dataset}`: cold query was killed by the operating system before a result row was written." for dataset in failed)
    path.write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--current-label", default="graphalytics-path-length-validated")
    parser.add_argument("--baseline-label", default="sql-match-cache-core-20260812")
    parser.add_argument("--diagnostic-label", default="graphalytics-path-length-diagnostic")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--failed", nargs="*", default=[])
    args = parser.parse_args()

    current = summarize_run(args.results_root, args.current_label)
    baseline = summarize_run(args.results_root, args.baseline_label)
    if not current:
        raise SystemExit(f"No results found for run label {args.current_label!r}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = args.output_dir / "graphalytics_path_length_summary.csv"
    comparison_csv = args.output_dir / "graphalytics_path_length_vs_count_only.csv"
    diagnostic_csv = args.output_dir / "graphalytics_path_length_diagnostics.csv"
    report_md = args.output_dir / "graphalytics_path_length_report.md"
    write_csv(summary_csv, current)
    comparison = comparison_rows(current, baseline)
    write_csv(comparison_csv, comparison)
    write_csv(diagnostic_csv, diagnostic_rows(args.results_root, args.diagnostic_label))
    write_markdown(report_md, current, comparison, args.failed)
    print(summary_csv)
    print(comparison_csv)
    print(diagnostic_csv)
    print(report_md)


if __name__ == "__main__":
    main()
