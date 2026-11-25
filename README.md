# Spark Sentinel demo

This repository shows [Spark Sentinel](https://github.com/r4otxrhs3/spark-sentinel)'s GitHub
Action catching a Spark performance regression in a pull request, before it's merged.

`job/daily_report.py` is a small PySpark job: 3 million orders, three queries. On every pull
request, [the workflow](.github/workflows/performance.yml) runs the job twice on the same
runner, once on the base branch and once on the PR, and Spark Sentinel compares the two event
logs. It comments on the PR with what got slower and why, and fails the check if a slowdown is
confirmed.

## Try it: open the bad pull request

```bash
./scripts/open_demo_pr.sh slow-udf
```

The PR looks harmless: it moves a three-line `when/otherwise` expression into a Python function
"so it stays in sync with the pricing service". But a Python UDF runs row by row in a Python
worker, so every one of the 3 million rows is pickled, sent to Python and back. This is the
comment it got in a local run:

> ## Spark Sentinel diff
>
> 🔴 **1 query is slower.**
>
> _daily-report: local-1795616876381 → local-1795616908858_  
> _run time 9.6s → 13.6s · task time 9.1s → 20.7s_
>
> | | Query | Time | Why |
> |---|---|---|---|
> | 🔴 slower | Query 2<br><sub>writes out/bands</sub> | 674 ms → 3.5s | it now runs a row-by-row Python UDF |
>
> #### 🔴 Query 2
>
> - slowest task 344 ms → 3.1s
> - task time 1.4s → 12.4s
>
> <details><summary>Plan changes</summary>
>
> ```diff
>  +- * Project
> -   +- * Project
> +   +- BatchEvalPython
>        +- * Range
> ```
>
> </details>
>
> **New problems**
>
> - MED `PLAN005` Query 2: Python UDF size_band runs row by row
>
> <sub>3 queries: 1 slower, 2 unchanged; 1 new problem(s). Changes under 30% or under 1.5s / 8.0 MB are ignored (profile: laptop). Spark Sentinel 0.2.0</sub>

The check fails, so the PR can't be merged by accident. The comment explains why, and
PLAN005's finding links to the fix: use a built-in expression, or an Arrow (vectorized) UDF.

## Two more pull requests to try

| Script | The change | What Sentinel says |
|---|---|---|
| `./scripts/open_demo_pr.sh no-broadcast` | Turns off broadcast joins "to avoid driver OOM" | Query 0 is 1.7x slower: the broadcast join became a sort-merge join, which adds 2 shuffles, after `spark.sql.autoBroadcastJoinThreshold` changed to -1. The check fails. |
| `./scripts/open_demo_pr.sh rename-column` | Renames an output column, nothing else | No performance regression: same speed, the columns it writes changed (orders → order_count). The check passes. |

The harmless PR matters as much as the bad ones: a check that cries wolf gets ignored.
[`sentinel.toml`](sentinel.toml) sets how big a change must be to count (30% and 1.5 s here),
and a slowdown only fails the check when something visible changed with it: the plan, the data
shuffled or spilled, or a setting.

## Set it up in your own repository

1. Copy [`.github/workflows/performance.yml`](.github/workflows/performance.yml) and
   [`sentinel.toml`](sentinel.toml).
2. Replace the two "Run the job" steps with a small, representative run of your own job, with
   event logging on (`spark.eventLog.enabled=true`, `spark.eventLog.dir` pointing at
   `$SPARK_EVENTS`). Use small, fixed input data: the same input for both runs.
3. Keep `permissions: pull-requests: write`, so the Action can comment.

Pull requests from forks get a read-only token: they get the report in the job summary and the
pass/fail check, but no comment.

## Run it locally

```bash
pip install -r requirements.txt spark-sentinel      # Java 17 or 21 needed for Spark
SPARK_EVENTS=/tmp/events/base python job/daily_report.py
git apply demo-prs/slow-udf.patch
SPARK_EVENTS=/tmp/events/pr python job/daily_report.py
git checkout job/daily_report.py
sentinel diff /tmp/events/base /tmp/events/pr --config sentinel.toml
```
