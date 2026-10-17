# Experiment Specification and Result Index

This document records the implemented benchmark methodology and identifies the
raw files that preserve each completed run. It is intentionally a methods and
traceability reference, not a duplicate of the project report.

## 1. Scope and experiment IDs

The canonical implementation is the multi-file Java 21 program in [`java/`](java/).
Sequential experiments compare Java file operations, PostgreSQL through JDBC,
and openGauss through JDBC. C1 compares PostgreSQL and openGauss only.

| Report ID | Dataset | Operation | Expected count | Raw CSV operation ID |
|---|---|---|---:|---|
| E1 | Original | Exact title: `Ardh Satya` | 1 | E1 |
| E2 | Original | Title contains `Batman` | 8 | E2 |
| E3 | Large | Exact title: `Ardh Satya` | 1 | E1 |
| E4 | Large | Title contains `Batman` | 880 | E2 |
| U1 | Original | Set `runtime=999` for `movieid=5` | 1 | U1 |
| U2 | Original | Set `runtime=999` for IDs 1–1000 | 1,000 | U2 |
| U3 | Large | Set `runtime=999` for `movieid=5` | 1 | U3 |
| U4 | Large | Update the first 1,000 IDs in each of 110 batches | 110,000 | U4 |
| C1 | Large | Concurrent E4-style substring queries | 880/query | C1 |

E3 and E4 deliberately reuse the E1 and E2 algorithms. The historical
large-query CSV therefore keeps `experiment_id=E1` and `experiment_id=E2`.
Interpret those rows together with `dataset=movies_large`:

```text
movies_large + E1 operation ID = report experiment E3
movies_large + E2 operation ID = report experiment E4
```

No historical operation IDs were rewritten during repository cleanup.

## 2. Dataset and database mapping

| Profile | Rows | Java source | PostgreSQL | openGauss |
|---|---:|---|---|---|
| Original | 9,204 | `data/working/originalCSV/movies` | `public.movies` | `gaussdb.movies` |
| Large | 1,012,440 | `data/working/largeCSV/movies_large.csv` | `public.movies_large` | `gaussdb.movies_large` |

Both files are UTF-8 pipe-delimited records with this header:

```text
movieid|title|country|year_released|runtime
```

Protected dataset hashes:

| Dataset | SHA-256 |
|---|---|
| Original | `39305f7e8e33b60dbc9d201d9e74560f5c343b15f4e656b4637501f15c4460ad` |
| Large | `5232ba48693176939ed1fea3f15c2ab269145b6096d71618cd8b3e5bfff8d2e4` |

The large dataset is a deterministic 110× expansion. Its generator and the
20-check validation record are in
[`data/working/largeCSV/`](data/working/largeCSV/).

## 3. Sequential E/U methodology

### 3.1 Repetitions and ordering

Each experiment/system combination uses:

- 5 warm-up iterations;
- 10 formal iterations;
- warm-ups before formal measurements;
- `System.nanoTime()` for elapsed time;
- append-only CSV output containing both run types.

The original and E3/E4 suites use three cyclic sessions to vary fixed system
position:

| Session position | System order |
|---|---|
| S1 | Java → PostgreSQL → openGauss |
| S2 | PostgreSQL → openGauss → Java |
| S3 | openGauss → Java → PostgreSQL |

U3 and U4 were launched separately, with the same three orders for each
experiment. The CSV records session IDs and order metadata so execution order
can be reconstructed.

### 3.2 Preflight and correctness

Untimed setup loads and validates the selected source, rejects duplicate IDs,
and derives expected query/update records. For the Original profile, database
rows are compared with the complete file data. Large-table preflight checks row
count, columns, primary key, and benchmark-relevant indexes without loading two
additional million-row snapshots.

Every timed attempt is checked before the next attempt:

- queries consume and compare the complete result set, not only its count;
- Java updates reread and compare the complete output file;
- JDBC updates check affected rows and target values inside the transaction;
- JDBC targets are reread after rollback to prove restoration;
- failures are logged as `FAIL` and stop the session.

### 3.3 Timing boundaries

| Operation | Included in elapsed time | Outside elapsed time |
|---|---|---|
| Java query | Open, read and parse every row, match, close | Baseline comparison and CSV append |
| JDBC query | `executeQuery`, transfer/read all five fields, ResultSet close | Connection and PreparedStatement setup, comparison, CSV append |
| Java update | Open source/output, parse, transform, write complete copy, close | Old-output cleanup, full-output validation, source safety check, CSV append |
| JDBC update | `executeUpdate()` only | Validation, rollback, post-rollback verification, CSV append |

Java always scans the complete source, including exact-title queries. JDBC
connections and prepared statements are established before timing.

The Java update figure includes a complete output-file rewrite. The JDBC update
figure is an uncommitted statement execution followed by rollback; it is not a
commit or durable transaction time. These persistence semantics are different
and must be considered when interpreting update comparisons.

### 3.4 Update safety

Java U1–U4 write only to session-specific files under
`result/benchmark-work/`; the source path is never an output target. Those work
files are generated artifacts and are not formal results.

JDBC U1–U4 disable auto-commit, run and validate the update in one transaction,
then call `rollback()`. The original targets are reread after rollback. U4 uses
110 disjoint `BETWEEN` ranges joined with `OR`; its JDBC predicate does not use
`MOD`. U3/U4 preflight uses plain `EXPLAIN UPDATE`, never `EXPLAIN ANALYZE`.

## 4. C1 concurrency methodology

C1 asks how increasing query concurrency affects PostgreSQL and openGauss for:

```sql
SELECT movieid, title, country, year_released, runtime
FROM <schema>.movies_large
WHERE title LIKE '%Batman%';
```

| Setting | Formal value |
|---|---|
| Systems | PostgreSQL and openGauss |
| Concurrency | 1, 2, 4, 8 workers |
| Warm-up | 5 seconds/configuration |
| Formal window | 20 seconds/configuration |
| Repetitions | 3 |
| Expected result | 880 complete records/query |

Each worker owns an independent read-only JDBC connection and PreparedStatement.
Workers wait on a `CountDownLatch` and begin from one coordinated monotonic start.
They issue continuous closed-loop queries without sleeps or a fixed query count.

A query is admitted only if its start timestamp is strictly before the deadline.
No new query starts after the deadline, but admitted work drains to completion.
Actual duration runs from coordinated release through the last worker finish.

```text
throughput = successful completed queries / actual drain-inclusive duration
```

Per-query latency includes `executeQuery`, transfer, all-five-field reading,
inline complete-result validation, and ResultSet close. Median uses the ordinary
middle value (or mean of the two middle values). P95 is nearest-rank
`ceil(0.95 × n)` over successful queries. Warm-up and formal rows are stored
separately; formal analysis must filter `phase=formal`.

The default two-system order is balanced across the 4×3 concurrency/repetition
matrix: each DBMS runs first six times.

## 5. Formal result index

### 5.0 Public measurement copies

The canonical, unmodified measurement files remain in the author's working
repository. This public repository publishes byte-for-byte copies of three CSVs
and public-redacted copies of the two CSVs whose `notes` fields recorded
workstation paths.

| File | Public copy | Original SHA-256 | Public SHA-256 |
|---|---|---|---|
| `benchmark_raw.csv` | 90 `notes` paths shortened to repository-relative suffixes | `5d0daabacdad01f63a7dd797f677eba8309d76b85d79e2cf5a3d2b839f799821` | `ecb34edc05a3e297cf01de7023614e6e0dc76176f1afdbea63f64183c3a3e68e` |
| `benchmark_u3_u4_large_raw.csv` | 90 `notes` paths shortened to repository-relative suffixes | `fe384882213798a4d45fc36c5d791edcefa24a7ec529e929c6407b3ae4b63b48` | `13ccaaa193992f3f78f3b6654b1efbbb49af41fd25ca66d99acb66b067d30d34` |
| `benchmark_e3_e4_large_raw.csv` | byte-for-byte copy | `f85cb22cb6742c5f74c22c49adeeeb77e70049ca14b42008ef038896f1a47928` | identical |
| `benchmark_c1_concurrency_raw.csv` | byte-for-byte copy | `34dd406d47a54bc60040745bf7fd16df1d357fe2d9c18dc36d4823415368b914` | identical |
| `benchmark_c1_concurrency_summary.csv` | byte-for-byte copy | `9e2845c4f997e8b150d13a62947107f15b9cfbbb5febf313c6debb355187c574` | identical |

For the two redacted files, only the workstation path prefix in Java-update
output paths inside `notes` was replaced with the repository-relative suffix.
The row counts, CSV schema, all numerical measurement text, session IDs,
statuses, operation IDs, and every other byte are unchanged. The removed prefix
is deliberately not reproduced in this public documentation.

| Suite | File | Contents | Public SHA-256 |
|---|---|---|---|
| Original E1/E2/U1/U2 | [`benchmark_raw.csv`](result/measurements/benchmark_raw.csv) | 540 rows; three 180-row sessions | `ecb34edc05a3e297cf01de7023614e6e0dc76176f1afdbea63f64183c3a3e68e` |
| Large E3/E4 | [`benchmark_e3_e4_large_raw.csv`](result/measurements/benchmark_e3_e4_large_raw.csv) | 270 rows; three 90-row sessions; CSV IDs E1/E2 | `f85cb22cb6742c5f74c22c49adeeeb77e70049ca14b42008ef038896f1a47928` |
| Large U3/U4 | [`benchmark_u3_u4_large_raw.csv`](result/measurements/benchmark_u3_u4_large_raw.csv) | 270 rows; six 45-row sessions | `13ccaaa193992f3f78f3b6654b1efbbb49af41fd25ca66d99acb66b067d30d34` |
| C1 per query | [`benchmark_c1_concurrency_raw.csv`](result/measurements/benchmark_c1_concurrency_raw.csv) | 21,123 completed-query rows | `34dd406d47a54bc60040745bf7fd16df1d357fe2d9c18dc36d4823415368b914` |
| C1 phase summary | [`benchmark_c1_concurrency_summary.csv`](result/measurements/benchmark_c1_concurrency_summary.csv) | 56 phase rows | `9e2845c4f997e8b150d13a62947107f15b9cfbbb5febf313c6debb355187c574` |

### 5.1 Sequential session IDs

| Suite | Session ID | Experiment(s) | System order |
|---|---|---|---|
| Original S1 | `20261014T153242234Z-1627cee0` | E1/E2/U1/U2 | Java → PostgreSQL → openGauss |
| Original S2 | `20261015T013330981Z-64511966` | E1/E2/U1/U2 | PostgreSQL → openGauss → Java |
| Original S3 | `20261015T013834669Z-addb068e` | E1/E2/U1/U2 | openGauss → Java → PostgreSQL |
| E3/E4 S1 | `20261015T075458965Z-593b2c19` | E3/E4 as CSV E1/E2 | Java → PostgreSQL → openGauss |
| E3/E4 S2 | `20261015T080634466Z-b8ab4c0e` | E3/E4 as CSV E1/E2 | PostgreSQL → openGauss → Java |
| E3/E4 S3 | `20261015T080724192Z-03d36945` | E3/E4 as CSV E1/E2 | openGauss → Java → PostgreSQL |
| U3 S1/S2/S3 | `20261015T130825180Z-ee785477`, `20261015T131302411Z-fc8fb12b`, `20261015T131522864Z-c579a63a` | U3 | Three cyclic orders |
| U4 S1/S2/S3 | `20261015T130905381Z-cd275844`, `20261015T131326311Z-c2da2cc0`, `20261015T132303269Z-837fe35c` | U4 | Three cyclic orders |

All sequential rows above have `status=PASS`. Each experiment/system combination
contains 5 warm-ups and 10 formal measurements.

### 5.2 C1 smoke and formal sessions

The C1 files intentionally retain two short smoke sessions as audit evidence:

| Session ID | Purpose | Systems/configurations |
|---|---|---|
| `20261016T010926771Z-5a015045` | PostgreSQL smoke | concurrency 1/2, repetition 1, 1 s + 2 s |
| `20261016T010952058Z-c3fce7e2` | openGauss smoke | concurrency 1/2, repetition 1, 1 s + 2 s |
| `20261016T013615925Z-b3b1668d` | Formal C1 matrix | both systems, concurrency 1/2/4/8, repetitions 1–3, 5 s + 20 s |

Formal C1 analysis uses only:

```text
session_id = 20261016T013615925Z-b3b1668d
phase = formal
status = PASS
```

That session contributes 48 summary rows: 24 warm-up and 24 formal. All C1 raw
and summary rows report `status=PASS`.

## 6. Source editions

- [`java/`](java/) is the **canonical formal implementation** that produced
  every formal measurement in Section 5.
- [`submission/BenchmarkRunner.java`](submission/BenchmarkRunner.java) is the
  same complete formal implementation packaged as one source file.
- [`submission-edition/BenchmarkRunner.java`](submission-edition/BenchmarkRunner.java)
  is the concise Blackboard edition. It omits the formal repetition, CSV,
  metadata and complete-validation framework and **must not** be used to
  reproduce or compare with the formal timings.

For the report-to-CSV mapping: report experiment E3 is the Large exact-title
search and is stored under raw `experiment_id=E1`; report experiment E4 is the
Large substring search and is stored under raw `experiment_id=E2`. These
historical operation IDs are intentionally preserved in the public CSVs.

Build and run instructions are in [`README.md`](README.md).
