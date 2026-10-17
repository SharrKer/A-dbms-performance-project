# DBMS Performance Project

A Java 21 benchmark comparing data operations in a plain text file, PostgreSQL,
and openGauss. The project asks:

1. How do file operations and database operations differ for search and update
   workloads?
2. How do PostgreSQL and openGauss compare under the same sequential workloads?
3. How does increasing query concurrency affect PostgreSQL and openGauss?

The repository contains the complete benchmark implementation and the immutable
raw measurements used by the project report. Detailed methodology and session
traceability are in [`experiment.md`](experiment.md).

## Systems and datasets

| System/profile | Description |
|---|---|
| Java file | UTF-8 pipe-delimited file scanned or rewritten by Java |
| PostgreSQL | JDBC against `public.movies` / `public.movies_large` |
| openGauss | JDBC against `gaussdb.movies` / `gaussdb.movies_large` |
| Original | 9,204 movie records |
| Large | 1,012,440 records, deterministic 110× expansion |

Completed experiments:

- E1/E2: Original exact-title and substring queries;
- E3/E4: Large exact-title and substring queries, reusing E1/E2 operations;
- U1/U2: Original single-row and 1,000-row updates;
- U3/U4: Large single-row and 110,000-row updates;
- C1: concurrent Large substring queries at concurrency 1, 2, 4, and 8.

## Repository layout

```text
.
├── java/                       Canonical multi-file formal benchmark
├── submission/                 Complete formal benchmark as one Java file
├── submission-edition/         Concise Blackboard source (not timing-equivalent)
├── data/working/largeCSV/      Portable generator and validation report
├── result/measurements/        Preserved raw and summary measurement CSVs
├── experiment.md               Methods, ID mapping, and formal session index
└── README.md                   Project entry point
```

### Source editions

- [`java/`](java/) is the canonical implementation that produced the formal CSVs.
- [`submission/BenchmarkRunner.java`](submission/BenchmarkRunner.java) packages
  the complete formal implementation into one source file.
- [`submission-edition/BenchmarkRunner.java`](submission-edition/BenchmarkRunner.java)
  is the final concise Blackboard edition. It demonstrates the core file, JDBC,
  transaction, and concurrency mechanisms, but omits the full measurement and
  validation framework. It cannot reproduce the report's formal timings.

Compile these editions separately because they intentionally reuse class names
in the default package.

## Requirements

- JDK 21 (the formal environment used JDK 21.0.10);
- PostgreSQL JDBC driver `lib/postgresql-42.7.14.jar`;
- openGauss JDBC driver `lib/opengauss-jdbc-7.0.0-RC3.jar`;
- PostgreSQL and/or openGauss with the required tables already loaded;
- authorized local copies of the dataset files described below.

Third-party jars and datasets are intentionally not redistributed in this public
repository. Place the driver jars under `lib/` and the data at these paths:

| Local file | Size/rows | SHA-256 |
|---|---:|---|
| `data/working/originalCSV/movies` | 9,204 rows | `39305f7e8e33b60dbc9d201d9e74560f5c343b15f4e656b4637501f15c4460ad` |
| `data/working/largeCSV/movies_large.csv` | 1,012,440 rows | `5232ba48693176939ed1fea3f15c2ab269145b6096d71618cd8b3e5bfff8d2e4` |

The large file can be generated deterministically from an authorized Original
copy with
[`generate_movies_large.py`](data/working/largeCSV/generate_movies_large.py).
Its expected structure and hash are recorded in
[`validation_report.md`](data/working/largeCSV/validation_report.md).

No redistribution licence was found in the supplied dataset files, so they stay
Git-ignored until their source and publication terms are confirmed.

## Database configuration

Passwords are read only from environment variables:

```powershell
$env:BENCHMARK_POSTGRES_PASSWORD = "<password>"
$env:BENCHMARK_OPENGAUSS_PASSWORD = "<password>"
```

Optional overrides:

| Variable | Default |
|---|---|
| `BENCHMARK_POSTGRES_URL` | `jdbc:postgresql://127.0.0.1:5432/filmdb1` |
| `BENCHMARK_POSTGRES_USER` | `postgres` |
| `BENCHMARK_POSTGRES_SCHEMA` | `public` |
| `BENCHMARK_OPENGAUSS_URL` | `jdbc:opengauss://127.0.0.1:8888/postgres` |
| `BENCHMARK_OPENGAUSS_USER` | `gaussdb` |
| `BENCHMARK_OPENGAUSS_SCHEMA` | `gaussdb` |

Do not store passwords in source files, shell scripts, `.env` files, command
history, or Git commits.

## Build

Run commands from the repository root in PowerShell.

### Canonical formal implementation

```powershell
Remove-Item out -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force out | Out-Null
$sources = Get-ChildItem java -File -Filter *.java | ForEach-Object { $_.FullName }
javac -encoding UTF-8 -Xlint:all -cp "lib/*" -d out $sources
```

### Complete single-file implementation

```powershell
Remove-Item out -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force out | Out-Null
javac -encoding UTF-8 -Xlint:all -cp "lib/*" -d out submission/BenchmarkRunner.java
```

### Blackboard Submission Edition

```powershell
Remove-Item out -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force out | Out-Null
javac -encoding UTF-8 -Xlint:all -cp "lib/*" -d out submission-edition/BenchmarkRunner.java
```

## Run the complete benchmark

Show sequential options:

```powershell
java -cp "out;lib/*" BenchmarkRunner --help
```

Representative selections:

```powershell
# Original E1/E2/U1/U2 using the configured systems
java -cp "out;lib/*" BenchmarkRunner --dataset original

# Report E3: Large exact-title query (CSV operation ID E1)
java -cp "out;lib/*" BenchmarkRunner --dataset large --experiment E1

# Report E4: Large substring query (CSV operation ID E2)
java -cp "out;lib/*" BenchmarkRunner --dataset large --experiment E2

# Large update experiments
java -cp "out;lib/*" BenchmarkRunner --dataset large --experiment U3
java -cp "out;lib/*" BenchmarkRunner --dataset large --experiment U4

# Example cyclic order
java -cp "out;lib/*" BenchmarkRunner --system-order postgresql,opengauss,java
```

C1 is a separate read-only runner:

```powershell
java -cp "out;lib/*" ConcurrencyBenchmarkRunner --help
java -cp "out;lib/*" ConcurrencyBenchmarkRunner
```

The default C1 command runs the complete 1/2/4/8 × two-system × three-repetition
matrix. It should not be used as a quick smoke test.

## Formal results

| Suite | Result file |
|---|---|
| Original E1/E2/U1/U2 | [`benchmark_raw.csv`](result/measurements/benchmark_raw.csv) |
| Large E3/E4 | [`benchmark_e3_e4_large_raw.csv`](result/measurements/benchmark_e3_e4_large_raw.csv) |
| Large U3/U4 | [`benchmark_u3_u4_large_raw.csv`](result/measurements/benchmark_u3_u4_large_raw.csv) |
| C1 per-query samples | [`benchmark_c1_concurrency_raw.csv`](result/measurements/benchmark_c1_concurrency_raw.csv) |
| C1 phase summaries | [`benchmark_c1_concurrency_summary.csv`](result/measurements/benchmark_c1_concurrency_summary.csv) |

Measurement files are append-only artifacts. C1 files contain two short smoke
sessions in addition to the formal session; use the filters documented in
[`experiment.md`](experiment.md). Do not treat warm-up rows as formal results.

### Public measurement copies

The canonical unmodified measurement files are retained in the author's working
repository. In this public repository:

- `benchmark_raw.csv` and `benchmark_u3_u4_large_raw.csv` are public-redacted
  copies. Only the workstation path prefix in 90 `notes` fields per file was
  replaced with the repository-relative suffix. All numerical measurements,
  metadata, row counts, schema, and operation IDs are unchanged.
  - `benchmark_raw.csv`: public SHA-256
    `ecb34edc05a3e297cf01de7023614e6e0dc76176f1afdbea63f64183c3a3e68e`;
    original SHA-256
    `5d0daabacdad01f63a7dd797f677eba8309d76b85d79e2cf5a3d2b839f799821`.
  - `benchmark_u3_u4_large_raw.csv`: public SHA-256
    `13ccaaa193992f3f78f3b6654b1efbbb49af41fd25ca66d99acb66b067d30d34`;
    original SHA-256
    `fe384882213798a4d45fc36c5d791edcefa24a7ec529e929c6407b3ae4b63b48`.
- The other three CSVs are byte-for-byte copies with unchanged SHA-256 values.

### Report-to-CSV experiment mapping

The raw CSVs keep their historical operation IDs. Report experiment E3 is the
Large exact-title search stored under `experiment_id=E1`; report experiment E4
is the Large substring search stored under `experiment_id=E2`. These IDs are
never rewritten; see [`experiment.md`](experiment.md) for the full mapping.

### Canonical implementation

[`java/`](java/) is the canonical formal implementation that produced the
measurement files above. `submission-edition/BenchmarkRunner.java` is the
concise Blackboard implementation and is not timing-equivalent.

No benchmark needs to be rerun to inspect or reproduce the reported analysis;
the preserved CSVs are the authoritative raw records.
