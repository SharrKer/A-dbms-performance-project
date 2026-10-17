import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ConcurrencyBenchmarkRunner {
    private static final String EXPERIMENT_ID = "C1";
    private static final String DATASET = "movies_large";
    private static final Path DATA_PATH = Path.of(
        "data", "working", "largeCSV", "movies_large.csv"
    );
    private static final int RECORD_COUNT = 1_012_440;
    private static final int EXPECTED_RESULT_COUNT = 880;
    private static final List<Integer> DEFAULT_CONCURRENCIES =
        List.of(1, 2, 4, 8);
    private static final int DEFAULT_WARMUP_SECONDS = 5;
    private static final int DEFAULT_FORMAL_SECONDS = 20;
    private static final int DEFAULT_REPETITIONS = 3;
    private static final long READY_TIMEOUT_SECONDS = 30;
    private static final DateTimeFormatter SESSION_TIME =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'")
            .withZone(ZoneOffset.UTC);

    private ConcurrencyBenchmarkRunner() {
    }

    public static void main(String[] args) {
        int exitCode = run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static int run(String[] args) {
        Configuration configuration;
        try {
            configuration = Configuration.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Configuration error: " + e.getMessage());
            System.err.println("Use --help for usage.");
            return 2;
        }

        if (configuration.help()) {
            printUsage();
            return 0;
        }

        try {
            ConcurrencyCsvLogger.validateFiles();
            BenchmarkData data = BenchmarkData.load(
                DATA_PATH,
                RECORD_COUNT,
                EXPECTED_RESULT_COUNT,
                false,
                List.of()
            );
            ExpectedResult expected = ExpectedResult.from(
                data.expectedQuery(BenchmarkExperiment.E2)
            );

            System.out.println("C1 concurrency session: "
                + configuration.sessionId());
            System.out.println("Dataset: " + DATASET + " ("
                + data.records().size() + " rows)");
            System.out.println("Query: title LIKE '%"
                + BenchmarkData.SUBSTRING + "%'");
            System.out.println("Expected complete result: "
                + expected.size() + " records");
            System.out.println("Systems: " + configuration.systems());
            System.out.println("Concurrency levels: "
                + configuration.concurrencies());
            System.out.println("Warm-up: "
                + configuration.warmupSeconds() + " s; formal: "
                + configuration.formalSeconds() + " s; repetitions: "
                + configuration.repetitions());
            System.out.println("Raw output: "
                + ConcurrencyCsvLogger.rawFile());
            System.out.println("Summary output: "
                + ConcurrencyCsvLogger.summaryFile());

            Map<BenchmarkSystem, DatabaseConfiguration> databases =
                databaseConfigurations(configuration.systems());
            preflight(configuration.systems(), databases, data);
            printExecutionSchedule(configuration);

            int configurationOrder = 0;
            for (int concurrencyIndex = 0;
                    concurrencyIndex
                        < configuration.concurrencies().size();
                    concurrencyIndex++) {
                int concurrency = configuration.concurrencies().get(
                    concurrencyIndex
                );
                for (int repetition = 1;
                        repetition <= configuration.repetitions();
                        repetition++) {
                    List<BenchmarkSystem> systemOrder =
                        configuration.systemOrder(
                            concurrencyIndex, repetition
                        );
                    String systemOrderValue = systemOrderValue(systemOrder);
                    for (BenchmarkSystem system : systemOrder) {
                        configurationOrder++;
                        runConfiguration(
                            configuration,
                            databases.get(system),
                            expected,
                            system,
                            concurrency,
                            repetition,
                            systemOrderValue,
                            configurationOrder
                        );
                    }
                }
            }

            System.out.println(
                "C1 session completed successfully. Formal results are "
                    + "separate from warm-up rows by the phase column."
            );
            return 0;
        } catch (Exception e) {
            System.err.println(
                "C1 session failed: " + failureMessage(e)
            );
            return 1;
        }
    }

    private static Map<BenchmarkSystem, DatabaseConfiguration>
        databaseConfigurations(List<BenchmarkSystem> systems) {
        Map<BenchmarkSystem, DatabaseConfiguration> configurations =
            new EnumMap<>(BenchmarkSystem.class);
        for (BenchmarkSystem system : systems) {
            configurations.put(
                system, DatabaseConfiguration.forSystem(system)
            );
        }
        return configurations;
    }

    private static void preflight(
        List<BenchmarkSystem> systems,
        Map<BenchmarkSystem, DatabaseConfiguration> databases,
        BenchmarkData data
    ) throws Exception {
        for (BenchmarkSystem system : systems) {
            DatabaseConfiguration database = databases.get(system);
            System.out.println("Preflight connecting to "
                + system.csvName() + "...");
            Class.forName(database.driverClass());
            try (Connection connection = DriverManager.getConnection(
                    database.url(),
                    database.user(),
                    database.password())) {
                connection.setReadOnly(true);
                JdbcBenchmark.Inspection inspection = JdbcBenchmark.inspect(
                    connection,
                    database.schema(),
                    DATASET,
                    false
                );
                if (inspection.rowCount() != data.records().size()) {
                    throw new IllegalStateException(
                        system.csvName() + " row count differs from source: "
                            + "expected=" + data.records().size()
                            + ", actual=" + inspection.rowCount()
                    );
                }
                System.out.println(
                    "Preflight passed for " + system.csvName()
                        + ": rows=" + inspection.rowCount()
                        + ", relevant indexes="
                        + inspection.tableMetadata().indexState()
                );
                for (String warning : inspection.warnings()) {
                    System.out.println("WARNING [" + system.csvName()
                        + "]: " + warning);
                }
            }
        }
    }

    private static void runConfiguration(
        Configuration configuration,
        DatabaseConfiguration database,
        ExpectedResult expected,
        BenchmarkSystem system,
        int concurrency,
        int repetition,
        String systemOrder,
        int configurationOrder
    ) throws Exception {
        System.out.println();
        System.out.println("Configuration " + configurationOrder + ": "
            + system.csvName() + ", concurrency=" + concurrency
            + ", repetition=" + repetition
            + ", system_order=" + systemOrder);
        System.out.println("  setup: opening " + concurrency
            + " independent connection(s) before timing");

        List<WorkerResource> resources = new ArrayList<>();
        PhaseResult warmup = null;
        PhaseResult formal = null;
        List<String> cleanupProblems = new ArrayList<>();
        Exception executionFailure = null;

        try {
            for (int workerId = 1;
                    workerId <= concurrency;
                    workerId++) {
                resources.add(WorkerResource.open(
                    workerId, database
                ));
            }

            System.out.println("  warmup: "
                + configuration.warmupSeconds() + " s");
            warmup = runPhase(
                "warmup",
                configuration.warmupSeconds(),
                resources,
                expected,
                system,
                concurrency,
                repetition
            );
            printPhaseSummary(warmup);
            if (!warmup.passed()) {
                throw new IllegalStateException(
                    "Warm-up failed; formal measurement was not started"
                );
            }

            System.out.println("  formal: "
                + configuration.formalSeconds() + " s");
            formal = runPhase(
                "formal",
                configuration.formalSeconds(),
                resources,
                expected,
                system,
                concurrency,
                repetition
            );
            printPhaseSummary(formal);
        } catch (Exception e) {
            executionFailure = e;
        } finally {
            System.out.println("  cleanup: closing statements and connections");
            closeResources(resources, cleanupProblems);
        }

        if (warmup != null) {
            persistPhase(
                configuration,
                warmup,
                system,
                concurrency,
                repetition,
                systemOrder,
                configurationOrder,
                cleanupProblems
            );
        }
        if (formal != null) {
            persistPhase(
                configuration,
                formal,
                system,
                concurrency,
                repetition,
                systemOrder,
                configurationOrder,
                cleanupProblems
            );
        }

        if (executionFailure != null) {
            throw executionFailure;
        }
        if (formal == null || !formal.passed()) {
            throw new IllegalStateException(
                "Formal phase failed for " + system.csvName()
                    + " at concurrency " + concurrency
            );
        }
        if (!cleanupProblems.isEmpty()) {
            throw new IllegalStateException(
                "Cleanup failed: " + String.join("; ", cleanupProblems)
            );
        }
    }

    private static PhaseResult runPhase(
        String phase,
        int durationSeconds,
        List<WorkerResource> resources,
        ExpectedResult expected,
        BenchmarkSystem system,
        int concurrency,
        int repetition
    ) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(resources.size());
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong phaseStart = new AtomicLong();
        AtomicBoolean stopRequested = new AtomicBoolean();
        List<String> threadFailures = Collections.synchronizedList(
            new ArrayList<>()
        );
        List<WorkerTask> tasks = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        long requestedDurationNs = TimeUnit.SECONDS.toNanos(
            durationSeconds
        );

        for (WorkerResource resource : resources) {
            WorkerTask task = new WorkerTask(
                resource,
                expected,
                requestedDurationNs,
                ready,
                start,
                phaseStart,
                stopRequested,
                threadFailures
            );
            Thread thread = new Thread(
                task,
                "c1-" + system.name().toLowerCase(Locale.ROOT)
                    + "-c" + concurrency + "-r" + repetition
                    + "-w" + resource.workerId()
                    + "-" + phase
            );
            tasks.add(task);
            threads.add(thread);
            thread.start();
        }

        boolean allReady = ready.await(
            READY_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        if (!allReady) {
            stopRequested.set(true);
            phaseStart.set(System.nanoTime());
            start.countDown();
            interruptAndJoin(threads);
            throw new IllegalStateException(
                "Workers did not become ready within "
                    + READY_TIMEOUT_SECONDS + " seconds"
            );
        }

        long startNs = System.nanoTime();
        phaseStart.set(startNs);
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        List<QuerySample> samples = new ArrayList<>();
        long endNs = startNs;
        for (WorkerTask task : tasks) {
            samples.addAll(task.samples());
            endNs = Math.max(endNs, task.finishedAtNs());
        }
        samples.sort(Comparator.comparingLong(QuerySample::startNs));

        return new PhaseResult(
            phase,
            durationSeconds * 1_000L,
            startNs,
            endNs,
            List.copyOf(samples),
            List.copyOf(threadFailures)
        );
    }

    private static void interruptAndJoin(List<Thread> threads)
        throws InterruptedException {
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static void closeResources(
        List<WorkerResource> resources,
        List<String> problems
    ) {
        for (int i = resources.size() - 1; i >= 0; i--) {
            WorkerResource resource = resources.get(i);
            try {
                resource.close();
            } catch (SQLException e) {
                problems.add(
                    "worker " + resource.workerId() + ": "
                        + failureMessage(e)
                );
            }
        }
    }

    private static void persistPhase(
        Configuration configuration,
        PhaseResult result,
        BenchmarkSystem system,
        int concurrency,
        int repetition,
        String systemOrder,
        int configurationOrder,
        List<String> cleanupProblems
    ) throws IOException {
        List<String[]> rawRows = new ArrayList<>();
        for (QuerySample sample : result.samples()) {
            rawRows.add(new String[] {
                configuration.sessionId(),
                EXPERIMENT_ID,
                system.csvName(),
                DATASET,
                Integer.toString(RECORD_COUNT),
                Integer.toString(concurrency),
                Integer.toString(repetition),
                result.phase(),
                Long.toString(result.requestedDurationMs()),
                Long.toString(result.actualDurationNs()),
                systemOrder,
                Integer.toString(configurationOrder),
                Integer.toString(sample.workerId()),
                Long.toString(sample.querySequence()),
                Long.toString(sample.startNs() - result.startNs()),
                Long.toString(sample.endNs() - result.startNs()),
                Long.toString(sample.latencyNs()),
                formatMillis(sample.latencyNs()),
                Integer.toString(sample.resultCount()),
                sample.correct() ? "PASS" : "FAIL",
                sample.status(),
                sample.error()
            });
        }
        ConcurrencyCsvLogger.appendRawRows(rawRows);

        boolean cleanupPassed = cleanupProblems.isEmpty();
        boolean passed = result.passed() && cleanupPassed;
        String notes = "completion_rule=start_before_deadline_then_drain"
            + ";latency_scope=executeQuery+transfer+full_result_read"
            + "+inline_validation+resultset_close"
            + ";expected_result_count=" + EXPECTED_RESULT_COUNT
            + ";cleanup=" + (cleanupPassed ? "PASS" : "FAIL");
        if (!result.threadFailures().isEmpty()) {
            notes += ";thread_failures="
                + String.join(" | ", result.threadFailures());
        }
        if (!cleanupProblems.isEmpty()) {
            notes += ";cleanup_errors="
                + String.join(" | ", cleanupProblems);
        }

        ConcurrencyCsvLogger.appendSummaryRow(new String[] {
            Instant.now().toString(),
            configuration.sessionId(),
            EXPERIMENT_ID,
            system.csvName(),
            DATASET,
            Integer.toString(RECORD_COUNT),
            Integer.toString(concurrency),
            Integer.toString(repetition),
            result.phase(),
            Long.toString(result.requestedDurationMs()),
            Long.toString(result.actualDurationNs()),
            formatMillis(result.actualDurationNs()),
            Integer.toString(result.startedQueries()),
            Integer.toString(result.completedQueries()),
            Integer.toString(result.successfulQueries()),
            Integer.toString(result.errorCount()),
            formatDecimal(result.throughputQps()),
            formatNullable(result.medianLatencyMs()),
            formatNullable(result.p95LatencyMs()),
            result.correctness(),
            passed ? "PASS" : "FAIL",
            systemOrder,
            Integer.toString(configurationOrder),
            notes
        });
    }

    private static void printPhaseSummary(PhaseResult result) {
        System.out.printf(
            Locale.ROOT,
            "    %s completed=%d, success=%d, errors=%d, "
                + "actual=%.6f s, throughput=%.3f q/s, "
                + "median=%.3f ms, p95=%.3f ms, correctness=%s%n",
            result.phase(),
            result.completedQueries(),
            result.successfulQueries(),
            result.errorCount(),
            result.actualDurationNs() / 1_000_000_000.0,
            result.throughputQps(),
            result.medianLatencyMs() == null
                ? Double.NaN : result.medianLatencyMs(),
            result.p95LatencyMs() == null
                ? Double.NaN : result.p95LatencyMs(),
            result.correctness()
        );
    }

    private static void printExecutionSchedule(
        Configuration configuration
    ) {
        System.out.println();
        System.out.println("Execution schedule:");
        int order = 0;
        for (int concurrencyIndex = 0;
                concurrencyIndex < configuration.concurrencies().size();
                concurrencyIndex++) {
            int concurrency = configuration.concurrencies().get(
                concurrencyIndex
            );
            for (int repetition = 1;
                    repetition <= configuration.repetitions();
                    repetition++) {
                List<BenchmarkSystem> systems = configuration.systemOrder(
                    concurrencyIndex, repetition
                );
                for (BenchmarkSystem system : systems) {
                    order++;
                    System.out.println("  " + order + ". concurrency="
                        + concurrency + ", repetition=" + repetition
                        + ", system=" + system.csvName()
                        + ", pair_order=" + systemOrderValue(systems));
                }
            }
        }
        System.out.println(
            "Connections and PreparedStatements are created before each "
                + "configuration's warm-up phase."
        );
    }

    private static String systemOrderValue(
        List<BenchmarkSystem> systems
    ) {
        return String.join(">", systems.stream()
            .map(system -> switch (system) {
                case POSTGRESQL -> "postgresql";
                case OPENGAUSS -> "opengauss";
                case JAVA_FILE -> throw new IllegalArgumentException(
                    "C1 does not support Java file operations"
                );
            })
            .toList());
    }

    private static String formatMillis(long nanoseconds) {
        return String.format(
            Locale.ROOT, "%.6f", nanoseconds / 1_000_000.0
        );
    }

    private static String formatDecimal(double value) {
        return String.format(Locale.ROOT, "%.6f", value);
    }

    private static String formatNullable(Double value) {
        return value == null ? "" : String.format(
            Locale.ROOT, "%.6f", value
        );
    }

    private static String failureMessage(Exception exception) {
        String message = exception.getMessage();
        String result = message == null || message.isBlank()
            ? exception.getClass().getSimpleName()
            : exception.getClass().getSimpleName() + ": " + message;
        return result.length() <= 1_000
            ? result
            : result.substring(0, 1_000);
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println(
            "  java -cp \"out;lib/*\" ConcurrencyBenchmarkRunner [options]"
        );
        System.out.println();
        System.out.println("Options:");
        System.out.println(
            "  --system all|postgresql|opengauss   Default: all"
        );
        System.out.println(
            "  --system-order A,B                  Default base order: "
                + "postgresql,opengauss"
        );
        System.out.println(
            "  --concurrency 1,2,4,8               Default: 1,2,4,8"
        );
        System.out.println(
            "  --warmup-seconds N                  Default: 5"
        );
        System.out.println(
            "  --formal-seconds N                  Default: 20"
        );
        System.out.println(
            "  --repetitions N                     Default: 3"
        );
        System.out.println(
            "  --session-id ID                     Default: generated"
        );
        System.out.println("  --help");
        System.out.println();
        System.out.println(
            "Each worker uses an independent read-only JDBC connection. "
                + "Only SELECT title LIKE '%Batman%' is executed."
        );
        System.out.println(
            "For two systems, order alternates by repetition and concurrency "
                + "level to balance which DBMS runs first."
        );
        System.out.println();
        System.out.println("Required password environment variables:");
        System.out.println("  BENCHMARK_POSTGRES_PASSWORD");
        System.out.println("  BENCHMARK_OPENGAUSS_PASSWORD");
        System.out.println(
            "Only the selected system's password is required."
        );
    }

    private record QuerySample(
        int workerId,
        long querySequence,
        long startNs,
        long endNs,
        int resultCount,
        boolean correct,
        String status,
        String error
    ) {
        private long latencyNs() {
            return endNs - startNs;
        }

        private boolean successful() {
            return "PASS".equals(status) && correct;
        }
    }

    private record PhaseResult(
        String phase,
        long requestedDurationMs,
        long startNs,
        long endNs,
        List<QuerySample> samples,
        List<String> threadFailures
    ) {
        private long actualDurationNs() {
            return Math.max(0, endNs - startNs);
        }

        private int startedQueries() {
            return samples.size();
        }

        private int completedQueries() {
            return samples.size();
        }

        private int successfulQueries() {
            return (int) samples.stream()
                .filter(QuerySample::successful)
                .count();
        }

        private int errorCount() {
            return samples.size() - successfulQueries()
                + threadFailures.size();
        }

        private boolean passed() {
            return errorCount() == 0 && !samples.isEmpty();
        }

        private String correctness() {
            return passed() ? "PASS" : "FAIL";
        }

        private double throughputQps() {
            if (actualDurationNs() == 0) {
                return 0.0;
            }
            return successfulQueries()
                / (actualDurationNs() / 1_000_000_000.0);
        }

        private Double medianLatencyMs() {
            List<Long> values = successfulLatencies();
            if (values.isEmpty()) {
                return null;
            }
            int middle = values.size() / 2;
            double medianNs = values.size() % 2 == 0
                ? (values.get(middle - 1) + values.get(middle)) / 2.0
                : values.get(middle);
            return medianNs / 1_000_000.0;
        }

        private Double p95LatencyMs() {
            List<Long> values = successfulLatencies();
            if (values.isEmpty()) {
                return null;
            }
            int rank = (int) Math.ceil(0.95 * values.size());
            return values.get(Math.max(0, rank - 1)) / 1_000_000.0;
        }

        private List<Long> successfulLatencies() {
            List<Long> values = samples.stream()
                .filter(QuerySample::successful)
                .map(QuerySample::latencyNs)
                .sorted()
                .toList();
            return values;
        }
    }

    private record ExpectedResult(
        List<MovieRecord> records,
        Map<Integer, Integer> positionByMovieId
    ) {
        private static ExpectedResult from(
            List<MovieRecord> records
        ) {
            Map<Integer, Integer> positions = new HashMap<>();
            for (int i = 0; i < records.size(); i++) {
                MovieRecord record = records.get(i);
                if (positions.put(record.movieId(), i) != null) {
                    throw new IllegalStateException(
                        "Duplicate expected E2 movieid: "
                            + record.movieId()
                    );
                }
            }
            return new ExpectedResult(
                List.copyOf(records), Map.copyOf(positions)
            );
        }

        private int size() {
            return records.size();
        }
    }

    private static final class WorkerTask implements Runnable {
        private final WorkerResource resource;
        private final ExpectedResult expected;
        private final long requestedDurationNs;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final AtomicLong phaseStart;
        private final AtomicBoolean stopRequested;
        private final List<String> threadFailures;
        private final List<QuerySample> samples = new ArrayList<>();
        private long finishedAtNs;

        private WorkerTask(
            WorkerResource resource,
            ExpectedResult expected,
            long requestedDurationNs,
            CountDownLatch ready,
            CountDownLatch start,
            AtomicLong phaseStart,
            AtomicBoolean stopRequested,
            List<String> threadFailures
        ) {
            this.resource = resource;
            this.expected = expected;
            this.requestedDurationNs = requestedDurationNs;
            this.ready = ready;
            this.start = start;
            this.phaseStart = phaseStart;
            this.stopRequested = stopRequested;
            this.threadFailures = threadFailures;
        }

        @Override
        public void run() {
            try {
                ready.countDown();
                start.await();
                long deadline = phaseStart.get() + requestedDurationNs;
                BitSet seen = new BitSet(expected.size());
                long querySequence = 0;

                while (!stopRequested.get()) {
                    seen.clear();
                    QuerySample sample = executeQuery(
                        querySequence + 1, deadline, seen
                    );
                    if (sample == null) {
                        break;
                    }
                    querySequence++;
                    samples.add(sample);
                    if (!sample.successful()) {
                        stopRequested.set(true);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopRequested.set(true);
                threadFailures.add(
                    "worker " + resource.workerId()
                        + " interrupted"
                );
            } catch (RuntimeException e) {
                stopRequested.set(true);
                threadFailures.add(
                    "worker " + resource.workerId() + ": "
                        + failureMessage(e)
                );
            } finally {
                finishedAtNs = System.nanoTime();
            }
        }

        private QuerySample executeQuery(
            long querySequence,
            long deadline,
            BitSet seen
        ) {
            int resultCount = 0;
            boolean correct = true;
            String status = "PASS";
            String error = "";
            long queryStart = System.nanoTime();
            if (queryStart >= deadline) {
                return null;
            }

            try (ResultSet results =
                    resource.statement().executeQuery()) {
                while (results.next()) {
                    resultCount++;
                    int movieId = results.getInt("movieid");
                    Integer position = expected.positionByMovieId().get(
                        movieId
                    );
                    if (position == null) {
                        correct = false;
                        if (error.isEmpty()) {
                            error = "unexpected movieid " + movieId;
                        }
                        continue;
                    }
                    if (seen.get(position)) {
                        correct = false;
                        if (error.isEmpty()) {
                            error = "duplicate movieid " + movieId;
                        }
                    }
                    seen.set(position);

                    MovieRecord expectedRecord = expected.records().get(
                        position
                    );
                    int runtimeValue = results.getInt("runtime");
                    Integer runtime = results.wasNull()
                        ? null : runtimeValue;
                    String country = results.getString("country");
                    if (country != null) {
                        country = country.stripTrailing();
                    }
                    if (!expectedRecord.title().equals(
                            results.getString("title"))
                            || !expectedRecord.country().equals(country)
                            || expectedRecord.yearReleased()
                                != results.getInt("year_released")
                            || !java.util.Objects.equals(
                                expectedRecord.runtime(), runtime)) {
                        correct = false;
                        if (error.isEmpty()) {
                            error = "record mismatch for movieid " + movieId;
                        }
                    }
                }

                if (resultCount != expected.size()
                        || seen.cardinality() != expected.size()) {
                    correct = false;
                    if (error.isEmpty()) {
                        error = "result count expected=" + expected.size()
                            + ", actual=" + resultCount
                            + ", unique_expected_ids_seen="
                            + seen.cardinality();
                    }
                }
                if (!correct) {
                    status = "CORRECTNESS_FAIL";
                }
            } catch (SQLException e) {
                correct = false;
                status = "SQL_ERROR";
                error = failureMessage(e);
            } catch (RuntimeException e) {
                correct = false;
                status = "CLIENT_ERROR";
                error = failureMessage(e);
            }

            long queryEnd = System.nanoTime();
            return new QuerySample(
                resource.workerId(),
                querySequence,
                queryStart,
                queryEnd,
                resultCount,
                correct,
                status,
                error
            );
        }

        private List<QuerySample> samples() {
            return samples;
        }

        private long finishedAtNs() {
            return finishedAtNs;
        }
    }

    private record WorkerResource(
        int workerId,
        Connection connection,
        PreparedStatement statement
    ) implements AutoCloseable {
        private static WorkerResource open(
            int workerId,
            DatabaseConfiguration database
        ) throws Exception {
            Class.forName(database.driverClass());
            Connection connection = DriverManager.getConnection(
                database.url(), database.user(), database.password()
            );
            try {
                connection.setReadOnly(true);
                connection.setAutoCommit(true);
                PreparedStatement statement =
                    JdbcBenchmark.prepareSubstringSearch(
                        connection, database.schema(), DATASET
                    );
                return new WorkerResource(
                    workerId, connection, statement
                );
            } catch (Exception e) {
                try {
                    connection.close();
                } catch (SQLException closeError) {
                    e.addSuppressed(closeError);
                }
                throw e;
            }
        }

        @Override
        public void close() throws SQLException {
            SQLException failure = null;
            try {
                statement.close();
            } catch (SQLException e) {
                failure = e;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private record Configuration(
        List<BenchmarkSystem> systems,
        List<BenchmarkSystem> baseSystemOrder,
        List<Integer> concurrencies,
        int warmupSeconds,
        int formalSeconds,
        int repetitions,
        String sessionId,
        boolean help
    ) {
        private static Configuration parse(String[] args) {
            List<BenchmarkSystem> systems = List.of(
                BenchmarkSystem.POSTGRESQL,
                BenchmarkSystem.OPENGAUSS
            );
            List<BenchmarkSystem> requestedOrder = null;
            boolean systemExplicit = false;
            List<Integer> concurrencies = DEFAULT_CONCURRENCIES;
            int warmupSeconds = DEFAULT_WARMUP_SECONDS;
            int formalSeconds = DEFAULT_FORMAL_SECONDS;
            int repetitions = DEFAULT_REPETITIONS;
            String sessionId = generatedSessionId();
            boolean help = false;

            for (int i = 0; i < args.length; i++) {
                String option = args[i];
                switch (option) {
                    case "--help", "-h" -> help = true;
                    case "--system" -> {
                        systems = parseSystems(
                            requireValue(args, ++i, option)
                        );
                        systemExplicit = true;
                    }
                    case "--system-order" -> {
                        if (requestedOrder != null) {
                            throw new IllegalArgumentException(
                                "--system-order may be specified only once"
                            );
                        }
                        requestedOrder = parseSystemOrder(
                            requireValue(args, ++i, option)
                        );
                    }
                    case "--concurrency" -> concurrencies =
                        parseConcurrencies(
                            requireValue(args, ++i, option)
                        );
                    case "--warmup-seconds" -> warmupSeconds =
                        parsePositiveInt(
                            requireValue(args, ++i, option), option
                        );
                    case "--formal-seconds" -> formalSeconds =
                        parsePositiveInt(
                            requireValue(args, ++i, option), option
                        );
                    case "--repetitions" -> repetitions =
                        parsePositiveInt(
                            requireValue(args, ++i, option), option
                        );
                    case "--session-id" -> {
                        String value = requireValue(args, ++i, option);
                        if (!value.matches("[A-Za-z0-9._-]{1,80}")) {
                            throw new IllegalArgumentException(
                                "--session-id must contain 1-80 letters, "
                                    + "digits, dots, underscores, or hyphens"
                            );
                        }
                        sessionId = value;
                    }
                    default -> throw new IllegalArgumentException(
                        "Unknown option: " + option
                    );
                }
            }

            if (requestedOrder != null) {
                if (systemExplicit && systems.size() != 2) {
                    throw new IllegalArgumentException(
                        "--system-order requires both database systems"
                    );
                }
                systems = requestedOrder;
            }
            List<BenchmarkSystem> baseOrder = requestedOrder == null
                ? systems : requestedOrder;

            return new Configuration(
                List.copyOf(systems),
                List.copyOf(baseOrder),
                List.copyOf(concurrencies),
                warmupSeconds,
                formalSeconds,
                repetitions,
                sessionId,
                help
            );
        }

        private List<BenchmarkSystem> systemOrder(
            int concurrencyIndex,
            int repetition
        ) {
            if (baseSystemOrder.size() == 1) {
                return baseSystemOrder;
            }
            boolean reverse = (concurrencyIndex + repetition - 1) % 2 == 1;
            return reverse
                ? List.of(baseSystemOrder.get(1), baseSystemOrder.get(0))
                : baseSystemOrder;
        }

        private static List<BenchmarkSystem> parseSystems(String value) {
            if (value.equalsIgnoreCase("all")) {
                return List.of(
                    BenchmarkSystem.POSTGRESQL,
                    BenchmarkSystem.OPENGAUSS
                );
            }
            return List.of(parseDatabaseSystem(value));
        }

        private static List<BenchmarkSystem> parseSystemOrder(
            String value
        ) {
            String[] names = value.split(",", -1);
            if (names.length != 2) {
                throw new IllegalArgumentException(
                    "--system-order requires exactly two comma-separated "
                        + "systems"
                );
            }
            List<BenchmarkSystem> order = new ArrayList<>();
            for (String rawName : names) {
                String name = rawName.trim();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException(
                        "--system-order contains an empty system"
                    );
                }
                BenchmarkSystem system = parseDatabaseSystem(name);
                if (order.contains(system)) {
                    throw new IllegalArgumentException(
                        "--system-order contains duplicate system: " + name
                    );
                }
                order.add(system);
            }
            return List.copyOf(order);
        }

        private static BenchmarkSystem parseDatabaseSystem(
            String value
        ) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "postgresql", "postgresql-jdbc" ->
                    BenchmarkSystem.POSTGRESQL;
                case "opengauss", "opengauss-jdbc" ->
                    BenchmarkSystem.OPENGAUSS;
                default -> throw new IllegalArgumentException(
                    "Invalid C1 system: " + value
                        + "; expected postgresql or opengauss"
                );
            };
        }

        private static List<Integer> parseConcurrencies(String value) {
            String[] values = value.split(",", -1);
            List<Integer> result = new ArrayList<>();
            for (String raw : values) {
                String item = raw.trim();
                if (item.isEmpty()) {
                    throw new IllegalArgumentException(
                        "--concurrency contains an empty value"
                    );
                }
                int concurrency = parsePositiveInt(item, "--concurrency");
                if (!DEFAULT_CONCURRENCIES.contains(concurrency)) {
                    throw new IllegalArgumentException(
                        "Unsupported concurrency " + concurrency
                            + "; expected a subset of 1,2,4,8"
                    );
                }
                if (result.contains(concurrency)) {
                    throw new IllegalArgumentException(
                        "Duplicate concurrency: " + concurrency
                    );
                }
                result.add(concurrency);
            }
            if (result.isEmpty()) {
                throw new IllegalArgumentException(
                    "--concurrency requires at least one value"
                );
            }
            return List.copyOf(result);
        }

        private static int parsePositiveInt(
            String value,
            String option
        ) {
            try {
                int parsed = Integer.parseInt(value);
                if (parsed < 1) {
                    throw new IllegalArgumentException(
                        option + " must be at least 1"
                    );
                }
                return parsed;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    option + " requires an integer, got: " + value
                );
            }
        }

        private static String requireValue(
            String[] args,
            int index,
            String option
        ) {
            if (index >= args.length) {
                throw new IllegalArgumentException(
                    "Missing value for " + option
                );
            }
            return args[index];
        }

        private static String generatedSessionId() {
            return SESSION_TIME.format(Instant.now()) + "-"
                + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
