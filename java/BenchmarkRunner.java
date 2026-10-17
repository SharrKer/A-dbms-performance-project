import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public final class BenchmarkRunner {
    private static final int DEFAULT_WARMUP_RUNS = 5;
    private static final int DEFAULT_FORMAL_RUNS = 10;
    private static final Path DEFAULT_DATA_PATH = Path.of(
        "data", "working", "originalCSV", "movies"
    );
    private static final Path LARGE_UPDATE_OUTPUT = Path.of(
        "result", "measurements", "benchmark_u3_u4_large_raw.csv"
    );
    private static final DateTimeFormatter SESSION_TIME =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'")
            .withZone(ZoneOffset.UTC);

    private BenchmarkRunner() {
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

        String initialSourceHash = null;
        BenchmarkData data = null;
        FileBenchmark fileBenchmark = null;
        Map<BenchmarkSystem, Connection> connections =
            new EnumMap<>(BenchmarkSystem.class);
        Map<BenchmarkSystem, JdbcBenchmark> jdbcBenchmarks =
            new EnumMap<>(BenchmarkSystem.class);
        int exitCode = 0;

        try {
            initialSourceHash = BenchmarkData.sha256(
                configuration.dataPath()
            );
            List<BenchmarkExperiment> baselineUpdates =
                configuration.dataset().baselineUpdateExperiments(
                    configuration.experiments()
                );
            data = BenchmarkData.load(
                configuration.dataPath(),
                configuration.dataset().expectedRecordCount,
                configuration.dataset().expectedSubstringCount,
                configuration.dataset() == DatasetProfile.ORIGINAL,
                baselineUpdates
            );

            System.out.println("Benchmark session: "
                + configuration.sessionId());
            System.out.println("Dataset profile: "
                + configuration.dataset().cliName);
            System.out.println("JDBC table: "
                + configuration.dataset().tableName);
            System.out.println("Experiments: "
                + configuration.experiments());
            System.out.println("Measurement output: "
                + configuration.outputFile());
            System.out.println("Source dataset: " + data.source());
            System.out.println("Source SHA-256: " + initialSourceHash);
            System.out.println("Source records: " + data.records().size());
            System.out.println(
                "Expected query matches: E1="
                    + data.expectedQuery(BenchmarkExperiment.E1).size()
                    + ", E2="
                    + data.expectedQuery(BenchmarkExperiment.E2).size()
            );
            if (!baselineUpdates.isEmpty()) {
                List<String> derivedTargetCounts = new ArrayList<>();
                for (BenchmarkExperiment experiment : baselineUpdates) {
                    derivedTargetCounts.add(
                        experiment + "="
                            + data.expectedUpdateCount(experiment)
                    );
                }
                System.out.println(
                    "Derived update targets: "
                        + String.join(", ", derivedTargetCounts)
                );
            }

            Map<BenchmarkSystem, JdbcBenchmark.Inspection> inspections =
                connectAndPreflight(
                    configuration,
                    data,
                    connections
                );
            Map<BenchmarkSystem, String> indexStates =
                validateCrossDatabaseMetadata(
                    configuration, inspections
                );

            BenchmarkCsvLogger.validateFile(
                configuration.outputFile()
            );

            if (configuration.systems().contains(
                    BenchmarkSystem.JAVA_FILE)) {
                fileBenchmark = new FileBenchmark(
                    data,
                    Path.of(
                        "result",
                        "benchmark-work",
                        configuration.sessionId()
                    ),
                    initialSourceHash
                );
            }

            for (Map.Entry<BenchmarkSystem, Connection> entry
                    : connections.entrySet()) {
                DatabaseConfiguration database =
                    DatabaseConfiguration.forSystem(entry.getKey());
                jdbcBenchmarks.put(
                    entry.getKey(),
                    new JdbcBenchmark(
                        entry.getValue(),
                        database.schema(),
                        configuration.dataset().tableName,
                        data
                    )
                );
            }

            printExecutionOrder(configuration);
            int combinationOrder = 0;
            for (BenchmarkExperiment experiment
                    : configuration.experiments()) {
                for (BenchmarkSystem system : configuration.systems()) {
                    combinationOrder++;
                    runCombination(
                        configuration,
                        experiment,
                        system,
                        combinationOrder,
                        fileBenchmark,
                        jdbcBenchmarks.get(system),
                        indexStates.getOrDefault(
                            system, "not_applicable"
                        ),
                        data.records().size()
                    );
                }
            }

            System.out.println(
                "Benchmark session completed successfully. Raw CSV: "
                    + configuration.outputFile()
            );
        } catch (Exception e) {
            exitCode = 1;
            System.err.println(
                "Benchmark session failed: " + failureMessage(e)
            );
        } finally {
            if (!closeResources(jdbcBenchmarks, connections)) {
                exitCode = 1;
            }

            if (data != null && initialSourceHash != null) {
                try {
                    String finalHash = BenchmarkData.sha256(
                        data.source()
                    );
                    if (!initialSourceHash.equals(finalHash)) {
                        exitCode = 1;
                        System.err.println(
                            "DATA SAFETY FAILURE: source SHA-256 changed "
                                + "from " + initialSourceHash + " to "
                                + finalHash
                        );
                    } else {
                        System.out.println(
                            "Source dataset safety check passed: SHA-256 "
                                + "unchanged."
                        );
                    }
                } catch (IOException e) {
                    exitCode = 1;
                    System.err.println(
                        "Could not verify final source hash: "
                            + failureMessage(e)
                    );
                }
            }
        }

        return exitCode;
    }

    private static Map<BenchmarkSystem, JdbcBenchmark.Inspection>
        connectAndPreflight(
            Configuration configuration,
            BenchmarkData data,
            Map<BenchmarkSystem, Connection> connections
        ) throws Exception {
        Map<BenchmarkSystem, JdbcBenchmark.Inspection> inspections =
            new EnumMap<>(BenchmarkSystem.class);

        for (BenchmarkSystem system : configuration.systems()) {
            if (system == BenchmarkSystem.JAVA_FILE) {
                continue;
            }

            DatabaseConfiguration database =
                DatabaseConfiguration.forSystem(system);
            System.out.println(
                "Connecting to " + system.csvName()
                    + " (schema " + database.schema() + ")..."
            );
            Class.forName(database.driverClass());
            Connection connection = DriverManager.getConnection(
                database.url(),
                database.user(),
                database.password()
            );
            connections.put(system, connection);

            JdbcBenchmark.Inspection inspection = JdbcBenchmark.inspect(
                connection,
                database.schema(),
                configuration.dataset().tableName,
                configuration.dataset().completeDatabasePreflight
            );
            int expectedRows = data.records().size();
            if (inspection.rowCount() != expectedRows) {
                throw new IllegalStateException(
                    system.csvName() + " row count differs from the source: "
                        + "expected=" + expectedRows + ", actual="
                        + inspection.rowCount()
                );
            }
            if (configuration.dataset().completeDatabasePreflight
                    && !BenchmarkData.sameRecords(
                        data.records(), inspection.rows())) {
                throw new IllegalStateException(
                    system.csvName() + " data differs from the source: "
                        + BenchmarkData.describeDifference(
                            data.records(), inspection.rows()
                        )
                );
            }

            inspections.put(system, inspection);
            System.out.println(
                "Preflight passed for " + system.csvName() + ": "
                    + inspection.productName() + " "
                    + inspection.productVersion() + ", rows="
                    + inspection.rowCount() + ", relevant indexes="
                    + inspection.tableMetadata().indexState()
            );
            for (String warning : inspection.warnings()) {
                System.out.println(
                    "WARNING [" + system.csvName() + "]: " + warning
                );
            }

            List<BenchmarkExperiment> updatePlans =
                configuration.largeUpdateExperiments();
            if (!updatePlans.isEmpty()) {
                Map<BenchmarkExperiment, List<String>> plans =
                    JdbcBenchmark.explainUpdatePlans(
                        connection,
                        database.schema(),
                        configuration.dataset().tableName,
                        updatePlans
                    );
                for (BenchmarkExperiment experiment : updatePlans) {
                    System.out.println(
                        "Read-only EXPLAIN plan for " + experiment + " / "
                            + system.csvName() + ":"
                    );
                    for (String line : plans.get(experiment)) {
                        System.out.println("  " + line);
                    }
                }
            }
        }

        return inspections;
    }

    private static Map<BenchmarkSystem, String>
        validateCrossDatabaseMetadata(
            Configuration configuration,
            Map<BenchmarkSystem, JdbcBenchmark.Inspection> inspections
        ) {
        Map<BenchmarkSystem, String> states =
            new EnumMap<>(BenchmarkSystem.class);
        states.put(BenchmarkSystem.JAVA_FILE, "not_applicable");

        for (Map.Entry<BenchmarkSystem, JdbcBenchmark.Inspection> entry
                : inspections.entrySet()) {
            states.put(
                entry.getKey(),
                entry.getValue().tableMetadata().indexState()
            );
        }

        JdbcBenchmark.Inspection postgres = inspections.get(
            BenchmarkSystem.POSTGRESQL
        );
        JdbcBenchmark.Inspection openGauss = inspections.get(
            BenchmarkSystem.OPENGAUSS
        );

        if (postgres != null && openGauss != null) {
            JdbcBenchmark.TableMetadata postgresMetadata =
                postgres.tableMetadata();
            JdbcBenchmark.TableMetadata openGaussMetadata =
                openGauss.tableMetadata();

            if (!postgresMetadata.relevantIndexes().equals(
                    openGaussMetadata.relevantIndexes())) {
                throw new IllegalStateException(
                    "Benchmark-relevant indexes differ. PostgreSQL="
                        + postgresMetadata.indexState()
                        + "; openGauss="
                        + openGaussMetadata.indexState()
                );
            }

            if (!postgresMetadata.columns().equals(
                    openGaussMetadata.columns())) {
                printMetadataWarning(
                    "column metadata",
                    postgresMetadata.columns(),
                    openGaussMetadata.columns()
                );
            }
            if (!postgresMetadata.indexes().equals(
                    openGaussMetadata.indexes())) {
                printMetadataWarning(
                    "unrelated/all index metadata",
                    postgresMetadata.indexes(),
                    openGaussMetadata.indexes()
                );
            }
            if (!postgresMetadata.foreignKeys().equals(
                    openGaussMetadata.foreignKeys())) {
                printMetadataWarning(
                    "foreign-key metadata",
                    postgresMetadata.foreignKeys(),
                    openGaussMetadata.foreignKeys()
                );
            }
            if (!postgresMetadata.checkConstraints().equals(
                    openGaussMetadata.checkConstraints())) {
                printMetadataWarning(
                    "check-constraint metadata",
                    postgresMetadata.checkConstraints(),
                    openGaussMetadata.checkConstraints()
                );
            }

            System.out.println(
                "Cross-database preflight passed: complete movies data "
                    + "and benchmark-relevant indexes match."
            );
        } else if (!inspections.isEmpty()
                && configuration.systems().size() < 3) {
            System.out.println(
                "WARNING: Cross-database index comparison was skipped "
                    + "because only one database system was selected."
            );
        }

        return states;
    }

    private static void printMetadataWarning(
        String category,
        Object postgres,
        Object openGauss
    ) {
        System.out.println(
            "WARNING: PostgreSQL/openGauss " + category
                + " differs (non-blocking). PostgreSQL=" + postgres
                + "; openGauss=" + openGauss
        );
    }

    private static void runCombination(
        Configuration configuration,
        BenchmarkExperiment experiment,
        BenchmarkSystem system,
        int combinationOrder,
        FileBenchmark fileBenchmark,
        JdbcBenchmark jdbcBenchmark,
        String indexState,
        int recordCount
    ) throws IOException {
        System.out.println();
        System.out.println(
            "Combination " + combinationOrder + ": " + experiment
                + " / " + system.csvName()
                + " (warm-up then formal)"
        );

        for (int iteration = 1;
                iteration <= configuration.warmupRuns();
                iteration++) {
            BenchmarkResult result = execute(
                experiment, system, fileBenchmark, jdbcBenchmark
            );
            logResult(
                configuration,
                experiment,
                system,
                combinationOrder,
                "warmup",
                iteration,
                result,
                indexState,
                recordCount
            );
            requirePass(
                experiment, system, "warmup", iteration, result
            );
        }

        List<Long> formalTimes = new ArrayList<>();
        for (int iteration = 1;
                iteration <= configuration.formalRuns();
                iteration++) {
            BenchmarkResult result = execute(
                experiment, system, fileBenchmark, jdbcBenchmark
            );
            logResult(
                configuration,
                experiment,
                system,
                combinationOrder,
                "formal",
                iteration,
                result,
                indexState,
                recordCount
            );
            requirePass(
                experiment, system, "formal", iteration, result
            );
            if (result.passed()) {
                formalTimes.add(result.elapsedNs());
            }
        }

        printSummary(experiment, system, formalTimes);
    }

    private static BenchmarkResult execute(
        BenchmarkExperiment experiment,
        BenchmarkSystem system,
        FileBenchmark fileBenchmark,
        JdbcBenchmark jdbcBenchmark
    ) {
        if (system == BenchmarkSystem.JAVA_FILE) {
            if (fileBenchmark == null) {
                return BenchmarkResult.failure(
                    0, null, null, "Java file benchmark is unavailable"
                );
            }
            return fileBenchmark.run(experiment);
        }
        if (jdbcBenchmark == null) {
            return BenchmarkResult.failure(
                0, null, null, "JDBC benchmark is unavailable"
            );
        }
        return jdbcBenchmark.run(experiment);
    }

    private static void logResult(
        Configuration configuration,
        BenchmarkExperiment experiment,
        BenchmarkSystem system,
        int combinationOrder,
        String runType,
        int iteration,
        BenchmarkResult result,
        String indexState,
        int recordCount
    ) throws IOException {
        String notes = "system_order="
            + configuration.systemOrderValue()
            + ";combination_order=" + combinationOrder;
        if (!result.notes().isBlank()) {
            notes += ";" + result.notes();
        }

        BenchmarkCsvLogger.appendRow(
            configuration.outputFile(),
            new String[] {
            Instant.now().toString(),
            configuration.sessionId(),
            experiment.name(),
            system.csvName(),
            configuration.dataset().tableName,
            Integer.toString(recordCount),
            indexState,
            runType,
            Integer.toString(iteration),
            Long.toString(result.elapsedNs()),
            String.format(
                Locale.ROOT,
                "%.6f",
                result.elapsedNs() / 1_000_000.0
            ),
            nullableInteger(result.resultCount()),
            nullableInteger(result.rowsUpdated()),
            result.passed() ? "PASS" : "FAIL",
            notes
        });
    }

    private static void requirePass(
        BenchmarkExperiment experiment,
        BenchmarkSystem system,
        String runType,
        int iteration,
        BenchmarkResult result
    ) {
        if (!result.passed()) {
            throw new IllegalStateException(
                experiment + " / " + system.csvName() + " " + runType
                    + " iteration " + iteration + " failed: "
                    + result.notes()
            );
        }
    }

    private static void printSummary(
        BenchmarkExperiment experiment,
        BenchmarkSystem system,
        List<Long> formalTimes
    ) {
        if (formalTimes.isEmpty()) {
            System.out.println("No passing formal measurements to summarize.");
            return;
        }

        List<Long> sorted = new ArrayList<>(formalTimes);
        Collections.sort(sorted);
        double total = 0;
        for (long value : sorted) {
            total += value;
        }
        double meanMs = total / sorted.size() / 1_000_000.0;
        double medianNs;
        int middle = sorted.size() / 2;
        if (sorted.size() % 2 == 0) {
            medianNs = (sorted.get(middle - 1) + sorted.get(middle))
                / 2.0;
        } else {
            medianNs = sorted.get(middle);
        }

        System.out.printf(
            Locale.ROOT,
            "PASS summary %s / %s: n=%d, min=%.6f ms, "
                + "median=%.6f ms, mean=%.6f ms%n",
            experiment,
            system.csvName(),
            sorted.size(),
            sorted.get(0) / 1_000_000.0,
            medianNs / 1_000_000.0,
            meanMs
        );
    }

    private static void printExecutionOrder(
        Configuration configuration
    ) {
        System.out.println();
        System.out.println(
            "System order: " + configuration.systemOrderDisplay()
        );
        System.out.println(
            "Execution nesting: experiment -> system -> "
                + "warm-up iteration -> formal iteration."
        );
        int order = 0;
        for (BenchmarkExperiment experiment
                : configuration.experiments()) {
            for (BenchmarkSystem system : configuration.systems()) {
                order++;
                System.out.println(
                    "  " + order + ". " + experiment + " / "
                        + system.csvName()
                );
            }
        }
        System.out.println(
            "Caution: caching, JIT, OS, and fixed-order effects mean one "
                + "session is not a definitive DBMS ranking."
        );
    }

    private static boolean closeResources(
        Map<BenchmarkSystem, JdbcBenchmark> benchmarks,
        Map<BenchmarkSystem, Connection> connections
    ) {
        boolean success = true;
        for (Map.Entry<BenchmarkSystem, JdbcBenchmark> entry
                : benchmarks.entrySet()) {
            try {
                entry.getValue().close();
            } catch (SQLException e) {
                success = false;
                System.err.println(
                    "Could not safely close "
                        + entry.getKey().csvName() + " statements: "
                        + failureMessage(e)
                );
            }
        }

        for (Map.Entry<BenchmarkSystem, Connection> entry
                : connections.entrySet()) {
            try {
                entry.getValue().close();
            } catch (SQLException e) {
                success = false;
                System.err.println(
                    "Could not close " + entry.getKey().csvName()
                        + " connection: " + failureMessage(e)
                );
            }
        }
        return success;
    }

    private static String nullableInteger(Integer value) {
        return value == null ? "" : Integer.toString(value);
    }

    private static String failureMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank()
            ? exception.getClass().getSimpleName()
            : exception.getClass().getSimpleName() + ": " + message;
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println(
            "  java -cp \"out;lib/*\" BenchmarkRunner [options]"
        );
        System.out.println();
        System.out.println("Options:");
        System.out.println(
            "  --experiment all|E1|E2|U1|U2|U3|U4   Default: all"
        );
        System.out.println(
            "  --system all|java|postgresql|opengauss   Default: all"
        );
        System.out.println(
            "  --system-order A,B,C           Default: "
                + "java,postgresql,opengauss"
        );
        System.out.println(
            "  --warmup N                      Default: 5"
        );
        System.out.println(
            "  --formal N                      Default: 10"
        );
        System.out.println(
            "  --dataset original|large       Default: original"
        );
        System.out.println(
            "  --data PATH                     Original dataset override; "
                + "default: " + DEFAULT_DATA_PATH
        );
        System.out.println(
            "  --session-id ID                 Default: generated"
        );
        System.out.println("  --help");
        System.out.println();
        System.out.println(
            "Large dataset defaults to E1/E2. Explicit U3/U4 runs write to "
                + LARGE_UPDATE_OUTPUT + "; E1/E2 continue to write to "
                + "result/measurements/benchmark_e3_e4_large_raw.csv."
        );
        System.out.println();
        System.out.println("Required password environment variables:");
        System.out.println("  BENCHMARK_POSTGRES_PASSWORD");
        System.out.println("  BENCHMARK_OPENGAUSS_PASSWORD");
        System.out.println(
            "Only the selected database system's password is required."
        );
    }

    private enum DatasetProfile {
        ORIGINAL(
            "original",
            DEFAULT_DATA_PATH,
            "movies",
            9_204,
            8,
            true,
            BenchmarkCsvLogger.defaultFile()
        ),
        LARGE(
            "large",
            Path.of(
                "data", "working", "largeCSV", "movies_large.csv"
            ),
            "movies_large",
            1_012_440,
            880,
            false,
            Path.of(
                "result",
                "measurements",
                "benchmark_e3_e4_large_raw.csv"
            )
        );

        private final String cliName;
        private final Path defaultPath;
        private final String tableName;
        private final int expectedRecordCount;
        private final int expectedSubstringCount;
        private final boolean completeDatabasePreflight;
        private final Path outputFile;

        DatasetProfile(
            String cliName,
            Path defaultPath,
            String tableName,
            int expectedRecordCount,
            int expectedSubstringCount,
            boolean completeDatabasePreflight,
            Path outputFile
        ) {
            this.cliName = cliName;
            this.defaultPath = defaultPath;
            this.tableName = tableName;
            this.expectedRecordCount = expectedRecordCount;
            this.expectedSubstringCount = expectedSubstringCount;
            this.completeDatabasePreflight = completeDatabasePreflight;
            this.outputFile = outputFile;
        }

        private static DatasetProfile parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "original" -> ORIGINAL;
                case "large" -> LARGE;
                default -> throw new IllegalArgumentException(
                    "Invalid dataset: " + value
                        + "; expected original or large"
                );
            };
        }

        private List<BenchmarkExperiment> defaultExperiments() {
            return switch (this) {
                case ORIGINAL -> List.of(
                    BenchmarkExperiment.E1,
                    BenchmarkExperiment.E2,
                    BenchmarkExperiment.U1,
                    BenchmarkExperiment.U2
                );
                case LARGE -> List.of(
                    BenchmarkExperiment.E1,
                    BenchmarkExperiment.E2
                );
            };
        }

        private boolean supports(BenchmarkExperiment experiment) {
            return switch (this) {
                case ORIGINAL -> experiment != BenchmarkExperiment.U3
                    && experiment != BenchmarkExperiment.U4;
                case LARGE -> experiment != BenchmarkExperiment.U1
                    && experiment != BenchmarkExperiment.U2;
            };
        }

        private List<BenchmarkExperiment> baselineUpdateExperiments(
            List<BenchmarkExperiment> selectedExperiments
        ) {
            if (this == ORIGINAL) {
                return List.of(
                    BenchmarkExperiment.U1,
                    BenchmarkExperiment.U2
                );
            }
            return selectedExperiments.stream()
                .filter(experiment -> experiment == BenchmarkExperiment.U3
                    || experiment == BenchmarkExperiment.U4)
                .toList();
        }

        private Path outputFile(
            List<BenchmarkExperiment> selectedExperiments
        ) {
            if (this == LARGE && selectedExperiments.stream().anyMatch(
                    experiment -> experiment == BenchmarkExperiment.U3
                        || experiment == BenchmarkExperiment.U4)) {
                return LARGE_UPDATE_OUTPUT;
            }
            return outputFile;
        }
    }

    private record Configuration(
        List<BenchmarkExperiment> experiments,
        List<BenchmarkSystem> systems,
        DatasetProfile dataset,
        int warmupRuns,
        int formalRuns,
        Path dataPath,
        String sessionId,
        boolean help
    ) {
        private static Configuration parse(String[] args) {
            List<BenchmarkExperiment> experiments =
                DatasetProfile.ORIGINAL.defaultExperiments();
            List<BenchmarkSystem> systems = List.of(
                BenchmarkSystem.values()
            );
            List<BenchmarkSystem> requestedSystemOrder = null;
            boolean systemSelectionExplicit = false;
            boolean experimentSelectionExplicit = false;
            boolean dataPathExplicit = false;
            DatasetProfile dataset = DatasetProfile.ORIGINAL;
            int warmupRuns = DEFAULT_WARMUP_RUNS;
            int formalRuns = DEFAULT_FORMAL_RUNS;
            Path dataPath = DEFAULT_DATA_PATH;
            String sessionId = generatedSessionId();
            boolean help = false;

            for (int i = 0; i < args.length; i++) {
                String option = args[i];
                switch (option) {
                    case "--help", "-h" -> help = true;
                    case "--experiment" -> {
                        String value = requireValue(args, ++i, option);
                        experiments = parseExperiments(value);
                        experimentSelectionExplicit = true;
                    }
                    case "--system" -> {
                        String value = requireValue(args, ++i, option);
                        systems = parseSystems(value);
                        systemSelectionExplicit = true;
                    }
                    case "--system-order" -> {
                        if (requestedSystemOrder != null) {
                            throw new IllegalArgumentException(
                                "--system-order may be specified only once"
                            );
                        }
                        String value = requireValue(args, ++i, option);
                        requestedSystemOrder = parseSystemOrder(value);
                    }
                    case "--warmup" -> {
                        String value = requireValue(args, ++i, option);
                        warmupRuns = parseNonNegativeInt(
                            value, option
                        );
                    }
                    case "--formal" -> {
                        String value = requireValue(args, ++i, option);
                        formalRuns = parsePositiveInt(value, option);
                    }
                    case "--dataset" -> {
                        String value = requireValue(args, ++i, option);
                        dataset = DatasetProfile.parse(value);
                    }
                    case "--data" -> {
                        String value = requireValue(args, ++i, option);
                        dataPath = Path.of(value);
                        dataPathExplicit = true;
                    }
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

            if (requestedSystemOrder != null) {
                if (systemSelectionExplicit
                        && systems.size()
                            != BenchmarkSystem.values().length) {
                    throw new IllegalArgumentException(
                        "--system-order requires all three systems and "
                            + "cannot be combined with a single-system "
                            + "--system selection"
                    );
                }
                systems = requestedSystemOrder;
            }

            if (dataset == DatasetProfile.LARGE) {
                if (dataPathExplicit) {
                    throw new IllegalArgumentException(
                        "--data cannot be combined with --dataset large; "
                            + "the large dataset path is fixed"
                    );
                }
                dataPath = dataset.defaultPath;
            } else if (!dataPathExplicit) {
                dataPath = dataset.defaultPath;
            }

            if (!experimentSelectionExplicit || experiments.size() > 1) {
                experiments = dataset.defaultExperiments();
            } else {
                BenchmarkExperiment experiment = experiments.get(0);
                if (!dataset.supports(experiment)) {
                    throw new IllegalArgumentException(
                        "--dataset " + dataset.cliName
                            + " does not support " + experiment
                    );
                }
            }

            return new Configuration(
                List.copyOf(experiments),
                List.copyOf(systems),
                dataset,
                warmupRuns,
                formalRuns,
                dataPath.toAbsolutePath().normalize(),
                sessionId,
                help
            );
        }

        private static List<BenchmarkExperiment> parseExperiments(
            String value
        ) {
            if (value.equalsIgnoreCase("all")) {
                return List.of(BenchmarkExperiment.values());
            }
            try {
                return List.of(BenchmarkExperiment.valueOf(
                    value.toUpperCase(Locale.ROOT)
                ));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Invalid experiment: " + value
                );
            }
        }

        private static List<BenchmarkSystem> parseSystems(
            String value
        ) {
            if (value.equalsIgnoreCase("all")) {
                return List.of(BenchmarkSystem.values());
            }
            return List.of(parseSystemName(value));
        }

        private static List<BenchmarkSystem> parseSystemOrder(
            String value
        ) {
            String[] names = value.split(",", -1);
            int requiredCount = BenchmarkSystem.values().length;
            if (names.length != requiredCount) {
                throw new IllegalArgumentException(
                    "--system-order requires exactly three comma-separated "
                        + "systems; got " + names.length
                );
            }

            List<BenchmarkSystem> order = new ArrayList<>();
            for (int i = 0; i < names.length; i++) {
                String name = names[i].trim();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException(
                        "--system-order contains an empty system at "
                            + "position " + (i + 1)
                    );
                }

                BenchmarkSystem system = parseSystemName(name);
                if (order.contains(system)) {
                    throw new IllegalArgumentException(
                        "--system-order contains duplicate system: "
                            + name
                    );
                }
                order.add(system);
            }

            if (order.size() != requiredCount) {
                throw new IllegalArgumentException(
                    "--system-order must include java, postgresql, and "
                        + "opengauss exactly once"
                );
            }
            return List.copyOf(order);
        }

        private static BenchmarkSystem parseSystemName(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "java", "java-file" -> BenchmarkSystem.JAVA_FILE;
                case "postgresql", "postgresql-jdbc" ->
                    BenchmarkSystem.POSTGRESQL;
                case "opengauss", "opengauss-jdbc" ->
                    BenchmarkSystem.OPENGAUSS;
                default -> throw new IllegalArgumentException(
                    "Invalid system: " + value
                );
            };
        }

        private static int parseNonNegativeInt(
            String value,
            String option
        ) {
            int parsed = parseInt(value, option);
            if (parsed < 0) {
                throw new IllegalArgumentException(
                    option + " must be at least 0"
                );
            }
            return parsed;
        }

        private static int parsePositiveInt(
            String value,
            String option
        ) {
            int parsed = parseInt(value, option);
            if (parsed < 1) {
                throw new IllegalArgumentException(
                    option + " must be at least 1"
                );
            }
            return parsed;
        }

        private static int parseInt(String value, String option) {
            try {
                return Integer.parseInt(value);
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

        private Path outputFile() {
            return dataset.outputFile(experiments);
        }

        private List<BenchmarkExperiment> largeUpdateExperiments() {
            if (dataset != DatasetProfile.LARGE) {
                return List.of();
            }
            return experiments.stream()
                .filter(experiment -> experiment == BenchmarkExperiment.U3
                    || experiment == BenchmarkExperiment.U4)
                .toList();
        }

        private String systemOrderValue() {
            return String.join(
                ">",
                systems.stream()
                    .map(Configuration::systemCliName)
                    .toList()
            );
        }

        private String systemOrderDisplay() {
            return String.join(
                " -> ",
                systems.stream()
                    .map(BenchmarkSystem::csvName)
                    .toList()
            );
        }

        private static String systemCliName(BenchmarkSystem system) {
            return switch (system) {
                case JAVA_FILE -> "java";
                case POSTGRESQL -> "postgresql";
                case OPENGAUSS -> "opengauss";
            };
        }

        private static String generatedSessionId() {
            return SESSION_TIME.format(Instant.now()) + "-"
                + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
