import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;



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
        if (args.length > 0 && args[0].equalsIgnoreCase("c1")) {
            ConcurrencyBenchmarkRunner.main(
                Arrays.copyOfRange(args, 1, args.length)
            );
            return;
        }

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


enum BenchmarkExperiment {
    E1,
    E2,
    U1,
    U2,
    U3,
    U4
}



enum BenchmarkSystem {
    JAVA_FILE("Java-File"),
    POSTGRESQL("PostgreSQL-JDBC"),
    OPENGAUSS("openGauss-JDBC");

    private final String csvName;

    BenchmarkSystem(String csvName) {
        this.csvName = csvName;
    }

    public String csvName() {
        return csvName;
    }
}



record BenchmarkResult(
    long elapsedNs,
    Integer resultCount,
    Integer rowsUpdated,
    boolean passed,
    String notes
) {
    public BenchmarkResult {
        if (elapsedNs < 0) {
            throw new IllegalArgumentException("elapsedNs cannot be negative");
        }
        if (notes == null) {
            notes = "";
        }
    }

    public static BenchmarkResult queryPass(
        long elapsedNs,
        int resultCount,
        String notes
    ) {
        return new BenchmarkResult(
            elapsedNs, resultCount, null, true, notes
        );
    }

    public static BenchmarkResult updatePass(
        long elapsedNs,
        int rowsUpdated,
        String notes
    ) {
        return new BenchmarkResult(
            elapsedNs, null, rowsUpdated, true, notes
        );
    }

    public static BenchmarkResult failure(
        long elapsedNs,
        Integer resultCount,
        Integer rowsUpdated,
        String notes
    ) {
        return new BenchmarkResult(
            elapsedNs, resultCount, rowsUpdated, false, notes
        );
    }
}



record MovieRecord(
    int movieId,
    String title,
    String country,
    int yearReleased,
    Integer runtime
) {
    public MovieRecord {
        Objects.requireNonNull(title, "title");
        Objects.requireNonNull(country, "country");
    }

    public static MovieRecord parse(String line, int lineNumber) {
        String[] rawFields = line.split("\\|", -1);
        if (rawFields.length != 5) {
            throw new IllegalArgumentException(
                "Line " + lineNumber + " has " + rawFields.length
                    + " fields; expected 5"
            );
        }

        String[] fields = new String[rawFields.length];
        for (int i = 0; i < rawFields.length; i++) {
            fields[i] = decodeField(rawFields[i], lineNumber);
        }

        try {
            Integer runtime = fields[4].equalsIgnoreCase("null")
                ? null
                : Integer.valueOf(fields[4]);

            return new MovieRecord(
                Integer.parseInt(fields[0]),
                fields[1],
                fields[2],
                Integer.parseInt(fields[3]),
                runtime
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Line " + lineNumber + " contains an invalid number", e
            );
        }
    }

    public MovieRecord withRuntime(int newRuntime) {
        return new MovieRecord(
            movieId, title, country, yearReleased, newRuntime
        );
    }

    public String toPipeDelimited() {
        return movieId + "|" + encodeField(title) + "|"
            + encodeField(country) + "|" + yearReleased + "|"
            + (runtime == null ? "null" : runtime);
    }

    private static String decodeField(String value, int lineNumber) {
        if (value.length() < 2
                || value.charAt(0) != '"'
                || value.charAt(value.length() - 1) != '"') {
            return value;
        }

        String inner = value.substring(1, value.length() - 1);
        StringBuilder decoded = new StringBuilder(inner.length());
        for (int i = 0; i < inner.length(); i++) {
            char current = inner.charAt(i);
            if (current != '"') {
                decoded.append(current);
                continue;
            }
            if (i + 1 >= inner.length()
                    || inner.charAt(i + 1) != '"') {
                throw new IllegalArgumentException(
                    "Line " + lineNumber
                        + " contains an invalid quoted field"
                );
            }
            decoded.append('"');
            i++;
        }
        return decoded.toString();
    }

    private static String encodeField(String value) {
        if (value.indexOf('|') < 0
                && value.indexOf('"') < 0
                && value.indexOf('\r') < 0
                && value.indexOf('\n') < 0) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}




record DatabaseConfiguration(
    String driverClass,
    String url,
    String user,
    String password,
    String schema
) {
    public static DatabaseConfiguration forSystem(
        BenchmarkSystem system
    ) {
        return switch (system) {
            case POSTGRESQL -> new DatabaseConfiguration(
                "org.postgresql.Driver",
                environment(
                    "BENCHMARK_POSTGRES_URL",
                    "jdbc:postgresql://127.0.0.1:5432/filmdb1"
                ),
                environment("BENCHMARK_POSTGRES_USER", "postgres"),
                requiredEnvironment("BENCHMARK_POSTGRES_PASSWORD"),
                environment("BENCHMARK_POSTGRES_SCHEMA", "public")
            );
            case OPENGAUSS -> new DatabaseConfiguration(
                "org.opengauss.Driver",
                environment(
                    "BENCHMARK_OPENGAUSS_URL",
                    "jdbc:opengauss://127.0.0.1:8888/postgres"
                ),
                environment("BENCHMARK_OPENGAUSS_USER", "gaussdb"),
                requiredEnvironment("BENCHMARK_OPENGAUSS_PASSWORD"),
                environment("BENCHMARK_OPENGAUSS_SCHEMA", "gaussdb")
            );
            case JAVA_FILE -> throw new IllegalArgumentException(
                "Java file operations do not use a database"
            );
        };
    }

    private static String environment(
        String name,
        String defaultValue
    ) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                "Required environment variable is not set: " + name
            );
        }
        return value;
    }

    @Override
    public String toString() {
        return "DatabaseConfiguration[driverClass=" + driverClass
            + ", url=" + url + ", user=" + user
            + ", password=<redacted>, schema=" + schema + "]";
    }
}





final class BenchmarkCsvLogger {

    private static final Path FILE =
        Path.of("result", "measurements", "benchmark_raw.csv");

    private static final String HEADER =
        "timestamp,session_id,experiment_id,system,dataset,record_count,"
      + "index_state,run_type,iteration,elapsed_ns,elapsed_ms,"
      + "result_count,rows_updated,status,notes";

    private static final int FIELD_COUNT = 15;

    private BenchmarkCsvLogger() {
    }

    public static synchronized void appendRow(
        Path file,
        String[] values
    ) throws IOException {
        if (values.length != FIELD_COUNT) {
            throw new IllegalArgumentException(
                "Expected " + FIELD_COUNT + " CSV fields, got "
                    + values.length
            );
        }

        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        boolean writeHeader = !Files.exists(file) || Files.size(file) == 0;
        if (!writeHeader) {
            validateFile(file);
        }

        try (BufferedWriter out = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {
            if (writeHeader) {
                out.write(HEADER);
                out.newLine();
            }

            String line = Arrays.stream(values)
                .map(BenchmarkCsvLogger::escapeCsv)
                .collect(Collectors.joining(","));

            out.write(line);
            out.newLine();
        }
    }

    public static synchronized void validateFile(Path file)
        throws IOException {
        if (!Files.exists(file) || Files.size(file) == 0) {
            return;
        }

        try (var reader = Files.newBufferedReader(
                file, StandardCharsets.UTF_8)) {
            String existingHeader = reader.readLine();
            if (!HEADER.equals(existingHeader)) {
                throw new IOException(
                    "Existing CSV header is incompatible: " + file
                );
            }
        }
    }

    public static Path defaultFile() {
        return FILE;
    }

    static String escapeCsv(String value) {
        if (value == null) {
            value = "";
        }

        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}





final class BenchmarkData {
    public static final String HEADER =
        "movieid|title|country|year_released|runtime";
    public static final String EXACT_TITLE = "Ardh Satya";
    public static final String SUBSTRING = "Batman";
    public static final int NEW_RUNTIME = 999;
    public static final int EXPECTED_EXACT_COUNT = 1;
    public static final int LARGE_BATCH_SIZE = 9_204;
    public static final int LARGE_BATCH_COUNT = 110;
    public static final int LARGE_BATCH_UPDATE_SIZE = 1_000;
    public static final int EXPECTED_U3_UPDATE_COUNT = 1;
    public static final int EXPECTED_U4_UPDATE_COUNT =
        LARGE_BATCH_COUNT * LARGE_BATCH_UPDATE_SIZE;

    private static final MovieRecord EXPECTED_EXACT_RECORD =
        new MovieRecord(5, "Ardh Satya", "in", 1983, 130);

    private final Path source;
    private final List<MovieRecord> records;
    private final List<MovieRecord> exactMatches;
    private final List<MovieRecord> substringMatches;
    private final List<MovieRecord> u1Targets;
    private final List<MovieRecord> u2Targets;
    private final List<MovieRecord> u3Targets;
    private final List<MovieRecord> u4Targets;
    private final List<MovieRecord> u1ExpectedRecords;
    private final List<MovieRecord> u2ExpectedRecords;
    private final List<MovieRecord> u3ExpectedRecords;
    private final List<MovieRecord> u4ExpectedRecords;

    private BenchmarkData(
        Path source,
        List<MovieRecord> records,
        List<BenchmarkExperiment> enabledUpdates
    ) {
        this.source = source;
        this.records = List.copyOf(records);
        this.exactMatches = matching(
            records, record -> record.title().equals(EXACT_TITLE)
        );
        this.substringMatches = matching(
            records, record -> record.title().contains(SUBSTRING)
        );

        if (enabledUpdates.contains(BenchmarkExperiment.U1)) {
            this.u1Targets = matching(
                records, record -> record.movieId() == 5
            );
            this.u1ExpectedRecords = transformed(
                records, record -> record.movieId() == 5
            );
        } else {
            this.u1Targets = List.of();
            this.u1ExpectedRecords = List.of();
        }

        if (enabledUpdates.contains(BenchmarkExperiment.U2)) {
            this.u2Targets = matching(records, record ->
                record.movieId() >= 1 && record.movieId() <= 1_000
            );
            this.u2ExpectedRecords = transformed(records, record ->
                record.movieId() >= 1 && record.movieId() <= 1_000
            );
        } else {
            this.u2Targets = List.of();
            this.u2ExpectedRecords = List.of();
        }

        if (enabledUpdates.contains(BenchmarkExperiment.U3)) {
            this.u3Targets = matching(
                records, record -> record.movieId() == 5
            );
            this.u3ExpectedRecords = transformed(
                records, record -> record.movieId() == 5
            );
        } else {
            this.u3Targets = List.of();
            this.u3ExpectedRecords = List.of();
        }

        if (enabledUpdates.contains(BenchmarkExperiment.U4)) {
            this.u4Targets = matching(
                records,
                record -> isLargeBatchUpdateTarget(record.movieId())
            );
            this.u4ExpectedRecords = transformed(
                records,
                record -> isLargeBatchUpdateTarget(record.movieId())
            );
        } else {
            this.u4Targets = List.of();
            this.u4ExpectedRecords = List.of();
        }
    }

    public static BenchmarkData load(
        Path source,
        int expectedRecordCount,
        int expectedSubstringCount,
        boolean validateOriginalExactRecord,
        List<BenchmarkExperiment> enabledUpdates
    ) throws IOException {
        for (BenchmarkExperiment experiment : enabledUpdates) {
            if (experiment != BenchmarkExperiment.U1
                    && experiment != BenchmarkExperiment.U2
                    && experiment != BenchmarkExperiment.U3
                    && experiment != BenchmarkExperiment.U4) {
                throw new IllegalArgumentException(
                    experiment + " is not an update experiment"
                );
            }
        }

        Path normalizedSource = source.toAbsolutePath().normalize();
        List<MovieRecord> records = readRecords(normalizedSource);

        if (records.size() != expectedRecordCount) {
            throw new IllegalStateException(
                "Source dataset has " + records.size() + " records; expected "
                    + expectedRecordCount
            );
        }

        Set<Integer> ids = new HashSet<>();
        for (MovieRecord record : records) {
            if (!ids.add(record.movieId())) {
                throw new IllegalStateException(
                    "Duplicate movieid in source dataset: "
                        + record.movieId()
                );
            }
        }

        BenchmarkData data = new BenchmarkData(
            normalizedSource, records, List.copyOf(enabledUpdates)
        );
        if (data.exactMatches.size() != EXPECTED_EXACT_COUNT) {
            throw new IllegalStateException(
                "E1 baseline has " + data.exactMatches.size()
                    + " matches; expected " + EXPECTED_EXACT_COUNT
            );
        }
        if (validateOriginalExactRecord
                && !data.exactMatches.equals(
                    List.of(EXPECTED_EXACT_RECORD))) {
            throw new IllegalStateException(
                "E1 baseline mismatch: expected " + EXPECTED_EXACT_RECORD
                    + ", found " + data.exactMatches
            );
        }
        if (data.substringMatches.size() != expectedSubstringCount) {
            throw new IllegalStateException(
                "E2 baseline has " + data.substringMatches.size()
                    + " matches; expected " + expectedSubstringCount
            );
        }
        if (enabledUpdates.contains(BenchmarkExperiment.U1)
                && data.u1Targets.isEmpty()) {
            throw new IllegalStateException(
                "U1 baseline has no movieid 5 target"
            );
        }
        if (enabledUpdates.contains(BenchmarkExperiment.U2)
                && data.u2Targets.isEmpty()) {
            throw new IllegalStateException(
                "U2 baseline has no movieid 1 through 1000 targets"
            );
        }
        requireUpdateCount(
            enabledUpdates,
            BenchmarkExperiment.U3,
            data.u3Targets.size(),
            EXPECTED_U3_UPDATE_COUNT
        );
        requireUpdateCount(
            enabledUpdates,
            BenchmarkExperiment.U4,
            data.u4Targets.size(),
            EXPECTED_U4_UPDATE_COUNT
        );

        return data;
    }

    public static List<MovieRecord> readRecords(Path path)
        throws IOException {
        List<MovieRecord> records = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(
                path, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (!HEADER.equals(header)) {
                throw new IOException(
                    "Unexpected header in " + path + ": " + header
                );
            }

            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                records.add(MovieRecord.parse(line, lineNumber));
            }
        }

        return records;
    }

    public static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8_192];
                int bytesRead;
                while ((bytesRead = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, bytesRead);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    public Path source() {
        return source;
    }

    public List<MovieRecord> records() {
        return records;
    }

    public List<MovieRecord> expectedQuery(BenchmarkExperiment experiment) {
        return switch (experiment) {
            case E1 -> exactMatches;
            case E2 -> substringMatches;
            default -> throw new IllegalArgumentException(
                experiment + " is not a query experiment"
            );
        };
    }

    public List<MovieRecord> updateTargets(
        BenchmarkExperiment experiment
    ) {
        return switch (experiment) {
            case U1 -> u1Targets;
            case U2 -> u2Targets;
            case U3 -> u3Targets;
            case U4 -> u4Targets;
            default -> throw new IllegalArgumentException(
                experiment + " is not an update experiment"
            );
        };
    }

    public List<MovieRecord> expectedUpdatedRecords(
        BenchmarkExperiment experiment
    ) {
        return switch (experiment) {
            case U1 -> u1ExpectedRecords;
            case U2 -> u2ExpectedRecords;
            case U3 -> u3ExpectedRecords;
            case U4 -> u4ExpectedRecords;
            default -> throw new IllegalArgumentException(
                experiment + " is not an update experiment"
            );
        };
    }

    public List<MovieRecord> expectedUpdatedTargets(
        BenchmarkExperiment experiment
    ) {
        return updateTargets(experiment).stream()
            .map(record -> record.withRuntime(NEW_RUNTIME))
            .toList();
    }

    public int expectedUpdateCount(BenchmarkExperiment experiment) {
        return updateTargets(experiment).size();
    }

    public boolean isUpdateTarget(
        BenchmarkExperiment experiment,
        int movieId
    ) {
        return switch (experiment) {
            case U1, U3 -> movieId == 5;
            case U2 -> movieId >= 1 && movieId <= 1_000;
            case U4 -> isLargeBatchUpdateTarget(movieId);
            default -> false;
        };
    }

    private static boolean isLargeBatchUpdateTarget(int movieId) {
        return (movieId - 1) % LARGE_BATCH_SIZE
            < LARGE_BATCH_UPDATE_SIZE;
    }

    private static void requireUpdateCount(
        List<BenchmarkExperiment> enabledUpdates,
        BenchmarkExperiment experiment,
        int actual,
        int expected
    ) {
        if (enabledUpdates.contains(experiment) && actual != expected) {
            throw new IllegalStateException(
                experiment + " baseline has " + actual
                    + " targets; expected " + expected
            );
        }
    }

    public static boolean sameRecords(
        List<MovieRecord> expected,
        List<MovieRecord> actual
    ) {
        return sorted(expected).equals(sorted(actual));
    }

    public static String describeDifference(
        List<MovieRecord> expected,
        List<MovieRecord> actual
    ) {
        if (expected.size() != actual.size()) {
            return "record count expected=" + expected.size()
                + ", actual=" + actual.size();
        }

        Map<Integer, MovieRecord> expectedById = byId(expected);
        Map<Integer, MovieRecord> actualById = byId(actual);
        for (Map.Entry<Integer, MovieRecord> entry
                : expectedById.entrySet()) {
            MovieRecord actualRecord = actualById.get(entry.getKey());
            if (!entry.getValue().equals(actualRecord)) {
                return "movieid " + entry.getKey() + " expected="
                    + entry.getValue() + ", actual=" + actualRecord;
            }
        }

        for (Integer movieId : actualById.keySet()) {
            if (!expectedById.containsKey(movieId)) {
                return "unexpected movieid " + movieId;
            }
        }

        return "record order differs";
    }

    private static List<MovieRecord> matching(
        List<MovieRecord> source,
        Predicate<MovieRecord> predicate
    ) {
        return source.stream().filter(predicate).toList();
    }

    private static List<MovieRecord> transformed(
        List<MovieRecord> source,
        Predicate<MovieRecord> predicate
    ) {
        return source.stream()
            .map(record -> predicate.test(record)
                ? record.withRuntime(NEW_RUNTIME)
                : record)
            .toList();
    }

    private static List<MovieRecord> sorted(List<MovieRecord> records) {
        return records.stream()
            .sorted(Comparator.comparingInt(MovieRecord::movieId))
            .toList();
    }

    private static Map<Integer, MovieRecord> byId(
        List<MovieRecord> records
    ) {
        return records.stream().collect(Collectors.toMap(
            MovieRecord::movieId,
            record -> record,
            (first, second) -> first,
            LinkedHashMap::new
        ));
    }
}





final class FileBenchmark {
    private final BenchmarkData data;
    private final Path workDirectory;
    private final String sourceHash;

    public FileBenchmark(
        BenchmarkData data,
        Path workDirectory,
        String sourceHash
    ) throws IOException {
        this.data = data;
        this.workDirectory = workDirectory.toAbsolutePath().normalize();
        this.sourceHash = sourceHash;
        Files.createDirectories(this.workDirectory);
    }

    public BenchmarkResult run(BenchmarkExperiment experiment) {
        return switch (experiment) {
            case E1 -> runQuery(
                experiment,
                record -> record.title().equals(BenchmarkData.EXACT_TITLE)
            );
            case E2 -> runQuery(
                experiment,
                record -> record.title().contains(BenchmarkData.SUBSTRING)
            );
            case U1, U2, U3, U4 -> runUpdate(experiment);
        };
    }

    private BenchmarkResult runQuery(
        BenchmarkExperiment experiment,
        Predicate<MovieRecord> predicate
    ) {
        List<MovieRecord> matches = new ArrayList<>();
        int recordsRead = 0;
        long start = System.nanoTime();
        long elapsedNs = -1;

        try {
            try (BufferedReader reader = Files.newBufferedReader(
                    data.source(), StandardCharsets.UTF_8)) {
                String header = reader.readLine();
                if (!BenchmarkData.HEADER.equals(header)) {
                    throw new IOException(
                        "Unexpected source header: " + header
                    );
                }

                String line;
                int lineNumber = 1;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    MovieRecord record = MovieRecord.parse(
                        line, lineNumber
                    );
                    recordsRead++;
                    if (predicate.test(record)) {
                        matches.add(record);
                    }
                }
            }
            elapsedNs = System.nanoTime() - start;

            List<MovieRecord> expected = data.expectedQuery(experiment);
            if (recordsRead != data.records().size()) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    matches.size(),
                    null,
                    "Validation failed: scanned " + recordsRead
                        + " records; expected " + data.records().size()
                );
            }
            if (!BenchmarkData.sameRecords(expected, matches)) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    matches.size(),
                    null,
                    "Validation failed: "
                        + BenchmarkData.describeDifference(
                            expected, matches
                        )
                );
            }

            return BenchmarkResult.queryPass(
                elapsedNs,
                matches.size(),
                "validated_full_result_set;records_scanned="
                    + recordsRead
            );
        } catch (Exception e) {
            if (elapsedNs < 0) {
                elapsedNs = System.nanoTime() - start;
            }
            return BenchmarkResult.failure(
                elapsedNs,
                matches.size(),
                null,
                failureMessage(e)
            );
        }
    }

    private BenchmarkResult runUpdate(BenchmarkExperiment experiment) {
        Path output = workDirectory.resolve(
            updateOutputFileName(experiment)
        );

        try {
            Files.deleteIfExists(output);
        } catch (IOException e) {
            return BenchmarkResult.failure(
                0,
                null,
                null,
                "Output cleanup failed before timing: "
                    + failureMessage(e)
            );
        }

        int recordsRead = 0;
        int rowsUpdated = 0;
        long start = System.nanoTime();
        long elapsedNs = -1;

        try {
            try (
                BufferedReader reader = Files.newBufferedReader(
                    data.source(), StandardCharsets.UTF_8
                );
                BufferedWriter writer = Files.newBufferedWriter(
                    output, StandardCharsets.UTF_8
                )
            ) {
                String header = reader.readLine();
                if (!BenchmarkData.HEADER.equals(header)) {
                    throw new IOException(
                        "Unexpected source header: " + header
                    );
                }
                writer.write(header);
                writer.newLine();

                String line;
                int lineNumber = 1;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    MovieRecord record = MovieRecord.parse(
                        line, lineNumber
                    );
                    recordsRead++;

                    if (data.isUpdateTarget(
                            experiment, record.movieId())) {
                        record = record.withRuntime(
                            BenchmarkData.NEW_RUNTIME
                        );
                        rowsUpdated++;
                    }

                    writer.write(record.toPipeDelimited());
                    writer.newLine();
                }
            }
            elapsedNs = System.nanoTime() - start;

            int expectedCount = data.expectedUpdateCount(experiment);
            if (recordsRead != data.records().size()) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    null,
                    rowsUpdated,
                    "Validation failed: read " + recordsRead
                        + " records; expected " + data.records().size()
                );
            }
            if (rowsUpdated != expectedCount) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    null,
                    rowsUpdated,
                    "Validation failed: updated " + rowsUpdated
                        + " records; expected " + expectedCount
                );
            }

            List<MovieRecord> outputRecords =
                BenchmarkData.readRecords(output);
            List<MovieRecord> expectedRecords =
                data.expectedUpdatedRecords(experiment);
            if (!expectedRecords.equals(outputRecords)) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    null,
                    rowsUpdated,
                    "Output validation failed: "
                        + BenchmarkData.describeDifference(
                            expectedRecords, outputRecords
                        )
                );
            }

            String currentSourceHash = BenchmarkData.sha256(
                data.source()
            );
            if (!sourceHash.equals(currentSourceHash)) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    null,
                    rowsUpdated,
                    "Source dataset hash changed during file update"
                );
            }

            return BenchmarkResult.updatePass(
                elapsedNs,
                rowsUpdated,
                "validated_complete_output;source_hash_unchanged;output="
                    + output
            );
        } catch (Exception e) {
            if (elapsedNs < 0) {
                elapsedNs = System.nanoTime() - start;
            }
            return BenchmarkResult.failure(
                elapsedNs,
                null,
                rowsUpdated,
                failureMessage(e)
            );
        }
    }

    private static String updateOutputFileName(
        BenchmarkExperiment experiment
    ) {
        return switch (experiment) {
            case U1 -> "movies_update_single.txt";
            case U2 -> "movies_update_multiple.txt";
            case U3 -> "movies_large_update_u3_single.txt";
            case U4 -> "movies_large_update_u4_batches.txt";
            default -> throw new IllegalArgumentException(
                experiment + " is not an update experiment"
            );
        };
    }

    private static String failureMessage(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            message = exception.getClass().getSimpleName();
        } else {
            message = exception.getClass().getSimpleName() + ": "
                + message;
        }
        return message.length() <= 1_000
            ? message
            : message.substring(0, 1_000);
    }
}





final class JdbcBenchmark implements AutoCloseable {
    private static final String SELECT_COLUMNS =
        "movieid, title, country, year_released, runtime";

    private final Connection connection;
    private final BenchmarkData data;
    private final PreparedStatement exactSearch;
    private final PreparedStatement substringSearch;
    private final PreparedStatement singleUpdate;
    private final PreparedStatement multipleUpdate;
    private final PreparedStatement largeSingleUpdate;
    private final PreparedStatement largeBatchUpdate;
    private final PreparedStatement singleTargetQuery;
    private final PreparedStatement multipleTargetQuery;
    private final PreparedStatement largeSingleTargetQuery;
    private final PreparedStatement largeBatchTargetQuery;
    private final List<PreparedStatement> statements;

    public JdbcBenchmark(
        Connection connection,
        String schema,
        String tableName,
        BenchmarkData data
    ) throws SQLException {
        this.connection = connection;
        this.data = data;

        String table = qualifiedTable(connection, schema, tableName);
        exactSearch = connection.prepareStatement(
            "SELECT " + SELECT_COLUMNS + " FROM " + table
                + " WHERE title = ?"
        );
        exactSearch.setString(1, BenchmarkData.EXACT_TITLE);

        substringSearch = prepareSubstringSearch(
            connection, schema, tableName
        );

        singleUpdate = connection.prepareStatement(
            updateSql(table, BenchmarkExperiment.U1)
        );
        multipleUpdate = connection.prepareStatement(
            updateSql(table, BenchmarkExperiment.U2)
        );
        largeSingleUpdate = connection.prepareStatement(
            updateSql(table, BenchmarkExperiment.U3)
        );
        largeBatchUpdate = connection.prepareStatement(
            updateSql(table, BenchmarkExperiment.U4)
        );

        singleTargetQuery = connection.prepareStatement(
            targetQuerySql(table, BenchmarkExperiment.U1)
        );
        multipleTargetQuery = connection.prepareStatement(
            targetQuerySql(table, BenchmarkExperiment.U2)
        );
        largeSingleTargetQuery = connection.prepareStatement(
            targetQuerySql(table, BenchmarkExperiment.U3)
        );
        largeBatchTargetQuery = connection.prepareStatement(
            targetQuerySql(table, BenchmarkExperiment.U4)
        );

        statements = List.of(
            exactSearch,
            substringSearch,
            singleUpdate,
            multipleUpdate,
            largeSingleUpdate,
            largeBatchUpdate,
            singleTargetQuery,
            multipleTargetQuery,
            largeSingleTargetQuery,
            largeBatchTargetQuery
        );
    }

    static PreparedStatement prepareSubstringSearch(
        Connection connection,
        String schema,
        String tableName
    ) throws SQLException {
        String table = qualifiedTable(connection, schema, tableName);
        PreparedStatement statement = connection.prepareStatement(
            "SELECT " + SELECT_COLUMNS + " FROM " + table
                + " WHERE title LIKE ?"
        );
        try {
            statement.setString(
                1, "%" + BenchmarkData.SUBSTRING + "%"
            );
            return statement;
        } catch (SQLException e) {
            try {
                statement.close();
            } catch (SQLException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    public BenchmarkResult run(BenchmarkExperiment experiment) {
        return switch (experiment) {
            case E1 -> runQuery(experiment, exactSearch);
            case E2 -> runQuery(experiment, substringSearch);
            case U1 -> runUpdate(
                experiment, singleUpdate, singleTargetQuery
            );
            case U2 -> runUpdate(
                experiment, multipleUpdate, multipleTargetQuery
            );
            case U3 -> runUpdate(
                experiment, largeSingleUpdate, largeSingleTargetQuery
            );
            case U4 -> runUpdate(
                experiment, largeBatchUpdate, largeBatchTargetQuery
            );
        };
    }

    public static Inspection inspect(
        Connection connection,
        String schema,
        String tableName,
        boolean includeCompleteRows
    ) throws SQLException {
        String table = qualifiedTable(connection, schema, tableName);
        List<MovieRecord> rows;
        long rowCount;

        if (includeCompleteRows) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT " + SELECT_COLUMNS + " FROM " + table
                        + " ORDER BY movieid")) {
                rows = executeAndRead(statement);
            }
            rowCount = rows.size();
        } else {
            rows = List.of();
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT COUNT(*) FROM " + table);
                 ResultSet result = statement.executeQuery()) {
                result.next();
                rowCount = result.getLong(1);
            }
        }

        TableMetadata tableMetadata = inspectTableMetadata(
            connection, schema, tableName
        );
        List<String> warnings = new ArrayList<>(
            tableMetadata.warnings()
        );
        validateRequiredMetadata(tableMetadata, warnings, tableName);

        DatabaseMetaData metadata = connection.getMetaData();
        return new Inspection(
            metadata.getDatabaseProductName(),
            metadata.getDatabaseProductVersion(),
            rowCount,
            rows,
            tableMetadata,
            List.copyOf(warnings)
        );
    }

    public static Map<BenchmarkExperiment, List<String>> explainUpdatePlans(
        Connection connection,
        String schema,
        String tableName,
        List<BenchmarkExperiment> experiments
    ) throws SQLException {
        String table = qualifiedTable(connection, schema, tableName);
        Map<BenchmarkExperiment, List<String>> plans =
            new LinkedHashMap<>();

        for (BenchmarkExperiment experiment : experiments) {
            if (experiment != BenchmarkExperiment.U3
                    && experiment != BenchmarkExperiment.U4) {
                throw new IllegalArgumentException(
                    "EXPLAIN is only configured for U3 and U4; got "
                        + experiment
                );
            }

            List<String> planLines = new ArrayList<>();
            String sql = "EXPLAIN " + updateSql(table, experiment);
            try (PreparedStatement statement = connection.prepareStatement(
                    sql);
                 ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    planLines.add(results.getString(1));
                }
            }
            if (planLines.isEmpty()) {
                throw new SQLException(
                    "EXPLAIN returned no plan rows for " + experiment
                );
            }
            plans.put(experiment, List.copyOf(planLines));
        }

        return Map.copyOf(plans);
    }

    private static String updateSql(
        String table,
        BenchmarkExperiment experiment
    ) {
        return "UPDATE " + table + " SET runtime = "
            + BenchmarkData.NEW_RUNTIME + " WHERE "
            + updatePredicate(experiment);
    }

    private static String targetQuerySql(
        String table,
        BenchmarkExperiment experiment
    ) {
        return "SELECT " + SELECT_COLUMNS + " FROM " + table
            + " WHERE " + updatePredicate(experiment)
            + " ORDER BY movieid";
    }

    private static String updatePredicate(
        BenchmarkExperiment experiment
    ) {
        return switch (experiment) {
            case U1, U3 -> "movieid = 5";
            case U2 -> "movieid BETWEEN 1 AND 1000";
            case U4 -> largeBatchRangePredicate();
            default -> throw new IllegalArgumentException(
                experiment + " is not an update experiment"
            );
        };
    }

    private static String largeBatchRangePredicate() {
        StringBuilder predicate = new StringBuilder("(");
        for (int batch = 0;
                batch < BenchmarkData.LARGE_BATCH_COUNT;
                batch++) {
            if (batch > 0) {
                predicate.append(" OR ");
            }
            int firstMovieId = batch * BenchmarkData.LARGE_BATCH_SIZE + 1;
            int lastMovieId = firstMovieId
                + BenchmarkData.LARGE_BATCH_UPDATE_SIZE - 1;
            predicate.append("movieid BETWEEN ")
                .append(firstMovieId)
                .append(" AND ")
                .append(lastMovieId);
        }
        return predicate.append(')').toString();
    }

    private BenchmarkResult runQuery(
        BenchmarkExperiment experiment,
        PreparedStatement statement
    ) {
        List<MovieRecord> actual = List.of();
        long start = System.nanoTime();
        long elapsedNs = -1;

        try {
            actual = executeAndRead(statement);
            elapsedNs = System.nanoTime() - start;

            List<MovieRecord> expected = data.expectedQuery(experiment);
            if (!BenchmarkData.sameRecords(expected, actual)) {
                return BenchmarkResult.failure(
                    elapsedNs,
                    actual.size(),
                    null,
                    "Validation failed: "
                        + BenchmarkData.describeDifference(
                            expected, actual
                        )
                );
            }

            return BenchmarkResult.queryPass(
                elapsedNs,
                actual.size(),
                "validated_full_result_set"
            );
        } catch (Exception e) {
            if (elapsedNs < 0) {
                elapsedNs = System.nanoTime() - start;
            }
            return BenchmarkResult.failure(
                elapsedNs,
                actual.size(),
                null,
                failureMessage(e)
            );
        }
    }

    private BenchmarkResult runUpdate(
        BenchmarkExperiment experiment,
        PreparedStatement updateStatement,
        PreparedStatement targetQuery
    ) {
        int affectedRows = -1;
        long elapsedNs = 0;
        StringBuilder problems = new StringBuilder();
        boolean rollbackSucceeded = false;

        try {
            prepareForUpdate();

            long start = System.nanoTime();
            try {
                affectedRows = updateStatement.executeUpdate();
            } finally {
                elapsedNs = System.nanoTime() - start;
            }

            int expectedCount = data.expectedUpdateCount(experiment);
            if (affectedRows != expectedCount) {
                appendProblem(
                    problems,
                    "affected rows expected=" + expectedCount
                        + ", actual=" + affectedRows
                );
            }

            List<MovieRecord> updatedTargets = executeAndRead(
                targetQuery
            );
            List<MovieRecord> expectedUpdatedTargets =
                data.expectedUpdatedTargets(experiment);
            if (!BenchmarkData.sameRecords(
                    expectedUpdatedTargets, updatedTargets)) {
                appendProblem(
                    problems,
                    "in-transaction validation failed: "
                        + BenchmarkData.describeDifference(
                            expectedUpdatedTargets, updatedTargets
                        )
                );
            }
        } catch (Exception e) {
            appendProblem(problems, failureMessage(e));
        } finally {
            try {
                if (connection.getAutoCommit()) {
                    rollbackSucceeded = true;
                } else {
                    connection.rollback();
                    rollbackSucceeded = true;
                }
            } catch (SQLException rollbackError) {
                appendProblem(
                    problems,
                    "rollback failed: " + failureMessage(rollbackError)
                );
            }
        }

        if (rollbackSucceeded) {
            verifyRollbackAndRestoreAutoCommit(
                experiment, targetQuery, problems
            );
        }

        Integer recordedRows = affectedRows < 0 ? null : affectedRows;
        if (problems.length() > 0) {
            return BenchmarkResult.failure(
                elapsedNs,
                null,
                recordedRows,
                problems.toString()
            );
        }

        return BenchmarkResult.updatePass(
            elapsedNs,
            affectedRows,
            "validated_updated_targets=" + affectedRows
                + ";rollback_verified"
        );
    }

    private void prepareForUpdate() throws SQLException {
        if (!connection.getAutoCommit()) {
            connection.rollback();
            connection.setAutoCommit(true);
        }
        connection.setAutoCommit(false);
    }

    private void verifyRollbackAndRestoreAutoCommit(
        BenchmarkExperiment experiment,
        PreparedStatement targetQuery,
        StringBuilder problems
    ) {
        boolean finalRollbackSucceeded = false;

        try {
            List<MovieRecord> afterRollback = executeAndRead(
                targetQuery
            );
            List<MovieRecord> baselineTargets =
                data.updateTargets(experiment);
            if (!BenchmarkData.sameRecords(
                    baselineTargets, afterRollback)) {
                appendProblem(
                    problems,
                    "post-rollback validation failed: "
                        + BenchmarkData.describeDifference(
                            baselineTargets, afterRollback
                        )
                );
            }
        } catch (Exception e) {
            appendProblem(
                problems,
                "post-rollback query failed: " + failureMessage(e)
            );
        } finally {
            try {
                if (connection.getAutoCommit()) {
                    finalRollbackSucceeded = true;
                } else {
                    connection.rollback();
                    finalRollbackSucceeded = true;
                }
            } catch (SQLException rollbackError) {
                appendProblem(
                    problems,
                    "post-validation rollback failed: "
                        + failureMessage(rollbackError)
                );
            }
        }

        if (finalRollbackSucceeded) {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException autoCommitError) {
                appendProblem(
                    problems,
                    "could not restore auto-commit: "
                        + failureMessage(autoCommitError)
                );
            }
        }
    }

    private static List<MovieRecord> executeAndRead(
        PreparedStatement statement
    ) throws SQLException {
        List<MovieRecord> records = new ArrayList<>();
        try (ResultSet results = statement.executeQuery()) {
            while (results.next()) {
                records.add(readMovie(results));
            }
        }
        return records;
    }

    private static MovieRecord readMovie(ResultSet results)
        throws SQLException {
        int runtimeValue = results.getInt("runtime");
        Integer runtime = results.wasNull() ? null : runtimeValue;
        String country = results.getString("country");
        if (country != null) {
            country = country.stripTrailing();
        }

        return new MovieRecord(
            results.getInt("movieid"),
            results.getString("title"),
            country,
            results.getInt("year_released"),
            runtime
        );
    }

    private static TableMetadata inspectTableMetadata(
        Connection connection,
        String schema,
        String tableName
    ) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        List<ColumnDefinition> columns = new ArrayList<>();
        try (ResultSet results = metadata.getColumns(
                null, schema, tableName, null)) {
            while (results.next()) {
                columns.add(new ColumnDefinition(
                    results.getInt("ORDINAL_POSITION"),
                    results.getString("COLUMN_NAME").toLowerCase(
                        Locale.ROOT
                    ),
                    results.getInt("DATA_TYPE"),
                    results.getString("TYPE_NAME").toLowerCase(
                        Locale.ROOT
                    ),
                    results.getInt("COLUMN_SIZE"),
                    results.getInt("NULLABLE")
                        != DatabaseMetaData.columnNoNulls
                ));
            }
        }
        columns.sort(Comparator.comparingInt(ColumnDefinition::ordinal));

        TreeMap<Short, String> primaryKeyColumns = new TreeMap<>();
        try (ResultSet results = metadata.getPrimaryKeys(
                null, schema, tableName)) {
            while (results.next()) {
                primaryKeyColumns.put(
                    results.getShort("KEY_SEQ"),
                    results.getString("COLUMN_NAME").toLowerCase(
                        Locale.ROOT
                    )
                );
            }
        }

        List<IndexDefinition> indexes = readIndexes(
            metadata, schema, tableName
        );
        List<IndexDefinition> relevantIndexes = indexes.stream()
            .filter(IndexDefinition::isBenchmarkRelevant)
            .toList();

        List<String> foreignKeys = new ArrayList<>();
        try (ResultSet results = metadata.getImportedKeys(
                null, schema, tableName)) {
            while (results.next()) {
                foreignKeys.add(
                    results.getString("FKCOLUMN_NAME").toLowerCase(
                        Locale.ROOT
                    )
                    + "->"
                    + results.getString("PKTABLE_NAME").toLowerCase(
                        Locale.ROOT
                    )
                    + "."
                    + results.getString("PKCOLUMN_NAME").toLowerCase(
                        Locale.ROOT
                    )
                    + ":update=" + results.getShort("UPDATE_RULE")
                    + ":delete=" + results.getShort("DELETE_RULE")
                );
            }
        }
        foreignKeys.sort(String::compareTo);

        List<String> warnings = new ArrayList<>();
        List<String> checks = new ArrayList<>();
        String checkSql =
            "SELECT pg_get_constraintdef(c.oid) AS definition "
            + "FROM pg_catalog.pg_constraint c "
            + "JOIN pg_catalog.pg_class t ON t.oid = c.conrelid "
            + "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace "
            + "WHERE n.nspname = ? AND t.relname = ? "
            + "AND c.contype = 'c' ORDER BY definition";
        try (PreparedStatement statement = connection.prepareStatement(
                checkSql)) {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    checks.add(normalizeConstraint(
                        results.getString("definition"), schema
                    ));
                }
            }
        } catch (SQLException e) {
            warnings.add(
                "Could not inspect check constraints: "
                    + failureMessage(e)
            );
        }
        checks.sort(String::compareTo);

        return new TableMetadata(
            List.copyOf(columns),
            List.copyOf(primaryKeyColumns.values()),
            indexes,
            relevantIndexes,
            List.copyOf(foreignKeys),
            List.copyOf(checks),
            List.copyOf(warnings)
        );
    }

    private static List<IndexDefinition> readIndexes(
        DatabaseMetaData metadata,
        String schema,
        String tableName
    ) throws SQLException {
        Map<String, IndexBuilder> builders = new LinkedHashMap<>();
        int unnamedIndex = 0;

        try (ResultSet results = metadata.getIndexInfo(
                null, schema, tableName, false, false)) {
            while (results.next()) {
                short type = results.getShort("TYPE");
                if (type == DatabaseMetaData.tableIndexStatistic) {
                    continue;
                }

                String name = results.getString("INDEX_NAME");
                if (name == null) {
                    name = "<unnamed-" + (++unnamedIndex) + ">";
                }
                boolean unique = !results.getBoolean("NON_UNIQUE");
                IndexBuilder builder = builders.computeIfAbsent(
                    name,
                    ignored -> new IndexBuilder(unique)
                );

                String column = results.getString("COLUMN_NAME");
                if (column == null) {
                    column = "<expression>";
                } else {
                    column = column.toLowerCase(Locale.ROOT);
                }
                short position = results.getShort("ORDINAL_POSITION");
                builder.columns.put(position, column);

                String filter = results.getString("FILTER_CONDITION");
                if (filter != null && !filter.isBlank()) {
                    builder.filter = filter.toLowerCase(Locale.ROOT)
                        .replaceAll("\\s+", " ")
                        .trim();
                }
            }
        }

        List<IndexDefinition> indexes = builders.values().stream()
            .map(IndexBuilder::build)
            .sorted(Comparator.comparing(IndexDefinition::canonical))
            .toList();
        return List.copyOf(indexes);
    }

    private static void validateRequiredMetadata(
        TableMetadata metadata,
        List<String> warnings,
        String tableName
    ) throws SQLException {
        Map<String, ColumnDefinition> byName = new LinkedHashMap<>();
        for (ColumnDefinition column : metadata.columns()) {
            byName.put(column.name(), column);
        }

        requireColumn(byName, "movieid", Types.INTEGER, tableName);
        requireColumn(byName, "title", Types.VARCHAR, tableName);
        requireColumn(byName, "country", Types.CHAR, tableName);
        requireColumn(byName, "year_released", Types.INTEGER, tableName);
        requireColumn(byName, "runtime", Types.INTEGER, tableName);

        if (byName.size() != 5) {
            warnings.add(
                tableName + " has additional columns: " + byName.keySet()
            );
        }

        List<String> expectedOrder = List.of(
            "movieid", "title", "country", "year_released", "runtime"
        );
        List<String> actualOrder = metadata.columns().stream()
            .map(ColumnDefinition::name)
            .toList();
        if (!expectedOrder.equals(actualOrder)) {
            warnings.add(
                "Column order differs from the source file: "
                    + actualOrder
            );
        }

        if (!metadata.primaryKey().equals(List.of("movieid"))) {
            throw new SQLException(
                tableName + " primary key must be [movieid], found "
                    + metadata.primaryKey()
            );
        }

        boolean hasMovieIdIndex = metadata.relevantIndexes().stream()
            .anyMatch(index -> index.firstColumnIs("movieid"));
        boolean hasTitleIndex = metadata.relevantIndexes().stream()
            .anyMatch(index -> index.firstColumnIs("title"));
        if (!hasMovieIdIndex || !hasTitleIndex) {
            throw new SQLException(
                "Missing benchmark-relevant index on " + tableName
                    + ": movieid="
                    + hasMovieIdIndex + ", title=" + hasTitleIndex
                    + "; found " + metadata.indexState()
            );
        }

        ColumnDefinition title = byName.get("title");
        if (title.size() != 100) {
            warnings.add(
                "title column size is " + title.size() + ", expected 100"
            );
        }
        ColumnDefinition country = byName.get("country");
        if (country.size() != 2) {
            warnings.add(
                "country column size is " + country.size()
                    + ", expected 2"
            );
        }

        for (ColumnDefinition column : metadata.columns()) {
            boolean expectedNullable = column.name().equals("runtime");
            if (column.nullable() != expectedNullable) {
                warnings.add(
                    "Nullability differs for " + column.name()
                        + ": nullable=" + column.nullable()
                );
            }
        }
    }

    private static void requireColumn(
        Map<String, ColumnDefinition> byName,
        String name,
        int expectedType,
        String tableName
    ) throws SQLException {
        ColumnDefinition column = byName.get(name);
        if (column == null) {
            throw new SQLException(
                "Required " + tableName + " column is missing: " + name
            );
        }
        if (column.jdbcType() != expectedType) {
            throw new SQLException(
                "Column " + name + " has JDBC type "
                    + column.jdbcType() + " (" + column.typeName()
                    + "); expected " + expectedType
            );
        }
    }

    private static String qualifiedTable(
        Connection connection,
        String schema,
        String tableName
    ) throws SQLException {
        if (schema == null
                || !schema.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
            throw new SQLException(
                "Invalid schema identifier: " + schema
            );
        }
        if (tableName == null
                || !tableName.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
            throw new SQLException(
                "Invalid table identifier: " + tableName
            );
        }

        String quote = connection.getMetaData()
            .getIdentifierQuoteString();
        if (quote == null || quote.isBlank()) {
            return schema + "." + tableName;
        }
        quote = quote.trim();
        return quote + schema + quote + "."
            + quote + tableName + quote;
    }

    private static String normalizeConstraint(
        String definition,
        String schema
    ) {
        return definition.toLowerCase(Locale.ROOT)
            .replace("\"", "")
            .replace(schema.toLowerCase(Locale.ROOT) + ".", "")
            .replaceAll(
                "::(?:character varying|varchar|text|bpchar|integer|int4)"
                    + "(?:\\(\\d+\\))?",
                ""
            )
            .replaceAll("[\\s()]", "");
    }

    private static void appendProblem(
        StringBuilder problems,
        String problem
    ) {
        if (problems.length() > 0) {
            problems.append("; ");
        }
        problems.append(problem);
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

    @Override
    public void close() throws SQLException {
        SQLException failure = null;

        try {
            if (!connection.isClosed() && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            failure = e;
        }

        for (PreparedStatement statement : statements) {
            try {
                statement.close();
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    public record Inspection(
        String productName,
        String productVersion,
        long rowCount,
        List<MovieRecord> rows,
        TableMetadata tableMetadata,
        List<String> warnings
    ) {
    }

    public record TableMetadata(
        List<ColumnDefinition> columns,
        List<String> primaryKey,
        List<IndexDefinition> indexes,
        List<IndexDefinition> relevantIndexes,
        List<String> foreignKeys,
        List<String> checkConstraints,
        List<String> warnings
    ) {
        public String indexState() {
            return relevantIndexes.stream()
                .map(IndexDefinition::canonical)
                .reduce((left, right) -> left + ";" + right)
                .orElse("none");
        }

        public String advisorySignature() {
            return "columns=" + columns
                + ";indexes=" + indexes
                + ";foreign_keys=" + foreignKeys
                + ";checks=" + checkConstraints;
        }
    }

    public record ColumnDefinition(
        int ordinal,
        String name,
        int jdbcType,
        String typeName,
        int size,
        boolean nullable
    ) {
    }

    public record IndexDefinition(
        boolean unique,
        List<String> columns,
        String filter
    ) {
        public boolean firstColumnIs(String column) {
            return !columns.isEmpty() && columns.get(0).equals(column);
        }

        public boolean isBenchmarkRelevant() {
            return firstColumnIs("movieid") || firstColumnIs("title");
        }

        public String canonical() {
            String result = (unique ? "unique" : "nonunique")
                + "(" + String.join("+", columns) + ")";
            return filter == null || filter.isBlank()
                ? result
                : result + " where " + filter;
        }
    }

    private static final class IndexBuilder {
        private final boolean unique;
        private final TreeMap<Short, String> columns = new TreeMap<>();
        private String filter = "";

        private IndexBuilder(boolean unique) {
            this.unique = unique;
        }

        private IndexDefinition build() {
            return new IndexDefinition(
                unique,
                List.copyOf(columns.values()),
                filter
            );
        }
    }
}


// C1 CSV output


final class ConcurrencyCsvLogger {
    private static final Path RAW_FILE = Path.of(
        "result", "measurements", "benchmark_c1_concurrency_raw.csv"
    );
    private static final Path SUMMARY_FILE = Path.of(
        "result", "measurements", "benchmark_c1_concurrency_summary.csv"
    );

    private static final String RAW_HEADER =
        "session_id,experiment_id,system,dataset,record_count,concurrency,"
        + "repetition,phase,requested_duration_ms,actual_duration_ns,"
        + "system_order,configuration_order,worker_id,query_sequence,"
        + "start_offset_ns,end_offset_ns,latency_ns,latency_ms,"
        + "result_count,correctness,status,error";
    private static final String SUMMARY_HEADER =
        "timestamp,session_id,experiment_id,system,dataset,record_count,"
        + "concurrency,repetition,phase,requested_duration_ms,"
        + "actual_duration_ns,actual_duration_ms,started_queries,"
        + "total_completed_queries,successful_queries,error_count,"
        + "throughput_qps,median_latency_ms,p95_latency_ms,correctness,"
        + "status,system_order,configuration_order,notes";

    private static final int RAW_FIELD_COUNT = 22;
    private static final int SUMMARY_FIELD_COUNT = 24;

    private ConcurrencyCsvLogger() {
    }

    public static Path rawFile() {
        return RAW_FILE;
    }

    public static Path summaryFile() {
        return SUMMARY_FILE;
    }

    public static synchronized void validateFiles() throws IOException {
        validateFile(RAW_FILE, RAW_HEADER);
        validateFile(SUMMARY_FILE, SUMMARY_HEADER);
    }

    public static synchronized void appendRawRows(
        List<String[]> rows
    ) throws IOException {
        appendRows(RAW_FILE, RAW_HEADER, RAW_FIELD_COUNT, rows);
    }

    public static synchronized void appendSummaryRow(
        String[] row
    ) throws IOException {
        appendRows(
            SUMMARY_FILE,
            SUMMARY_HEADER,
            SUMMARY_FIELD_COUNT,
            List.<String[]>of(row)
        );
    }

    private static void appendRows(
        Path file,
        String header,
        int fieldCount,
        List<String[]> rows
    ) throws IOException {
        if (rows.isEmpty()) {
            return;
        }
        for (String[] row : rows) {
            if (row.length != fieldCount) {
                throw new IllegalArgumentException(
                    "Expected " + fieldCount + " CSV fields for " + file
                        + ", got " + row.length
                );
            }
        }

        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        boolean writeHeader = !Files.exists(file) || Files.size(file) == 0;
        if (!writeHeader) {
            validateFile(file, header);
        }

        try (BufferedWriter output = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {
            if (writeHeader) {
                output.write(header);
                output.newLine();
            }
            for (String[] row : rows) {
                output.write(Arrays.stream(row)
                    .map(BenchmarkCsvLogger::escapeCsv)
                    .collect(Collectors.joining(",")));
                output.newLine();
            }
        }
    }

    private static void validateFile(
        Path file,
        String expectedHeader
    ) throws IOException {
        if (!Files.exists(file) || Files.size(file) == 0) {
            return;
        }
        try (var reader = Files.newBufferedReader(
                file, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (!expectedHeader.equals(header)) {
                throw new IOException(
                    "Existing C1 CSV header is incompatible: " + file
                );
            }
        }
    }
}


// C1 concurrent benchmark


final class ConcurrencyBenchmarkRunner {
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
