import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class BenchmarkRunner {
    private static final String SUBMISSION_NOTICE = """
        SUBMISSION EDITION: this program demonstrates the core implementation only.
        It does not reproduce the formal E/U/C1 performance results in the report.
        The complete measurement implementation and historical results remain in GitHub.
        """;

    private BenchmarkRunner() {
    }

    public static void main(String[] args) {
        int exitCode = run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static int run(String[] args) {
        Options options;
        try {
            options = Options.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Configuration error: " + e.getMessage());
            System.err.println("Use --help for usage.");
            return 2;
        }

        if (options.help()) {
            printUsage();
            return 0;
        }

        System.out.println(SUBMISSION_NOTICE);
        System.out.printf(
            "Experiment=%s, system=%s, dataset=%s%n",
            options.experiment(),
            options.system().cliName(),
            options.dataset().cliName()
        );

        try {
            if (options.experiment() == Experiment.C1) {
                DatabaseConfig database = DatabaseConfig.forSystem(options.system());
                ConcurrencyExperiment.run(options, database);
                return 0;
            }

            Measurement measurement;
            if (options.system() == SystemTarget.JAVA) {
                measurement = FileExperiments.run(
                    options.experiment(), options.dataset()
                );
            } else {
                DatabaseConfig database = DatabaseConfig.forSystem(options.system());
                Class.forName(database.driverClass());
                try (Connection connection = DriverManager.getConnection(
                        database.url(), database.user(), database.password())) {
                    measurement = JdbcExperiments.run(
                        connection,
                        database.schema(),
                        options.dataset(),
                        options.experiment()
                    );
                }
            }

            printMeasurement(options, measurement);
            if (measurement.count() != options.experiment().expectedCount()) {
                System.err.printf(
                    "Count check failed: expected=%d, actual=%d%n",
                    options.experiment().expectedCount(),
                    measurement.count()
                );
                return 1;
            }
            System.out.println("Count check: PASS");
            return 0;
        } catch (Exception e) {
            System.err.println("Execution failed: " + ErrorText.describe(e));
            for (Throwable suppressed : e.getSuppressed()) {
                System.err.println("  suppressed: " + ErrorText.describe(suppressed));
            }
            return 1;
        }
    }

    private static void printMeasurement(Options options, Measurement result) {
        String countLabel = options.experiment().isQuery()
            ? "result_count" : "rows_updated";
        System.out.printf(
            Locale.ROOT,
            "%s=%d%nelapsed_ns=%d%nelapsed_ms=%.6f%n%s%n",
            countLabel,
            result.count(),
            result.elapsedNs(),
            result.elapsedNs() / 1_000_000.0,
            result.detail()
        );
    }

    private static void printUsage() {
        System.out.println("""
            Usage:
              java -cp "out;lib/*" BenchmarkRunner
                --experiment E1|E2|E3|E4|U1|U2|U3|U4|C1
                --system java|postgresql|opengauss [options]

            Core options:
              --dataset original|large   Optional; inferred from experiment
              --help

            C1-only options:
              --concurrency 1|2|4|8      Default: 1
              --warmup-seconds N         Default: 5
              --seconds N                Formal window; default: 20

            Dataset mapping:
              E1/E2/U1/U2 -> original
              E3/E4/U3/U4/C1 -> large
              E3 reuses E1 exact-title logic on the large dataset.
              E4 reuses E2 substring logic on the large dataset.

            Required database passwords:
              BENCHMARK_POSTGRES_PASSWORD
              BENCHMARK_OPENGAUSS_PASSWORD

            This concise Submission Edition is not an equivalent replacement for
            the complete formal measurement program retained in GitHub.
            """);
    }
}

record Options(
    Experiment experiment,
    SystemTarget system,
    Dataset dataset,
    int concurrency,
    int warmupSeconds,
    int formalSeconds,
    boolean help
) {
    private static final Set<Integer> C1_CONCURRENCIES = Set.of(1, 2, 4, 8);

    static Options parse(String[] args) {
        Experiment experiment = null;
        SystemTarget system = null;
        Dataset dataset = null;
        int concurrency = 1;
        int warmupSeconds = 5;
        int formalSeconds = 20;
        boolean c1OptionUsed = false;
        boolean help = false;

        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            switch (option) {
                case "--help", "-h" -> help = true;
                case "--experiment" -> {
                    ensureUnset(experiment, option);
                    experiment = Experiment.parse(requireValue(args, ++i, option));
                }
                case "--system" -> {
                    ensureUnset(system, option);
                    system = SystemTarget.parse(requireValue(args, ++i, option));
                }
                case "--dataset" -> {
                    ensureUnset(dataset, option);
                    dataset = Dataset.parse(requireValue(args, ++i, option));
                }
                case "--concurrency" -> {
                    concurrency = positiveInt(requireValue(args, ++i, option), option);
                    c1OptionUsed = true;
                }
                case "--warmup-seconds" -> {
                    warmupSeconds = positiveInt(requireValue(args, ++i, option), option);
                    c1OptionUsed = true;
                }
                case "--seconds" -> {
                    formalSeconds = positiveInt(requireValue(args, ++i, option), option);
                    c1OptionUsed = true;
                }
                default -> throw new IllegalArgumentException("Unknown option: " + option);
            }
        }

        if (help) {
            return new Options(null, null, null, concurrency, warmupSeconds, formalSeconds, true);
        }
        if (experiment == null) {
            throw new IllegalArgumentException("--experiment is required");
        }
        if (system == null) {
            throw new IllegalArgumentException("--system is required");
        }

        Dataset requiredDataset = experiment.dataset();
        if (dataset == null) {
            dataset = requiredDataset;
        } else if (dataset != requiredDataset) {
            throw new IllegalArgumentException(
                experiment + " requires --dataset " + requiredDataset.cliName()
            );
        }

        if (experiment == Experiment.C1) {
            if (system == SystemTarget.JAVA) {
                throw new IllegalArgumentException("C1 supports PostgreSQL or openGauss only");
            }
            if (!C1_CONCURRENCIES.contains(concurrency)) {
                throw new IllegalArgumentException(
                    "C1 concurrency must be one of 1,2,4,8"
                );
            }
        } else if (c1OptionUsed) {
            throw new IllegalArgumentException(
                "--concurrency, --warmup-seconds, and --seconds are C1-only"
            );
        }

        return new Options(
            experiment,
            system,
            dataset,
            concurrency,
            warmupSeconds,
            formalSeconds,
            false
        );
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }

    private static int positiveInt(String value, String option) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(option + " must be at least 1");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                option + " requires an integer, got: " + value
            );
        }
    }

    private static void ensureUnset(Object current, String option) {
        if (current != null) {
            throw new IllegalArgumentException(option + " may be specified only once");
        }
    }
}

enum Experiment {
    E1(Dataset.ORIGINAL, Operation.EXACT_QUERY, 1),
    E2(Dataset.ORIGINAL, Operation.SUBSTRING_QUERY, 8),
    E3(Dataset.LARGE, Operation.EXACT_QUERY, 1),
    E4(Dataset.LARGE, Operation.SUBSTRING_QUERY, 880),
    U1(Dataset.ORIGINAL, Operation.SINGLE_UPDATE, 1),
    U2(Dataset.ORIGINAL, Operation.FIRST_THOUSAND_UPDATE, 1_000),
    U3(Dataset.LARGE, Operation.SINGLE_UPDATE, 1),
    U4(Dataset.LARGE, Operation.BATCH_UPDATE, 110_000),
    C1(Dataset.LARGE, Operation.CONCURRENCY_QUERY, 880);

    static final String EXACT_TITLE = "Ardh Satya";
    static final String SUBSTRING = "Batman";
    static final int NEW_RUNTIME = 999;

    private final Dataset dataset;
    private final Operation operation;
    private final int expectedCount;

    Experiment(Dataset dataset, Operation operation, int expectedCount) {
        this.dataset = dataset;
        this.operation = operation;
        this.expectedCount = expectedCount;
    }

    static Experiment parse(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid experiment: " + value);
        }
    }

    Dataset dataset() {
        return dataset;
    }

    int expectedCount() {
        return expectedCount;
    }

    boolean isQuery() {
        return operation == Operation.EXACT_QUERY
            || operation == Operation.SUBSTRING_QUERY;
    }

    boolean isUpdate() {
        return operation == Operation.SINGLE_UPDATE
            || operation == Operation.FIRST_THOUSAND_UPDATE
            || operation == Operation.BATCH_UPDATE;
    }

    boolean usesExactSearch() {
        return operation == Operation.EXACT_QUERY;
    }

    boolean matches(Movie movie) {
        return switch (operation) {
            case EXACT_QUERY -> movie.title().equals(EXACT_TITLE);
            case SUBSTRING_QUERY -> movie.title().contains(SUBSTRING);
            default -> throw new IllegalStateException(this + " is not a query experiment");
        };
    }

    boolean isUpdateTarget(int movieId) {
        return switch (operation) {
            case SINGLE_UPDATE -> movieId == 5;
            case FIRST_THOUSAND_UPDATE -> movieId >= 1 && movieId <= 1_000;
            case BATCH_UPDATE -> (movieId - 1) % Dataset.LARGE_BATCH_SIZE
                < Dataset.LARGE_BATCH_UPDATE_SIZE;
            default -> throw new IllegalStateException(this + " is not an update experiment");
        };
    }

    private enum Operation {
        EXACT_QUERY,
        SUBSTRING_QUERY,
        SINGLE_UPDATE,
        FIRST_THOUSAND_UPDATE,
        BATCH_UPDATE,
        CONCURRENCY_QUERY
    }
}

enum SystemTarget {
    JAVA("java"),
    POSTGRESQL("postgresql"),
    OPENGAUSS("opengauss");

    private final String cliName;

    SystemTarget(String cliName) {
        this.cliName = cliName;
    }

    static SystemTarget parse(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "java", "java-file" -> JAVA;
            case "postgresql", "postgresql-jdbc" -> POSTGRESQL;
            case "opengauss", "opengauss-jdbc" -> OPENGAUSS;
            default -> throw new IllegalArgumentException("Invalid system: " + value);
        };
    }

    String cliName() {
        return cliName;
    }
}

enum Dataset {
    ORIGINAL(
        "original",
        Path.of("data", "working", "originalCSV", "movies"),
        "movies",
        9_204
    ),
    LARGE(
        "large",
        Path.of("data", "working", "largeCSV", "movies_large.csv"),
        "movies_large",
        1_012_440
    );

    static final int LARGE_BATCH_SIZE = 9_204;
    static final int LARGE_BATCH_COUNT = 110;
    static final int LARGE_BATCH_UPDATE_SIZE = 1_000;

    private final String cliName;
    private final Path file;
    private final String table;
    private final int recordCount;

    Dataset(String cliName, Path file, String table, int recordCount) {
        this.cliName = cliName;
        this.file = file;
        this.table = table;
        this.recordCount = recordCount;
    }

    static Dataset parse(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "original" -> ORIGINAL;
            case "large" -> LARGE;
            default -> throw new IllegalArgumentException("Invalid dataset: " + value);
        };
    }

    String cliName() {
        return cliName;
    }

    Path file() {
        return file;
    }

    String table() {
        return table;
    }

    int recordCount() {
        return recordCount;
    }
}

record Measurement(long elapsedNs, int count, String detail) {
    Measurement {
        if (elapsedNs < 0) {
            throw new IllegalArgumentException("elapsedNs cannot be negative");
        }
        Objects.requireNonNull(detail, "detail");
    }
}

record Movie(
    int movieId,
    String title,
    String country,
    int yearReleased,
    Integer runtime
) {
    static final String HEADER = "movieid|title|country|year_released|runtime";

    Movie {
        Objects.requireNonNull(title, "title");
        Objects.requireNonNull(country, "country");
    }

    static Movie parse(String line, int lineNumber) {
        String[] raw = line.split("\\|", -1);
        if (raw.length != 5) {
            throw new IllegalArgumentException(
                "Line " + lineNumber + " has " + raw.length + " fields; expected 5"
            );
        }

        String[] fields = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            fields[i] = decodeField(raw[i], lineNumber);
        }

        try {
            Integer runtime = fields[4].equalsIgnoreCase("null")
                ? null : Integer.valueOf(fields[4]);
            return new Movie(
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

    Movie withRuntime(int newRuntime) {
        return new Movie(movieId, title, country, yearReleased, newRuntime);
    }

    String toPipeDelimited() {
        return movieId + "|" + encodeField(title) + "|" + encodeField(country) + "|"
            + yearReleased + "|" + (runtime == null ? "null" : runtime);
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
            } else if (i + 1 < inner.length() && inner.charAt(i + 1) == '"') {
                decoded.append('"');
                i++;
            } else {
                throw new IllegalArgumentException(
                    "Line " + lineNumber + " contains an invalid quoted field"
                );
            }
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

record DatabaseConfig(
    String driverClass,
    String url,
    String user,
    String password,
    String schema
) {
    static DatabaseConfig forSystem(SystemTarget system) {
        return switch (system) {
            case POSTGRESQL -> new DatabaseConfig(
                "org.postgresql.Driver",
                environment(
                    "BENCHMARK_POSTGRES_URL",
                    "jdbc:postgresql://127.0.0.1:5432/filmdb1"
                ),
                environment("BENCHMARK_POSTGRES_USER", "postgres"),
                requiredEnvironment("BENCHMARK_POSTGRES_PASSWORD"),
                environment("BENCHMARK_POSTGRES_SCHEMA", "public")
            );
            case OPENGAUSS -> new DatabaseConfig(
                "org.opengauss.Driver",
                environment(
                    "BENCHMARK_OPENGAUSS_URL",
                    "jdbc:opengauss://127.0.0.1:8888/postgres"
                ),
                environment("BENCHMARK_OPENGAUSS_USER", "gaussdb"),
                requiredEnvironment("BENCHMARK_OPENGAUSS_PASSWORD"),
                environment("BENCHMARK_OPENGAUSS_SCHEMA", "gaussdb")
            );
            case JAVA -> throw new IllegalArgumentException(
                "Java file experiments do not use a database"
            );
        };
    }

    private static String environment(String name, String defaultValue) {
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
        return "DatabaseConfig[driverClass=" + driverClass + ", url=" + url
            + ", user=" + user + ", password=<redacted>, schema=" + schema + "]";
    }
}

final class FileExperiments {
    private FileExperiments() {
    }

    static Measurement run(Experiment experiment, Dataset dataset) throws Exception {
        if (experiment.isQuery()) {
            return runQuery(experiment, dataset);
        }
        if (experiment.isUpdate()) {
            return runUpdate(experiment, dataset);
        }
        throw new IllegalArgumentException("Unsupported file experiment: " + experiment);
    }

    private static Measurement runQuery(Experiment experiment, Dataset dataset)
        throws IOException {
        int recordsRead = 0;
        int matches = 0;
        long start = System.nanoTime();

        try (BufferedReader reader = Files.newBufferedReader(
                dataset.file(), StandardCharsets.UTF_8)) {
            requireHeader(reader, dataset.file());
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                Movie movie = Movie.parse(line, lineNumber);
                recordsRead++;
                if (experiment.matches(movie)) {
                    matches++;
                }
            }
        }

        long elapsedNs = System.nanoTime() - start;
        requireRecordCount(dataset, recordsRead);
        return new Measurement(
            elapsedNs,
            matches,
            "records_read=" + recordsRead + ";full_file_scan=true"
        );
    }

    private static Measurement runUpdate(Experiment experiment, Dataset dataset)
        throws Exception {
        Path output = Files.createTempFile(
            "dbms-submission-" + experiment.name().toLowerCase(Locale.ROOT) + "-",
            ".txt"
        );
        Exception operationFailure = null;

        try {
            int recordsRead = 0;
            int rowsUpdated = 0;
            long start = System.nanoTime();

            try (
                BufferedReader reader = Files.newBufferedReader(
                    dataset.file(), StandardCharsets.UTF_8
                );
                BufferedWriter writer = Files.newBufferedWriter(
                    output, StandardCharsets.UTF_8
                )
            ) {
                requireHeader(reader, dataset.file());
                writer.write(Movie.HEADER);
                writer.newLine();

                String line;
                int lineNumber = 1;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    Movie movie = Movie.parse(line, lineNumber);
                    recordsRead++;
                    if (experiment.isUpdateTarget(movie.movieId())) {
                        movie = movie.withRuntime(Experiment.NEW_RUNTIME);
                        rowsUpdated++;
                    }
                    writer.write(movie.toPipeDelimited());
                    writer.newLine();
                }
            }

            long elapsedNs = System.nanoTime() - start;
            requireRecordCount(dataset, recordsRead);
            return new Measurement(
                elapsedNs,
                rowsUpdated,
                "records_read=" + recordsRead
                    + ";full_file_rewrite=true;temporary_output=true"
            );
        } catch (Exception e) {
            operationFailure = e;
            throw e;
        } finally {
            try {
                Files.deleteIfExists(output);
            } catch (IOException cleanupFailure) {
                if (operationFailure != null) {
                    operationFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
        }
    }

    private static void requireHeader(BufferedReader reader, Path path)
        throws IOException {
        String header = reader.readLine();
        if (!Movie.HEADER.equals(header)) {
            throw new IOException("Unexpected header in " + path + ": " + header);
        }
    }

    private static void requireRecordCount(Dataset dataset, int actual) {
        if (actual != dataset.recordCount()) {
            throw new IllegalStateException(
                "Dataset row count expected=" + dataset.recordCount() + ", actual=" + actual
            );
        }
    }
}

final class JdbcExperiments {
    static final String SELECT_COLUMNS =
        "movieid, title, country, year_released, runtime";

    private JdbcExperiments() {
    }

    static Measurement run(
        Connection connection,
        String schema,
        Dataset dataset,
        Experiment experiment
    ) throws Exception {
        if (experiment.isQuery()) {
            return runQuery(connection, schema, dataset, experiment);
        }
        if (experiment.isUpdate()) {
            return runUpdate(connection, schema, dataset, experiment);
        }
        throw new IllegalArgumentException("Unsupported JDBC experiment: " + experiment);
    }

    private static Measurement runQuery(
        Connection connection,
        String schema,
        Dataset dataset,
        Experiment experiment
    ) throws SQLException {
        try (PreparedStatement statement = prepareQuery(
                connection,
                schema,
                dataset.table(),
                experiment.usesExactSearch())) {
            int resultCount = 0;
            long checksum = 0;
            long start = System.nanoTime();

            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    resultCount++;
                    checksum = 31 * checksum + consumeRow(results);
                }
            }

            long elapsedNs = System.nanoTime() - start;
            return new Measurement(
                elapsedNs,
                resultCount,
                "all_five_columns_consumed=true;checksum=" + checksum
            );
        }
    }

    private static Measurement runUpdate(
        Connection connection,
        String schema,
        Dataset dataset,
        Experiment experiment
    ) throws Exception {
        String table = qualifiedTable(connection, schema, dataset.table());
        String sql = "UPDATE " + table + " SET runtime = "
            + Experiment.NEW_RUNTIME + " WHERE " + updatePredicate(experiment);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            boolean originalAutoCommit = connection.getAutoCommit();
            if (!originalAutoCommit) {
                throw new SQLException("Fresh benchmark connection is not in auto-commit mode");
            }
            connection.setAutoCommit(false);

            Measurement measurement = null;
            Throwable operationFailure = null;
            long elapsedNs = 0;
            try {
                long start = System.nanoTime();
                int rowsUpdated;
                try {
                    rowsUpdated = statement.executeUpdate();
                } finally {
                    elapsedNs = System.nanoTime() - start;
                }
                measurement = new Measurement(
                    elapsedNs,
                    rowsUpdated,
                    "transaction_rolled_back=true;timed_scope=executeUpdate"
                );
            } catch (Throwable failure) {
                operationFailure = failure;
            }

            SQLException rollbackFailure = null;
            try {
                connection.rollback();
            } catch (SQLException failure) {
                rollbackFailure = failure;
            }

            if (rollbackFailure != null) {
                if (operationFailure != null) {
                    rollbackFailure.addSuppressed(operationFailure);
                }
                closeAfterTransactionFailure(connection, rollbackFailure);
                throw new SQLException(
                    "Rollback failed; connection was closed without restoring auto-commit",
                    rollbackFailure
                );
            }

            try {
                connection.setAutoCommit(originalAutoCommit);
            } catch (SQLException restoreFailure) {
                if (operationFailure != null) {
                    restoreFailure.addSuppressed(operationFailure);
                }
                closeAfterTransactionFailure(connection, restoreFailure);
                throw new SQLException(
                    "Rollback succeeded but auto-commit restoration failed; connection closed",
                    restoreFailure
                );
            }

            if (operationFailure != null) {
                rethrow(operationFailure);
            }
            return Objects.requireNonNull(measurement, "measurement");
        }
    }

    static PreparedStatement prepareQuery(
        Connection connection,
        String schema,
        String tableName,
        boolean exact
    ) throws SQLException {
        String table = qualifiedTable(connection, schema, tableName);
        PreparedStatement statement = connection.prepareStatement(
            "SELECT " + SELECT_COLUMNS + " FROM " + table
                + (exact ? " WHERE title = ?" : " WHERE title LIKE ?")
        );
        try {
            statement.setString(
                1,
                exact ? Experiment.EXACT_TITLE : "%" + Experiment.SUBSTRING + "%"
            );
            return statement;
        } catch (SQLException failure) {
            try {
                statement.close();
            } catch (SQLException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    static String updatePredicate(Experiment experiment) {
        return switch (experiment) {
            case U1, U3 -> "movieid = 5";
            case U2 -> "movieid BETWEEN 1 AND 1000";
            case U4 -> largeBatchPredicate();
            default -> throw new IllegalArgumentException(
                experiment + " is not an update experiment"
            );
        };
    }

    static String largeBatchPredicate() {
        StringBuilder predicate = new StringBuilder("(");
        for (int batch = 0; batch < Dataset.LARGE_BATCH_COUNT; batch++) {
            if (batch > 0) {
                predicate.append(" OR ");
            }
            int firstMovieId = batch * Dataset.LARGE_BATCH_SIZE + 1;
            int lastMovieId = firstMovieId + Dataset.LARGE_BATCH_UPDATE_SIZE - 1;
            predicate.append("movieid BETWEEN ")
                .append(firstMovieId)
                .append(" AND ")
                .append(lastMovieId);
        }
        return predicate.append(')').toString();
    }

    static long consumeRow(ResultSet results) throws SQLException {
        int movieId = results.getInt("movieid");
        String title = results.getString("title");
        String country = results.getString("country");
        int yearReleased = results.getInt("year_released");
        int runtimeValue = results.getInt("runtime");
        Integer runtime = results.wasNull() ? null : runtimeValue;

        long hash = movieId;
        hash = 31 * hash + Objects.hashCode(title);
        hash = 31 * hash + Objects.hashCode(country);
        hash = 31 * hash + yearReleased;
        return 31 * hash + Objects.hashCode(runtime);
    }

    static String qualifiedTable(
        Connection connection,
        String schema,
        String tableName
    ) throws SQLException {
        requireIdentifier(schema, "schema");
        requireIdentifier(tableName, "table");
        String quote = connection.getMetaData().getIdentifierQuoteString();
        if (quote == null || quote.isBlank()) {
            return schema + "." + tableName;
        }
        quote = quote.trim();
        return quote + schema + quote + "." + quote + tableName + quote;
    }

    private static void requireIdentifier(String value, String label)
        throws SQLException {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
            throw new SQLException("Invalid " + label + " identifier: " + value);
        }
    }

    private static void closeAfterTransactionFailure(
        Connection connection,
        SQLException failure
    ) {
        try {
            connection.close();
        } catch (SQLException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new RuntimeException(failure);
    }
}

final class ConcurrencyExperiment {
    private static final int READY_TIMEOUT_SECONDS = 30;

    private ConcurrencyExperiment() {
    }

    static void run(Options options, DatabaseConfig database) throws Exception {
        System.out.println(
            "C1 NOTE: full inline record validation is omitted. All five fields are "
                + "consumed and the 880-row count is checked, but this changes the "
                + "timed client workload; these values are not comparable to formal C1 data."
        );

        Class.forName(database.driverClass());
        List<WorkerResource> resources = openWorkers(
            options.concurrency(), database, options.dataset()
        );
        Throwable executionFailure = null;

        try {
            PhaseResult warmup = runPhase(
                "warmup", options.warmupSeconds(), resources
            );
            printPhase(warmup);
            if (!warmup.passed()) {
                throw new IllegalStateException("C1 warm-up failed; formal phase skipped");
            }

            PhaseResult formal = runPhase(
                "formal", options.formalSeconds(), resources
            );
            printPhase(formal);
            if (!formal.passed()) {
                throw new IllegalStateException("C1 formal phase failed");
            }
        } catch (Throwable failure) {
            executionFailure = failure;
        }

        SQLException closeFailure = closeResources(resources);
        if (executionFailure != null) {
            if (closeFailure != null) {
                executionFailure.addSuppressed(closeFailure);
            }
            rethrow(executionFailure);
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private static List<WorkerResource> openWorkers(
        int concurrency,
        DatabaseConfig database,
        Dataset dataset
    ) throws Exception {
        List<WorkerResource> resources = new ArrayList<>();
        try {
            for (int workerId = 1; workerId <= concurrency; workerId++) {
                resources.add(WorkerResource.open(workerId, database, dataset));
            }
            return resources;
        } catch (Exception failure) {
            SQLException closeFailure = closeResources(resources);
            if (closeFailure != null) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static PhaseResult runPhase(
        String name,
        int durationSeconds,
        List<WorkerResource> resources
    ) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(resources.size());
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong phaseStart = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        List<String> threadFailures = Collections.synchronizedList(
            new ArrayList<>()
        );
        List<WorkerTask> tasks = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        long durationNs = TimeUnit.SECONDS.toNanos(durationSeconds);

        for (WorkerResource resource : resources) {
            WorkerTask task = new WorkerTask(
                resource,
                durationNs,
                ready,
                start,
                phaseStart,
                stop,
                threadFailures
            );
            Thread thread = new Thread(task, "submission-c1-" + name
                + "-worker-" + resource.workerId());
            tasks.add(task);
            threads.add(thread);
            thread.start();
        }

        if (!ready.await(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            stop.set(true);
            phaseStart.set(System.nanoTime());
            start.countDown();
            interruptAndJoin(threads);
            throw new IllegalStateException(
                "Workers did not become ready within " + READY_TIMEOUT_SECONDS + " seconds"
            );
        }

        long startNs = System.nanoTime();
        phaseStart.set(startNs);
        start.countDown();
        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException interrupted) {
            stop.set(true);
            interruptAndJoin(threads);
            Thread.currentThread().interrupt();
            throw interrupted;
        }

        List<QuerySample> samples = new ArrayList<>();
        long endNs = startNs;
        for (WorkerTask task : tasks) {
            samples.addAll(task.samples());
            endNs = Math.max(endNs, task.finishedAtNs());
        }
        return new PhaseResult(
            name,
            durationSeconds,
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

    private static void printPhase(PhaseResult result) {
        System.out.printf(
            Locale.ROOT,
            "%s: requested=%ds, actual=%.6fs, completed=%d, successful=%d, "
                + "errors=%d, throughput=%.3f q/s, median=%.3f ms, p95=%.3f ms, "
                + "status=%s%n",
            result.name(),
            result.requestedSeconds(),
            result.actualDurationNs() / 1_000_000_000.0,
            result.completedQueries(),
            result.successfulQueries(),
            result.errorCount(),
            result.throughputQps(),
            result.medianLatencyMs(),
            result.p95LatencyMs(),
            result.passed() ? "PASS" : "FAIL"
        );
        for (String failure : result.threadFailures()) {
            System.out.println("  worker failure: " + failure);
        }
    }

    private static SQLException closeResources(List<WorkerResource> resources) {
        SQLException failure = null;
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (SQLException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        return failure;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new RuntimeException(failure);
    }

    private record QuerySample(
        long latencyNs,
        int resultCount,
        long checksum,
        String error
    ) {
        boolean successful() {
            return error.isEmpty() && resultCount == Experiment.C1.expectedCount();
        }
    }

    private record PhaseResult(
        String name,
        int requestedSeconds,
        long startNs,
        long endNs,
        List<QuerySample> samples,
        List<String> threadFailures
    ) {
        long actualDurationNs() {
            return Math.max(0, endNs - startNs);
        }

        int completedQueries() {
            return samples.size();
        }

        int successfulQueries() {
            return (int) samples.stream().filter(QuerySample::successful).count();
        }

        int errorCount() {
            return samples.size() - successfulQueries() + threadFailures.size();
        }

        boolean passed() {
            return !samples.isEmpty() && errorCount() == 0;
        }

        double throughputQps() {
            if (actualDurationNs() == 0) {
                return 0.0;
            }
            return successfulQueries() / (actualDurationNs() / 1_000_000_000.0);
        }

        double medianLatencyMs() {
            List<Long> values = successfulLatencies();
            if (values.isEmpty()) {
                return Double.NaN;
            }
            int middle = values.size() / 2;
            double medianNs = values.size() % 2 == 0
                ? (values.get(middle - 1) + values.get(middle)) / 2.0
                : values.get(middle);
            return medianNs / 1_000_000.0;
        }

        double p95LatencyMs() {
            List<Long> values = successfulLatencies();
            if (values.isEmpty()) {
                return Double.NaN;
            }
            int rank = (int) Math.ceil(0.95 * values.size());
            return values.get(Math.max(0, rank - 1)) / 1_000_000.0;
        }

        private List<Long> successfulLatencies() {
            return samples.stream()
                .filter(QuerySample::successful)
                .map(QuerySample::latencyNs)
                .sorted()
                .toList();
        }
    }

    private static final class WorkerTask implements Runnable {
        private final WorkerResource resource;
        private final long durationNs;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final AtomicLong phaseStart;
        private final AtomicBoolean stop;
        private final List<String> threadFailures;
        private final List<QuerySample> samples = new ArrayList<>();
        private long finishedAtNs;

        private WorkerTask(
            WorkerResource resource,
            long durationNs,
            CountDownLatch ready,
            CountDownLatch start,
            AtomicLong phaseStart,
            AtomicBoolean stop,
            List<String> threadFailures
        ) {
            this.resource = resource;
            this.durationNs = durationNs;
            this.ready = ready;
            this.start = start;
            this.phaseStart = phaseStart;
            this.stop = stop;
            this.threadFailures = threadFailures;
        }

        @Override
        public void run() {
            try {
                ready.countDown();
                start.await();
                long deadline = phaseStart.get() + durationNs;

                while (!stop.get()) {
                    long queryStart = System.nanoTime();
                    if (queryStart >= deadline) {
                        break;
                    }
                    QuerySample sample = executeQuery(queryStart);
                    samples.add(sample);
                    if (!sample.successful()) {
                        stop.set(true);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop.set(true);
                threadFailures.add("worker " + resource.workerId() + " interrupted");
            } catch (RuntimeException e) {
                stop.set(true);
                threadFailures.add(
                    "worker " + resource.workerId() + ": " + ErrorText.describe(e)
                );
            } finally {
                finishedAtNs = System.nanoTime();
            }
        }

        private QuerySample executeQuery(long queryStart) {
            int resultCount = 0;
            long checksum = 0;
            String error = "";
            try (ResultSet results = resource.statement().executeQuery()) {
                while (results.next()) {
                    resultCount++;
                    checksum = 31 * checksum + JdbcExperiments.consumeRow(results);
                }
            } catch (SQLException | RuntimeException e) {
                error = ErrorText.describe(e);
            }

            if (error.isEmpty() && resultCount != Experiment.C1.expectedCount()) {
                error = "result count expected=" + Experiment.C1.expectedCount()
                    + ", actual=" + resultCount;
            }
            long queryEnd = System.nanoTime();
            return new QuerySample(
                queryEnd - queryStart,
                resultCount,
                checksum,
                error
            );
        }

        List<QuerySample> samples() {
            return samples;
        }

        long finishedAtNs() {
            return finishedAtNs;
        }
    }

    private record WorkerResource(
        int workerId,
        Connection connection,
        PreparedStatement statement
    ) implements AutoCloseable {
        static WorkerResource open(
            int workerId,
            DatabaseConfig database,
            Dataset dataset
        ) throws Exception {
            Connection connection = DriverManager.getConnection(
                database.url(), database.user(), database.password()
            );
            try {
                connection.setReadOnly(true);
                connection.setAutoCommit(true);
                PreparedStatement statement = JdbcExperiments.prepareQuery(
                    connection,
                    database.schema(),
                    dataset.table(),
                    false
                );
                return new WorkerResource(workerId, connection, statement);
            } catch (Exception failure) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        @Override
        public void close() throws SQLException {
            SQLException failure = null;
            try {
                statement.close();
            } catch (SQLException closeFailure) {
                failure = closeFailure;
            }
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}

final class ErrorText {
    private ErrorText() {
    }

    static String describe(Throwable failure) {
        String message = failure.getMessage();
        String text = message == null || message.isBlank()
            ? failure.getClass().getSimpleName()
            : failure.getClass().getSimpleName() + ": " + message;
        return text.length() <= 1_000 ? text : text.substring(0, 1_000);
    }
}
