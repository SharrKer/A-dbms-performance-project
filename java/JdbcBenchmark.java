import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public final class JdbcBenchmark implements AutoCloseable {
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
        BenchmarkData data
    ) throws SQLException {
        this(connection, schema, "movies", data);
    }

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
        String schema
    ) throws SQLException {
        return inspect(connection, schema, "movies", true);
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
