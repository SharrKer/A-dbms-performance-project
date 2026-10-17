import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class FileBenchmark {
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
