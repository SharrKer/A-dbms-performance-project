import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class BenchmarkData {
    public static final String HEADER =
        "movieid|title|country|year_released|runtime";
    public static final String EXACT_TITLE = "Ardh Satya";
    public static final String SUBSTRING = "Batman";
    public static final int NEW_RUNTIME = 999;
    public static final int EXPECTED_RECORD_COUNT = 9_204;
    public static final int EXPECTED_SUBSTRING_COUNT = 8;
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

    public static BenchmarkData load(Path source) throws IOException {
        return load(
            source,
            EXPECTED_RECORD_COUNT,
            EXPECTED_SUBSTRING_COUNT,
            true
        );
    }

    public static BenchmarkData load(
        Path source,
        int expectedRecordCount,
        int expectedSubstringCount,
        boolean updatesEnabled
    ) throws IOException {
        return load(
            source,
            expectedRecordCount,
            expectedSubstringCount,
            updatesEnabled,
            updatesEnabled
                ? List.of(
                    BenchmarkExperiment.U1,
                    BenchmarkExperiment.U2
                )
                : List.of()
        );
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
