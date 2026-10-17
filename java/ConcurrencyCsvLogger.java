import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class ConcurrencyCsvLogger {
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
