import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class BenchmarkCsvLogger {

    private static final Path FILE =
        Paths.get("result", "measurements", "benchmark_raw.csv");

    private static final String HEADER =
        "timestamp,session_id,experiment_id,system,dataset,record_count,"
      + "index_state,run_type,iteration,elapsed_ns,elapsed_ms,"
      + "result_count,rows_updated,status,notes";

    private static final int FIELD_COUNT = 15;

    private BenchmarkCsvLogger() {
    }

    public static void appendRow(String[] values) throws IOException {
        appendRow(FILE, values);
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

    public static synchronized void validateDefaultFile()
        throws IOException {
        validateFile(FILE);
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

    static String header() {
        return HEADER;
    }

    static String escapeCsv(String value) {
        if (value == null) {
            value = "";
        }

        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
