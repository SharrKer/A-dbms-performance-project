public record DatabaseConfiguration(
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
