public enum BenchmarkSystem {
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
