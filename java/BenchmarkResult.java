public record BenchmarkResult(
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
