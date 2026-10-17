import java.util.Objects;

public record MovieRecord(
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
