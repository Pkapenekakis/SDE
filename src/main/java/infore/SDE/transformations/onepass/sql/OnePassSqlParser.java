package infore.SDE.transformations.onepass.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class OnePassSqlParser {

    /*
     * Supported OnePass SQL form:
     *
     * SELECT * FROM wq3_alias ROOT c LIMIT 1000000
     * SELECT c.c_custkey, o.o_orderkey, l.l_linenumber FROM wq3_alias ROOT c LIMIT 1000000
     * SELECT c.c_custkey, o.o_orderkey FROM wq3_alias ROOT c WEIGHTED BY o.o_totalprice LIMIT 1000000
     *
     * The query name after FROM must exist in the external catalog JSON.
     */
    private static final Pattern MAIN_PATTERN = Pattern.compile(
            "(?is)^\\s*SELECT\\s+(.+?)\\s+FROM\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+" +
                    "(?:ROOT\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+)?" +
                    "(?:WEIGHTED\\s+BY\\s+(.+?)\\s+)?" +
                    "LIMIT\\s+(\\d+)\\s*$"
    );

    private static final Pattern COMMENT_PATTERN = Pattern.compile("(?s)/\\*(.*?)\\*/");
    private static final Pattern OPTION_QUOTED_PATTERN =
            Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*'([^']*)'");
    private static final Pattern OPTION_RAW_PATTERN =
            Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*([^,\\s]+)");

    private OnePassSqlParser() {
    }

    public static OnePassSqlRequest parse(String sql) {
        if (isBlank(sql)) {
            throw new IllegalArgumentException("onePassSql is blank");
        }

        String comment = extractComment(sql);
        Map<String, String> options = parseOptions(comment);

        String sqlWithoutComment = COMMENT_PATTERN.matcher(sql).replaceAll(" ");
        String normalized = sqlWithoutComment.replaceAll("\\s+", " ").trim();

        Matcher matcher = MAIN_PATTERN.matcher(normalized);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid OnePass SQL. Expected pattern: " +
                            "SELECT <projection|*> FROM <queryName> [ROOT <rootAlias>] " +
                            "[WEIGHTED BY <expression>] LIMIT <n> " +
                            "/* catalog='...', seed='...', scalefactor=1 */. Got: " + sql
            );
        }

        String projectionText = trimToNull(matcher.group(1));
        String queryName = matcher.group(2);
        String rootAlias = trimToNull(matcher.group(3));
        String weightOverride = trimToNull(matcher.group(4));
        int sampleSize = Integer.parseInt(matcher.group(5));
        List<String> projection = parseProjection(projectionText);

        List<String> projectionOverride = parseProjection(projectionText);

        String catalogRef = firstNonBlank(
                options.get("catalog"),
                options.get("db"),
                "tpch-onepass-catalog.json"
        );

        String seed = firstNonBlank(options.get("seed"), "test123");

        int scaleFactor = 1;
        String scaleFactorValue = firstNonBlank(
                options.get("scalefactor"),
                options.get("scaleFactor"),
                null
        );

        if (!isBlank(scaleFactorValue)) {
            scaleFactor = Integer.parseInt(scaleFactorValue);
        }

        return new OnePassSqlRequest(
                projection,
                queryName,
                rootAlias,
                weightOverride,
                sampleSize,
                catalogRef,
                seed,
                scaleFactor
        );
    }

    private static List<String> parseProjection(String projectionText) {
        List<String> out = new ArrayList<String>();

        if (isBlank(projectionText)) {
            throw new IllegalArgumentException("SELECT projection is missing");
        }

        String trimmed = projectionText.trim();

        if ("*".equals(trimmed)) {
            return out;
        }

        String[] pieces = trimmed.split(",");

        for (String piece : pieces) {
            String p = trimToNull(piece);

            if (p == null) {
                throw new IllegalArgumentException(
                        "Invalid SELECT projection list: " + projectionText
                );
            }

            out.add(p);
        }

        if (out.isEmpty()) {
            throw new IllegalArgumentException("SELECT projection is empty");
        }

        return out;
    }

    private static String extractComment(String sql) {
        Matcher matcher = COMMENT_PATTERN.matcher(sql);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "";
    }

    private static Map<String, String> parseOptions(String comment) {
        Map<String, String> out = new LinkedHashMap<String, String>();

        if (comment == null) {
            return out;
        }

        Matcher quoted = OPTION_QUOTED_PATTERN.matcher(comment);

        while (quoted.find()) {
            out.put(quoted.group(1), quoted.group(2));
        }

        Matcher raw = OPTION_RAW_PATTERN.matcher(comment);

        while (raw.find()) {
            String key = raw.group(1);

            if (!out.containsKey(key)) {
                out.put(key, raw.group(2));
            }
        }

        return out;
    }

    private static String trimToNull(String value) {
        if (isBlank(value)) {
            return null;
        }

        return value.trim();
    }

    private static String firstNonBlank(String first, String second) {
        return isBlank(first) ? second : first;
    }

    private static String firstNonBlank(String first, String second, String third) {
        if (!isBlank(first)) {
            return first;
        }

        if (!isBlank(second)) {
            return second;
        }

        return third;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}