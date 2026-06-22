package infore.SDE.transformations.onepass.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses One-pass* WEIGHTED BY expressions written with relation aliases:
 *
 * WEIGHTED BY (
 *     (l1.l_extendedprice * (1 - l1.l_discount))
 *     * o1.o_totalprice
 *     * (l2.l_extendedprice * (1 - l2.l_discount))
 *     * o2.o_totalprice
 * )
 *
 * It converts the joined-row expression into alias-local base weights:
 *
 * l1 -> l_extendedprice * (1 - l_discount)
 * o1 -> o_totalprice
 * l2 -> l_extendedprice * (1 - l_discount)
 * o2 -> o_totalprice
 *
 * Important:
 * This parser intentionally rejects non-factorizable expressions, e.g.
 *
 * WEIGHTED BY (o1.o_totalprice + l1.l_extendedprice)
 *
 * because One-pass* requires product-of-base-tuple weights.
 */
public final class OnePassWeightedByParser {

    private static final Pattern QUALIFIED_FIELD =
            Pattern.compile("\\b([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)\\b");

    private OnePassWeightedByParser() {
    }

    /**
     * Extracts the WEIGHTED BY expression from SQL.
     *
     * Returns null if the query has no WEIGHTED BY clause.
     */
    public static String extractWeightedByExpression(String sql) {
        if (sql == null) {
            return null;
        }

        int start = indexOfWeightedBy(sql);

        if (start < 0) {
            return null;
        }

        int exprStart = start + "WEIGHTED BY".length();
        int exprEnd = indexOfLimitAfter(sql, exprStart);

        if (exprEnd < 0) {
            exprEnd = indexOfCommentAfter(sql, exprStart);
        }

        if (exprEnd < 0) {
            exprEnd = sql.length();
        }

        String expression = sql.substring(exprStart, exprEnd).trim();

        if (expression.isEmpty()) {
            throw new IllegalArgumentException("WEIGHTED BY expression is empty");
        }

        return stripOneOuterParenthesisLayer(expression);
    }

    /**
     * Removes the WEIGHTED BY clause from SQL so the existing SQL parser can
     * continue parsing SELECT/FROM/ROOT/LIMIT as before.
     */
    public static String removeWeightedByClause(String sql) {
        if (sql == null) {
            return null;
        }

        int start = indexOfWeightedBy(sql);

        if (start < 0) {
            return sql;
        }

        int exprStart = start + "WEIGHTED BY".length();
        int end = indexOfLimitAfter(sql, exprStart);

        if (end < 0) {
            end = indexOfCommentAfter(sql, exprStart);
        }

        if (end < 0) {
            end = sql.length();
        }

        return (sql.substring(0, start) + " " + sql.substring(end)).trim();
    }

    /**
     * Converts a WEIGHTED BY expression into alias-specific tuple weights.
     */
    public static Map<String, String> parseWeightsByAlias(
            String weightedByExpression,
            Set<String> validAliases) {

        if (weightedByExpression == null
                || weightedByExpression.trim().isEmpty()) {

            return new LinkedHashMap<String, String>();
        }

        String expression = stripOneOuterParenthesisLayer(
                weightedByExpression.trim()
        );

        List<String> factors = splitTopLevelMultiplication(expression);

        Map<String, String> weightsByAlias =
                new LinkedHashMap<String, String>();

        for (String rawFactor : factors) {
            String factor = stripOneOuterParenthesisLayer(rawFactor.trim());

            if (factor.isEmpty()) {
                continue;
            }

            String alias = singleAliasUsedByFactor(factor);

            if (alias == null) {
                /*
                 * Top-level constant factor.
                 *
                 * For now, only allow 1 because multiplying every joined row
                 * by the same constant does not change the distribution, but
                 * supporting arbitrary constants would complicate total-weight
                 * accounting.
                 */
                if ("1".equals(factor) || "1.0".equals(factor)) {
                    continue;
                }

                throw new IllegalArgumentException(
                        "Top-level WEIGHTED BY factor does not reference an alias: "
                                + factor
                );
            }

            if (validAliases != null && !validAliases.contains(alias)) {
                throw new IllegalArgumentException(
                        "WEIGHTED BY uses unknown alias '" + alias + "' in factor: "
                                + factor
                );
            }

            String localExpression = removeAliasQualifier(factor, alias);

            String previous = weightsByAlias.get(alias);

            if (previous == null || previous.trim().isEmpty()) {
                weightsByAlias.put(alias, localExpression);
            } else {
                weightsByAlias.put(
                        alias,
                        "(" + previous + ") * (" + localExpression + ")"
                );
            }
        }

        return weightsByAlias;
    }

    private static String singleAliasUsedByFactor(String factor) {
        Matcher matcher = QUALIFIED_FIELD.matcher(factor);

        String alias = null;

        while (matcher.find()) {
            String currentAlias = matcher.group(1);

            if (alias == null) {
                alias = currentAlias;
            } else if (!alias.equals(currentAlias)) {
                throw new IllegalArgumentException(
                        "WEIGHTED BY factor references more than one alias: "
                                + factor
                );
            }
        }

        return alias;
    }

    private static String removeAliasQualifier(String factor, String alias) {
        Matcher matcher = QUALIFIED_FIELD.matcher(factor);
        StringBuffer buffer = new StringBuffer();

        while (matcher.find()) {
            String currentAlias = matcher.group(1);
            String field = matcher.group(2);

            if (!alias.equals(currentAlias)) {
                throw new IllegalArgumentException(
                        "Unexpected alias '" + currentAlias
                                + "' while rewriting factor: " + factor
                );
            }

            matcher.appendReplacement(buffer, field);
        }

        matcher.appendTail(buffer);

        return buffer.toString();
    }

    private static List<String> splitTopLevelMultiplication(String expression) {
        List<String> parts = new ArrayList<String>();

        int depth = 0;
        int start = 0;

        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);

            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;

                if (depth < 0) {
                    throw new IllegalArgumentException(
                            "Unbalanced parentheses in WEIGHTED BY expression: "
                                    + expression
                    );
                }
            } else if (c == '*' && depth == 0) {
                parts.add(expression.substring(start, i).trim());
                start = i + 1;
            }
        }

        if (depth != 0) {
            throw new IllegalArgumentException(
                    "Unbalanced parentheses in WEIGHTED BY expression: "
                            + expression
            );
        }

        parts.add(expression.substring(start).trim());

        return parts;
    }

    private static String stripOneOuterParenthesisLayer(String s) {
        String value = s.trim();

        while (value.startsWith("(")
                && value.endsWith(")")
                && outerParenthesesWrapWholeExpression(value)) {

            value = value.substring(1, value.length() - 1).trim();
        }

        return value;
    }

    private static boolean outerParenthesesWrapWholeExpression(String s) {
        int depth = 0;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;

                if (depth == 0 && i < s.length() - 1) {
                    return false;
                }
            }

            if (depth < 0) {
                return false;
            }
        }

        return depth == 0;
    }

    private static int indexOfWeightedBy(String sql) {
        return indexOfKeywordOutsideParentheses(sql, "WEIGHTED BY", 0);
    }

    private static int indexOfLimitAfter(String sql, int from) {
        return indexOfKeywordOutsideParentheses(sql, "LIMIT", from);
    }

    private static int indexOfCommentAfter(String sql, int from) {
        int idx = sql.indexOf("/*", from);
        return idx < 0 ? -1 : idx;
    }

    private static int indexOfKeywordOutsideParentheses(
            String sql,
            String keyword,
            int from) {

        String upperSql = sql.toUpperCase();
        String upperKeyword = keyword.toUpperCase();

        int depth = 0;

        for (int i = from; i <= sql.length() - keyword.length(); i++) {
            char c = sql.charAt(i);

            if (c == '(') {
                depth++;
                continue;
            }

            if (c == ')') {
                depth--;
                continue;
            }

            if (depth == 0
                    && upperSql.regionMatches(i, upperKeyword, 0,
                    upperKeyword.length())
                    && isBoundary(sql, i - 1)
                    && isBoundary(sql, i + upperKeyword.length())) {

                return i;
            }
        }

        return -1;
    }

    private static boolean isBoundary(String sql, int index) {
        if (index < 0 || index >= sql.length()) {
            return true;
        }

        char c = sql.charAt(index);

        return !Character.isLetterOrDigit(c) && c != '_';
    }
}