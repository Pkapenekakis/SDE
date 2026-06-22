package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Small arithmetic evaluator for tuple-local weight expressions.
 *
 * Supported:
 * - numeric constants
 * - tuple field names
 * - +, -, *, /
 * - parentheses
 * - unary minus
 *
 * Example:
 *   l_extendedprice * (1 - l_discount)
 */
public final class TupleWeightExpressionEvaluator {

    private TupleWeightExpressionEvaluator() {
    }

    public static double evaluate(String expression, JsonNode tupleJson) {
        if (expression == null || expression.trim().isEmpty()) {
            return 1.0d;
        }

        Parser parser = new Parser(expression, tupleJson);
        double value = parser.parseExpression();
        parser.skipWhitespace();

        if (!parser.isAtEnd()) {
            throw new IllegalArgumentException(
                    "Unexpected token in weight expression near position "
                            + parser.position()
                            + ": " + expression
            );
        }

        return value;
    }

    private static final class Parser {

        private final String expression;
        private final JsonNode tupleJson;
        private int pos;

        private Parser(String expression, JsonNode tupleJson) {
            this.expression = expression;
            this.tupleJson = tupleJson;
            this.pos = 0;
        }

        private double parseExpression() {
            double value = parseTerm();

            while (true) {
                skipWhitespace();

                if (match('+')) {
                    value += parseTerm();
                } else if (match('-')) {
                    value -= parseTerm();
                } else {
                    return value;
                }
            }
        }

        private double parseTerm() {
            double value = parseFactor();

            while (true) {
                skipWhitespace();

                if (match('*')) {
                    value *= parseFactor();
                } else if (match('/')) {
                    double denominator = parseFactor();

                    if (denominator == 0.0d) {
                        throw new IllegalArgumentException(
                                "Division by zero in weight expression: "
                                        + expression
                        );
                    }

                    value /= denominator;
                } else {
                    return value;
                }
            }
        }

        private double parseFactor() {
            skipWhitespace();

            if (match('-')) {
                return -parseFactor();
            }

            if (match('+')) {
                return parseFactor();
            }

            if (match('(')) {
                double value = parseExpression();
                expect(')');
                return value;
            }

            if (isNumberStart(peek())) {
                return parseNumber();
            }

            if (isIdentifierStart(peek())) {
                String fieldName = parseIdentifier();
                return fieldValue(fieldName);
            }

            throw new IllegalArgumentException(
                    "Unexpected token in weight expression near position "
                            + pos
                            + ": " + expression
            );
        }

        private double parseNumber() {
            int start = pos;

            while (!isAtEnd()) {
                char c = peek();

                if (Character.isDigit(c)
                        || c == '.'
                        || c == 'e'
                        || c == 'E'
                        || c == '+'
                        || c == '-') {

                    /*
                     * Allow + or - only after e/E for scientific notation.
                     */
                    if ((c == '+' || c == '-') && pos > start) {
                        char previous = expression.charAt(pos - 1);

                        if (previous != 'e' && previous != 'E') {
                            break;
                        }
                    }

                    pos++;
                } else {
                    break;
                }
            }

            return Double.parseDouble(expression.substring(start, pos));
        }

        private String parseIdentifier() {
            int start = pos;
            pos++;

            while (!isAtEnd() && isIdentifierPart(peek())) {
                pos++;
            }

            return expression.substring(start, pos);
        }

        private double fieldValue(String fieldName) {
            if (tupleJson == null) {
                throw new IllegalArgumentException(
                        "Cannot evaluate field '" + fieldName
                                + "' because tupleJson is null"
                );
            }

            JsonNode node = tupleJson.get(fieldName);

            if (node == null || node.isNull()) {
                throw new IllegalArgumentException(
                        "Missing field '" + fieldName
                                + "' while evaluating weight expression: "
                                + expression
                                + " tuple=" + tupleJson
                );
            }

            if (!node.isNumber()) {
                throw new IllegalArgumentException(
                        "Field '" + fieldName
                                + "' is not numeric in weight expression: "
                                + expression
                                + " value=" + node
                );
            }

            return node.asDouble();
        }

        private void expect(char expected) {
            skipWhitespace();

            if (!match(expected)) {
                throw new IllegalArgumentException(
                        "Expected '" + expected
                                + "' in weight expression near position "
                                + pos
                                + ": " + expression
                );
            }
        }

        private boolean match(char expected) {
            if (!isAtEnd() && expression.charAt(pos) == expected) {
                pos++;
                return true;
            }

            return false;
        }

        private char peek() {
            if (isAtEnd()) {
                return '\0';
            }

            return expression.charAt(pos);
        }

        private boolean isAtEnd() {
            return pos >= expression.length();
        }

        private int position() {
            return pos;
        }

        private void skipWhitespace() {
            while (!isAtEnd()
                    && Character.isWhitespace(expression.charAt(pos))) {
                pos++;
            }
        }

        private static boolean isNumberStart(char c) {
            return Character.isDigit(c) || c == '.';
        }

        private static boolean isIdentifierStart(char c) {
            return Character.isLetter(c) || c == '_';
        }

        private static boolean isIdentifierPart(char c) {
            return Character.isLetterOrDigit(c) || c == '_';
        }
    }
}