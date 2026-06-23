package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import infore.SDE.messages.Onepass.OnePassRequestValidator;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.transformations.onepass.sql.OnePassSqlCompiler;

public final class OnePassRequestParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassRequestParser() {
    }

    public static OnePassParams parse(Request rq) {
        if (rq == null) {
            throw new IllegalArgumentException("Request is null");
        }

        if (rq.getNoOfP() != 1) {
            throw new IllegalArgumentException("OnePass* initial implementation supports only noOfP = 1, got " +
                    rq.getNoOfP());
        }

        JsonNode parameters = rq.getParameters();

        if (parameters != null && !parameters.isNull()) {
            JsonNode onePassParamsNode = parameters.get("onePassParams");

            if (onePassParamsNode != null && !onePassParamsNode.isNull()) {
                try {
                    OnePassParams params = MAPPER.treeToValue(onePassParamsNode, OnePassParams.class);

                    OnePassRequestValidator.validate(params);

                    return params;
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse parameters.onePassParams: " + e.getMessage(),e);
                }
            }

            JsonNode onePassSqlNode = parameters.get("onePassSql");

            if (onePassSqlNode != null && !onePassSqlNode.isNull()) {
                String sql = onePassSqlNode.asText();

                if (sql != null && !sql.trim().isEmpty()) {
                    OnePassParams params = infore.SDE.transformations.onepass.sql.OnePassSqlCompiler.compile(sql.trim());

                    OnePassRequestValidator.validate(params);

                    return params;
                }
            }
        }

        /*
         * Optional fallback only.
         * This should not be the canonical interface, but it makes debugging
         * and old requests safer.
         */
        String[] param =
                rq.getParam();

        if (param != null && param.length > 0) {
            String maybeSql = param[0];

            if (maybeSql != null && maybeSql.trim().toUpperCase().startsWith("SELECT")) {
                OnePassParams params = infore.SDE.transformations.onepass.sql.OnePassSqlCompiler
                        .compile(maybeSql.trim());

                OnePassRequestValidator.validate(params);

                return params;
            }
        }

        throw new IllegalArgumentException("Missing OnePass request payload. Expected one of: " +
                "parameters.onePassParams, parameters.onePassSql, " + "or param[0] containing a SELECT query.");
    }
}