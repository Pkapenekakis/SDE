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

        JsonNode parameters = rq.getParameters();
        if (parameters == null || parameters.isNull()) {
            throw new IllegalArgumentException("Request.parameters is missing");
        }

        try {
            OnePassParams params;

            JsonNode onePassNode = parameters.get("onePassParams");

            if (onePassNode != null && !onePassNode.isNull()) {
                params = MAPPER.treeToValue(onePassNode, OnePassParams.class);
            } else {
                JsonNode onePassSqlNode = parameters.get("onePassSql");

                if (onePassSqlNode == null || onePassSqlNode.isNull()
                        || !onePassSqlNode.isTextual()) {
                    throw new IllegalArgumentException(
                            "Request.parameters must contain either onePassParams or textual onePassSql"
                    );
                }

                params = OnePassSqlCompiler.compile(onePassSqlNode.asText());
            }

            if (rq.getNoOfP() != 1) {
                throw new IllegalArgumentException(
                        "OnePass* initial implementation supports only noOfP = 1, got " + rq.getNoOfP()
                );
            }

            OnePassRequestValidator.validate(params);
            return params;

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse OnePass request: " + e.getMessage(), e);
        }
    }
}