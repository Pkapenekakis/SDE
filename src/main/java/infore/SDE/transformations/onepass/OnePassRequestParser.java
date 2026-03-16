package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import infore.SDE.messages.Onepass.OnePassRequestValidator;
import infore.SDE.messages.Onepass.OnePassParams;

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

        JsonNode onePassNode = parameters.get("onePassParams");
        if (onePassNode == null || onePassNode.isNull()) {
            throw new IllegalArgumentException("Request.parameters.onePassParams is missing");
        }

        try {
            OnePassParams params = MAPPER.treeToValue(onePassNode, OnePassParams.class);

            // first version constraint
            if (rq.getNoOfP() != 1) {
                throw new IllegalArgumentException(
                        "OnePass* initial implementation supports only noOfP = 1, got " + rq.getNoOfP()
                );
            }

            OnePassRequestValidator.validate(params);
            return params;

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse onePassParams: " + e.getMessage(), e);
        }
    }
}
