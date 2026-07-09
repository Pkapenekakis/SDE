package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

/**
 * Chooses the correct OnePass* reducer based on the local result type.
 */
public final class OnePassReduceFunctionFactory {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassReduceFunctionFactory() {
    }

    public static ReduceFunction create(Estimation value) {
        String type = resolveType(value);

        if ("LOCAL_PHASE1_RESULT".equals(type) || value.getRequestID() == 72) {
            return new OnePassIndexReduceFunction(
                    value.getNoOfP(),
                    0,
                    value.getParam(),
                    value.getSynopsisID(),
                    value.getRequestID()
            );
        }

        if ("LOCAL_PHASE2_ROOT_SUMMARY".equals(type) || value.getRequestID() == 82) {
            return new OnePassSampleReduceFunction(
                    value.getNoOfP(),
                    0,
                    value.getParam(),
                    value.getSynopsisID(),
                    value.getRequestID()
            );
        }

        if ("LOCAL_PHASE3_ALIAS_RESULT".equals(type) || value.getRequestID() == 92) {
            return new OnePassAliasReduceFunction(
                    value.getNoOfP(),
                    0,
                    value.getParam(),
                    value.getSynopsisID(),
                    value.getRequestID()
            );
        }

        System.out.println("[OnePassReduceFactory] Unsupported OnePass reduce type: " + type
                + ", requestID=" + value.getRequestID());

        return null;
    }

    private static String resolveType(Estimation value) {
        String[] param = value.getParam();

        if (param != null && param.length > 0 && param[0] != null && !param[0].trim().isEmpty()) {
            return param[0].trim();
        }

        try {
            Object estimation = value.getEstimation();

            if (estimation == null) {
                return "";
            }

            JsonNode node;

            if (estimation instanceof JsonNode) {
                node = (JsonNode) estimation;
            } else if (estimation instanceof String) {
                node = MAPPER.readTree(((String) estimation).trim());
            } else {
                node = MAPPER.valueToTree(estimation);
            }

            JsonNode typeNode = node.get("type");

            if (typeNode != null && !typeNode.isNull()) {
                return typeNode.asText("");
            }

        } catch (Exception ignored) {
        }

        return "";
    }
}