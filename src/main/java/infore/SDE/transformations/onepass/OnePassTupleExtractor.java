package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;

public final class OnePassTupleExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassTupleExtractor() {
    }

    /**
     * Extracts a OnePassTuple from the JSON payload passed into synopsis.add(...).
     * For OnePass, this method expects that payload to be a JSON object string with the following format,
     * e.g. {"table":"orders","o_orderkey":1,"o_custkey":42}    --Could have more fields
     */
    public static OnePassTuple extract(String valuesJson) {
        return extract((Object) valuesJson);
    }

    public static OnePassTuple extract(Object payload) {
        JsonNode tupleNode = toJsonNode(payload);

        if (tupleNode == null || tupleNode.isNull() || !tupleNode.isObject()) {
            throw new IllegalArgumentException("Datapoint values is not a valid JSON object: " + payload);
        }

        JsonNode aliasNode = tupleNode.get("alias");
        JsonNode tableNode = tupleNode.get("table");

        String relationId = null;

        if (aliasNode != null && !aliasNode.isNull() && !aliasNode.asText().trim().isEmpty()) {
            relationId = aliasNode.asText().trim();
        } else if (tableNode != null && !tableNode.isNull() && !tableNode.asText().trim().isEmpty()) {
            relationId = tableNode.asText().trim();
        }

        if (relationId == null) {
            throw new IllegalArgumentException(
                    "Missing required relation identifier. Expected 'alias' or 'table' in datapoint values: "
                            + tupleNode.toString());
        }

        return new OnePassTuple(relationId, tupleNode);
    }

    private static JsonNode toJsonNode(Object payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Datapoint values payload is null");
        }

        try {
            if (payload instanceof JsonNode) {
                return (JsonNode) payload;
            }

            if (payload instanceof String) {
                String s = ((String) payload).trim();
                if (s.isEmpty()) {
                    throw new IllegalArgumentException("Datapoint values JSON is empty");
                }
                return MAPPER.readTree(s);
            }

            return MAPPER.valueToTree(payload);

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse OnePass datapoint payload", e);
        }
    }

}