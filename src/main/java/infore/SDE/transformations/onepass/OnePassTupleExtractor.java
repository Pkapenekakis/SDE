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
        if (valuesJson == null || valuesJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Datapoint values JSON is null or empty");
        }

        try {
            JsonNode tupleNode = MAPPER.readTree(valuesJson);

            if (tupleNode == null || tupleNode.isNull() || !tupleNode.isObject()) {
                throw new IllegalArgumentException("Datapoint values is not a valid JSON object: " + valuesJson);
            }

            JsonNode tableNode = tupleNode.get("table");
            if (tableNode == null || tableNode.isNull() || tableNode.asText().trim().isEmpty()) {
                throw new IllegalArgumentException("Missing required field 'table' in datapoint values: " + valuesJson);
            }

            return new OnePassTuple(tableNode.asText(), tupleNode);

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse OnePass datapoint values JSON", e);
        }
    }
}