package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Request;

public enum OnePassExecutionMode {

    SHARDED("SHARDED_PHASE1_V1"),
    REPLICATED("REPLICATED_PHASE1_V1");

    private final String phaseOneProtocol;
    OnePassExecutionMode(String phaseOneProtocol) {
        this.phaseOneProtocol = phaseOneProtocol;
    }

    public String phaseOneProtocol() {
        return phaseOneProtocol;
    }

    public static OnePassExecutionMode fromString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return SHARDED;
        }

        String normalized = value.trim().toUpperCase();
        if ("SHARDED".equals(normalized) || "SHARDED_COMPUTE".equals(normalized) || "SHARDED_INDEXES".equals(normalized)) {
            return SHARDED;
        }

        if ("REPLICATED".equals(normalized) || "REPLICATED_LEVEL_SYNC".equals(normalized) ||
                "REPLICATED_INDEXES".equals(normalized) || "FULL_INDEX".equals(normalized)) {
            return REPLICATED;
        }

        throw new IllegalArgumentException("Unknown OnePass execution mode '" + value + "'. Expected SHARDED or REPLICATED."
        );
    }

    public static OnePassExecutionMode fromRequest(Request request) {
        if (request == null) {
            return SHARDED;
        }

        JsonNode parameters = request.getParameters();
        if (parameters == null || parameters.isNull()) {
            return SHARDED;
        }

        JsonNode node = parameters.get("onePassExecutionMode");
        if (node == null || node.isNull()) {
            return SHARDED;
        }

        return fromString(node.asText(""));
    }
}