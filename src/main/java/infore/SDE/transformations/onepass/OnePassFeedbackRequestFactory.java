package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

/**
 * Converts OnePass feedback Estimations into RequestTopic requests.
 *
 * This class is intentionally OnePass-specific so that the generic
 * Request and Estimation behavior remains unchanged for all other synopses.
 */
public final class OnePassFeedbackRequestFactory {

    private static final int ONEPASS_SYNOPSIS_ID = 30;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassFeedbackRequestFactory() {
    }

    public static Request fromEstimation(Estimation estimation) {
        if (estimation == null) {
            throw new IllegalArgumentException("estimation must not be null");
        }

        if (estimation.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            throw new IllegalArgumentException("OnePassFeedbackRequestFactory only supports synopsisID="
                            + ONEPASS_SYNOPSIS_ID + ", received synopsisID=" + estimation.getSynopsisID());
        }

        //Use the original generic conversion first.This preserves the established SDE field mapping.
        Request request = new Request(estimation);

        //Only OnePass feedback requests receive the additional JSON payload.

        request.setParameters(convertPayload(estimation.getEstimation()));

        return request;
    }

    private static JsonNode convertPayload(Object payload) {
        if (payload == null) {
            return null;
        }

        if (payload instanceof JsonNode) {
            return ((JsonNode) payload).deepCopy();
        }

        if (payload instanceof String) {
            String text = ((String) payload).trim();

            if (text.isEmpty()) {
                return null;
            }

            try {
                return MAPPER.readTree(text);
            } catch (IOException exception) {
                throw new IllegalArgumentException("OnePass feedback payload is not valid JSON", exception);
            }
        }

        try {
            return MAPPER.valueToTree(payload);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Could not convert OnePass feedback payload to JSON", exception);
        }
    }
}