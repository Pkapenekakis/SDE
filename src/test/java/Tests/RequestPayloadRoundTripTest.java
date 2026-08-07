package infore.SDE.messages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.transformations.onepass.OnePassFeedbackRequestFactory;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Regression tests for Estimation -> Request conversion.
 *
 * The tests verify two separate behaviours:
 *
 * 1. OnePass feedback messages preserve their JSON payload through
 *    OnePassFeedbackRequestFactory and the Kafka JSON boundary.
 *
 * 2. Existing non-OnePass synopses retain the original generic
 *    Request(Estimation) behaviour and do not receive a populated
 *    Request.parameters field.
 */
public class RequestPayloadRoundTripTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ONEPASS_SYNOPSIS_ID = 30;
    private static final int GENERIC_SYNOPSIS_ID = 10;

    /**
     * Verifies that a OnePass global-state message is copied from:
     *
     * Estimation.estimation
     *      -> Request.parameters
     *      -> serialized JSON
     *      -> deserialized Request.parameters
     */
    @Test
    public void shouldPreserveOnePassPayloadAcrossRequestJsonRoundTrip()
            throws Exception {

        ObjectNode payload = MAPPER.createObjectNode();

        payload.put("type", "GLOBAL_STATE_CHUNK");
        payload.put("stateType", "GLOBAL_PHASE1_INDEX");
        payload.put("stateRef", "123_PHASE1_l_GLOBAL_STATE");
        payload.put("stateVersion", 1);
        payload.put("phase", "PHASE1");
        payload.put("activeAlias", "l");
        payload.put("chunkId", 0);
        payload.put("chunkCount", 2);
        payload.put("expectedWorkers", 4);

        payload.putArray("entries")
                .addObject()
                .put("edgeId", "o-l")
                .put("joinKey", "100")
                .put("globalWeight", 42.0d);

        Estimation estimation = new Estimation(
                123,
                "onepass-phase1-123",
                7,
                ONEPASS_SYNOPSIS_ID,
                "onepass-phase1-123",
                payload,
                new String[]{
                        "GLOBAL_STATE_CHUNK",
                        "123_PHASE1_l_GLOBAL_STATE",
                        "0",
                        "2"
                },
                4
        );

        Request request =
                OnePassFeedbackRequestFactory.fromEstimation(estimation);

        /*
         * Verify the standard Request fields.
         */
        assertEquals(
                "onepass-phase1-123",
                request.getDataSetkey()
        );

        assertEquals(7, request.getRequestID());
        assertEquals(ONEPASS_SYNOPSIS_ID, request.getSynopsisID());
        assertEquals(123, request.getUID());
        assertEquals(4, request.getNoOfP());

        assertArrayEquals(
                new String[]{
                        "GLOBAL_STATE_CHUNK",
                        "123_PHASE1_l_GLOBAL_STATE",
                        "0",
                        "2"
                },
                request.getParam()
        );

        /*
         * Verify that the OnePass-specific JSON payload was preserved.
         */
        assertNotNull(request.getParameters());

        assertEquals(
                "GLOBAL_STATE_CHUNK",
                request.getParameters()
                        .get("type")
                        .asText()
        );

        assertEquals(
                "GLOBAL_PHASE1_INDEX",
                request.getParameters()
                        .get("stateType")
                        .asText()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                request.getParameters()
                        .get("stateRef")
                        .asText()
        );

        assertEquals(
                1,
                request.getParameters()
                        .get("stateVersion")
                        .asInt()
        );

        assertEquals(
                0,
                request.getParameters()
                        .get("chunkId")
                        .asInt()
        );

        assertEquals(
                2,
                request.getParameters()
                        .get("chunkCount")
                        .asInt()
        );

        assertEquals(
                42.0d,
                request.getParameters()
                        .get("entries")
                        .get(0)
                        .get("globalWeight")
                        .asDouble(),
                0.0d
        );

        /*
         * Simulate the Kafka JSON boundary.
         *
         * Request.toKafkaJson() serializes the request and the normal
         * Request Kafka source later deserializes the same JSON.
         */
        byte[] kafkaBytes = request.toKafkaJson();

        Request deserialized =
                MAPPER.readValue(kafkaBytes, Request.class);

        assertEquals(
                "onepass-phase1-123",
                deserialized.getDataSetkey()
        );

        assertEquals(7, deserialized.getRequestID());
        assertEquals(ONEPASS_SYNOPSIS_ID, deserialized.getSynopsisID());
        assertEquals(123, deserialized.getUID());
        assertEquals(4, deserialized.getNoOfP());

        assertNotNull(deserialized.getParameters());

        assertEquals(
                "GLOBAL_STATE_CHUNK",
                deserialized.getParameters()
                        .get("type")
                        .asText()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                deserialized.getParameters()
                        .get("stateRef")
                        .asText()
        );

        assertEquals(
                2,
                deserialized.getParameters()
                        .get("chunkCount")
                        .asInt()
        );

        assertEquals(
                "o-l",
                deserialized.getParameters()
                        .get("entries")
                        .get(0)
                        .get("edgeId")
                        .asText()
        );

        assertEquals(
                "100",
                deserialized.getParameters()
                        .get("entries")
                        .get(0)
                        .get("joinKey")
                        .asText()
        );
    }

    /**
     * Verifies that the original generic Request(Estimation) conversion
     * remains unchanged for existing non-OnePass synopses.
     *
     * Even when Estimation.estimation contains a JSON-looking value, the
     * generic constructor must not copy it into Request.parameters.
     */
    @Test
    public void shouldPreserveOriginalBehaviourForExistingSynopsis()
            throws Exception {

        Estimation genericEstimation = new Estimation(
                456,
                "generic-stream-key",
                7,
                GENERIC_SYNOPSIS_ID,
                "generic-stream-key",
                "{\"mustNotBeCopied\":true}",
                new String[]{
                        "GENERIC_UPDATE",
                        "existing-value"
                },
                4
        );

        /*
         * Existing synopses continue to use the original generic constructor.
         */
        Request genericRequest =
                new Request(genericEstimation);

        assertEquals(
                "generic-stream-key",
                genericRequest.getDataSetkey()
        );

        assertEquals(7, genericRequest.getRequestID());
        assertEquals(GENERIC_SYNOPSIS_ID, genericRequest.getSynopsisID());
        assertEquals(456, genericRequest.getUID());
        assertEquals(4, genericRequest.getNoOfP());

        assertArrayEquals(
                new String[]{
                        "GENERIC_UPDATE",
                        "existing-value"
                },
                genericRequest.getParam()
        );

        /*
         * Critical backward-compatibility assertion:
         *
         * The generic constructor must not populate parameters from
         * Estimation.estimation.
         */
        assertNoParameters(genericRequest);

        /*
         * Verify that the same behaviour survives serialization and
         * deserialization.
         */
        byte[] kafkaBytes =
                genericRequest.toKafkaJson();

        Request deserialized =
                MAPPER.readValue(kafkaBytes, Request.class);

        assertEquals(
                "generic-stream-key",
                deserialized.getDataSetkey()
        );

        assertEquals(7, deserialized.getRequestID());
        assertEquals(GENERIC_SYNOPSIS_ID, deserialized.getSynopsisID());
        assertEquals(456, deserialized.getUID());
        assertEquals(4, deserialized.getNoOfP());

        assertArrayEquals(
                new String[]{
                        "GENERIC_UPDATE",
                        "existing-value"
                },
                deserialized.getParam()
        );

        /*
         * The older synopsis wire format remains unchanged.
         */
        assertNoParameters(deserialized);
    }

    /**
     * The OnePass-specific factory must never accidentally process
     * another synopsis type.
     */
    @Test(expected = IllegalArgumentException.class)
    public void onePassFactoryShouldRejectExistingSynopsisTypes() {

        Estimation genericEstimation = new Estimation(
                456,
                "generic-stream-key",
                7,
                GENERIC_SYNOPSIS_ID,
                "generic-stream-key",
                "{\"unexpected\":\"payload\"}",
                new String[]{"GENERIC_UPDATE"},
                4
        );

        OnePassFeedbackRequestFactory.fromEstimation(
                genericEstimation
        );
    }

    /**
     * Also verify that a OnePass payload supplied as a JSON String,
     * rather than an existing JsonNode, is parsed correctly.
     */
    @Test
    public void shouldParseOnePassStringPayload() {

        String payload =
                "{"
                        + "\"type\":\"GLOBAL_STATE_COMMIT\","
                        + "\"stateType\":\"GLOBAL_PHASE1_INDEX\","
                        + "\"stateRef\":\"123_PHASE1_l_GLOBAL_STATE\","
                        + "\"stateVersion\":1,"
                        + "\"chunkCount\":2"
                        + "}";

        Estimation estimation = new Estimation(
                123,
                "onepass-phase1-123",
                7,
                ONEPASS_SYNOPSIS_ID,
                "onepass-phase1-123",
                payload,
                new String[]{
                        "GLOBAL_STATE_COMMIT",
                        "123_PHASE1_l_GLOBAL_STATE"
                },
                4
        );

        Request request =
                OnePassFeedbackRequestFactory.fromEstimation(estimation);

        assertNotNull(request.getParameters());

        assertEquals(
                "GLOBAL_STATE_COMMIT",
                request.getParameters()
                        .get("type")
                        .asText()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                request.getParameters()
                        .get("stateRef")
                        .asText()
        );

        assertEquals(
                1,
                request.getParameters()
                        .get("stateVersion")
                        .asInt()
        );

        assertEquals(
                2,
                request.getParameters()
                        .get("chunkCount")
                        .asInt()
        );
    }

    /**
     * Malformed OnePass protocol JSON should fail instead of being silently
     * converted into a plain text node.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectInvalidOnePassJsonPayload() {

        Estimation estimation = new Estimation(
                123,
                "onepass-phase1-123",
                7,
                ONEPASS_SYNOPSIS_ID,
                "onepass-phase1-123",
                "{invalid-json",
                new String[]{"GLOBAL_STATE_CHUNK"},
                4
        );

        OnePassFeedbackRequestFactory.fromEstimation(
                estimation
        );
    }

    private static void assertNoParameters(Request request) {
        if (request == null) {
            throw new AssertionError("request must not be null");
        }

        JsonNode parameters = request.getParameters();

        assertEquals(
                "Expected parameters to be absent or JSON null",
                true,
                parameters == null || parameters.isNull()
        );
    }
}