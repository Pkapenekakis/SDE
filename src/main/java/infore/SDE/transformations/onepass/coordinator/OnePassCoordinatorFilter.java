package infore.SDE.transformations.onepass.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Lightweight OnePass feedback coordinator.
 *
 * Logical responsibilities:
 *
 *   GLOBAL_STATE_BEGIN -> forward
 *
 *   GLOBAL_STATE_CHUNK-> forward
 *
 *   GLOBAL_STATE_COMMIT
 *       -> forward
 *       -> emit START_NEXT_ALIAS or START_PHASE_2
 *
 * It intentionally does not:
 *
 *   - wait for worker installation acknowledgements;
 *   - count workers;
 *   - retain global indexes;
 *   - reconstruct state;
 *   - merge results;
 *   - maintain the OnePass query plan.
 *
 * Although the architecture calls this component a coordinator filter,
 * it is implemented as a FlatMap because one COMMIT input produces two
 * outputs: COMMIT followed by the transition request.
 */
public final class OnePassCoordinatorFilter extends RichFlatMapFunction<Estimation, Estimation> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    private static final int ONEPASS_SYNOPSIS_ID = 30;
    private static final int ONEPASS_FEEDBACK_REQUEST_ID = 7;

    private static final String TYPE_GLOBAL_STATE_BEGIN = "GLOBAL_STATE_BEGIN";
    private static final String TYPE_GLOBAL_STATE_CHUNK = "GLOBAL_STATE_CHUNK";
    private static final String TYPE_GLOBAL_STATE_COMMIT = "GLOBAL_STATE_COMMIT";
    private static final String COMMAND_START_NEXT_ALIAS = "START_NEXT_ALIAS";
    private static final String COMMAND_START_PHASE_2 = "START_PHASE_2";

    @Override
    public void flatMap(Estimation value, Collector<Estimation> out) throws Exception {

        if (value == null) {
            return;
        }

        if (value.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        if (value.getRequestID() != ONEPASS_FEEDBACK_REQUEST_ID) {
            return;
        }

        JsonNode payload = parsePayload(value.getEstimation());

        if (payload == null || payload.isNull()) {
            return;
        }

        String type = textField(payload, "type", "");

        if (TYPE_GLOBAL_STATE_BEGIN.equals(type) || TYPE_GLOBAL_STATE_CHUNK.equals(type)) {

            out.collect(value);
            return;
        }

        if (TYPE_GLOBAL_STATE_COMMIT.equals(type)) {

            /*
             * Preserve order in this output stream:
             *
             *   COMMIT
             *   transition request
             */
            out.collect(value);

            Estimation transition = buildTransitionRequest(value, payload);

            out.collect(transition);
        }
    }

    private Estimation buildTransitionRequest(Estimation commit, JsonNode commitPayload) {

        int uid = intField(commitPayload, "uid", commit.getUID());
        int expectedWorkers = intField(commitPayload, "expectedWorkers", commit.getNoOfP());

        if (expectedWorkers <= 0) {
            expectedWorkers = commit.getNoOfP() > 0 ? commit.getNoOfP() : 1;
        }

        String baseKey = textField(commitPayload, "baseKey", "");

        if (baseKey == null || baseKey.trim().isEmpty()) {
            throw new IllegalStateException("GLOBAL_STATE_COMMIT is missing baseKey. uid=" + uid);
        }

        String stateRef = textField(commitPayload, "stateRef", "");

        if (stateRef == null || stateRef.trim().isEmpty()) {
            throw new IllegalStateException("GLOBAL_STATE_COMMIT is missing stateRef. uid=" + uid);
        }

        String nextCommand = textField(commitPayload, "nextCommand", "");
        String nextAlias = textField(commitPayload, "nextAlias", "");

        if (!COMMAND_START_NEXT_ALIAS.equals(nextCommand) && !COMMAND_START_PHASE_2.equals(nextCommand)) {

            throw new IllegalStateException("Unsupported Phase 1 transition command '" + nextCommand
                            + "'. Expected START_NEXT_ALIAS or START_PHASE_2. " + "uid="
                            + uid + ", stateRef=" + stateRef);
        }

        if (nextAlias == null || nextAlias.trim().isEmpty()) {
            throw new IllegalStateException("GLOBAL_STATE_COMMIT is missing nextAlias. " + "uid=" + uid
                            + ", stateRef=" + stateRef + ", nextCommand=" + nextCommand);
        }

        ObjectNode transitionPayload = MAPPER.createObjectNode();

        transitionPayload.put("type", nextCommand);
        transitionPayload.put("uid", uid);
        transitionPayload.put("synopsisID", ONEPASS_SYNOPSIS_ID);
        transitionPayload.put("baseKey", baseKey);
        transitionPayload.put("nextAlias", nextAlias);
        transitionPayload.put("requiredStateRef", stateRef);

        copyIfPresent(commitPayload, transitionPayload, "phase");
        copyIfPresent(commitPayload, transitionPayload, "resultId");
        copyIfPresent(commitPayload, transitionPayload, "rootAlias");
        copyIfPresent(commitPayload, transitionPayload, "activeAlias");
        copyIfPresent(commitPayload, transitionPayload, "activeEdgeId");

        copyIfPresent(commitPayload, transitionPayload, "stateType");

        String[] param = new String[] {nextCommand, stateRef, nextAlias};

        /*
         * estimationkey must remain the logical base key.
         *
         * OnePassFeedbackRequestFactory converts this Estimation into a
         * Request, and the generic Request constructor maps estimationkey
         * to Request.DataSetkey.
         */
        Estimation transition = new Estimation(
                        uid,
                        baseKey,
                        ONEPASS_FEEDBACK_REQUEST_ID,
                        ONEPASS_SYNOPSIS_ID,
                        baseKey,
                        transitionPayload,
                        param,
                        expectedWorkers
                );

        transition.setStreamID(commit.getStreamID());

        return transition;
    }

    private static JsonNode parsePayload(
            Object payload) throws Exception {

        if (payload == null) {
            return null;
        }

        if (payload instanceof JsonNode) {
            return (JsonNode) payload;
        }

        if (payload instanceof String) {
            String text = ((String) payload).trim();

            if (text.isEmpty()) {
                return null;
            }

            return MAPPER.readTree(text);
        }

        return MAPPER.valueToTree(payload);
    }

    private static void copyIfPresent(JsonNode source, ObjectNode target, String fieldName) {

        if (source == null || source.isNull()) {
            return;
        }

        JsonNode value = source.get(fieldName);

        if (value != null && !value.isNull()) {
            target.set(fieldName, value.deepCopy());
        }
    }

    private static String textField(JsonNode node, String fieldName, String defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        String value =
                field.asText();

        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int intField(JsonNode node, String fieldName, int defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asInt(defaultValue);
    }
}