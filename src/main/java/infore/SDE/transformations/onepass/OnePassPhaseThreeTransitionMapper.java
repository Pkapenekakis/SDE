package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Stateless Phase-3 lifecycle transition mapper.
 * request 86 -> START_PHASE_3_ALIAS(first non-root alias)
 * request 91 -> START_PHASE_3_ALIAS(next alias), unless the final alias has
 *               already completed.
 */
public final class OnePassPhaseThreeTransitionMapper
        extends RichFlatMapFunction<Estimation, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ONEPASS_SYNOPSIS_ID = 30;
    private static final int REQUEST_START_ALIAS = 7;
    private static final int REQUEST_PHASE2_INSTALLED = 86;
    private static final int REQUEST_PHASE3_INSTALLED = 91;

    @Override
    public void flatMap(Estimation value, Collector<Estimation> out)
            throws Exception {

        if (value == null || value.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        if (value.getRequestID() != REQUEST_PHASE2_INSTALLED
                && value.getRequestID() != REQUEST_PHASE3_INSTALLED) {
            return;
        }

        JsonNode payload = asJson(value.getEstimation());
        String type = textField(payload, "type", "");

        if (value.getRequestID() == REQUEST_PHASE2_INSTALLED) {
            if (!"GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED".equals(type)) {
                return;
            }

            int aliasCount = intField(payload, "phaseThreeAliasCount", 0);
            String firstAlias = textField(payload, "firstPhaseThreeAlias", "");

            if (aliasCount <= 0) {
                // A normal OnePass join has at least one non-root alias, but
                // keeping this no-op makes the mapper safe for degenerate plans.
                return;
            }
            if (firstAlias.isEmpty()) {
                throw new IllegalStateException(
                        "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED declares Phase-3 aliases "
                                + "but has no firstPhaseThreeAlias: " + payload);
            }

            out.collect(startAliasRequest(payload, firstAlias, 0));
            return;
        }

        if (!"GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED".equals(type)) {
            return;
        }

        if (booleanField(payload, "phaseThreeComplete", false)) {
            return;
        }

        String nextAlias = textField(payload, "nextAlias", "");
        if (nextAlias.isEmpty()) {
            throw new IllegalStateException("Non-final GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED " +
                    "has no nextAlias: " + payload);
        }

        int completedAliasIndex = intField(payload, "aliasIndex", -1);
        if (completedAliasIndex < 0) {
            throw new IllegalStateException("GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED has invalid aliasIndex: " + payload);
        }

        out.collect(startAliasRequest(payload, nextAlias, completedAliasIndex + 1));
    }

    private static Estimation startAliasRequest(JsonNode trigger, String alias, int aliasIndex) {

        int uid = intField(trigger, "uid", -1);
        int expectedWorkers = intField(trigger, "expectedWorkers", 0);
        String baseKey = textField(trigger, "baseKey", "");

        if (uid < 0 || expectedWorkers <= 0 || baseKey.isEmpty()) {
            throw new IllegalStateException("Cannot create Phase-3 transition from incomplete metadata: " + trigger);
        }

        String resultId = "PHASE3_" + alias + "_" + uid;

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("type", "START_PHASE_3_ALIAS");
        payload.put("onePassCommand", "START_PHASE_3_ALIAS");
        payload.put("protocol", "SHARDED_PHASE3_V1");
        payload.put("phase", "PHASE3");
        payload.put("uid", uid);
        payload.put("baseKey", baseKey);
        payload.put("expectedWorkers", expectedWorkers);
        payload.put("alias", alias);
        payload.put("phaseThreeAlias", alias);
        payload.put("onePassAlias", alias);
        payload.put("aliasIndex", aliasIndex);
        payload.put("resultId", resultId);
        payload.put("triggerStateRef", textField(trigger, "stateRef", ""));
        payload.put("phaseThreeAliasCount", intField(trigger, "phaseThreeAliasCount", 0));

        return new Estimation(uid, baseKey, REQUEST_START_ALIAS, ONEPASS_SYNOPSIS_ID, baseKey, payload.toString(),
                new String[] {
                        "START_PHASE_3_ALIAS",
                        alias,
                        Integer.toString(aliasIndex),
                        resultId
                },
                expectedWorkers);
    }

    private static JsonNode asJson(Object value) throws Exception {
        if (value instanceof JsonNode) {
            return (JsonNode) value;
        }
        if (value instanceof String) {
            return MAPPER.readTree((String) value);
        }
        return MAPPER.valueToTree(value);
    }

    private static String textField(JsonNode node, String field, String defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        String text = value.asText();
        return text == null || text.trim().isEmpty() ? defaultValue : text.trim();
    }

    private static int intField(JsonNode node, String field, int defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asInt(defaultValue);
    }

    private static boolean booleanField(JsonNode node, String field, boolean defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asBoolean(defaultValue);
    }
}