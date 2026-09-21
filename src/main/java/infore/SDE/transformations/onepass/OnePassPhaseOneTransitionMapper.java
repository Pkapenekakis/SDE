package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Stateless Phase-1 transition mapper.
 *
 * SHARDED:
 *   request 77 GLOBAL_PHASE1_ALIAS_READY
 *       -> START_NEXT_ALIAS / START_PHASE_2
 *
 * REPLICATED:
 *   request 75 GLOBAL_PHASE1_INDEX_INSTALLED
 *       -> START_NEXT_ALIAS / START_PHASE_2
 *
 * The replicated path deliberately waits until EVERY worker has installed
 * the completed active index before releasing the next alias.
 */
public final class OnePassPhaseOneTransitionMapper extends RichFlatMapFunction<Estimation, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(Estimation value, Collector<Estimation> out) throws Exception {

        if (value == null || value.getSynopsisID() != 30) {
            return;
        }

        JsonNode payload = value.getEstimation() instanceof String ?
                MAPPER.readTree((String) value.getEstimation()) : MAPPER.valueToTree(value.getEstimation());

        if (value.getRequestID() == 77 && "GLOBAL_PHASE1_ALIAS_READY".equals(textField(payload, "type", ""))) {
            emitTransition(payload, "SHARDED_PHASE1_V1", textField(payload, "alias", ""), out);
            return;
        }

        if (value.getRequestID() == 75 && "GLOBAL_PHASE1_INDEX_INSTALLED".equals(textField(payload, "type", ""))) {
            emitTransition(payload, "REPLICATED_PHASE1_V1", textField(payload, "activeAlias",
                    textField(payload, "alias", "")), out);
        }
    }

    private void emitTransition(JsonNode ready, String protocol, String completedAlias, Collector<Estimation> out) {
        String nextCommand = textField(ready, "nextCommand", "");
        if (!"START_NEXT_ALIAS".equals(nextCommand) && !"START_PHASE_2".equals(nextCommand)) {
            throw new IllegalStateException("Invalid Phase-1 nextCommand: " + ready);
        }

        int uid = intField(ready, "uid", -1);
        int completedEpoch = intField(ready, "epoch", -1);
        int expectedWorkers = intField(ready, "expectedWorkers", -1);
        String baseKey = textField(ready, "baseKey", "");
        String nextAlias = textField(ready, "nextAlias", "");
        String resultId = textField(ready, "resultId", "");

        if (uid < 0 || completedEpoch <= 0 || expectedWorkers <= 0 || baseKey.isEmpty() || completedAlias.isEmpty() || nextAlias.isEmpty() || resultId.isEmpty()) {
            throw new IllegalStateException("Incomplete Phase-1 transition metadata: " + ready);
        }

        ObjectNode transition = MAPPER.createObjectNode();

        transition.put("type", nextCommand);
        transition.put("onePassCommand", nextCommand);
        transition.put("protocol", protocol);
        transition.put("phase", "PHASE1");
        transition.put("uid", uid);
        transition.put("completedAlias", completedAlias);
        transition.put("completedEpoch", completedEpoch);
        transition.put("epoch", completedEpoch + 1);
        transition.put("nextAlias", nextAlias);
        transition.put("onePassAlias", nextAlias);
        transition.put("resultId", resultId);
        transition.put("expectedWorkers", expectedWorkers);
        transition.put("baseKey", baseKey);
        transition.put("activeEdgeId", textField(ready, "activeEdgeId", ""));
        transition.put("globalSeenTuples", longField(ready, "globalSeenTuples", 0L));
        transition.put("globalKeyCount", longField(ready, "globalKeyCount", 0L));
        transition.put("globalTotalWeight", doubleField(ready, "globalTotalWeight", 0.0d));
        Estimation request = new Estimation(uid, baseKey, 7, 30, baseKey, transition.toString(),
                new String[]{nextCommand, nextAlias, Integer.toString(completedEpoch + 1), resultId}, expectedWorkers);

        out.collect(request);
    }

    private static String textField(JsonNode node, String field, String defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }

        String text = value.asText();
        return text == null || text.trim().isEmpty() ? defaultValue : text.trim();
    }

    private static int intField(JsonNode node, String field, int defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asInt(defaultValue);
    }

    private static long longField(JsonNode node, String field, long defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String field, double defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asDouble(defaultValue);
    }
}