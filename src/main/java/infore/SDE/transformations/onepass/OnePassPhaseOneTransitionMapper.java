package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Stateless replacement for a Phase-1 coordinator.
 * GLOBAL_PHASE1_ALIAS_READY -> START_NEXT_ALIAS / START_PHASE_2 RequestTopic message.
 */
public final class OnePassPhaseOneTransitionMapper
        extends RichFlatMapFunction<Estimation, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(Estimation value, Collector<Estimation> out) throws Exception {
        if (value == null || value.getSynopsisID() != 30 || value.getRequestID() != 77) {
            return;
        }

        JsonNode ready = value.getEstimation() instanceof String ? MAPPER.readTree((String) value.getEstimation())
                : MAPPER.valueToTree(value.getEstimation());

        if (!"GLOBAL_PHASE1_ALIAS_READY".equals(ready.get("type").asText(""))) {
            return;
        }

        String nextCommand = ready.get("nextCommand").asText("");
        if (!"START_NEXT_ALIAS".equals(nextCommand) && !"START_PHASE_2".equals(nextCommand)) {
            throw new IllegalStateException("Invalid nextCommand in GLOBAL_PHASE1_ALIAS_READY: " + ready);
        }

        int uid = ready.get("uid").asInt();
        int completedEpoch = ready.get("epoch").asInt();
        int expectedWorkers = ready.get("expectedWorkers").asInt();
        String baseKey = ready.get("baseKey").asText("");
        String nextAlias = ready.get("nextAlias").asText("");
        String resultId = ready.get("resultId").asText("");

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("type", nextCommand);
        payload.put("onePassCommand", nextCommand);
        payload.put("protocol", "SHARDED_PHASE1_V1");
        payload.put("phase", "PHASE1");
        payload.put("uid", uid);
        payload.put("completedAlias", ready.get("alias").asText(""));
        payload.put("completedEpoch", completedEpoch);
        payload.put("epoch", completedEpoch + 1);
        payload.put("nextAlias", nextAlias);
        payload.put("onePassAlias", nextAlias);
        payload.put("resultId", resultId);
        payload.put("expectedWorkers", expectedWorkers);
        payload.put("baseKey", baseKey);
        payload.put("activeEdgeId", ready.get("activeEdgeId").asText(""));
        payload.put("globalSeenTuples", ready.get("globalSeenTuples").asLong(0L));
        payload.put("globalKeyCount", ready.get("globalKeyCount").asLong(0L));
        payload.put("globalTotalWeight", ready.get("globalTotalWeight").asDouble(0.0d));

        /*
         * Request(Estimation) uses estimationkey as DataSetkey, therefore this
         * MUST be baseKey (not a unique result key).
         */
        Estimation request = new Estimation(uid, baseKey, 7, 30, baseKey, payload.toString(),
                new String[] {
                        nextCommand,
                        nextAlias,
                        Integer.toString(completedEpoch + 1),
                        resultId
                }, expectedWorkers
        );

        out.collect(request);
    }
}