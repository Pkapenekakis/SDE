package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.util.*;

/** Tiny federated readiness reduction; no Phase-1 index entries are merged. */
public final class OnePassPhaseOneReadyReduceFunction extends ReduceFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<Integer> workers = new LinkedHashSet<Integer>();

    private int uid = -1;
    private int epoch = -1;
    private String alias = "";
    private String resultId = "";
    private String nextCommand = "";
    private String nextAlias = "";
    private String baseKey = "";
    private String activeEdgeId = "";

    private long globalSeenTuples = 0L;
    private long globalKeyCount = 0L;
    private double globalTotalWeight = 0.0d;

    public OnePassPhaseOneReadyReduceFunction(
            int nOfP,
            int count,
            String[] parameters,
            int synID,
            int rqid) {
        super(nOfP, count, parameters, synID, rqid);
    }

    @Override
    public boolean add(Estimation estimation) {
        try {
            JsonNode node = asJson(estimation.getEstimation());

            int workerId = node.get("workerId").asInt(-1);
            if (workerId < 0 || workerId >= nOfP) {
                throw new IllegalStateException("Invalid workerId in LOCAL_PHASE1_SHARD_READY: " + node);
            }

            if (!workers.add(workerId)) {
                return workers.size() >= nOfP; // duplicate local-ready is idempotent
            }

            if (uid < 0) {
                uid = node.get("uid").asInt(-1);
                epoch = node.get("epoch").asInt(-1);
                alias = text(node, "alias");
                resultId = text(node, "resultId");
                nextCommand = text(node, "nextCommand");
                nextAlias = text(node, "nextAlias");
                baseKey = text(node, "baseKey");
                activeEdgeId = text(node, "activeEdgeId");
            } else {
                requireSame(node, "uid", uid);
                requireSame(node, "epoch", epoch);
                requireSame(node, "alias", alias);
                requireSame(node, "resultId", resultId);
                requireSame(node, "nextCommand", nextCommand);
                requireSame(node, "nextAlias", nextAlias);
                requireSame(node, "baseKey", baseKey);
                requireSame(node, "activeEdgeId", activeEdgeId);
            }

            globalSeenTuples += node.get("localSeenTuples").asLong(0L);
            globalKeyCount += node.get("localKeyCount").asLong(0L);
            globalTotalWeight += node.get("localTotalWeight").asDouble(0.0d);
            count = workers.size();

            return workers.size() >= nOfP;

        } catch (Exception e) {
            throw new IllegalStateException("Could not reduce LOCAL_PHASE1_SHARD_READY", e);
        }
    }

    @Override
    public Object reduce() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("type", "GLOBAL_PHASE1_ALIAS_READY");
        out.put("protocol", "SHARDED_PHASE1_V1");
        out.put("phase", "PHASE1");
        out.put("uid", uid);
        out.put("epoch", epoch);
        out.put("alias", alias);
        out.put("resultId", resultId);
        out.put("nextCommand", nextCommand);
        out.put("nextAlias", nextAlias);
        out.put("baseKey", baseKey);
        out.put("activeEdgeId", activeEdgeId);
        out.put("expectedWorkers", nOfP);
        out.put("receivedWorkers", new ArrayList<Integer>(workers));
        out.put("globalSeenTuples", globalSeenTuples);
        out.put("globalKeyCount", globalKeyCount);
        out.put("globalTotalWeight", globalTotalWeight);

        try {
            return MAPPER.writeValueAsString(out);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialize GLOBAL_PHASE1_ALIAS_READY", e);
        }
    }

    private static JsonNode asJson(Object value) throws Exception {
        if (value instanceof JsonNode) return (JsonNode) value;
        if (value instanceof String) return MAPPER.readTree((String) value);
        return MAPPER.valueToTree(value);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? "" : value.asText("");
    }

    private static void requireSame(JsonNode node, String field, int expected) {
        if (node.get(field).asInt(Integer.MIN_VALUE) != expected) {
            throw new IllegalStateException("Conflicting " + field + " in shard-ready reduction: " + node);
        }
    }

    private static void requireSame(JsonNode node, String field, String expected) {
        if (!expected.equals(text(node, field))) {
            throw new IllegalStateException("Conflicting " + field + " in shard-ready reduction: " + node);
        }
    }
}