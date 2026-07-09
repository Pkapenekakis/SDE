package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Phase 1 parallel merge for OnePass*.
 *
 * Input:
 *   LOCAL_PHASE1_RESULT from each worker.
 *
 * Output:
 *   GLOBAL_PHASE1_RESULT as a JSON string.
 *
 * For now this reducer merges the debug/exported edge index representation:
 *
 *   edgeIndexes: {
 *      edgeId: {
 *          joinKeyString: weight
 *      }
 *   }
 */
public final class OnePassIndexReduceFunction extends ReduceFunction {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<Integer> receivedWorkers = new HashSet<Integer>();

    private final Map<String, Map<String, Double>> mergedEdgeIndexes =
            new LinkedHashMap<String, Map<String, Double>>();

    private final Map<String, Long> mergedSeenTuplesByAlias =
            new LinkedHashMap<String, Long>();

    private int uid = -1;
    private String phase = "PHASE1";
    private String resultId = "PHASE1_RESULT";
    private String queryName = "";
    private String rootAlias = "";
    private int expectedWorkers;

    public OnePassIndexReduceFunction(
            int nOfP,
            int count,
            String[] parameters,
            int synID,
            int rqid) {

        super(nOfP, count, parameters, synID, rqid);
        this.expectedWorkers = nOfP <= 0 ? 1 : nOfP;
    }

    @Override
    public boolean add(Estimation e) {
        try {
            JsonNode payload = parsePayload(e.getEstimation());

            if (payload == null || payload.isNull()) {
                System.out.println("[OnePassPhase1Reduce] Ignoring null payload.");
                return false;
            }

            String type = textField(payload, "type", "");

            if (!"LOCAL_PHASE1_RESULT".equals(type)) {
                System.out.println("[OnePassPhase1Reduce] Ignoring unexpected payload type: " + type);
                return false;
            }

            uid = intField(payload, "uid", e.getUID());
            phase = textField(payload, "phase", "PHASE1");
            resultId = textField(payload, "resultId", "PHASE1_RESULT_" + uid);
            queryName = textField(payload, "queryName", "");
            rootAlias = textField(payload, "rootAlias", "");

            int workerId = intField(payload, "workerId", -1);
            int payloadExpectedWorkers = intField(payload, "expectedWorkers", e.getNoOfP());

            if (payloadExpectedWorkers > 0) {
                expectedWorkers = Math.max(expectedWorkers, payloadExpectedWorkers);
            }

            if (workerId < 0) {
                System.out.println("[OnePassPhase1Reduce] Ignoring result with invalid workerId: " + payload);
                return false;
            }

            if (!receivedWorkers.add(workerId)) {
                System.out.println("[OnePassPhase1Reduce] Duplicate worker ignored: workerId=" + workerId
                        + ", resultId=" + resultId);
                return false;
            }

            JsonNode phaseOneResult = payload.get("phaseOneResult");

            if (phaseOneResult != null && !phaseOneResult.isNull()) {
                mergePhaseOneResult(phaseOneResult);
            }

            count = receivedWorkers.size();

            System.out.println("[OnePassPhase1Reduce] Received worker "
                    + workerId + " for " + resultId
                    + " (" + count + "/" + expectedWorkers + ")");

            return count >= expectedWorkers;

        } catch (Exception ex) {
            throw new IllegalStateException("Could not add LOCAL_PHASE1_RESULT to OnePass Phase 1 reducer.", ex);
        }
    }

    @Override
    public Object reduce() {
        try {
            Map<String, Object> payload = new LinkedHashMap<String, Object>();

            ArrayList<Integer> workers = new ArrayList<Integer>(receivedWorkers);
            Collections.sort(workers);

            payload.put("type", "GLOBAL_PHASE1_RESULT");
            payload.put("uid", uid);
            payload.put("phase", phase);
            payload.put("resultId", resultId);
            payload.put("queryName", queryName);
            payload.put("rootAlias", rootAlias);
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("receivedWorkers", workers);
            payload.put("localResultCount", receivedWorkers.size());

            Map<String, Object> globalPhaseOneResult = new LinkedHashMap<String, Object>();
            globalPhaseOneResult.put("queryName", queryName);
            globalPhaseOneResult.put("rootAlias", rootAlias);
            globalPhaseOneResult.put("seenTuplesByAlias", new LinkedHashMap<String, Long>(mergedSeenTuplesByAlias));
            globalPhaseOneResult.put("edgeIndexes", deepCopyEdgeIndexes());
            globalPhaseOneResult.put("edgeSummaries", buildEdgeSummaries());

            payload.put("globalPhaseOneResult", globalPhaseOneResult);

            return MAPPER.writeValueAsString(payload);

        } catch (Exception ex) {
            throw new IllegalStateException("Could not serialize GLOBAL_PHASE1_RESULT.", ex);
        }
    }

    private void mergePhaseOneResult(JsonNode phaseOneResult) {
        JsonNode seenTuples = phaseOneResult.get("seenTuplesByAlias");

        if (seenTuples != null && seenTuples.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = seenTuples.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();

                String alias = entry.getKey();
                long value = entry.getValue().asLong(0L);

                Long current = mergedSeenTuplesByAlias.get(alias);

                if (current == null) {
                    current = 0L;
                }

                mergedSeenTuplesByAlias.put(alias, current + value);
            }
        }

        JsonNode edgeIndexes = phaseOneResult.get("edgeIndexes");

        if (edgeIndexes == null || !edgeIndexes.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> edgeFields = edgeIndexes.fields();

        while (edgeFields.hasNext()) {
            Map.Entry<String, JsonNode> edgeEntry = edgeFields.next();

            String edgeId = edgeEntry.getKey();
            JsonNode joinWeights = edgeEntry.getValue();

            if (joinWeights == null || !joinWeights.isObject()) {
                continue;
            }

            Map<String, Double> mergedJoinWeights = mergedEdgeIndexes.get(edgeId);

            if (mergedJoinWeights == null) {
                mergedJoinWeights = new LinkedHashMap<String, Double>();
                mergedEdgeIndexes.put(edgeId, mergedJoinWeights);
            }

            Iterator<Map.Entry<String, JsonNode>> joinFields = joinWeights.fields();

            while (joinFields.hasNext()) {
                Map.Entry<String, JsonNode> joinEntry = joinFields.next();

                String joinKey = joinEntry.getKey();
                double weight = joinEntry.getValue().asDouble(0.0d);

                Double current = mergedJoinWeights.get(joinKey);

                if (current == null) {
                    current = 0.0d;
                }

                mergedJoinWeights.put(joinKey, current + weight);
            }
        }
    }

    private Map<String, Map<String, Double>> deepCopyEdgeIndexes() {
        Map<String, Map<String, Double>> copy = new LinkedHashMap<String, Map<String, Double>>();

        for (Map.Entry<String, Map<String, Double>> edgeEntry : mergedEdgeIndexes.entrySet()) {
            copy.put(edgeEntry.getKey(), new LinkedHashMap<String, Double>(edgeEntry.getValue()));
        }

        return copy;
    }

    private Map<String, Map<String, Object>> buildEdgeSummaries() {
        Map<String, Map<String, Object>> summaries =
                new LinkedHashMap<String, Map<String, Object>>();

        for (Map.Entry<String, Map<String, Double>> edgeEntry : mergedEdgeIndexes.entrySet()) {
            String edgeId = edgeEntry.getKey();
            Map<String, Double> joinWeights = edgeEntry.getValue();

            double totalWeight = 0.0d;

            for (Double weight : joinWeights.values()) {
                if (weight != null) {
                    totalWeight += weight;
                }
            }

            Map<String, Object> summary = new LinkedHashMap<String, Object>();
            summary.put("numberOfKeys", joinWeights.size());
            summary.put("totalWeight", totalWeight);

            summaries.put(edgeId, summary);
        }

        return summaries;
    }

    private static JsonNode parsePayload(Object estimation) throws Exception {
        if (estimation == null) {
            return null;
        }

        if (estimation instanceof JsonNode) {
            return (JsonNode) estimation;
        }

        if (estimation instanceof String) {
            String s = ((String) estimation).trim();

            if (s.isEmpty()) {
                return null;
            }

            return MAPPER.readTree(s);
        }

        return MAPPER.valueToTree(estimation);
    }

    private static String textField(JsonNode node, String fieldName, String defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        String value = field.asText();

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