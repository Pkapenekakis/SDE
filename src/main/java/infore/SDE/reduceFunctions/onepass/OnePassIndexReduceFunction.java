package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.io.Serializable;
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
 * This reducer supports multi-level / multi-alias Phase 1.
 *
 * Example WQ3:
 *
 *   1. activeAlias = l, activeEdgeId = l<->o
 *      -> sum l<->o across workers
 *
 *   2. activeAlias = o, activeEdgeId = c<->o
 *      -> every worker already has the global l<->o index
 *      -> copy l<->o only once
 *      -> sum only c<->o across workers
 *
 * This prevents already-global child indexes from being multiplied by
 * the number of workers during later Phase 1 aliases.
 */
public final class OnePassIndexReduceFunction extends ReduceFunction implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<Integer> receivedWorkers = new HashSet<Integer>();

    private final Map<String, Map<String, Double>> mergedEdgeIndexes =
            new LinkedHashMap<String, Map<String, Double>>();

    private final Map<String, Long> mergedSeenTuplesByAlias =
            new LinkedHashMap<String, Long>();

    private boolean stableEdgesCopied = false;
    private boolean stableSeenCopied = false;

    private int uid = -1;
    private String phase = "PHASE1";
    private String resultId = "PHASE1_RESULT";
    private String queryName = "";
    private String rootAlias = "";
    private String baseKey = "";
    private String activeAlias = "";
    private String activeEdgeId = "";
    private String nextCommand = "";
    private String nextAlias = "";

    private int expectedWorkers;

    public OnePassIndexReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {
        super(nOfP, count, parameters, synID, rqid);
        this.expectedWorkers = nOfP <= 0 ? 1 : nOfP;
    }

    @Override
    public boolean add(Estimation e) {
        try {
            JsonNode payload = parsePayload(e.getEstimation());
            boolean includesStableState = booleanField( payload,"includesStableState", true);

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

            String payloadBaseKey = textField(payload, "baseKey", "");

            if (payloadBaseKey != null && !payloadBaseKey.trim().isEmpty()) {
                if (baseKey == null || baseKey.trim().isEmpty()) {
                    baseKey = payloadBaseKey.trim();
                }
            }

            String payloadActiveAlias = resolveActiveAlias(payload, e);
            String payloadActiveEdgeId = textField(payload, "activeEdgeId", "");

            if (activeAlias == null || activeAlias.trim().isEmpty()) {
                activeAlias = payloadActiveAlias;
            } else if (payloadActiveAlias != null
                    && !payloadActiveAlias.trim().isEmpty()
                    && !activeAlias.equals(payloadActiveAlias)) {
                throw new IllegalStateException(
                        "Mismatching activeAlias values in same reduce group: "
                                + activeAlias + " vs " + payloadActiveAlias
                );
            }

            if (activeEdgeId == null || activeEdgeId.trim().isEmpty()) {
                activeEdgeId = payloadActiveEdgeId;
            } else if (payloadActiveEdgeId != null
                    && !payloadActiveEdgeId.trim().isEmpty()
                    && !activeEdgeId.equals(payloadActiveEdgeId)) {
                throw new IllegalStateException(
                        "Mismatching activeEdgeId values in same reduce group: "
                                + activeEdgeId + " vs " + payloadActiveEdgeId
                );
            }

            String payloadNextCommand = textField(payload, "nextCommand", "");
            String payloadNextAlias = textField(payload, "nextAlias", "");

            if (nextCommand == null || nextCommand.trim().isEmpty()) {
                nextCommand = payloadNextCommand;

            } else if (payloadNextCommand != null && !payloadNextCommand.trim().isEmpty()
                    && !nextCommand.equals(payloadNextCommand)) {

                throw new IllegalStateException("Mismatching nextCommand values in same reduce group: " +
                        nextCommand + " vs " + payloadNextCommand);
            }

            if (nextAlias == null || nextAlias.trim().isEmpty()) {
                nextAlias = payloadNextAlias;
            } else if (payloadNextAlias != null && !payloadNextAlias.trim().isEmpty() && !nextAlias.equals(payloadNextAlias)) {
                throw new IllegalStateException("Mismatching nextAlias values in same reduce group: " +
                        nextAlias + " vs " + payloadNextAlias);
            }

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
                        + ", resultId=" + resultId
                        + ", activeAlias=" + activeAlias);
                return false;
            }

            JsonNode phaseOneResult = payload.get("phaseOneResult");

            if (phaseOneResult != null && !phaseOneResult.isNull()) {
                mergePhaseOneResult(phaseOneResult, activeAlias, activeEdgeId, includesStableState);
            }

            count = receivedWorkers.size();

            System.out.println("[OnePassPhase1Reduce] Received worker "
                    + workerId
                    + " for resultId=" + resultId
                    + ", activeAlias=" + activeAlias
                    + ", activeEdgeId=" + activeEdgeId
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

            if (baseKey == null || baseKey.trim().isEmpty()) {
                baseKey = "onepass-phase1-" + uid;
            }

            payload.put("type", "GLOBAL_PHASE1_RESULT");
            payload.put("uid", uid);
            payload.put("phase", phase);
            payload.put("resultId", resultId);
            payload.put("queryName", queryName);
            payload.put("rootAlias", rootAlias);
            payload.put("baseKey", baseKey);
            payload.put("activeAlias", activeAlias == null ? "" : activeAlias);
            payload.put("activeEdgeId", activeEdgeId == null ? "" : activeEdgeId);
            payload.put("nextCommand", nextCommand == null ? "" : nextCommand);
            payload.put("nextAlias", nextAlias == null ? "" : nextAlias);
            payload.put("stateRef", uid + "_PHASE1_" + resultId + "_GLOBAL_STATE");
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("receivedWorkers", workers);
            payload.put("localResultCount", receivedWorkers.size());

            Map<String, Object> globalPhaseOneResult = new LinkedHashMap<String, Object>();
            globalPhaseOneResult.put("queryName", queryName);
            globalPhaseOneResult.put("rootAlias", rootAlias);
            globalPhaseOneResult.put("activeAlias", activeAlias == null ? "" : activeAlias);
            globalPhaseOneResult.put("activeEdgeId", activeEdgeId == null ? "" : activeEdgeId);
            globalPhaseOneResult.put("seenTuplesByAlias", new LinkedHashMap<String, Long>(mergedSeenTuplesByAlias));
            globalPhaseOneResult.put("edgeIndexes", deepCopyEdgeIndexes());
            globalPhaseOneResult.put("edgeSummaries", buildEdgeSummaries());

            payload.put("globalPhaseOneResult", globalPhaseOneResult);

            return MAPPER.writeValueAsString(payload);

        } catch (Exception ex) {
            throw new IllegalStateException("Could not serialize GLOBAL_PHASE1_RESULT.", ex);
        }
    }

    private void mergePhaseOneResult(JsonNode phaseOneResult, String activeAlias, String activeEdgeId,
                                     boolean includesStableState) {

        JsonNode seenTuples = phaseOneResult.get("seenTuplesByAlias");
        mergeSeenTuples(seenTuples, activeAlias, includesStableState);

        JsonNode edgeIndexes = phaseOneResult.get("edgeIndexes");

        if (edgeIndexes == null || !edgeIndexes.isObject()) {
            return;
        }

        if (activeEdgeId == null || activeEdgeId.trim().isEmpty()) {
            /*
             * Backward-compatible fallback for older one-alias tests.
             * Without activeEdgeId, the only safe assumption is the old behavior:
             * sum all edges across workers.
             */
            mergeAllEdges(edgeIndexes);
            return;
        }

        /*
         * Copy already-global/stable edges once.
         * Sum only the currently active edge across workers.
         */
        if (includesStableState && !stableEdgesCopied ) {
            copyStableEdgesOnce(edgeIndexes, activeEdgeId);
            stableEdgesCopied = true;
        }

        JsonNode activeIndex = edgeIndexes.get(activeEdgeId);

        if (activeIndex != null && activeIndex.isObject()) {
            mergeOneEdge(activeEdgeId, activeIndex);
        } else if (!mergedEdgeIndexes.containsKey(activeEdgeId)) {
            mergedEdgeIndexes.put(activeEdgeId, new LinkedHashMap<String, Double>());
        }
    }

    private void copyStableEdgesOnce(JsonNode edgeIndexes, String activeEdgeId) {
        if (edgeIndexes == null || !edgeIndexes.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> edgeFields = edgeIndexes.fields();

        while (edgeFields.hasNext()) {
            Map.Entry<String, JsonNode> edgeEntry = edgeFields.next();

            String edgeId = edgeEntry.getKey();

            if (activeEdgeId != null && activeEdgeId.equals(edgeId)) {
                continue;
            }

            JsonNode joinWeights = edgeEntry.getValue();

            Map<String, Double> target = mergedEdgeIndexes.get(edgeId);

            if (target == null) {
                target = new LinkedHashMap<String, Double>();
                mergedEdgeIndexes.put(edgeId, target);
            }

            if (joinWeights == null || !joinWeights.isObject()) {
                continue;
            }

            Iterator<Map.Entry<String, JsonNode>> joinFields = joinWeights.fields();

            while (joinFields.hasNext()) {
                Map.Entry<String, JsonNode> joinEntry = joinFields.next();

                target.put(joinEntry.getKey(), joinEntry.getValue().asDouble(0.0d));
            }
        }
    }

    private void mergeOneEdge(String edgeId, JsonNode joinWeights) {
        if (edgeId == null || edgeId.trim().isEmpty()) {
            return;
        }

        Map<String, Double> target = mergedEdgeIndexes.get(edgeId);

        if (target == null) {
            target = new LinkedHashMap<String, Double>();
            mergedEdgeIndexes.put(edgeId, target);
        }

        if (joinWeights == null || !joinWeights.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> joinFields = joinWeights.fields();

        while (joinFields.hasNext()) {
            Map.Entry<String, JsonNode> joinEntry = joinFields.next();

            String joinKey = joinEntry.getKey();
            double delta = joinEntry.getValue().asDouble(0.0d);

            Double current = target.get(joinKey);

            if (current == null) {
                current = 0.0d;
            }

            target.put(joinKey, current + delta);
        }
    }

    private void mergeAllEdges(JsonNode edgeIndexes) {
        if (edgeIndexes == null || !edgeIndexes.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> edgeFields = edgeIndexes.fields();

        while (edgeFields.hasNext()) {
            Map.Entry<String, JsonNode> edgeEntry = edgeFields.next();

            mergeOneEdge(edgeEntry.getKey(), edgeEntry.getValue());
        }
    }

    private void mergeSeenTuples(JsonNode seenTuplesByAlias, String activeAlias, boolean includesStableState) {
        if (seenTuplesByAlias == null || !seenTuplesByAlias.isObject()) {
            return;
        }

        if (activeAlias == null || activeAlias.trim().isEmpty()) {
            Iterator<Map.Entry<String, JsonNode>> fields = seenTuplesByAlias.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();

                addSeen(entry.getKey(), entry.getValue().asLong(0L));
            }

            return;
        }

        if (includesStableState && !stableSeenCopied) {
            Iterator<Map.Entry<String, JsonNode>> fields = seenTuplesByAlias.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();

                if (!activeAlias.equals(entry.getKey())) {
                    mergedSeenTuplesByAlias.put(entry.getKey(), entry.getValue().asLong(0L));
                }
            }

            stableSeenCopied = true;
        }

        JsonNode activeSeen = seenTuplesByAlias.get(activeAlias);

        if (activeSeen != null && !activeSeen.isNull()) {
            addSeen(activeAlias, activeSeen.asLong(0L));
        }
    }

    private void addSeen(String alias, long delta) {
        if (alias == null || alias.trim().isEmpty()) {
            return;
        }

        Long current = mergedSeenTuplesByAlias.get(alias);

        if (current == null) {
            current = 0L;
        }

        mergedSeenTuplesByAlias.put(alias, current + delta);
    }

    private Map<String, Map<String, Double>> deepCopyEdgeIndexes() {
        Map<String, Map<String, Double>> copy = new LinkedHashMap<String, Map<String, Double>>();

        for (Map.Entry<String, Map<String, Double>> edgeEntry : mergedEdgeIndexes.entrySet()) {
            copy.put(edgeEntry.getKey(), new LinkedHashMap<String, Double>(edgeEntry.getValue()));
        }

        return copy;
    }

    private Map<String, Map<String, Object>> buildEdgeSummaries() {
        Map<String, Map<String, Object>> summaries = new LinkedHashMap<String, Map<String, Object>>();

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

    private static String resolveActiveAlias(JsonNode payload, Estimation e) {
        String value = textField(payload, "activeAlias", "");

        if (value != null && !value.trim().isEmpty()) {
            return value.trim();
        }

        String[] param = e.getParam();

        if (param != null && param.length > 3 && param[3] != null) {
            value = param[3];

            if (!value.trim().isEmpty()) {
                return value.trim();
            }
        }

        return "";
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

    private static boolean booleanField(JsonNode node, String fieldName, boolean defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asBoolean(defaultValue);
    }
}
