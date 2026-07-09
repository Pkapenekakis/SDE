package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Phase 3 alias-level merge for distributed OnePass*.
 *
 * Each worker has the same Phase 1 index and the same global Phase 2 root sample.
 * During one Phase 3 alias replay, each worker sees only its partition of the
 * child alias stream. For every sample instance, the worker exports:
 *
 *   - cumulativeWeight of all matching local child candidates,
 *   - candidatesSeen locally,
 *   - one locally selected tuple representing that local partition.
 *
 * This reducer merges those local representatives by choosing one worker's
 * representative with probability proportional to the worker's cumulativeWeight.
 * It uses an exponential key per (sampleInstanceId, workerId) so the merge is
 * order-independent.
 */
public final class OnePassAliasReduceFunction extends ReduceFunction {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<Integer> receivedWorkers = new HashSet<Integer>();
    private final Map<Long, MergedChoice> choicesBySampleId = new LinkedHashMap<Long, MergedChoice>();

    private int uid = -1;
    private String phase = "PHASE3";
    private String resultId = "PHASE3_ALIAS_RESULT";
    private String queryName = "";
    private String rootAlias = "";
    private String phaseThreeAlias = "";
    private String parentEdgeId = "";
    private String baseKey = "";
    private int expectedWorkers;
    private int sampleSize;

    public OnePassAliasReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {
        super(nOfP, count, parameters, synID, rqid);
        this.expectedWorkers = nOfP <= 0 ? 1 : nOfP;
    }

    @Override
    public boolean add(Estimation e) {
        try {
            JsonNode payload = parsePayload(e.getEstimation());

            if (payload == null || payload.isNull()) {
                System.out.println("[OnePassPhase3AliasReduce] Ignoring null payload.");
                return false;
            }

            String type = textField(payload, "type", "");

            if (!"LOCAL_PHASE3_ALIAS_RESULT".equals(type)) {
                System.out.println("[OnePassPhase3AliasReduce] Ignoring unexpected payload type: " + type);
                return false;
            }

            uid = intField(payload, "uid", e.getUID());
            phase = textField(payload, "phase", "PHASE3");
            resultId = textField(payload, "resultId", "PHASE3_ALIAS_RESULT_" + uid);
            queryName = textField(payload, "queryName", "");
            rootAlias = textField(payload, "rootAlias", "");
            phaseThreeAlias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));
            parentEdgeId = textField(payload, "parentEdgeId", "");
            baseKey = chooseNonBlank(baseKey, textField(payload, "baseKey", ""));
            sampleSize = Math.max(sampleSize, intField(payload, "sampleSize", 0));

            int workerId = intField(payload, "workerId", -1);
            int payloadExpectedWorkers = intField(payload, "expectedWorkers", e.getNoOfP());

            if (payloadExpectedWorkers > 0) {
                expectedWorkers = Math.max(expectedWorkers, payloadExpectedWorkers);
            }

            if (workerId < 0) {
                System.out.println("[OnePassPhase3AliasReduce] Ignoring result with invalid workerId: " + payload);
                return false;
            }

            if (!receivedWorkers.add(workerId)) {
                System.out.println("[OnePassPhase3AliasReduce] Duplicate worker ignored: workerId="
                        + workerId + ", resultId=" + resultId);
                return false;
            }

            JsonNode selections = payload.get("selections");

            if (selections != null && selections.isArray()) {
                for (JsonNode selection : selections) {
                    mergeSelection(workerId, selection);
                }
            }

            count = receivedWorkers.size();

            System.out.println("[OnePassPhase3AliasReduce] Received worker "
                    + workerId + " for " + resultId
                    + " alias=" + phaseThreeAlias
                    + " (" + count + "/" + expectedWorkers + ")");

            return count >= expectedWorkers;

        } catch (Exception ex) {
            throw new IllegalStateException("Could not add LOCAL_PHASE3_ALIAS_RESULT to reducer.", ex);
        }
    }

    @Override
    public Object reduce() {
        try {
            Map<String, Object> payload = new LinkedHashMap<String, Object>();

            ArrayList<Integer> workers = new ArrayList<Integer>(receivedWorkers);
            Collections.sort(workers);

            List<Map<String, Object>> selections = new ArrayList<Map<String, Object>>();
            long totalCandidatesSeen = 0L;
            double totalCandidateWeight = 0.0d;

            for (Map.Entry<Long, MergedChoice> entry : choicesBySampleId.entrySet()) {
                long sampleInstanceId = entry.getKey();
                MergedChoice choice = entry.getValue();

                totalCandidatesSeen += choice.candidatesSeen;
                totalCandidateWeight += choice.cumulativeWeight;

                Map<String, Object> out = new LinkedHashMap<String, Object>();
                out.put("sampleInstanceId", sampleInstanceId);
                out.put("phaseThreeAlias", phaseThreeAlias);
                out.put("alias", phaseThreeAlias);
                out.put("parentEdgeId", parentEdgeId);
                out.put("hasSelection", choice.selectedTuple != null);
                out.put("candidatesSeen", choice.candidatesSeen);
                out.put("cumulativeWeight", choice.cumulativeWeight);
                out.put("selectedWorkerId", choice.selectedWorkerId);
                out.put("selectedWeight", choice.selectedWeight);

                if (choice.selectedTuple != null) {
                    out.put("tuple", MAPPER.convertValue(choice.selectedTuple, Object.class));
                }

                selections.add(out);
            }

            payload.put("type", "GLOBAL_PHASE3_ALIAS_RESULT");
            payload.put("uid", uid);
            payload.put("phase", phase);
            payload.put("resultId", resultId);
            payload.put("queryName", queryName);
            payload.put("rootAlias", rootAlias);
            payload.put("phaseThreeAlias", phaseThreeAlias);
            payload.put("alias", phaseThreeAlias);
            payload.put("parentEdgeId", parentEdgeId);
            payload.put("baseKey", baseKey);
            payload.put("stateRef", uid + "_PHASE3_ALIAS_" + resultId + "_GLOBAL_SELECTIONS");
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("receivedWorkers", workers);
            payload.put("localResultCount", receivedWorkers.size());
            payload.put("sampleSize", sampleSize);
            payload.put("selectionCount", selections.size());
            payload.put("totalCandidatesSeen", totalCandidatesSeen);
            payload.put("totalCandidateWeight", totalCandidateWeight);
            payload.put("selections", selections);

            return MAPPER.writeValueAsString(payload);

        } catch (Exception ex) {
            throw new IllegalStateException("Could not serialize GLOBAL_PHASE3_ALIAS_RESULT.", ex);
        }
    }

    private void mergeSelection(int workerId, JsonNode selection) {
        if (selection == null || selection.isNull()) {
            return;
        }

        long sampleInstanceId = longField(selection, "sampleInstanceId", -1L);

        if (sampleInstanceId < 0L) {
            return;
        }

        long candidatesSeen = longField(selection, "candidatesSeen", 0L);
        double cumulativeWeight = doubleField(selection, "cumulativeWeight", 0.0d);
        boolean hasSelection = booleanField(selection, "hasSelection", false);

        MergedChoice merged = choicesBySampleId.get(sampleInstanceId);

        if (merged == null) {
            merged = new MergedChoice();
            choicesBySampleId.put(sampleInstanceId, merged);
        }

        merged.candidatesSeen += candidatesSeen;
        merged.cumulativeWeight += cumulativeWeight;

        if (!hasSelection || cumulativeWeight <= 0.0d) {
            return;
        }

        JsonNode tuple = selection.get("tuple");

        if (tuple == null || tuple.isNull()) {
            return;
        }

        double key = exponentialKey(uid, resultId, phaseThreeAlias, sampleInstanceId, workerId, cumulativeWeight);

        if (merged.selectedTuple == null || key < merged.bestKey) {
            merged.bestKey = key;
            merged.selectedTuple = tuple.deepCopy();
            merged.selectedWorkerId = workerId;
            merged.selectedWeight = doubleField(selection, "selectedWeight", cumulativeWeight);
        }
    }

    private static double exponentialKey(int uid, String resultId, String alias,
                                         long sampleInstanceId, int workerId,
                                         double weight) {
        double u = stableUnitDouble(uid + "|" + resultId + "|" + alias + "|"
                + sampleInstanceId + "|" + workerId);
        return -Math.log(u) / weight;
    }

    private static double stableUnitDouble(String seed) {
        long h = 1125899906842597L;
        String s = seed == null ? "" : seed;

        for (int i = 0; i < s.length(); i++) {
            h = 31L * h + s.charAt(i);
        }

        long positive = h & 0x7fffffffffffffffL;
        double u = (positive + 1.0d) / (Long.MAX_VALUE + 1.0d);

        if (u <= 0.0d) {
            return Double.MIN_VALUE;
        }

        if (u >= 1.0d) {
            return Math.nextDown(1.0d);
        }

        return u;
    }

    private static String chooseNonBlank(String current, String candidate) {
        if (current != null && !current.trim().isEmpty()) {
            return current.trim();
        }
        if (candidate != null && !candidate.trim().isEmpty()) {
            return candidate.trim();
        }
        return "";
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

    private static long longField(JsonNode node, String fieldName, long defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String fieldName, double defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asDouble(defaultValue);
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

    private static final class MergedChoice {
        private double cumulativeWeight = 0.0d;
        private long candidatesSeen = 0L;
        private JsonNode selectedTuple = null;
        private int selectedWorkerId = -1;
        private double selectedWeight = 0.0d;
        private double bestKey = Double.POSITIVE_INFINITY;
    }
}
