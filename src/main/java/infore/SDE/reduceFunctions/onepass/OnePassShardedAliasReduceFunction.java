package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strict-union reducer for coordinator-free sharded Phase 3.
 * Each sampleInstanceId is owned by exactly one worker for an active alias.
 * Consequently, this reducer never performs probabilistic representative merging.
 * It only validates and unions disjoint worker-local selections.
 */
public final class OnePassShardedAliasReduceFunction extends ReduceFunction implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<Integer> receivedWorkers = new HashSet<Integer>();
    private final Map<Long, JsonNode> selectionBySampleId =
            new LinkedHashMap<Long, JsonNode>();

    private boolean metadataInitialized = false;
    private int uid = -1;
    private String resultId = "";
    private String queryName = "";
    private String rootAlias = "";
    private String alias = "";
    private String baseKey = "";
    private final int expectedWorkers;
    private int sampleSize = -1;
    private int aliasIndex = -1;
    private boolean isLastAlias = false;
    private String nextAlias = "";

    public OnePassShardedAliasReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {

        super(nOfP, count, parameters, synID, rqid);
        this.expectedWorkers = nOfP <= 0 ? 1 : nOfP;
    }

    @Override
    public boolean add(Estimation estimation) {
        try {
            JsonNode payload = asJson(estimation.getEstimation());
            if (payload == null || payload.isNull()) {
                throw new IllegalStateException("LOCAL_PHASE3_ALIAS_SELECTIONS payload is null");
            }

            String type = textField(payload, "type", "");
            if (!"LOCAL_PHASE3_ALIAS_SELECTIONS".equals(type)) {
                throw new IllegalStateException("Unexpected Phase-3 reducer payload type: " + type);
            }

            int workerId = intField(payload, "workerId", -1);
            int incomingExpectedWorkers = intField(payload, "expectedWorkers", estimation.getNoOfP());

            if (incomingExpectedWorkers != expectedWorkers) {
                throw new IllegalStateException("Conflicting Phase-3 expectedWorkers. reducer=" +
                        expectedWorkers + ", payload=" + incomingExpectedWorkers);
            }
            if (workerId < 0 || workerId >= expectedWorkers) {
                throw new IllegalStateException("Invalid Phase-3 workerId=" + workerId +
                        ", expectedWorkers=" + expectedWorkers);
            }

            // Strict protocol: duplicate worker-local results are an error.
            if (!receivedWorkers.add(workerId)) {
                throw new IllegalStateException(
                        "Duplicate LOCAL_PHASE3_ALIAS_SELECTIONS from worker "
                                + workerId + ", resultId="
                                + textField(payload, "resultId", ""));
            }

            acceptMetadata(payload, estimation);

            JsonNode ownedSelections = payload.get("ownedSelections");
            if (ownedSelections == null || !ownedSelections.isArray()) {
                throw new IllegalStateException("LOCAL_PHASE3_ALIAS_SELECTIONS has no ownedSelections array");
            }

            for (JsonNode selection : ownedSelections) {
                long sampleId = longField(selection, "sampleInstanceId", -1L);

                if (sampleId < 0L) {
                    throw new IllegalStateException(
                            "Invalid Phase-3 sampleInstanceId: " + selection);
                }

                int ownerWorkerId = intField(selection, "ownerWorkerId", -1);
                if (ownerWorkerId != workerId) {
                    throw new IllegalStateException(
                            "Phase-3 owner mismatch for sample " + sampleId + ". resultWorker=" + workerId +
                                    ", selectionOwner=" + ownerWorkerId);
                }

                String selectionAlias = textField(selection, "alias", alias);
                if (!alias.equals(selectionAlias)) {
                    throw new IllegalStateException("Phase-3 selection alias mismatch for sample " + sampleId +
                            ". expected=" + alias + ", actual=" + selectionAlias);
                }

                JsonNode tuple = selection.get("tuple");
                if (tuple == null || tuple.isNull()) {
                    throw new IllegalStateException("Phase-3 selection has null tuple for sample " + sampleId);
                }

                if (selectionBySampleId.containsKey(sampleId)) {
                    throw new IllegalStateException("Phase-3 sampleInstanceId appeared on multiple workers: " + sampleId);
                }
                selectionBySampleId.put(sampleId, selection.deepCopy());
            }

            count = receivedWorkers.size();
            return count >= expectedWorkers;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Could not reduce LOCAL_PHASE3_ALIAS_SELECTIONS", e);
        }
    }

    private void acceptMetadata(JsonNode payload, Estimation estimation) {
        int incomingUid = intField(payload, "uid", estimation.getUID());
        String incomingResultId = textField(payload, "resultId", "");
        String incomingQueryName = textField(payload, "queryName", "");
        String incomingRootAlias = textField(payload, "rootAlias", "");
        String incomingAlias = textField(payload, "alias", textField(payload, "phaseThreeAlias", ""));
        String incomingBaseKey = textField(payload, "baseKey", "");
        int incomingSampleSize = intField(payload, "sampleSize", -1);
        int incomingAliasIndex = intField(payload, "aliasIndex", -1);
        boolean incomingIsLastAlias = booleanField(payload, "isLastAlias", false);
        String incomingNextAlias = textField(payload, "nextAlias", "");

        if (incomingResultId.isEmpty() || incomingAlias.isEmpty() || incomingBaseKey.isEmpty()) {
            throw new IllegalStateException(
                    "Incomplete Phase-3 reducer metadata: " + payload);
        }
        if (incomingSampleSize <= 0) {
            throw new IllegalStateException(
                    "Invalid Phase-3 sampleSize=" + incomingSampleSize);
        }
        if (incomingAliasIndex < 0) {
            throw new IllegalStateException(
                    "Invalid Phase-3 aliasIndex=" + incomingAliasIndex);
        }
        if (!incomingIsLastAlias && incomingNextAlias.isEmpty()) {
            throw new IllegalStateException(
                    "Non-final Phase-3 alias is missing nextAlias");
        }
        if (incomingIsLastAlias && !incomingNextAlias.isEmpty()) {
            throw new IllegalStateException(
                    "Final Phase-3 alias must have empty nextAlias");
        }

        if (!metadataInitialized) {
            uid = incomingUid;
            resultId = incomingResultId;
            queryName = incomingQueryName;
            rootAlias = incomingRootAlias;
            alias = incomingAlias;
            baseKey = incomingBaseKey;
            sampleSize = incomingSampleSize;
            aliasIndex = incomingAliasIndex;
            isLastAlias = incomingIsLastAlias;
            nextAlias = incomingNextAlias;
            metadataInitialized = true;
            return;
        }

        requireSame("uid", uid, incomingUid);
        requireSame("resultId", resultId, incomingResultId);
        requireSame("queryName", queryName, incomingQueryName);
        requireSame("rootAlias", rootAlias, incomingRootAlias);
        requireSame("alias", alias, incomingAlias);
        requireSame("baseKey", baseKey, incomingBaseKey);
        requireSame("sampleSize", sampleSize, incomingSampleSize);
        requireSame("aliasIndex", aliasIndex, incomingAliasIndex);

        if (isLastAlias != incomingIsLastAlias) {
            throw new IllegalStateException(
                    "Conflicting isLastAlias. expected=" + isLastAlias + ", actual=" + incomingIsLastAlias);
        }
        requireSame("nextAlias", nextAlias, incomingNextAlias);
    }

    @Override
    public Object reduce() {
        if (!metadataInitialized) {
            throw new IllegalStateException("Phase-3 strict-union reducer has no metadata");
        }
        if (receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException(
                    "Phase-3 strict-union reducer is incomplete. expectedWorkers="
                            + expectedWorkers + ", received=" + receivedWorkers.size());
        }
        if (selectionBySampleId.size() != sampleSize) {
            throw new IllegalStateException("Global Phase-3 selection count mismatch. expected=" +
                    sampleSize + ", actual=" + selectionBySampleId.size());
        }

        List<Long> sampleIds = new ArrayList<Long>(selectionBySampleId.keySet());
        Collections.sort(sampleIds);

        List<Integer> workers = new ArrayList<Integer>(receivedWorkers);
        Collections.sort(workers);

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("type", "GLOBAL_PHASE3_ALIAS_SELECTIONS");
        payload.put("protocol", "SHARDED_PHASE3_V1");
        payload.put("phase", "PHASE3");
        payload.put("uid", uid);
        payload.put("resultId", resultId);
        payload.put("stateRef", uid + "_PHASE3_" + resultId + "_GLOBAL_ALIAS_SELECTIONS");
        payload.put("queryName", queryName);
        payload.put("rootAlias", rootAlias);
        payload.put("alias", alias);
        payload.put("phaseThreeAlias", alias);
        payload.put("baseKey", baseKey);
        payload.put("expectedWorkers", expectedWorkers);
        payload.put("sampleSize", sampleSize);
        payload.put("selectionCount", selectionBySampleId.size());
        payload.put("aliasIndex", aliasIndex);
        payload.put("isLastAlias", isLastAlias);
        payload.put("nextAlias", nextAlias);
        payload.put("localResultCount", receivedWorkers.size());
        payload.set("receivedWorkers", MAPPER.valueToTree(workers));

        ArrayNode selections = payload.putArray("selections");
        for (Long sampleId : sampleIds) {
            selections.add(selectionBySampleId.get(sampleId));
        }

        try {
            return MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialize GLOBAL_PHASE3_ALIAS_SELECTIONS", e);
        }
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

    private static String textField(
            JsonNode node,
            String field,
            String defaultValue) {

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

    private static long longField(JsonNode node, String field, long defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong(defaultValue);
    }

    private static boolean booleanField(JsonNode node, String field, boolean defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asBoolean(defaultValue);
    }

    private static void requireSame(String field, String expected, String actual) {
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Conflicting " + field + ". expected=" + expected + ", actual=" + actual);
        }
    }

    private static void requireSame(String field, int expected, int actual) {

        if (expected != actual) {
            throw new IllegalStateException("Conflicting " + field + ". expected=" + expected
                    + ", actual=" + actual);
        }
    }
}