package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tiny federated readiness reducer for OnePass.
 * It carries metadata only, never Phase-1 indexes or bounded sample tuples.
 */
public final class OnePassWorkerReadyReduceFunction extends ReduceFunction implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String TYPE_PHASE1_LOCAL_READY = "LOCAL_PHASE1_SHARD_READY";
    private static final String TYPE_PHASE2_LOCAL_INSTALLED = "LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED";
    private static final String TYPE_PHASE3_LOCAL_INSTALLED = "LOCAL_PHASE3_ALIAS_SELECTIONS_INSTALLED";

    private final Set<Integer> workers = new LinkedHashSet<Integer>();

    private String mode = "";
    private int uid = -1;

    // Phase 1 metadata.
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

    // Phase 2 installed metadata.
    private String stateRef = "";
    private String rootAlias = "";
    private int sampleSize = -1;
    private int sampleInstanceCount = -1;
    private long rootTuplesSeen = -1L;
    private long positiveRootCandidatesSeen = -1L;
    private double totalRootGroupWeight = Double.NaN;
    private String firstPhaseThreeAlias = "";
    private int phaseThreeAliasCount = -1;

    // Phase 3 installed metadata.
    private int selectionCount = -1;
    private int aliasIndex = -1;
    private boolean isLastAlias = false;

    public OnePassWorkerReadyReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {
        super(nOfP, count, parameters, synID, rqid);
    }

    @Override
    public boolean add(Estimation estimation) {
        try {
            JsonNode node = asJson(estimation.getEstimation());
            String type = text(node, "type");

            if (!TYPE_PHASE1_LOCAL_READY.equals(type) && !TYPE_PHASE2_LOCAL_INSTALLED.equals(type) &&
                    !TYPE_PHASE3_LOCAL_INSTALLED.equals(type)) {
                throw new IllegalStateException("Unsupported OnePass worker-ready type: " + type);
            }

            if (mode.isEmpty()) {
                mode = type;
            } else if (!mode.equals(type)) {
                throw new IllegalStateException("Mixed OnePass readiness types in the same reduction. " +
                        "expected=" + mode + ", received=" + type);
            }

            int workerId = intField(node, "workerId", -1);
            if (workerId < 0 || workerId >= nOfP) {
                throw new IllegalStateException("Invalid workerId in OnePass readiness: " + node);
            }

            int payloadExpectedWorkers = intField(node, "expectedWorkers", nOfP);
            if (payloadExpectedWorkers != nOfP) {
                throw new IllegalStateException("Readiness expectedWorkers mismatch. reducer=" + nOfP +
                        ", payload=" + payloadExpectedWorkers);
            }

            // Readiness messages are idempotent on duplicate delivery.
            if (!workers.add(workerId)) {
                return workers.size() >= nOfP;
            }

            if (TYPE_PHASE1_LOCAL_READY.equals(mode)) {
                acceptPhaseOne(node);
            } else if (TYPE_PHASE2_LOCAL_INSTALLED.equals(mode)) {
                acceptPhaseTwoInstall(node);
            } else {
                acceptPhaseThreeInstall(node);
            }

            count = workers.size();
            return workers.size() >= nOfP;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Could not reduce OnePass worker readiness", e);
        }
    }

    private void acceptPhaseOne(JsonNode node) {
        if (uid < 0) {
            uid = intField(node, "uid", -1);
            epoch = intField(node, "epoch", -1);
            alias = text(node, "alias");
            resultId = text(node, "resultId");
            nextCommand = text(node, "nextCommand");
            nextAlias = text(node, "nextAlias");
            baseKey = text(node, "baseKey");
            activeEdgeId = text(node, "activeEdgeId");
        } else {
            requireSameInt(node, "uid", uid);
            requireSameInt(node, "epoch", epoch);
            requireSameText(node, "alias", alias);
            requireSameText(node, "resultId", resultId);
            requireSameText(node, "nextCommand", nextCommand);
            requireSameText(node, "nextAlias", nextAlias);
            requireSameText(node, "baseKey", baseKey);
            requireSameText(node, "activeEdgeId", activeEdgeId);
        }

        globalSeenTuples += longField(node, "localSeenTuples", 0L);
        globalKeyCount += longField(node, "localKeyCount", 0L);
        globalTotalWeight += doubleField(node, "localTotalWeight", 0.0d);
    }

    private void acceptPhaseTwoInstall(JsonNode node) {
        if (uid < 0) {
            uid = intField(node, "uid", -1);
            resultId = text(node, "resultId");
            stateRef = text(node, "stateRef");
            rootAlias = text(node, "rootAlias");
            baseKey = text(node, "baseKey");
            sampleSize = intField(node, "sampleSize", -1);
            sampleInstanceCount = intField(node, "sampleInstanceCount", -1);
            rootTuplesSeen = longField(node, "rootTuplesSeen", -1L);
            positiveRootCandidatesSeen = longField(node, "positiveRootCandidatesSeen", -1L);
            totalRootGroupWeight = doubleField(node, "totalRootGroupWeight", Double.NaN);
            firstPhaseThreeAlias = text(node, "firstPhaseThreeAlias");
            phaseThreeAliasCount = intField(node, "phaseThreeAliasCount", -1);

            if (stateRef.isEmpty()) {
                throw new IllegalStateException("LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED has no stateRef");
            }
            if (sampleSize <= 0 || sampleInstanceCount < 0) {
                throw new IllegalStateException("Invalid installed Phase-2 sample metadata: " + node);
            }
            if (phaseThreeAliasCount < 0) {
                throw new IllegalStateException("Invalid phaseThreeAliasCount=" + phaseThreeAliasCount);
            }
            if (phaseThreeAliasCount > 0 && firstPhaseThreeAlias.isEmpty()) {
                throw new IllegalStateException("Phase-2 install is missing firstPhaseThreeAlias");
            }
        } else {
            requireSameInt(node, "uid", uid);
            requireSameText(node, "resultId", resultId);
            requireSameText(node, "stateRef", stateRef);
            requireSameText(node, "rootAlias", rootAlias);
            requireSameText(node, "baseKey", baseKey);
            requireSameInt(node, "sampleSize", sampleSize);
            requireSameInt(node, "sampleInstanceCount", sampleInstanceCount);
            requireSameLong(node, "rootTuplesSeen", rootTuplesSeen);
            requireSameLong(node, "positiveRootCandidatesSeen", positiveRootCandidatesSeen);
            requireSameDoubleBits(node, "totalRootGroupWeight", totalRootGroupWeight);
            requireSameText(node, "firstPhaseThreeAlias", firstPhaseThreeAlias);
            requireSameInt(node, "phaseThreeAliasCount", phaseThreeAliasCount);
        }
    }

    private void acceptPhaseThreeInstall(JsonNode node) {
        if (uid < 0) {
            uid = intField(node, "uid", -1);
            resultId = text(node, "resultId");
            stateRef = text(node, "stateRef");
            alias = text(node, "alias");
            if (alias.isEmpty()) {
                alias = text(node, "phaseThreeAlias");
            }
            baseKey = text(node, "baseKey");
            sampleSize = intField(node, "sampleSize", -1);
            selectionCount = intField(node, "selectionCount", -1);
            aliasIndex = intField(node, "aliasIndex", -1);
            isLastAlias = booleanField(node, "isLastAlias", false);
            nextAlias = text(node, "nextAlias");
            phaseThreeAliasCount = intField(node, "phaseThreeAliasCount", -1);

            if (stateRef.isEmpty() || resultId.isEmpty() || alias.isEmpty() || baseKey.isEmpty()) {
                throw new IllegalStateException("Incomplete LOCAL_PHASE3_ALIAS_SELECTIONS_INSTALLED metadata: " + node);
            }
            if (sampleSize <= 0 || selectionCount != sampleSize) {
                throw new IllegalStateException("Invalid installed Phase-3 selection count. sampleSize=" +
                        sampleSize + ", selectionCount=" + selectionCount);
            }
            if (aliasIndex < 0) {
                throw new IllegalStateException("Invalid Phase-3 aliasIndex=" + aliasIndex);
            }
            if (!isLastAlias && nextAlias.isEmpty()) {
                throw new IllegalStateException("Non-final Phase-3 install is missing nextAlias");
            }
            if (isLastAlias && !nextAlias.isEmpty()) {
                throw new IllegalStateException("Final Phase-3 install must have empty nextAlias");
            }
        } else {
            requireSameInt(node, "uid", uid);
            requireSameText(node, "resultId", resultId);
            requireSameText(node, "stateRef", stateRef);
            requireSameText(node, "baseKey", baseKey);
            requireSameInt(node, "sampleSize", sampleSize);
            requireSameInt(node, "selectionCount", selectionCount);
            requireSameInt(node, "aliasIndex", aliasIndex);
            requireSameBoolean(node, "isLastAlias", isLastAlias);
            requireSameText(node, "nextAlias", nextAlias);
            requireSameInt(node, "phaseThreeAliasCount", phaseThreeAliasCount);
            String incomingAlias = text(node, "alias");
            if (incomingAlias.isEmpty()) {
                incomingAlias = text(node, "phaseThreeAlias");
            }
            if (!alias.equals(incomingAlias)) {
                throw new IllegalStateException("Conflicting alias: expected=" + alias + ", actual=" + incomingAlias);
            }
        }
    }

    @Override
    public Object reduce() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();

        if (TYPE_PHASE1_LOCAL_READY.equals(mode)) {
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
        } else if (TYPE_PHASE2_LOCAL_INSTALLED.equals(mode)) {
            out.put("type", "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED");
            out.put("protocol", "SHARDED_PHASE2_V1");
            out.put("phase", "PHASE2");
            out.put("uid", uid);
            out.put("resultId", resultId);
            out.put("stateRef", stateRef);
            out.put("rootAlias", rootAlias);
            out.put("baseKey", baseKey);
            out.put("sampleSize", sampleSize);
            out.put("sampleInstanceCount", sampleInstanceCount);
            out.put("rootTuplesSeen", rootTuplesSeen);
            out.put("positiveRootCandidatesSeen", positiveRootCandidatesSeen);
            out.put("totalRootGroupWeight", totalRootGroupWeight);
            out.put("firstPhaseThreeAlias", firstPhaseThreeAlias);
            out.put("phaseThreeAliasCount", phaseThreeAliasCount);
            out.put("expectedWorkers", nOfP);
            out.put("installedWorkerCount", workers.size());
            out.put("receivedWorkers", new ArrayList<Integer>(workers));

        } else if (TYPE_PHASE3_LOCAL_INSTALLED.equals(mode)) {
            out.put("type", "GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED");
            out.put("protocol", "SHARDED_PHASE3_V1");
            out.put("phase", "PHASE3");
            out.put("uid", uid);
            out.put("resultId", resultId);
            out.put("stateRef", stateRef);
            out.put("alias", alias);
            out.put("phaseThreeAlias", alias);
            out.put("baseKey", baseKey);
            out.put("sampleSize", sampleSize);
            out.put("selectionCount", selectionCount);
            out.put("aliasIndex", aliasIndex);
            out.put("isLastAlias", isLastAlias);
            out.put("nextAlias", nextAlias);
            out.put("phaseThreeAliasCount", phaseThreeAliasCount);
            out.put("phaseThreeComplete", isLastAlias);
            out.put("expectedWorkers", nOfP);
            out.put("installedWorkerCount", workers.size());
            out.put("receivedWorkers", new ArrayList<Integer>(workers));
        } else {
            throw new IllegalStateException(
                    "OnePass readiness reducer has no mode");
        }

        try {
            return MAPPER.writeValueAsString(out);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Could not serialize OnePass global readiness", e);
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

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? "" : value.asText("");
    }

    private static int intField(JsonNode node, String field, int defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asInt(defaultValue);
    }

    private static long longField(JsonNode node, String field, long defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String field, double defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asDouble(defaultValue);
    }

    private static boolean booleanField(
            JsonNode node,
            String field,
            boolean defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull()
                ? defaultValue
                : value.asBoolean(defaultValue);
    }

    private static void requireSameText(JsonNode node, String field, String expected) {
        String actual = text(node, field);
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Conflicting " + field + ": expected=" + expected + ", actual=" + actual);
        }
    }

    private static void requireSameInt(JsonNode node, String field, int expected) {
        int actual = intField(node, field, Integer.MIN_VALUE);
        if (actual != expected) {
            throw new IllegalStateException("Conflicting " + field + ": expected=" + expected + ", actual=" + actual);
        }
    }

    private static void requireSameLong(JsonNode node, String field, long expected) {
        long actual = longField(node, field, Long.MIN_VALUE);
        if (actual != expected) {
            throw new IllegalStateException("Conflicting " + field + ": expected=" + expected + ", actual=" + actual);
        }
    }

    private static void requireSameDoubleBits(JsonNode node, String field, double expected) {
        double actual = doubleField(node, field, Double.NaN);
        if (Double.doubleToLongBits(actual) != Double.doubleToLongBits(expected)) {
            throw new IllegalStateException("Conflicting " + field + ": expected=" + expected + ", actual=" + actual);
        }
    }

    private static void requireSameBoolean(JsonNode node, String field, boolean expected) {
        boolean actual = booleanField(node, field, !expected);
        if (actual != expected) {
            throw new IllegalStateException("Conflicting " + field + ": expected=" + expected + ", actual=" + actual);
        }
    }
}