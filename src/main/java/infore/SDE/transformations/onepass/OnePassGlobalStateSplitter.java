package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits merged OnePass global state into Kafka-installable chunks.
 *
 * Supported states:
 *
 *   GLOBAL_PHASE1_RESULT
 *      -> GLOBAL_STATE_CHUNK records with stateType=GLOBAL_PHASE1_INDEX
 *
 *   GLOBAL_PHASE2_ROOT_SAMPLE
 *      -> GLOBAL_STATE_CHUNK records with stateType=GLOBAL_PHASE2_ROOT_SAMPLE
 *
 *   GLOBAL_PHASE3_ALIAS_RESULT
 *      -> GLOBAL_STATE_CHUNK records with stateType=GLOBAL_PHASE3_ALIAS_SELECTIONS
 *
 * This class does not install the state into workers.
 * It only prepares the payload path through globalStateTopic.
 */
public final class OnePassGlobalStateSplitter extends RichFlatMapFunction<Estimation, String> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ONEPASS_SYNOPSIS_ID = 30;

    private static final int GLOBAL_PHASE1_RESULT_REQUEST_ID = 73;
    private static final int GLOBAL_PHASE2_ROOT_SAMPLE_REQUEST_ID = 83;
    private static final int GLOBAL_PHASE3_ALIAS_RESULT_REQUEST_ID = 93;

    private static final String TYPE_GLOBAL_PHASE1_RESULT = "GLOBAL_PHASE1_RESULT";
    private static final String TYPE_GLOBAL_PHASE2_ROOT_SAMPLE = "GLOBAL_PHASE2_ROOT_SAMPLE";
    private static final String TYPE_GLOBAL_PHASE3_ALIAS_RESULT = "GLOBAL_PHASE3_ALIAS_RESULT";

    private static final String TYPE_GLOBAL_STATE_CHUNK = "GLOBAL_STATE_CHUNK";
    private static final String STATE_TYPE_GLOBAL_PHASE1_INDEX = "GLOBAL_PHASE1_INDEX";
    private static final String STATE_TYPE_GLOBAL_PHASE2_ROOT_SAMPLE = "GLOBAL_PHASE2_ROOT_SAMPLE";
    private static final String STATE_TYPE_GLOBAL_PHASE3_ALIAS_SELECTIONS = "GLOBAL_PHASE3_ALIAS_SELECTIONS";

    /*
     * For the current test this chunks by number of flattened index entries.
     * Later we can make this byte-based instead of entry-count-based.
     */
    private static final int DEFAULT_MAX_ENTRIES_PER_CHUNK = 500;

    private final int maxEntriesPerChunk;

    public OnePassGlobalStateSplitter() {
        this(DEFAULT_MAX_ENTRIES_PER_CHUNK);
    }

    public OnePassGlobalStateSplitter(int maxEntriesPerChunk) {
        if (maxEntriesPerChunk <= 0) {
            throw new IllegalArgumentException("maxEntriesPerChunk must be > 0");
        }

        this.maxEntriesPerChunk = maxEntriesPerChunk;
    }

    @Override
    public void flatMap(Estimation value, Collector<String> out) throws Exception {
        if (value == null) {
            return;
        }

        if (value.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        int requestId = value.getRequestID();

        if (requestId != GLOBAL_PHASE1_RESULT_REQUEST_ID
                && requestId != GLOBAL_PHASE2_ROOT_SAMPLE_REQUEST_ID
                && requestId != GLOBAL_PHASE3_ALIAS_RESULT_REQUEST_ID) {
            return;
        }

        JsonNode payload = parsePayload(value.getEstimation());

        if (payload == null || payload.isNull()) {
            return;
        }

        String type = textField(payload, "type", "");

        if (TYPE_GLOBAL_PHASE1_RESULT.equals(type)) {
            splitPhaseOneGlobalIndex(payload, value, out);
            return;
        }

        if (TYPE_GLOBAL_PHASE2_ROOT_SAMPLE.equals(type)) {
            splitPhaseTwoRootSample(payload, value, out);
            return;
        }

        if (TYPE_GLOBAL_PHASE3_ALIAS_RESULT.equals(type)) {
            splitPhaseThreeAliasSelections(payload, value, out);
            return;
        }
    }

    private void splitPhaseOneGlobalIndex(
            JsonNode payload,
            Estimation value,
            Collector<String> out) throws Exception {

        int uid = intField(payload, "uid", value.getUID());
        String phase = textField(payload, "phase", "PHASE1");
        String resultId = textField(payload, "resultId", "PHASE1_RESULT_" + uid);
        String queryName = textField(payload, "queryName", "");
        String rootAlias = textField(payload, "rootAlias", "");
        String baseKey = textField(payload, "baseKey", "");

        int expectedWorkers = intField(payload, "expectedWorkers", value.getNoOfP());
        String activeAlias = textField(payload, "activeAlias", "");
        String activeEdgeId = textField(payload, "activeEdgeId", "");

        if (expectedWorkers <= 0) {
            expectedWorkers = value.getNoOfP() > 0 ? value.getNoOfP() : 1;
        }

        if (baseKey == null || baseKey.trim().isEmpty()) {
            /*
             * Fallback only. In the normal path baseKey should come from
             * the merged GLOBAL_PHASE1_RESULT payload.
             */
            baseKey = "onepass-" + uid;
        }

        String stateRef = textField(
                payload,
                "stateRef",
                uid + "_PHASE1_" + resultId + "_GLOBAL_STATE"
        );

        JsonNode globalPhaseOneResult = payload.get("globalPhaseOneResult");

        if (globalPhaseOneResult == null || globalPhaseOneResult.isNull()) {
            System.out.println("[OnePassGlobalStateSplitter] Missing globalPhaseOneResult for uid="
                    + uid + ", resultId=" + resultId);
            return;
        }

        JsonNode seenTuplesByAlias = globalPhaseOneResult.get("seenTuplesByAlias");
        JsonNode edgeSummaries = globalPhaseOneResult.get("edgeSummaries");
        JsonNode edgeIndexes = globalPhaseOneResult.get("edgeIndexes");

        List<Map<String, Object>> entries = flattenPhaseOneEdgeIndexes(edgeIndexes);

        int chunkCount = Math.max(1, (entries.size() + maxEntriesPerChunk - 1) / maxEntriesPerChunk);

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {
            String workerKey = baseKey + "_" + expectedWorkers + "_KEYED_" + workerId;

            for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
                int from = chunkId * maxEntriesPerChunk;
                int to = Math.min(entries.size(), from + maxEntriesPerChunk);

                List<Map<String, Object>> chunkEntries;

                if (from >= entries.size()) {
                    chunkEntries = new ArrayList<Map<String, Object>>();
                } else {
                    chunkEntries = new ArrayList<Map<String, Object>>(entries.subList(from, to));
                }

                Map<String, Object> chunk = new LinkedHashMap<String, Object>();

                chunk.put("type", TYPE_GLOBAL_STATE_CHUNK);
                chunk.put("stateType", STATE_TYPE_GLOBAL_PHASE1_INDEX);
                chunk.put("uid", uid);
                chunk.put("synopsisID", ONEPASS_SYNOPSIS_ID);
                chunk.put("phase", phase);
                chunk.put("resultId", resultId);
                chunk.put("stateRef", stateRef);
                chunk.put("queryName", queryName);
                chunk.put("rootAlias", rootAlias);
                chunk.put("baseKey", baseKey);

                chunk.put("expectedWorkers", expectedWorkers);
                chunk.put("workerId", workerId);
                chunk.put("workerKey", workerKey);

                chunk.put("chunkId", chunkId);
                chunk.put("chunkCount", chunkCount);
                chunk.put("entryCount", chunkEntries.size());
                chunk.put("activeAlias", activeAlias);
                chunk.put("activeEdgeId", activeEdgeId);

                /*
                 * Repeated small metadata.
                 * This keeps each chunk self-describing.
                 */
                if (seenTuplesByAlias != null && !seenTuplesByAlias.isNull()) {
                    chunk.put("seenTuplesByAlias", MAPPER.convertValue(seenTuplesByAlias, Object.class));
                }

                if (edgeSummaries != null && !edgeSummaries.isNull()) {
                    chunk.put("edgeSummaries", MAPPER.convertValue(edgeSummaries, Object.class));
                }

                chunk.put("entries", chunkEntries);

                out.collect(MAPPER.writeValueAsString(chunk));
            }
        }

        System.out.println("[OnePassGlobalStateSplitter] Emitted Phase1 global state chunks: "
                + "uid=" + uid
                + ", resultId=" + resultId
                + ", stateRef=" + stateRef
                + ", entries=" + entries.size()
                + ", chunkCountPerWorker=" + chunkCount
                + ", expectedWorkers=" + expectedWorkers);
    }

    private void splitPhaseTwoRootSample(
            JsonNode payload,
            Estimation value,
            Collector<String> out) throws Exception {

        int uid = intField(payload, "uid", value.getUID());
        int expectedWorkers = intField(payload, "expectedWorkers", value.getNoOfP());

        if (expectedWorkers <= 0) {
            expectedWorkers = value.getNoOfP() > 0 ? value.getNoOfP() : 1;
        }

        String resultId = textField(payload, "resultId", "PHASE2_RESULT_" + uid);
        String stateRef = textField(payload, "stateRef", uid + "_PHASE2_" + resultId + "_GLOBAL_ROOT_SAMPLE");
        String baseKey = textField(payload, "baseKey", "");
        String queryName = textField(payload, "queryName", "");
        String rootAlias = textField(payload, "rootAlias", "");
        String datasetSeed = textField(payload, "datasetSeed", "");

        if (baseKey == null || baseKey.trim().isEmpty()) {
            baseKey = "onepass-" + uid;
        }

        JsonNode sampleInstances = payload.get("sampleInstances");

        if (sampleInstances == null || !sampleInstances.isArray()) {
            sampleInstances = MAPPER.createArrayNode();
        }

        /*
         * Large-scale mode:
         *
         * Do NOT put all sampleInstances into one Kafka message.
         * With LIMIT 10000, a single root-sample chunk can easily exceed the
         * Kafka producer max.request.size. Split Phase 2 exactly like Phase 3:
         * entries == a slice of sampleInstances.
         *
         * Also do NOT duplicate globalReservoir in every install chunk. Workers
         * only need the final explicit sampleInstances to initialize Phase 3.
         */
        int sampleInstanceCount = sampleInstances.size();
        int chunkCount = Math.max(1, (sampleInstanceCount + maxEntriesPerChunk - 1) / maxEntriesPerChunk);

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {
            String workerKey = baseKey + "_" + expectedWorkers + "_KEYED_" + workerId;

            for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
                int from = chunkId * maxEntriesPerChunk;
                int to = Math.min(sampleInstanceCount, from + maxEntriesPerChunk);

                ObjectNode chunk = MAPPER.createObjectNode();

                chunk.put("type", TYPE_GLOBAL_STATE_CHUNK);
                chunk.put("stateType", STATE_TYPE_GLOBAL_PHASE2_ROOT_SAMPLE);
                chunk.put("uid", uid);
                chunk.put("synopsisID", ONEPASS_SYNOPSIS_ID);
                chunk.put("phase", "PHASE2");
                chunk.put("resultId", resultId);
                chunk.put("stateRef", stateRef);
                chunk.put("queryName", queryName);
                chunk.put("rootAlias", rootAlias);
                chunk.put("baseKey", baseKey);
                chunk.put("datasetSeed", datasetSeed);
                chunk.put("expectedWorkers", expectedWorkers);
                chunk.put("workerId", workerId);
                chunk.put("workerKey", workerKey);
                chunk.put("chunkId", chunkId);
                chunk.put("chunkCount", chunkCount);
                chunk.put("sampleSize", intField(payload, "sampleSize", 0));
                chunk.put("rootTuplesSeen", longField(payload, "rootTuplesSeen", 0L));
                chunk.put("positiveRootCandidatesSeen", longField(payload, "positiveRootCandidatesSeen", 0L));
                chunk.put("totalRootGroupWeight", doubleField(payload, "totalRootGroupWeight", 0.0d));
                chunk.put("sampleInstanceCount", sampleInstanceCount);

                /*
                 * Reuse the generic chunk assembly field name.
                 * For Phase 2, entries == root sample instances.
                 */
                chunk.set("entries", sliceArray(sampleInstances, from, to));
                chunk.put("entryCount", chunk.get("entries").size());

                out.collect(MAPPER.writeValueAsString(chunk));
            }
        }

        System.out.println("[OnePassGlobalStateSplitter] Emitted Phase2 root sample chunks: "
                + "uid=" + uid
                + ", resultId=" + resultId
                + ", stateRef=" + stateRef
                + ", sampleInstances=" + sampleInstanceCount
                + ", chunkCountPerWorker=" + chunkCount
                + ", expectedWorkers=" + expectedWorkers);
    }

    private void splitPhaseThreeAliasSelections(
            JsonNode payload,
            Estimation value,
            Collector<String> out) throws Exception {

        int uid = intField(payload, "uid", value.getUID());
        int expectedWorkers = intField(payload, "expectedWorkers", value.getNoOfP());

        if (expectedWorkers <= 0) {
            expectedWorkers = value.getNoOfP() > 0 ? value.getNoOfP() : 1;
        }

        String resultId = textField(payload, "resultId", "PHASE3_ALIAS_RESULT_" + uid);
        String queryName = textField(payload, "queryName", "");
        String rootAlias = textField(payload, "rootAlias", "");
        String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));
        String parentEdgeId = textField(payload, "parentEdgeId", "");
        String baseKey = textField(payload, "baseKey", "");
        String stateRef = textField(
                payload,
                "stateRef",
                uid + "_PHASE3_ALIAS_" + resultId + "_GLOBAL_SELECTIONS"
        );

        if (baseKey == null || baseKey.trim().isEmpty()) {
            baseKey = "onepass-" + uid;
        }

        if (alias == null || alias.trim().isEmpty()) {
            System.out.println("[OnePassGlobalStateSplitter] Missing phaseThreeAlias for GLOBAL_PHASE3_ALIAS_RESULT: "
                    + payload);
            return;
        }

        JsonNode selections = payload.get("selections");

        if (selections == null || !selections.isArray()) {
            selections = MAPPER.createArrayNode();
        }

        int selectionCount = selections.size();
        int chunkCount = Math.max(1, (selectionCount + maxEntriesPerChunk - 1) / maxEntriesPerChunk);

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {
            String workerKey = baseKey + "_" + expectedWorkers + "_KEYED_" + workerId;

            for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
                int from = chunkId * maxEntriesPerChunk;
                int to = Math.min(selectionCount, from + maxEntriesPerChunk);

                ObjectNode chunk = MAPPER.createObjectNode();

                chunk.put("type", TYPE_GLOBAL_STATE_CHUNK);
                chunk.put("stateType", STATE_TYPE_GLOBAL_PHASE3_ALIAS_SELECTIONS);
                chunk.put("uid", uid);
                chunk.put("synopsisID", ONEPASS_SYNOPSIS_ID);
                chunk.put("phase", "PHASE3");
                chunk.put("resultId", resultId);
                chunk.put("stateRef", stateRef);
                chunk.put("queryName", queryName);
                chunk.put("rootAlias", rootAlias);
                chunk.put("phaseThreeAlias", alias);
                chunk.put("alias", alias);
                chunk.put("parentEdgeId", parentEdgeId);
                chunk.put("baseKey", baseKey);
                chunk.put("expectedWorkers", expectedWorkers);
                chunk.put("workerId", workerId);
                chunk.put("workerKey", workerKey);
                chunk.put("chunkId", chunkId);
                chunk.put("chunkCount", chunkCount);
                chunk.put("sampleSize", intField(payload, "sampleSize", 0));
                chunk.put("selectionCount", selectionCount);
                chunk.put("totalCandidatesSeen", longField(payload, "totalCandidatesSeen", 0L));
                chunk.put("totalCandidateWeight", doubleField(payload, "totalCandidateWeight", 0.0d));

                /*
                 * Reuse the generic chunk assembly field name.
                 * For Phase 3, entries == globally merged alias selections.
                 */
                chunk.set("entries", sliceArray(selections, from, to));
                chunk.put("entryCount", chunk.get("entries").size());

                out.collect(MAPPER.writeValueAsString(chunk));
            }
        }

        System.out.println("[OnePassGlobalStateSplitter] Emitted Phase3 alias selection chunks: "
                + "uid=" + uid
                + ", resultId=" + resultId
                + ", stateRef=" + stateRef
                + ", alias=" + alias
                + ", selections=" + selectionCount
                + ", chunkCountPerWorker=" + chunkCount
                + ", expectedWorkers=" + expectedWorkers);
    }

    private static com.fasterxml.jackson.databind.node.ArrayNode sliceArray(JsonNode array, int from, int to) {
        com.fasterxml.jackson.databind.node.ArrayNode sliced = MAPPER.createArrayNode();

        if (array == null || !array.isArray()) {
            return sliced;
        }

        int safeFrom = Math.max(0, from);
        int safeTo = Math.min(array.size(), Math.max(safeFrom, to));

        for (int i = safeFrom; i < safeTo; i++) {
            JsonNode item = array.get(i);

            if (item != null && !item.isNull()) {
                sliced.add(item.deepCopy());
            }
        }

        return sliced;
    }

    private static List<Map<String, Object>> flattenPhaseOneEdgeIndexes(JsonNode edgeIndexes) {
        List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();

        if (edgeIndexes == null || !edgeIndexes.isObject()) {
            return entries;
        }

        Iterator<Map.Entry<String, JsonNode>> edgeFields = edgeIndexes.fields();

        while (edgeFields.hasNext()) {
            Map.Entry<String, JsonNode> edgeEntry = edgeFields.next();

            String edgeId = edgeEntry.getKey();
            JsonNode joinWeights = edgeEntry.getValue();

            if (joinWeights == null || !joinWeights.isObject()) {
                continue;
            }

            Iterator<Map.Entry<String, JsonNode>> joinFields = joinWeights.fields();

            while (joinFields.hasNext()) {
                Map.Entry<String, JsonNode> joinEntry = joinFields.next();

                Map<String, Object> entry = new LinkedHashMap<String, Object>();

                entry.put("edgeId", edgeId);
                entry.put("joinKey", joinEntry.getKey());
                entry.put("globalWeight", joinEntry.getValue().asDouble(0.0d));

                entries.add(entry);
            }
        }

        return entries;
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
}
