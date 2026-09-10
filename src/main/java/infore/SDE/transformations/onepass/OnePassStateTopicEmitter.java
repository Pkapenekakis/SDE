package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Single OnePass egress component for the State Topic.
 * request 78: already-targeted Phase-1/2/3 sharded work.
 * request 83: bounded global Phase-2 root sample, one Kafka copy/chunk.
 * request 88: bounded global Phase-3 alias selections, one Kafka copy/chunk.
 * Replication of requests 83/88 happens only after Kafka in
 * OnePassStateTopicParser.
 */
public final class OnePassStateTopicEmitter
        extends RichFlatMapFunction<Estimation, String> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ONEPASS_SYNOPSIS_ID = 30;
    private static final int REQUEST_STATE_TRANSFER = 78;
    private static final int REQUEST_GLOBAL_PHASE2_ROOT_SAMPLE = 83;
    private static final int REQUEST_GLOBAL_PHASE3_ALIAS_SELECTIONS = 88;

    private static final String TYPE_GLOBAL_PHASE2_ROOT_SAMPLE = "GLOBAL_PHASE2_ROOT_SAMPLE";
    private static final String TYPE_GLOBAL_PHASE3_ALIAS_SELECTIONS = "GLOBAL_PHASE3_ALIAS_SELECTIONS";
    private static final String TYPE_GLOBAL_STATE_CHUNK = "GLOBAL_STATE_CHUNK";
    private static final String STATE_TYPE_GLOBAL_PHASE2_ROOT_SAMPLE = "GLOBAL_PHASE2_ROOT_SAMPLE";
    private static final String STATE_TYPE_GLOBAL_PHASE3_ALIAS_SELECTIONS = "GLOBAL_PHASE3_ALIAS_SELECTIONS";

    private static final int DEFAULT_MAX_ENTRIES_PER_CHUNK = 256;
    private static final int DEFAULT_MAX_APPROX_BYTES_PER_CHUNK = 256 * 1024;

    private final int maxEntriesPerChunk;
    private final int maxApproxBytesPerChunk;

    public OnePassStateTopicEmitter() {
        this(DEFAULT_MAX_ENTRIES_PER_CHUNK, DEFAULT_MAX_APPROX_BYTES_PER_CHUNK);
    }

    public OnePassStateTopicEmitter(int maxEntriesPerChunk, int maxApproxBytesPerChunk) {

        if (maxEntriesPerChunk <= 0) {
            throw new IllegalArgumentException("maxEntriesPerChunk must be > 0");
        }
        if (maxApproxBytesPerChunk <= 0) {
            throw new IllegalArgumentException("maxApproxBytesPerChunk must be > 0");
        }

        this.maxEntriesPerChunk = maxEntriesPerChunk;
        this.maxApproxBytesPerChunk = maxApproxBytesPerChunk;
    }

    @Override
    public void flatMap(Estimation value, Collector<String> out) throws Exception {

        if (value == null || value.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        if (value.getRequestID() == REQUEST_STATE_TRANSFER) {
            JsonNode payload = parsePayload(value.getEstimation());
            if (payload != null && !payload.isNull()) {
                out.collect(MAPPER.writeValueAsString(payload));
            }
            return;
        }

        JsonNode payload = parsePayload(value.getEstimation());
        if (payload == null || payload.isNull()) {
            return;
        }

        if (value.getRequestID() == REQUEST_GLOBAL_PHASE2_ROOT_SAMPLE &&
                TYPE_GLOBAL_PHASE2_ROOT_SAMPLE.equals(textField(payload, "type", ""))) {
            emitPhaseTwoRootSample(value, payload, out);
            return;
        }

        if (value.getRequestID() == REQUEST_GLOBAL_PHASE3_ALIAS_SELECTIONS &&
                TYPE_GLOBAL_PHASE3_ALIAS_SELECTIONS.equals(textField(payload, "type", ""))) {
            emitPhaseThreeAliasSelections(value, payload, out);
        }
    }

    private void emitPhaseTwoRootSample(Estimation value, JsonNode payload, Collector<String> out) throws Exception {

        int uid = intField(payload, "uid", value.getUID());
        int expectedWorkers = intField(payload, "expectedWorkers", value.getNoOfP());
        if (expectedWorkers <= 0) {
            expectedWorkers = value.getNoOfP() > 0 ? value.getNoOfP() : 1;
        }

        String resultId = textField(payload, "resultId", "PHASE2_RESULT_" + uid);
        String stateRef = textField(payload, "stateRef", uid + "_PHASE2_" + resultId + "_GLOBAL_ROOT_SAMPLE");
        String baseKey = textField(payload, "baseKey", "");
        if (baseKey.isEmpty()) {
            baseKey = "onepass-" + uid;
        }

        JsonNode sampleInstances = payload.get("sampleInstances");
        if (sampleInstances == null || !sampleInstances.isArray()) {
            throw new IllegalStateException("GLOBAL_PHASE2_ROOT_SAMPLE has no sampleInstances array. uid=" + uid +
                    ", resultId=" + resultId);
        }

        List<ChunkRange> ranges = buildChunkRanges(sampleInstances);
        int chunkCount = ranges.size();

        for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
            ChunkRange range = ranges.get(chunkId);
            ObjectNode chunk = MAPPER.createObjectNode();
            chunk.put("type", TYPE_GLOBAL_STATE_CHUNK);
            chunk.put("stateType", STATE_TYPE_GLOBAL_PHASE2_ROOT_SAMPLE);
            chunk.put("protocol", "SHARDED_PHASE2_V1");
            chunk.put("broadcastToWorkers", true);
            chunk.put("uid", uid);
            chunk.put("synopsisID", ONEPASS_SYNOPSIS_ID);
            chunk.put("phase", "PHASE2");
            chunk.put("resultId", resultId);
            chunk.put("stateRef", stateRef);
            chunk.put("queryName", textField(payload, "queryName", ""));
            chunk.put("rootAlias", textField(payload, "rootAlias", ""));
            chunk.put("baseKey", baseKey);
            chunk.put("datasetSeed", textField(payload, "datasetSeed", ""));
            chunk.put("expectedWorkers", expectedWorkers);
            chunk.put("chunkId", chunkId);
            chunk.put("chunkCount", chunkCount);
            chunk.put("sampleSize", intField(payload, "sampleSize", 0));
            chunk.put("sampleInstanceCount", sampleInstances.size());
            chunk.put("rootTuplesSeen", longField(payload, "rootTuplesSeen", 0L));
            chunk.put("positiveRootCandidatesSeen", longField(payload, "positiveRootCandidatesSeen", 0L));
            chunk.put("totalRootGroupWeight", doubleField(payload, "totalRootGroupWeight", 0.0d));

            ArrayNode entries = sliceArray(sampleInstances, range.from, range.to);
            chunk.set("entries", entries);
            chunk.put("entryCount", entries.size());

            // Intentionally no workerKey before Kafka.
            out.collect(MAPPER.writeValueAsString(chunk));
        }

        System.out.println("[OnePassStateTopicEmitter] GLOBAL_PHASE2_ROOT_SAMPLE emitted. "
                        + "uid=" + uid
                        + ", resultId=" + resultId
                        + ", stateRef=" + stateRef
                        + ", sampleInstances=" + sampleInstances.size()
                        + ", kafkaChunks=" + chunkCount
                        + ", logicalWorkers=" + expectedWorkers);
    }

    private void emitPhaseThreeAliasSelections(Estimation value, JsonNode payload, Collector<String> out) throws Exception {

        int uid = intField(payload, "uid", value.getUID());
        int expectedWorkers = intField(payload, "expectedWorkers", value.getNoOfP());
        if (expectedWorkers <= 0) {
            expectedWorkers = value.getNoOfP() > 0 ? value.getNoOfP() : 1;
        }

        String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));
        if (alias.isEmpty()) {
            throw new IllegalStateException("GLOBAL_PHASE3_ALIAS_SELECTIONS has no alias");
        }

        String resultId = textField(payload, "resultId", "PHASE3_" + alias + "_" + uid);
        String stateRef = textField(payload, "stateRef", uid + "_PHASE3_" + resultId + "_GLOBAL_ALIAS_SELECTIONS");
        String baseKey = textField(payload, "baseKey", "");
        if (baseKey.isEmpty()) {
            throw new IllegalStateException("GLOBAL_PHASE3_ALIAS_SELECTIONS has no baseKey");
        }

        JsonNode selections = payload.get("selections");
        if (selections == null || !selections.isArray()) {
            // Defensive compatibility with a producer that names the array entries.
            selections = payload.get("entries");
        }
        if (selections == null || !selections.isArray()) {
            throw new IllegalStateException("GLOBAL_PHASE3_ALIAS_SELECTIONS has no selections array");
        }

        int sampleSize = intField(payload, "sampleSize", -1);
        int selectionCount = intField(payload, "selectionCount", selections.size());
        if (sampleSize <= 0 || selectionCount != sampleSize || selections.size() != selectionCount) {
            throw new IllegalStateException("Invalid global Phase-3 selection metadata. sampleSize="
                            + sampleSize + ", selectionCount=" + selectionCount
                            + ", entries=" + selections.size());
        }

        List<ChunkRange> ranges = buildChunkRanges(selections);
        int chunkCount = ranges.size();

        for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
            ChunkRange range = ranges.get(chunkId);
            ObjectNode chunk = MAPPER.createObjectNode();
            chunk.put("type", TYPE_GLOBAL_STATE_CHUNK);
            chunk.put("stateType", STATE_TYPE_GLOBAL_PHASE3_ALIAS_SELECTIONS);
            chunk.put("protocol", "SHARDED_PHASE3_V1");
            chunk.put("phase", "PHASE3");
            chunk.put("broadcastToWorkers", true);
            chunk.put("uid", uid);
            chunk.put("synopsisID", ONEPASS_SYNOPSIS_ID);
            chunk.put("resultId", resultId);
            chunk.put("stateRef", stateRef);
            chunk.put("queryName", textField(payload, "queryName", ""));
            chunk.put("rootAlias", textField(payload, "rootAlias", ""));
            chunk.put("baseKey", baseKey);
            chunk.put("alias", alias);
            chunk.put("phaseThreeAlias", alias);
            chunk.put("expectedWorkers", expectedWorkers);
            chunk.put("sampleSize", sampleSize);
            chunk.put("selectionCount", selectionCount);
            chunk.put("aliasIndex", intField(payload, "aliasIndex", -1));
            chunk.put("isLastAlias",
                    booleanField(payload, "isLastAlias", false));
            chunk.put("nextAlias", textField(payload, "nextAlias", ""));
            chunk.put("chunkId", chunkId);
            chunk.put("chunkCount", chunkCount);

            ArrayNode entries = sliceArray(selections, range.from, range.to);
            chunk.set("entries", entries);
            chunk.put("entryCount", entries.size());

            // Intentionally no workerKey before Kafka. Parser fan-out is after Kafka.
            out.collect(MAPPER.writeValueAsString(chunk));
        }

        System.out.println("[OnePassStateTopicEmitter] GLOBAL_PHASE3_ALIAS_SELECTIONS emitted. "
                        + "uid=" + uid
                        + ", alias=" + alias
                        + ", resultId=" + resultId
                        + ", stateRef=" + stateRef
                        + ", selections=" + selectionCount
                        + ", kafkaChunks=" + chunkCount
                        + ", logicalWorkers=" + expectedWorkers);
    }

    private List<ChunkRange> buildChunkRanges(JsonNode entries) {
        List<ChunkRange> ranges = new ArrayList<ChunkRange>();

        if (entries == null || !entries.isArray() || entries.size() == 0) {
            ranges.add(new ChunkRange(0, 0));
            return ranges;
        }

        int from = 0;
        int count = 0;
        int approximateBytes = 0;

        for (int i = 0; i < entries.size(); i++) {
            JsonNode entry = entries.get(i);
            int entryBytes = approximateJsonBytes(entry);
            boolean countFull = count >= maxEntriesPerChunk;
            boolean bytesFull = count > 0 && approximateBytes + entryBytes > maxApproxBytesPerChunk;

            if (countFull || bytesFull) {
                ranges.add(new ChunkRange(from, i));
                from = i;
                count = 0;
                approximateBytes = 0;
            }

            count++;
            approximateBytes += entryBytes;
        }

        ranges.add(new ChunkRange(from, entries.size()));
        return ranges;
    }

    private static int approximateJsonBytes(JsonNode node) {
        if (node == null || node.isNull()) {
            return 32;
        }
        return 128 + node.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    private static ArrayNode sliceArray(JsonNode source, int from, int to) {
        ArrayNode result = MAPPER.createArrayNode();
        if (source == null || !source.isArray()) {
            return result;
        }

        int safeFrom = Math.max(0, from);
        int safeTo = Math.min(source.size(), Math.max(safeFrom, to));
        for (int i = safeFrom; i < safeTo; i++) {
            result.add(source.get(i));
        }
        return result;
    }

    private static JsonNode parsePayload(Object payload) throws Exception {
        if (payload == null) {
            return null;
        }
        if (payload instanceof JsonNode) {
            return (JsonNode) payload;
        }
        if (payload instanceof String) {
            String text = ((String) payload).trim();
            if (text.isEmpty()) {
                return null;
            }
            return MAPPER.readTree(text);
        }
        return MAPPER.valueToTree(payload);
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
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    private static int intField(JsonNode node, String fieldName, int defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? defaultValue : field.asInt(defaultValue);
    }

    private static long longField(JsonNode node, String fieldName, long defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? defaultValue : field.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String fieldName, double defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? defaultValue : field.asDouble(defaultValue);
    }

    private static boolean booleanField(JsonNode node, String fieldName, boolean defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? defaultValue : field.asBoolean(defaultValue);
    }

    private static final class ChunkRange {
        private final int from;
        private final int to;

        private ChunkRange(int from, int to) {
            this.from = from;
            this.to = to;
        }
    }
}