package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Map;

/**
 * Converts a merged OnePass Phase 1 result into RequestTopic-compatible
 * BEGIN / CHUNK / COMMIT messages.
 *
 * This class is OnePass-specific and does not affect any other synopsis.
 *
 * Important:
 * - It emits every chunk only once.
 * - RqRouterFlatMap will later duplicate the requests to all OnePass workers.
 * - It does not install state.
 * - It does not wait for worker acknowledgements.
 */
public final class OnePassPhaseOneRequestSplitter
        extends RichFlatMapFunction<Estimation, Estimation> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final int ONEPASS_SYNOPSIS_ID = 30;

    public static final int GLOBAL_PHASE1_RESULT_REQUEST_ID = 73;
    public static final int ONEPASS_FEEDBACK_REQUEST_ID = 7;

    public static final String TYPE_GLOBAL_PHASE1_RESULT = "GLOBAL_PHASE1_RESULT";
    public static final String TYPE_GLOBAL_STATE_BEGIN = "GLOBAL_STATE_BEGIN";
    public static final String TYPE_GLOBAL_STATE_CHUNK = "GLOBAL_STATE_CHUNK";
    public static final String TYPE_GLOBAL_STATE_COMMIT = "GLOBAL_STATE_COMMIT";
    public static final String STATE_TYPE_GLOBAL_PHASE1_INDEX = "GLOBAL_PHASE1_INDEX";

    public static final String COMMAND_START_NEXT_ALIAS = "START_NEXT_ALIAS";
    public static final String COMMAND_START_PHASE_2 = "START_PHASE_2";

    private static final int DEFAULT_MAX_ENTRIES_PER_CHUNK = 500;

    private final int maxEntriesPerChunk;

    public OnePassPhaseOneRequestSplitter() {
        this(DEFAULT_MAX_ENTRIES_PER_CHUNK);
    }

    public OnePassPhaseOneRequestSplitter(int maxEntriesPerChunk) {
        if (maxEntriesPerChunk <= 0) {
            throw new IllegalArgumentException("maxEntriesPerChunk must be greater than zero");
        }

        this.maxEntriesPerChunk = maxEntriesPerChunk;
    }

    @Override
    public void flatMap(Estimation value, Collector<Estimation> out) throws Exception {

        if (value == null) {
            return;
        }

        if (value.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        if (value.getRequestID() != GLOBAL_PHASE1_RESULT_REQUEST_ID) {
            return;
        }

        JsonNode sourcePayload = parsePayload(value.getEstimation());

        if (sourcePayload == null || sourcePayload.isNull()) {
            return;
        }

        String sourceType = textField(sourcePayload, "type", "");

        if (!TYPE_GLOBAL_PHASE1_RESULT.equals(sourceType)) {
            return;
        }

        emitPhaseOneMessages(value, sourcePayload, out);
    }

    private void emitPhaseOneMessages(Estimation source, JsonNode sourcePayload, Collector<Estimation> out) throws Exception {

        int uid = intField(sourcePayload, "uid", source.getUID());

        int expectedWorkers = intField(sourcePayload, "expectedWorkers", source.getNoOfP());

        if (expectedWorkers <= 0) {
            expectedWorkers = source.getNoOfP() > 0 ? source.getNoOfP() : 1;
        }

        String phase = textField(sourcePayload, "phase", "PHASE1");

        String resultId = textField(sourcePayload, "resultId", "PHASE1_RESULT_" + uid);

        String queryName = textField(sourcePayload, "queryName", "");

        String rootAlias = textField(sourcePayload, "rootAlias", "");

        String activeAlias = textField(sourcePayload, "activeAlias", "");

        String nextCommand = textField(sourcePayload, "nextCommand", "");

        String nextAlias = textField(sourcePayload, "nextAlias", "");

        if (!COMMAND_START_NEXT_ALIAS.equals(nextCommand) && !COMMAND_START_PHASE_2.equals(nextCommand)) {

            throw new IllegalStateException("GLOBAL_PHASE1_RESULT is missing a valid nextCommand. "
                            + "Expected START_NEXT_ALIAS or START_PHASE_2, " + "received '" + nextCommand
                            + "'. uid=" + uid + ", resultId=" + resultId);
        }

        if (nextAlias == null || nextAlias.trim().isEmpty()) {
            throw new IllegalStateException("GLOBAL_PHASE1_RESULT is missing nextAlias. " + "uid=" + uid
                            + ", resultId=" + resultId + ", nextCommand=" + nextCommand);
        }

        String activeEdgeId = textField(sourcePayload, "activeEdgeId", "");

        String baseKey = textField(sourcePayload, "baseKey", "");

        if (baseKey == null || baseKey.trim().isEmpty()) {
            throw new IllegalStateException("GLOBAL_PHASE1_RESULT is missing baseKey. uid=" + uid
                            + ", resultId=" + resultId);
        }

        String stateRef = textField(sourcePayload, "stateRef", uid + "_PHASE1_" + resultId + "_GLOBAL_STATE");

        JsonNode globalPhaseOneResult = sourcePayload.get("globalPhaseOneResult");

        if (globalPhaseOneResult == null || globalPhaseOneResult.isNull()) {

            throw new IllegalStateException(
                    "GLOBAL_PHASE1_RESULT is missing globalPhaseOneResult. "
                            + "uid="
                            + uid
                            + ", resultId="
                            + resultId
            );
        }

        ArrayNode flattenedEntries = flattenPhaseOneIndexes(globalPhaseOneResult.get("edgeIndexes"));

        int totalEntryCount = flattenedEntries.size();
        int chunkCount = Math.max(1, (totalEntryCount + maxEntriesPerChunk - 1) / maxEntriesPerChunk);

        String checksum = sha256JsonArray(flattenedEntries);

        ObjectNode begin = createBaseMessage(
                        uid,
                        phase,
                        resultId,
                        stateRef,
                        queryName,
                        rootAlias,
                        activeAlias,
                        activeEdgeId,
                        baseKey,
                        expectedWorkers);

        begin.put("type", TYPE_GLOBAL_STATE_BEGIN);
        begin.put("chunkCount", chunkCount);
        begin.put("totalEntryCount", totalEntryCount);
        begin.put("checksum", checksum);

        copyIfPresent(globalPhaseOneResult, begin, "seenTuplesByAlias");
        copyIfPresent(globalPhaseOneResult, begin, "edgeSummaries");

        emitFeedbackRequest(
                source,
                begin,
                TYPE_GLOBAL_STATE_BEGIN,
                stateRef,
                baseKey,
                expectedWorkers,
                out
        );

        for (int chunkId = 0;
             chunkId < chunkCount;
             chunkId++) {

            int from = chunkId * maxEntriesPerChunk;

            int to = Math.min(totalEntryCount, from + maxEntriesPerChunk);

            ObjectNode chunk = createBaseMessage(
                            uid,
                            phase,
                            resultId,
                            stateRef,
                            queryName,
                            rootAlias,
                            activeAlias,
                            activeEdgeId,
                            baseKey,
                            expectedWorkers
                    );

            chunk.put("type", TYPE_GLOBAL_STATE_CHUNK);
            chunk.put("chunkId", chunkId);
            chunk.put("chunkCount", chunkCount);
            chunk.put("totalEntryCount", totalEntryCount);

            ArrayNode chunkEntries = MAPPER.createArrayNode();

            for (int i = from; i < to; i++) {
                chunkEntries.add(flattenedEntries.get(i).deepCopy());
            }

            chunk.put("entryCount", chunkEntries.size());

            chunk.set("entries", chunkEntries);

            emitFeedbackRequest(
                    source,
                    chunk,
                    TYPE_GLOBAL_STATE_CHUNK,
                    stateRef,
                    baseKey,
                    expectedWorkers,
                    out
            );
        }

        ObjectNode commit =
                createBaseMessage(
                        uid,
                        phase,
                        resultId,
                        stateRef,
                        queryName,
                        rootAlias,
                        activeAlias,
                        activeEdgeId,
                        baseKey,
                        expectedWorkers
                );

        commit.put("type", TYPE_GLOBAL_STATE_COMMIT);
        commit.put("chunkCount", chunkCount);
        commit.put("totalEntryCount", totalEntryCount);
        commit.put("checksum", checksum);
        commit.put("nextCommand", nextCommand);
        commit.put("nextAlias", nextAlias);
        //The worker uses this exact state identifier as its readiness gate.
        commit.put("requiredStateRef", stateRef);

        emitFeedbackRequest(
                source,
                commit,
                TYPE_GLOBAL_STATE_COMMIT,
                stateRef,
                baseKey,
                expectedWorkers,
                out
        );
    }

    private ObjectNode createBaseMessage(
            int uid,
            String phase,
            String resultId,
            String stateRef,
            String queryName,
            String rootAlias,
            String activeAlias,
            String activeEdgeId,
            String baseKey,
            int expectedWorkers) {

        ObjectNode message =
                MAPPER.createObjectNode();

        message.put(
                "stateType",
                STATE_TYPE_GLOBAL_PHASE1_INDEX
        );

        message.put("uid", uid);
        message.put("synopsisID", ONEPASS_SYNOPSIS_ID);
        message.put("phase", phase);
        message.put("resultId", resultId);
        message.put("stateRef", stateRef);
        message.put("queryName", queryName);
        message.put("rootAlias", rootAlias);
        message.put("activeAlias", activeAlias);
        message.put("activeEdgeId", activeEdgeId);
        message.put("baseKey", baseKey);
        message.put("expectedWorkers", expectedWorkers);

        return message;
    }

    private void emitFeedbackRequest(
            Estimation source,
            ObjectNode payload,
            String messageType,
            String stateRef,
            String baseKey,
            int expectedWorkers,
            Collector<Estimation> out) {

        String chunkId =
                payload.has("chunkId")
                        ? Integer.toString(
                        payload.get("chunkId").asInt()
                )
                        : "-1";

        String chunkCount =
                payload.has("chunkCount")
                        ? Integer.toString(
                        payload.get("chunkCount").asInt()
                )
                        : "0";

        String[] param =
                new String[]{
                        messageType,
                        stateRef,
                        STATE_TYPE_GLOBAL_PHASE1_INDEX,
                        chunkId,
                        chunkCount
                };

        /*
         * estimationkey must be the logical base key because
         * Request(Estimation) maps estimationkey to Request.DataSetkey.
         */
        Estimation feedback =
                new Estimation(
                        payload.get("uid").asInt(),
                        baseKey,
                        ONEPASS_FEEDBACK_REQUEST_ID,
                        ONEPASS_SYNOPSIS_ID,
                        baseKey,
                        payload,
                        param,
                        expectedWorkers
                );

        feedback.setStreamID(source.getStreamID());

        out.collect(feedback);
    }

    private ArrayNode flattenPhaseOneIndexes(
            JsonNode edgeIndexes) {

        ArrayNode entries =
                MAPPER.createArrayNode();

        if (edgeIndexes == null
                || !edgeIndexes.isObject()) {

            return entries;
        }

        Iterator<Map.Entry<String, JsonNode>>
                edgeFields = edgeIndexes.fields();

        while (edgeFields.hasNext()) {
            Map.Entry<String, JsonNode> edgeEntry =
                    edgeFields.next();

            String edgeId =
                    edgeEntry.getKey();

            JsonNode joinWeights =
                    edgeEntry.getValue();

            if (joinWeights == null
                    || !joinWeights.isObject()) {

                continue;
            }

            Iterator<Map.Entry<String, JsonNode>>
                    joinFields = joinWeights.fields();

            while (joinFields.hasNext()) {
                Map.Entry<String, JsonNode> joinEntry =
                        joinFields.next();

                ObjectNode flattened =
                        MAPPER.createObjectNode();

                flattened.put("edgeId", edgeId);
                flattened.put(
                        "joinKey",
                        joinEntry.getKey()
                );

                flattened.put(
                        "globalWeight",
                        joinEntry.getValue()
                                .asDouble(0.0d)
                );

                entries.add(flattened);
            }
        }

        return entries;
    }

    private static JsonNode parsePayload(
            Object payload) throws Exception {

        if (payload == null) {
            return null;
        }

        if (payload instanceof JsonNode) {
            return (JsonNode) payload;
        }

        if (payload instanceof String) {
            String text =
                    ((String) payload).trim();

            if (text.isEmpty()) {
                return null;
            }

            return MAPPER.readTree(text);
        }

        return MAPPER.valueToTree(payload);
    }

    private static void copyIfPresent(
            JsonNode source,
            ObjectNode target,
            String fieldName) {

        if (source == null || source.isNull()) {
            return;
        }

        JsonNode value =
                source.get(fieldName);

        if (value != null && !value.isNull()) {
            target.set(
                    fieldName,
                    value.deepCopy()
            );
        }
    }

    private static String textField(
            JsonNode node,
            String fieldName,
            String defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        String value =
                field.asText();

        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int intField(
            JsonNode node,
            String fieldName,
            int defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asInt(defaultValue);
    }

    private static String sha256JsonArray(ArrayNode entries) throws Exception {

        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        /*
         * ArrayNode.toString() produces compact JSON:
         *
         * [entry0,entry1,...]
         *
         * Reproduce exactly the same byte sequence incrementally so
         * checksum semantics remain unchanged without allocating one
         * enormous String.
         */
        digest.update((byte) '[');

        boolean first = true;

        for (JsonNode entry : entries) {
            if (!first) {
                digest.update((byte) ',');
            }

            first = false;

            byte[] entryBytes = entry.toString().getBytes(StandardCharsets.UTF_8);

            digest.update(entryBytes);
        }

        digest.update((byte) ']');

        return toHex(digest.digest());
    }

    private static String toHex(byte[] hash) {

        StringBuilder out = new StringBuilder(hash.length * 2);

        for (byte b : hash) {
            out.append(String.format("%02x", b & 0xff));
        }

        return out.toString();
    }
}