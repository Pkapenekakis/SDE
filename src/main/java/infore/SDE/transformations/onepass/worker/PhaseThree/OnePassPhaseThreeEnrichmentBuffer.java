package infore.SDE.transformations.onepass.worker.PhaseThree;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.transformations.onepass.OnePassShardOwnership;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Sequence-aware StateTopic batching for sharded Phase-3 alias enrichment.
 * Stage model for an alias with N child edges:
 *   stage 0       raw tuple at child-edge-0 owner (not transported here)
 *   stage 1..N-1 remaining child continuation owners
 *   stage N       parent-edge selection owner
 * Leaf alias: N=0 and stage 0 is directly the parent-edge selection owner.
 */
public final class OnePassPhaseThreeEnrichmentBuffer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String PROTOCOL = "SHARDED_PHASE3_V1";
    public static final String TYPE_ENRICH_BATCH = "PHASE3_ALIAS_ENRICH_BATCH";
    public static final String TYPE_ENRICH_SOURCE_DONE = "PHASE3_ALIAS_ENRICH_SOURCE_DONE";
    public static final int REQUEST_STATE_TRANSFER = 78;
    public static final int SYNOPSIS_ID = 30;

    private final int maxEntriesPerBatch;
    private final int maxApproxBytesPerBatch;

    private final Map<BatchKey, PendingBatch> pending =
            new LinkedHashMap<BatchKey, PendingBatch>();
    private final Map<String, Integer> nextSequenceByTransfer =
            new HashMap<String, Integer>();

    public OnePassPhaseThreeEnrichmentBuffer() {this(Integer.getInteger("sde.onepass.phase3.enrichBatchEntries", 256),
            Integer.getInteger("sde.onepass.phase3.enrichBatchBytes", 256 * 1024));
    }

    public OnePassPhaseThreeEnrichmentBuffer(int maxEntriesPerBatch, int maxApproxBytesPerBatch) {

        if (maxEntriesPerBatch <= 0 || maxApproxBytesPerBatch <= 0) {
            throw new IllegalArgumentException("Phase-3 enrichment batch limits must be > 0");
        }

        this.maxEntriesPerBatch = maxEntriesPerBatch;
        this.maxApproxBytesPerBatch = maxApproxBytesPerBatch;
    }

    public List<Estimation> addRemoteWork(int uid, String resultId, String baseKey, int expectedWorkers,
                                          int sourceWorker, int targetWorker, String alias, int stageIndex,
                                          JsonNode tuplePayload, double partialWeight) {

        if (tuplePayload == null || tuplePayload.isNull()) {
            throw new IllegalArgumentException("tuplePayload must not be null");
        }
        if (partialWeight == 0.0d) {
            return Collections.emptyList();
        }
        validateFinitePositive(partialWeight, "partialWeight");

        BatchKey key = new BatchKey(uid, resultId, baseKey, expectedWorkers, sourceWorker, targetWorker, alias, stageIndex);
        PendingBatch batch = pending.get(key);

        if (batch == null) {
            batch = new PendingBatch();
            pending.put(key, batch);
        }

        JsonNode tupleCopy = tuplePayload.deepCopy();
        batch.items.add(new WorkItem(tupleCopy, partialWeight));
        batch.approxBytes += approximateBytes(tupleCopy);

        if (batch.items.size() >= maxEntriesPerBatch || batch.approxBytes >= maxApproxBytesPerBatch) {
            return Collections.singletonList(flushOne(key, batch));
        }

        return Collections.emptyList();
    }

    public List<Estimation> flushStage(int uid, String resultId, String alias, int stageIndex) {

        List<Estimation> out = new ArrayList<Estimation>();
        List<BatchKey> keys = new ArrayList<BatchKey>(pending.keySet());

        for (BatchKey key : keys) {
            if (key.uid == uid &&
                    key.resultId.equals(normalize(resultId)) &&
                    key.alias.equals(normalize(alias)) &&
                    key.stageIndex == stageIndex) {

                PendingBatch batch = pending.get(key);
                if (batch != null && !batch.items.isEmpty()) {
                    out.add(flushOne(key, batch));
                }
            }
        }
        return out;
    }

    /**
     * Every source declares completion to every remote destination, including
     * destinations for which it produced zero batches. lastSequence=-1 means
     * that no batch was emitted for that source/target/stage transfer.
     */
    public List<Estimation> buildStageDoneMessages(int uid, String resultId, String baseKey, int expectedWorkers,
                                                   int sourceWorker, String alias, int stageIndex) {

        List<Estimation> out = new ArrayList<Estimation>();
        for (int targetWorker = 0; targetWorker < expectedWorkers; targetWorker++) {
            if (targetWorker == sourceWorker) {
                continue;
            }

            String transferId = transferId(uid, resultId, alias, stageIndex, sourceWorker, targetWorker);

            Integer nextValue = nextSequenceByTransfer.get(transferId);
            int next = nextValue == null ? 0 : nextValue;
            int lastSequence = next - 1;

            String workerKey = OnePassShardOwnership.workerKey(baseKey, expectedWorkers, targetWorker);

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("type", TYPE_ENRICH_SOURCE_DONE);
            payload.put("protocol", PROTOCOL);
            payload.put("phase", "PHASE3");
            payload.put("uid", uid);
            payload.put("resultId", normalize(resultId));
            payload.put("alias", normalize(alias));
            payload.put("phaseThreeAlias", normalize(alias));
            payload.put("stageIndex", stageIndex);
            payload.put("sourceWorker", sourceWorker);
            payload.put("targetWorker", targetWorker);
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("lastSequence", lastSequence);
            payload.put("workerKey", workerKey);

            String estimationKey = transferId + "_DONE";
            out.add(new Estimation(uid, estimationKey, REQUEST_STATE_TRANSFER, SYNOPSIS_ID, workerKey, payload.toString(),
                    new String[] {
                            TYPE_ENRICH_SOURCE_DONE,
                            normalize(resultId),
                            normalize(alias),
                            Integer.toString(stageIndex),
                            Integer.toString(sourceWorker),
                            Integer.toString(targetWorker)
                    },
                    expectedWorkers));
        }

        return out;
    }

    public void clearUid(int uid) {
        List<BatchKey> batchKeys = new ArrayList<BatchKey>(pending.keySet());
        for (BatchKey key : batchKeys) {
            if (key.uid == uid) {
                pending.remove(key);
            }
        }

        String prefix = uid + "|";
        List<String> transferKeys = new ArrayList<String>(nextSequenceByTransfer.keySet());
        for (String key : transferKeys) {
            if (key.startsWith(prefix)) {
                nextSequenceByTransfer.remove(key);
            }
        }
    }

    private Estimation flushOne(BatchKey key, PendingBatch batch) {
        pending.remove(key);
        String transferId = transferId(key.uid, key.resultId, key.alias, key.stageIndex, key.sourceWorker, key.targetWorker);

        Integer sequenceValue = nextSequenceByTransfer.get(transferId);
        int sequence = sequenceValue == null ? 0 : sequenceValue;
        nextSequenceByTransfer.put(transferId, sequence + 1);

        String workerKey = OnePassShardOwnership.workerKey(key.baseKey, key.expectedWorkers, key.targetWorker);

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("type", TYPE_ENRICH_BATCH);
        payload.put("protocol", PROTOCOL);
        payload.put("phase", "PHASE3");
        payload.put("uid", key.uid);
        payload.put("resultId", key.resultId);
        payload.put("alias", key.alias);
        payload.put("phaseThreeAlias", key.alias);
        payload.put("stageIndex", key.stageIndex);
        payload.put("sourceWorker", key.sourceWorker);
        payload.put("targetWorker", key.targetWorker);
        payload.put("expectedWorkers", key.expectedWorkers);
        payload.put("sequence", sequence);
        payload.put("workerKey", workerKey);

        ArrayNode items = payload.putArray("items");
        for (WorkItem work : batch.items) {
            ObjectNode item = items.addObject();
            item.set("tuple", work.tuplePayload.deepCopy());
            item.put("partialWeight", work.partialWeight);
        }
        payload.put("entryCount", items.size());
        String estimationKey = transferId + "_BATCH_" + sequence;

        return new Estimation(key.uid, estimationKey, REQUEST_STATE_TRANSFER, SYNOPSIS_ID, workerKey, payload.toString(),
                new String[] {
                        TYPE_ENRICH_BATCH,
                        key.resultId,
                        key.alias,
                        Integer.toString(key.stageIndex),
                        Integer.toString(key.sourceWorker),
                        Integer.toString(key.targetWorker),
                        Integer.toString(sequence)
                },
                key.expectedWorkers);
    }

    private static String transferId(int uid, String resultId, String alias, int stageIndex, int sourceWorker, int targetWorker) {

        return uid + "|" + normalize(resultId) + "|" + normalize(alias) + "|PHASE3_STAGE_" + stageIndex + "|" +
                sourceWorker + "|" + targetWorker;
    }

    private static int approximateBytes(JsonNode tuplePayload) {
        if (tuplePayload == null || tuplePayload.isNull()) {
            return 64;
        }
        return 64 + tuplePayload.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }

    private static void validateFinitePositive(double value, String label) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value <= 0.0d) {
            throw new IllegalArgumentException(label + " must be positive and finite: " + value);
        }
    }

    private static final class WorkItem implements Serializable {
        private static final long serialVersionUID = 1L;

        private final JsonNode tuplePayload;
        private final double partialWeight;

        private WorkItem(JsonNode tuplePayload, double partialWeight) {
            this.tuplePayload = tuplePayload;
            this.partialWeight = partialWeight;
        }
    }

    private static final class PendingBatch implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<WorkItem> items = new ArrayList<WorkItem>();
        private int approxBytes = 0;
    }

    private static final class BatchKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int uid;
        private final String resultId;
        private final String baseKey;
        private final int expectedWorkers;
        private final int sourceWorker;
        private final int targetWorker;
        private final String alias;
        private final int stageIndex;

        private BatchKey(int uid, String resultId, String baseKey, int expectedWorkers, int sourceWorker,
                         int targetWorker, String alias, int stageIndex) {

            this.uid = uid;
            this.resultId = normalize(resultId);
            this.baseKey = baseKey == null ? "" : baseKey;
            this.expectedWorkers = expectedWorkers;
            this.sourceWorker = sourceWorker;
            this.targetWorker = targetWorker;
            this.alias = normalize(alias);
            this.stageIndex = stageIndex;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BatchKey)) {
                return false;
            }

            BatchKey o = (BatchKey) other;
            return uid == o.uid
                    && expectedWorkers == o.expectedWorkers
                    && sourceWorker == o.sourceWorker
                    && targetWorker == o.targetWorker
                    && stageIndex == o.stageIndex
                    && Objects.equals(resultId, o.resultId)
                    && Objects.equals(baseKey, o.baseKey)
                    && Objects.equals(alias, o.alias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, resultId, baseKey, expectedWorkers, sourceWorker, targetWorker, alias, stageIndex);
        }
    }
}