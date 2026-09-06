package infore.SDE.transformations.onepass.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.transformations.onepass.OnePassShardOwnership;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Worker-local batching for branching Phase-1 enrichment work.
 *
 * Unlike OnePassPhaseOneTransferBuffer, these messages do NOT contain final
 * numeric index deltas. They contain an original tuple plus a partial subtree
 * weight that must visit another child-index owner.
 *
 * Work is deliberately not combined by join key because two different source
 * tuples are not interchangeable.
 */
public final class OnePassPhaseOneEnrichmentBuffer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    public static final String TYPE_ENRICH_BATCH = "ENRICH_BATCH";
    public static final String TYPE_ENRICH_SOURCE_DONE = "ENRICH_SOURCE_DONE";

    public static final String PROTOCOL = OnePassPhaseOneTransferBuffer.PROTOCOL;

    public static final int REQUEST_STATE_TRANSFER = OnePassPhaseOneTransferBuffer.REQUEST_STATE_TRANSFER;

    public static final int SYNOPSIS_ID = OnePassPhaseOneTransferBuffer.SYNOPSIS_ID;

    private final int maxEntriesPerBatch;
    private final int maxApproxBytesPerBatch;

    private final Map<BatchKey, PendingBatch> pending = new LinkedHashMap<BatchKey, PendingBatch>();

    private final Map<String, Integer> nextSequenceByTransfer = new HashMap<String, Integer>();


    public OnePassPhaseOneEnrichmentBuffer() {

        this(Integer.getInteger("sde.onepass.phase1.enrichBatchEntries", 256),
                Integer.getInteger("sde.onepass.phase1.enrichBatchBytes", 256 * 1024));
    }


    public OnePassPhaseOneEnrichmentBuffer(int maxEntriesPerBatch, int maxApproxBytesPerBatch) {

        if (maxEntriesPerBatch <= 0 || maxApproxBytesPerBatch <= 0) {
            throw new IllegalArgumentException("Phase-1 enrichment batch limits must be > 0");
        }

        this.maxEntriesPerBatch = maxEntriesPerBatch;
        this.maxApproxBytesPerBatch = maxApproxBytesPerBatch;
    }


    /**
     * Adds one remote work item targeting childIndex on targetWorker.
     */
    public List<Estimation> addRemoteWork(int uid, String baseKey, int expectedWorkers, int sourceWorker,
                                          int targetWorker, int epoch, String alias, int childIndex,
                                          JsonNode tuplePayload, double partialWeight) {

        if (tuplePayload == null || tuplePayload.isNull()) {
            throw new IllegalArgumentException("tuplePayload must not be null");
        }

        if (partialWeight == 0.0d) {
            return Collections.emptyList();
        }

        BatchKey key = new BatchKey(uid, baseKey, expectedWorkers, sourceWorker, targetWorker, epoch, alias, childIndex);
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


    /**
     * Flushes all pending remote work that targets the specified child hop.
     */
    public List<Estimation> flushStage(int uid, int epoch, String alias, int childIndex) {

        List<Estimation> out = new ArrayList<Estimation>();
        List<BatchKey> keys = new ArrayList<BatchKey>(pending.keySet());

        for (BatchKey key : keys) {
            if (key.uid == uid && key.epoch == epoch && key.childIndex == childIndex && key.alias.equals(alias)) {
                PendingBatch batch = pending.get(key);
                if (batch != null && !batch.items.isEmpty()) {
                    out.add(flushOne(key, batch));
                }
            }
        }

        return out;
    }


    /**
     * Declares that this source worker will send no more work for the given target childIndex.
     *
     * One marker is emitted for every REMOTE destination, including targets to
     * which this source produced zero batches. The local destination is
     * completed directly through OnePassPhaseOneEnrichmentCompletionTracker.
     */
    public List<Estimation> buildStageDoneMessages(int uid, String baseKey, int expectedWorkers,
                                                   int sourceWorker, int epoch, String alias, int childIndex) {

        List<Estimation> out = new ArrayList<Estimation>();

        for (int targetWorker = 0; targetWorker < expectedWorkers; targetWorker++) {
            if (targetWorker == sourceWorker) {
                continue;
            }

            String transferId = transferId(uid, epoch, alias, childIndex, sourceWorker, targetWorker);

            int next = nextSequenceByTransfer.getOrDefault(transferId, 0);
            int lastSequence = next - 1;

            String workerKey = OnePassShardOwnership.workerKey(baseKey, expectedWorkers, targetWorker);

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("type", TYPE_ENRICH_SOURCE_DONE);
            payload.put("protocol", PROTOCOL);
            payload.put("phase", "PHASE1");
            payload.put("uid", uid);
            payload.put("alias", alias);
            payload.put("epoch", epoch);
            payload.put("childIndex", childIndex);
            payload.put("sourceWorker", sourceWorker);
            payload.put("targetWorker", targetWorker);
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("lastSequence", lastSequence);
            payload.put("workerKey", workerKey);
            String estimationKey = transferId + "_DONE";

            out.add(new Estimation(uid, estimationKey, REQUEST_STATE_TRANSFER, SYNOPSIS_ID, workerKey, payload.toString(),
                            new String[] {
                                    TYPE_ENRICH_SOURCE_DONE,
                                    Integer.toString(epoch),
                                    alias,
                                    Integer.toString(childIndex),
                                    Integer.toString(sourceWorker),
                                    Integer.toString(targetWorker)
                            },
                            expectedWorkers));
        }

        return out;
    }


    public void clearUid(int uid) {

        pending.keySet().removeIf(key -> key.uid == uid);
        String prefix = uid + "|";
        nextSequenceByTransfer.keySet().removeIf(key -> key.startsWith(prefix));
    }


    private Estimation flushOne(BatchKey key, PendingBatch batch) {

        pending.remove(key);

        String transferId = transferId(key.uid, key.epoch, key.alias, key.childIndex, key.sourceWorker, key.targetWorker);

        int sequence = nextSequenceByTransfer.getOrDefault(transferId, 0);

        nextSequenceByTransfer.put(transferId, sequence + 1);

        String workerKey = OnePassShardOwnership.workerKey(key.baseKey, key.expectedWorkers, key.targetWorker);
        ObjectNode payload = MAPPER.createObjectNode();

        payload.put("type", TYPE_ENRICH_BATCH);
        payload.put("protocol", PROTOCOL);

        payload.put("phase", "PHASE1");
        payload.put("uid", key.uid);
        payload.put("alias", key.alias);
        payload.put("epoch", key.epoch);
        payload.put("childIndex", key.childIndex);
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
                        Integer.toString(key.epoch),
                        key.alias,
                        Integer.toString(key.childIndex),
                        Integer.toString(key.sourceWorker),
                        Integer.toString(key.targetWorker),
                        Integer.toString(sequence)
                },
                key.expectedWorkers
        );
    }


    private static String transferId(int uid, int epoch, String alias, int childIndex, int sourceWorker, int targetWorker) {

        return uid + "|" + epoch + "|" + alias + "|CHILD_" + childIndex + "|" + sourceWorker + "|" + targetWorker;
    }


    private static int approximateBytes(JsonNode tuplePayload) {

        if (tuplePayload == null) {
            return 64;
        }

        /*
         * Debug-quality byte estimate only; the hard entry limit also bounds
         * the batch. Avoid introducing another serializer just for estimation.
         */
        return 64 + tuplePayload.toString().length() * 2;
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
        private final String baseKey;
        private final int expectedWorkers;
        private final int sourceWorker;
        private final int targetWorker;
        private final int epoch;
        private final String alias;
        private final int childIndex;


        private BatchKey(int uid, String baseKey, int expectedWorkers, int sourceWorker, int targetWorker,
                         int epoch, String alias, int childIndex) {

            this.uid = uid;
            this.baseKey = baseKey;
            this.expectedWorkers = expectedWorkers;
            this.sourceWorker = sourceWorker;
            this.targetWorker = targetWorker;
            this.epoch = epoch;
            this.alias = alias == null ? "" : alias;
            this.childIndex = childIndex;
        }


        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BatchKey)) {
                return false;
            }

            BatchKey o = (BatchKey) other;

            return uid == o.uid && expectedWorkers == o.expectedWorkers && sourceWorker == o.sourceWorker &&
                    targetWorker == o.targetWorker && epoch == o.epoch && childIndex == o.childIndex &&
                    Objects.equals(baseKey, o.baseKey) && Objects.equals(alias, o.alias);
        }


        @Override
        public int hashCode() {
            return Objects.hash(uid, baseKey, expectedWorkers, sourceWorker, targetWorker, epoch, alias, childIndex);
        }
    }
}