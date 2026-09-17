package infore.SDE.transformations.onepass.worker.PhaseOne;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneContribution;
import infore.SDE.transformations.onepass.OnePassShardOwnership;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Worker-local Phase-1 final-contribution combiner.
 * <p>
 * Remote final parent-index contributions are accumulated for the complete
 * alias instead of being flushed while tuples are still arriving.
 * <p>
 * For one:
 * <p>
 * uid / epoch / alias / edge / sourceWorker / targetWorker
 * <p>
 * every repeated JoinValue is combined into one numeric delta.
 * <p>
 * Only when flushAlias(...) is called do we split the already-combined map
 * into bounded SHARD_BATCH messages.
 * <p>
 * This gives the following property:
 * <p>
 * one source worker sends one distinct join key at most once
 * per alias / edge / destination worker.
 * <p>
 * The batch limits still bound each Kafka message. They no longer determine
 * when aggregation state is discarded.
 * <p>
 * NOTE:
 * This class handles FINAL Phase-1 parent-index contributions only.
 * Branching enrichment work still uses OnePassPhaseOneEnrichmentBuffer and
 * remains streamed in bounded batches.
 */
public final class OnePassPhaseOneTransferBuffer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String TYPE_SHARD_BATCH = "SHARD_BATCH";
    public static final String TYPE_SOURCE_DONE = "SOURCE_DONE";
    public static final String PROTOCOL = "SHARDED_PHASE1_V1";
    public static final int REQUEST_STATE_TRANSFER = 78;
    public static final int SYNOPSIS_ID = 30;

    /*
     * These limits now control only the outgoing messages created by
     * flushAlias(...).
     * They do NOT trigger an early flush while Phase 1 tuples are arriving.
     */
    private final int maxEntriesPerBatch;
    private final int maxApproxBytesPerBatch;

    /*
     * Complete alias-level aggregates.
     * One PendingAggregate exists for each:
     *   uid / baseKey / expectedWorkers / sourceWorker / targetWorker /
     *   epoch / alias / edgeId
     *
     * Inside it:
     *   JoinValue -> summed delta
     */
    private final Map<BatchKey, PendingAggregate> pending = new LinkedHashMap<BatchKey, PendingAggregate>();

    /*
     * Sequence numbers are still used by OnePassPhaseOneCompletionTracker.
     * They are assigned only when the alias aggregate is finally serialized
     * into SHARD_BATCH messages.
     */
    private final Map<String, Integer> nextSequenceByTransfer = new HashMap<String, Integer>();


    public OnePassPhaseOneTransferBuffer() {
        this(Integer.getInteger("sde.onepass.phase1.shardBatchEntries", 512),
                Integer.getInteger("sde.onepass.phase1.shardBatchBytes", 256 * 1024));
    }

    public OnePassPhaseOneTransferBuffer(int maxEntriesPerBatch, int maxApproxBytesPerBatch) {
        if (maxEntriesPerBatch <= 0 || maxApproxBytesPerBatch <= 0) {
            throw new IllegalArgumentException("Phase-1 batch limits must be > 0");
        }
        this.maxEntriesPerBatch = maxEntriesPerBatch;
        this.maxApproxBytesPerBatch = maxApproxBytesPerBatch;
    }


    /**
     * Adds one already-computed FINAL Phase-1 contribution.
     * <p>
     * IMPORTANT:
     * <p>
     * Nothing is emitted here anymore.
     * <p>
     * If the same join key is produced many times by this source worker for
     * the same target/edge during the alias, all deltas are combined here.
     */
    public void addRemoteContribution(int uid, String baseKey, int expectedWorkers, int sourceWorker,
                                      int targetWorker, int epoch, String alias, OnePassPhaseOneContribution contribution) {

        if (contribution == null || contribution.getDelta() == 0.0d) {
            return;
        }

        BatchKey key = new BatchKey(uid, baseKey, expectedWorkers, sourceWorker, targetWorker, epoch, alias, contribution.getEdgeId());
        PendingAggregate aggregate = pending.get(key);

        if (aggregate == null) {
            aggregate = new PendingAggregate();
            pending.put(key, aggregate);
        }

        JoinValue joinKey = contribution.getJoinKey();
        Double current = aggregate.entries.get(joinKey);
        double updated = (current == null ? 0.0d : current.doubleValue()) + contribution.getDelta();

        /*
         * Normally deltas are positive for the current OnePass workload.
         * Keeping the key even if updated == 0 preserves generic additive
         * semantics and avoids silently changing behavior if signed deltas are
         * introduced later.
         */
        aggregate.entries.put(joinKey, updated);
    }


    /**
     * Called only when this source worker can no longer generate any final
     * contribution for the specified alias.
     * <p>
     * For leaf / one-child aliases this happens after END_ALIAS.
     * <p>
     * For branching aliases this happens after:
     * <p>
     * END_ALIAS
     * -> enrichment flush
     * -> enrichment completion
     * -> all final contributions have been generated
     * <p>
     * The complete alias-level aggregate is now divided into bounded
     * SHARD_BATCH messages.
     */
    public List<Estimation> flushAlias(int uid, int epoch, String alias) {
        List<Estimation> out = new ArrayList<Estimation>();

        /*
         * Iterate over a detached key list because matching aggregates are
         * removed from pending as they are finalized.
         */
        List<BatchKey> keys = new ArrayList<BatchKey>(pending.keySet());

        for (BatchKey key : keys) {
            if (key.uid != uid || key.epoch != epoch || !key.alias.equals(alias)) {
                continue;
            }

            PendingAggregate aggregate = pending.remove(key);
            if (aggregate == null || aggregate.entries.isEmpty()) {
                continue;
            }
            appendBatches(key, aggregate, out);
        }

        return out;
    }


    /**
     * Splits one COMPLETE aggregate into bounded outgoing batches.
     * <p>
     * Every JoinValue is visited exactly once here, so one distinct join key
     * from this aggregate can occur in only one SHARD_BATCH.
     */
    private void appendBatches(BatchKey key, PendingAggregate aggregate, List<Estimation> out) {
        LinkedHashMap<JoinValue, Double> currentBatch = new LinkedHashMap<JoinValue, Double>();
        int currentApproxBytes = 0;

        for (Map.Entry<JoinValue, Double> entry : aggregate.entries.entrySet()) {
            JoinValue joinKey = entry.getKey();
            double delta = entry.getValue() == null ? 0.0d : entry.getValue();
            int entryBytes = approximateEntryBytes(joinKey, delta);
            boolean countFull = currentBatch.size() >= maxEntriesPerBatch;
            boolean bytesFull = !currentBatch.isEmpty() && currentApproxBytes + entryBytes > maxApproxBytesPerBatch;

            /*
             * Flush the current outgoing message BEFORE adding this key.
             * Therefore, the key is never split across two batches.
             */
            if (countFull || bytesFull) {
                out.add(buildBatch(key, currentBatch));
                currentBatch = new LinkedHashMap<JoinValue, Double>();
                currentApproxBytes = 0;
            }

            currentBatch.put(joinKey, delta);
            currentApproxBytes += entryBytes;
        }

        if (!currentBatch.isEmpty()) {
            out.add(buildBatch(key, currentBatch));
        }
    }


    /**
     * Must be called only after flushAlias().
     * <p>
     * One tiny marker is produced per remote target even when this source
     * produced zero SHARD_BATCH messages for that target.
     * <p>
     * The existing receiver-side sequence protocol remains unchanged.
     */
    public List<Estimation> buildSourceDoneMessages(int uid, String baseKey, int expectedWorkers, int sourceWorker, int epoch, String alias) {

        List<Estimation> out = new ArrayList<Estimation>();
        for (int targetWorker = 0; targetWorker < expectedWorkers; targetWorker++) {
            if (targetWorker == sourceWorker) {
                //Local source completion is recorded directly through OnePassPhaseOneCompletionTracker.
                continue;
            }

            String transferId = transferId(uid, epoch, alias, sourceWorker, targetWorker);

            int next = nextSequenceByTransfer.getOrDefault(transferId, 0);
            int lastSequence = next - 1;

            String workerKey = OnePassShardOwnership.workerKey(baseKey, expectedWorkers, targetWorker);
            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("type", TYPE_SOURCE_DONE);
            payload.put("protocol", PROTOCOL);
            payload.put("phase", "PHASE1");
            payload.put("uid", uid);
            payload.put("alias", alias);
            payload.put("epoch", epoch);
            payload.put("sourceWorker", sourceWorker);
            payload.put("targetWorker", targetWorker);
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("lastSequence", lastSequence);
            payload.put("workerKey", workerKey);

            String estimationKey = transferId + "_DONE";

            out.add(new Estimation(uid, estimationKey, REQUEST_STATE_TRANSFER, SYNOPSIS_ID, workerKey,
                    payload.toString(), new String[]{TYPE_SOURCE_DONE, Integer.toString(epoch), alias,
                    Integer.toString(sourceWorker), Integer.toString(targetWorker)}, expectedWorkers));
        }

        return out;
    }


    public void clearUid(int uid) {
        pending.keySet().removeIf(key -> key.uid == uid);
        String prefix = uid + "|";
        nextSequenceByTransfer.keySet().removeIf(key -> key.startsWith(prefix));
    }


    //Builds one bounded SHARD_BATCH from already-aggregated entries.
    private Estimation buildBatch(BatchKey key, LinkedHashMap<JoinValue, Double> entriesToSend) {

        String transferId = transferId(key.uid, key.epoch, key.alias, key.sourceWorker, key.targetWorker);
        int sequence = nextSequenceByTransfer.getOrDefault(transferId, 0);
        nextSequenceByTransfer.put(transferId, sequence + 1);

        String workerKey = OnePassShardOwnership.workerKey(key.baseKey, key.expectedWorkers, key.targetWorker);

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("type", TYPE_SHARD_BATCH);
        payload.put("protocol", PROTOCOL);
        payload.put("phase", "PHASE1");
        payload.put("uid", key.uid);
        payload.put("alias", key.alias);
        payload.put("epoch", key.epoch);
        payload.put("sourceWorker", key.sourceWorker);
        payload.put("targetWorker", key.targetWorker);
        payload.put("expectedWorkers", key.expectedWorkers);
        payload.put("edgeId", key.edgeId);
        payload.put("sequence", sequence);
        payload.put("workerKey", workerKey);

        ArrayNode entries = payload.putArray("entries");

        for (Map.Entry<JoinValue, Double> entry : entriesToSend.entrySet()) {
            ObjectNode item = entries.addObject();
            ArrayNode parts = item.putArray("joinKeyParts");
            for (String part : entry.getKey().getParts()) {
                parts.add(part);
            }

            item.put("delta", entry.getValue());
        }

        payload.put("entryCount", entries.size());
        String estimationKey = transferId + "_BATCH_" + sequence;

        return new Estimation(key.uid, estimationKey, REQUEST_STATE_TRANSFER, SYNOPSIS_ID, workerKey,
                payload.toString(), new String[]{TYPE_SHARD_BATCH, Integer.toString(key.epoch), key.alias,
                Integer.toString(key.sourceWorker), Integer.toString(key.targetWorker), Integer.toString(sequence)},
                key.expectedWorkers);
    }


    private static String transferId(int uid, int epoch, String alias, int sourceWorker, int targetWorker) {
        return uid + "|" + epoch + "|" + alias + "|" + sourceWorker + "|" + targetWorker;
    }


    /**
     * Conservative estimate used only to decide where to split the final
     * aggregate into Kafka messages.
     * <p>
     * The normal 512-entry cap is retained as an additional safety bound.
     */
    private static int approximateEntryBytes(JoinValue value, double delta) {
        int size = 96;
        if (value != null) {
            for (String part : value.getParts()) {
                size += 16;
                if (part != null) {
                    size += part.getBytes(StandardCharsets.UTF_8).length;
                }
            }
        }

        size += Double.toString(delta).length();
        return size;
    }


    //Full alias-level aggregate for one remote final-index destination.
    private static final class PendingAggregate implements Serializable {
        private static final long serialVersionUID = 1L;
        private final LinkedHashMap<JoinValue, Double> entries = new LinkedHashMap<JoinValue, Double>();
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
        private final String edgeId;


        private BatchKey(int uid, String baseKey, int expectedWorkers, int sourceWorker, int targetWorker, int epoch,
                         String alias, String edgeId) {

            this.uid = uid;
            this.baseKey = baseKey;
            this.expectedWorkers = expectedWorkers;
            this.sourceWorker = sourceWorker;
            this.targetWorker = targetWorker;
            this.epoch = epoch;
            this.alias = alias == null ? "" : alias;
            this.edgeId = edgeId == null ? "" : edgeId;
        }


        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BatchKey)) {
                return false;
            }

            BatchKey o = (BatchKey) other;

            return uid == o.uid && expectedWorkers == o.expectedWorkers && sourceWorker == o.sourceWorker &&
                    targetWorker == o.targetWorker && epoch == o.epoch &&
                    Objects.equals(baseKey, o.baseKey) && Objects.equals(alias, o.alias) && Objects.equals(edgeId, o.edgeId);
        }


        @Override
        public int hashCode() {
            return Objects.hash(uid, baseKey, expectedWorkers, sourceWorker, targetWorker, epoch, alias, edgeId);
        }
    }
}