package infore.SDE.transformations.onepass.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneContribution;
import infore.SDE.transformations.onepass.OnePassShardOwnership;

import java.io.Serializable;
import java.util.*;

/**
 * Worker-local Phase-1 combiner/batcher.
 *
 * It combines multiple deltas for the same join key before sending them to
 * Kafka, then emits bounded SHARD_BATCH payloads targeted at one worker.
 */
public final class OnePassPhaseOneTransferBuffer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String TYPE_SHARD_BATCH = "SHARD_BATCH";
    public static final String TYPE_SOURCE_DONE = "SOURCE_DONE";
    public static final String PROTOCOL = "SHARDED_PHASE1_V1";
    public static final int REQUEST_STATE_TRANSFER = 78;
    public static final int SYNOPSIS_ID = 30;

    private final int maxEntriesPerBatch;
    private final int maxApproxBytesPerBatch;

    private final Map<BatchKey, PendingBatch> pending =
            new LinkedHashMap<BatchKey, PendingBatch>();

    private final Map<String, Integer> nextSequenceByTransfer =
            new HashMap<String, Integer>();

    public OnePassPhaseOneTransferBuffer() {
        this(
                Integer.getInteger("sde.onepass.phase1.shardBatchEntries", 512),
                Integer.getInteger("sde.onepass.phase1.shardBatchBytes", 256 * 1024)
        );
    }

    public OnePassPhaseOneTransferBuffer(int maxEntriesPerBatch, int maxApproxBytesPerBatch) {
        if (maxEntriesPerBatch <= 0 || maxApproxBytesPerBatch <= 0) {
            throw new IllegalArgumentException("Phase-1 batch limits must be > 0");
        }
        this.maxEntriesPerBatch = maxEntriesPerBatch;
        this.maxApproxBytesPerBatch = maxApproxBytesPerBatch;
    }

    public List<Estimation> addRemoteContribution(
            int uid,
            String baseKey,
            int expectedWorkers,
            int sourceWorker,
            int targetWorker,
            int epoch,
            String alias,
            OnePassPhaseOneContribution contribution) {

        if (contribution == null || contribution.getDelta() == 0.0d) {
            return Collections.emptyList();
        }

        BatchKey key = new BatchKey(
                uid, baseKey, expectedWorkers, sourceWorker, targetWorker,
                epoch, alias, contribution.getEdgeId()
        );

        PendingBatch batch = pending.get(key);
        if (batch == null) {
            batch = new PendingBatch();
            pending.put(key, batch);
        }

        Double current = batch.entries.get(contribution.getJoinKey());
        batch.entries.put(
                contribution.getJoinKey(),
                (current == null ? 0.0d : current.doubleValue()) + contribution.getDelta()
        );

        batch.approxBytes += approximateBytes(contribution.getJoinKey());

        if (batch.entries.size() >= maxEntriesPerBatch
                || batch.approxBytes >= maxApproxBytesPerBatch) {
            return Collections.singletonList(flushOne(key, batch));
        }

        return Collections.emptyList();
    }

    public List<Estimation> flushAlias(int uid, int epoch, String alias) {
        List<Estimation> out = new ArrayList<Estimation>();
        List<BatchKey> keys = new ArrayList<BatchKey>(pending.keySet());

        for (BatchKey key : keys) {
            if (key.uid == uid && key.epoch == epoch && key.alias.equals(alias)) {
                PendingBatch batch = pending.get(key);
                if (batch != null && !batch.entries.isEmpty()) {
                    out.add(flushOne(key, batch));
                }
            }
        }

        return out;
    }

    /**
     * Must be called only after flushAlias(). One tiny marker is produced per
     * remote target even when no SHARD_BATCH was sent to that target.
     */
    public List<Estimation> buildSourceDoneMessages(
            int uid,
            String baseKey,
            int expectedWorkers,
            int sourceWorker,
            int epoch,
            String alias) {

        List<Estimation> out = new ArrayList<Estimation>();

        for (int targetWorker = 0; targetWorker < expectedWorkers; targetWorker++) {
            if (targetWorker == sourceWorker) {
                continue; // local source completion is recorded directly
            }

            String transferId = transferId(
                    uid, epoch, alias, sourceWorker, targetWorker
            );

            int next = nextSequenceByTransfer.containsKey(transferId)
                    ? nextSequenceByTransfer.get(transferId)
                    : 0;

            int lastSequence = next - 1;
            String workerKey = OnePassShardOwnership.workerKey(
                    baseKey, expectedWorkers, targetWorker
            );

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
            out.add(new Estimation(
                    uid,
                    estimationKey,
                    REQUEST_STATE_TRANSFER,
                    SYNOPSIS_ID,
                    workerKey,
                    payload.toString(),
                    new String[] {
                            TYPE_SOURCE_DONE,
                            Integer.toString(epoch),
                            alias,
                            Integer.toString(sourceWorker),
                            Integer.toString(targetWorker)
                    },
                    expectedWorkers
            ));
        }

        return out;
    }

    public void clearUid(int uid) {
        pending.keySet().removeIf(k -> k.uid == uid);
        String prefix = uid + "|";
        nextSequenceByTransfer.keySet().removeIf(k -> k.startsWith(prefix));
    }

    private Estimation flushOne(BatchKey key, PendingBatch batch) {
        pending.remove(key);

        String transferId = transferId(
                key.uid, key.epoch, key.alias, key.sourceWorker, key.targetWorker
        );

        int sequence = nextSequenceByTransfer.containsKey(transferId)
                ? nextSequenceByTransfer.get(transferId)
                : 0;
        nextSequenceByTransfer.put(transferId, sequence + 1);

        String workerKey = OnePassShardOwnership.workerKey(
                key.baseKey, key.expectedWorkers, key.targetWorker
        );

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

        for (Map.Entry<JoinValue, Double> entry : batch.entries.entrySet()) {
            ObjectNode item = entries.addObject();
            ArrayNode parts = item.putArray("joinKeyParts");
            for (String part : entry.getKey().getParts()) {
                parts.add(part);
            }
            item.put("delta", entry.getValue());
        }

        payload.put("entryCount", entries.size());

        String estimationKey = transferId + "_BATCH_" + sequence;

        return new Estimation(
                key.uid,
                estimationKey,
                REQUEST_STATE_TRANSFER,
                SYNOPSIS_ID,
                workerKey,
                payload.toString(),
                new String[] {
                        TYPE_SHARD_BATCH,
                        Integer.toString(key.epoch),
                        key.alias,
                        Integer.toString(key.sourceWorker),
                        Integer.toString(key.targetWorker),
                        Integer.toString(sequence)
                },
                key.expectedWorkers
        );
    }

    private static String transferId(
            int uid,
            int epoch,
            String alias,
            int sourceWorker,
            int targetWorker) {

        return uid + "|" + epoch + "|" + alias + "|" + sourceWorker + "|" + targetWorker;
    }

    private static int approximateBytes(JoinValue value) {
        int size = 32;
        for (String part : value.getParts()) {
            size += part == null ? 0 : part.length() * 2;
        }
        return size;
    }

    private static final class PendingBatch implements Serializable {
        private static final long serialVersionUID = 1L;
        private final LinkedHashMap<JoinValue, Double> entries =
                new LinkedHashMap<JoinValue, Double>();
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
        private final String edgeId;

        private BatchKey(
                int uid,
                String baseKey,
                int expectedWorkers,
                int sourceWorker,
                int targetWorker,
                int epoch,
                String alias,
                String edgeId) {
            this.uid = uid;
            this.baseKey = baseKey;
            this.expectedWorkers = expectedWorkers;
            this.sourceWorker = sourceWorker;
            this.targetWorker = targetWorker;
            this.epoch = epoch;
            this.alias = alias;
            this.edgeId = edgeId;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BatchKey)) return false;
            BatchKey o = (BatchKey) other;
            return uid == o.uid
                    && expectedWorkers == o.expectedWorkers
                    && sourceWorker == o.sourceWorker
                    && targetWorker == o.targetWorker
                    && epoch == o.epoch
                    && Objects.equals(baseKey, o.baseKey)
                    && Objects.equals(alias, o.alias)
                    && Objects.equals(edgeId, o.edgeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    uid, baseKey, expectedWorkers, sourceWorker,
                    targetWorker, epoch, alias, edgeId
            );
        }
    }
}