package infore.SDE.transformations.onepass.worker;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Destination-local completeness tracker for the FINAL Phase-1 parent-index
 * contribution stream of one alias epoch.
 *
 * A final shard is complete only when:
 *
 *   - this worker has seen local END_ALIAS metadata; and
 *   - every source worker is DONE for this destination; and
 *   - every sequence 0..lastSequence announced by every remote source exists.
 * END_ALIAS and local SOURCE_DONE are intentionally separate.
 */
public final class OnePassPhaseOneCompletionTracker implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Map<EpochKey, EpochState> states = new HashMap<EpochKey, EpochState>();


    /**
     * Returns false for a duplicate batch. Caller must apply SHARD_BATCH
     * entries only when this returns true.
     */
    public boolean acceptBatch(int uid, int epoch, String alias, int expectedWorkers, int sourceWorker, int sequence) {

        EpochState state = state(uid, epoch, alias, expectedWorkers);
        Set<Integer> sequences = state.receivedSequences.computeIfAbsent(sourceWorker, k -> new HashSet<Integer>());

        return sequences.add(sequence);
    }


    public void acceptSourceDone(int uid, int epoch, String alias, int expectedWorkers, int sourceWorker, int lastSequence) {

        EpochState state = state(uid, epoch, alias, expectedWorkers);

        Integer previous = state.lastSequenceBySource.get(sourceWorker);

        if (previous != null && previous.intValue() != lastSequence) {

            throw new IllegalStateException("Conflicting SOURCE_DONE for uid=" + uid + ", epoch=" + epoch +
                    ", alias=" + alias + ", source=" + sourceWorker + ": previous lastSequence=" + previous +
                    ", new=" + lastSequence);
        }

        state.lastSequenceBySource.put(sourceWorker, lastSequence);
    }
    /**
     * Records END_ALIAS metadata only.
     * This deliberately does NOT mark the local source done.
     */
    public void acceptLocalEndAlias(int uid, int epoch, String alias, int expectedWorkers, String resultId,
                                    String nextCommand, String nextAlias, String baseKey, String activeEdgeId) {

        EpochState state = state(uid, epoch, alias, expectedWorkers);
        state.localEndAliasSeen = true;
        state.resultId = resultId;
        state.nextCommand = nextCommand;
        state.nextAlias = nextAlias;
        state.baseKey = baseKey;
        state.activeEdgeId = activeEdgeId;
    }


    /**
     * Local fast-path completion for the final contribution stream.
     *
     * Must be called only after this worker can no longer generate any final
     * contribution for the alias.
     */
    public void acceptLocalSourceDone(int uid, int epoch, String alias, int expectedWorkers, int localWorker) {

        EpochState state = state(uid, epoch, alias, expectedWorkers);

        Integer previous = state.lastSequenceBySource.get(localWorker);

        if (previous != null && previous.intValue() != -1) {

            throw new IllegalStateException("Local final source already has non-local sequence metadata." +
                    " uid=" + uid + ", epoch=" + epoch + ", alias=" + alias + ", worker=" + localWorker +
                    ", previous=" + previous);
        }

        state.lastSequenceBySource.put(localWorker, -1);
    }


    public ReadySnapshot readySnapshotIfComplete(int uid, int epoch, String alias) {

        EpochState state = states.get(new EpochKey(uid, epoch, alias));

        if (state == null || state.readyEmitted || !state.localEndAliasSeen) {
            return null;
        }

        for (int source = 0; source < state.expectedWorkers; source++) {
            Integer last = state.lastSequenceBySource.get(source);

            if (last == null) {
                return null;
            }

            if (last >= 0) {
                Set<Integer> received = state.receivedSequences.get(source);

                if (received == null) {
                    return null;
                }

                for (int sequence = 0; sequence <= last.intValue(); sequence++) {
                    if (!received.contains(sequence)) {
                        return null;
                    }
                }
            }
        }

        state.readyEmitted = true;
        return new ReadySnapshot(uid, epoch, alias, state.expectedWorkers, state.resultId, state.nextCommand,
                state.nextAlias, state.baseKey, state.activeEdgeId);
    }

    public void clearUid(int uid) {

        states.keySet().removeIf(key -> key.uid == uid);
    }

    private EpochState state(int uid, int epoch, String alias, int expectedWorkers) {
        EpochKey key = new EpochKey(uid, epoch, alias);
        EpochState state = states.get(key);

        if (state == null) {
            state = new EpochState(expectedWorkers);
            states.put(key, state);
        } else if (state.expectedWorkers != expectedWorkers) {

            throw new IllegalStateException("Conflicting expectedWorkers for " + key +
                    ": " + state.expectedWorkers + " vs " + expectedWorkers);
        }
        return state;
    }


    public static final class ReadySnapshot implements Serializable {

        private static final long serialVersionUID = 1L;
        public final int uid;
        public final int epoch;
        public final String alias;
        public final int expectedWorkers;
        public final String resultId;
        public final String nextCommand;
        public final String nextAlias;
        public final String baseKey;
        public final String activeEdgeId;

        private ReadySnapshot(int uid, int epoch, String alias, int expectedWorkers, String resultId,
                              String nextCommand, String nextAlias, String baseKey, String activeEdgeId) {

            this.uid = uid;
            this.epoch = epoch;
            this.alias = alias;
            this.expectedWorkers = expectedWorkers;
            this.resultId = resultId;
            this.nextCommand = nextCommand;
            this.nextAlias = nextAlias;
            this.baseKey = baseKey;
            this.activeEdgeId = activeEdgeId;
        }
    }


    private static final class EpochState implements Serializable {

        private static final long serialVersionUID = 1L;
        private final int expectedWorkers;
        private final Map<Integer, Set<Integer>> receivedSequences = new HashMap<Integer, Set<Integer>>();
        private final Map<Integer, Integer> lastSequenceBySource = new HashMap<Integer, Integer>();
        private boolean localEndAliasSeen = false;
        private boolean readyEmitted = false;
        private String resultId = "";
        private String nextCommand = "";
        private String nextAlias = "";
        private String baseKey = "";
        private String activeEdgeId = "";

        private EpochState(int expectedWorkers) {
            this.expectedWorkers = expectedWorkers;
        }
    }

    private static final class EpochKey implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int uid;
        private final int epoch;
        private final String alias;

        private EpochKey(int uid, int epoch, String alias) {
            this.uid = uid;
            this.epoch = epoch;
            this.alias = alias == null ? "" : alias;
        }

        @Override
        public boolean equals(Object other) {

            if (!(other instanceof EpochKey)) {
                return false;
            }

            EpochKey o = (EpochKey) other;
            return uid == o.uid && epoch == o.epoch && Objects.equals(alias, o.alias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, epoch, alias);
        }

        @Override
        public String toString() {
            return uid + "|" + epoch + "|" + alias;
        }
    }
}