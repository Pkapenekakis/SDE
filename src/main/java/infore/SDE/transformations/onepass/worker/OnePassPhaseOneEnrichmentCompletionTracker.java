package infore.SDE.transformations.onepass.worker;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Destination-local completion tracker for one branching-enrichment hop.
 *
 * Stage key: (uid, epoch, alias, childIndex)
 *
 * The stage is complete on this destination only when every source worker has
 * declared ENRICH_SOURCE_DONE and every sequence 0..lastSequence from those
 * sources has been received.
 */
public final class OnePassPhaseOneEnrichmentCompletionTracker implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Map<StageKey, StageState> states = new HashMap<StageKey, StageState>();


    /**
     * @return false for a duplicate batch.
     */
    public boolean acceptBatch(int uid, int epoch, String alias, int childIndex, int expectedWorkers,
                               int sourceWorker, int sequence) {

        StageState state = state(uid, epoch, alias, childIndex, expectedWorkers);
        Set<Integer> sequences = state.receivedSequences.computeIfAbsent(sourceWorker, k -> new HashSet<Integer>());
        return sequences.add(sequence);
    }


    public void acceptSourceDone(int uid, int epoch, String alias, int childIndex, int expectedWorkers, int sourceWorker, int lastSequence) {

        StageState state = state(uid, epoch, alias, childIndex, expectedWorkers);
        Integer previous = state.lastSequenceBySource.get(sourceWorker);

        if (previous != null && previous.intValue() != lastSequence) {

            throw new IllegalStateException(
                    "Conflicting ENRICH_SOURCE_DONE for uid=" + uid + ", epoch=" + epoch + ", alias=" + alias +
                            ", childIndex=" + childIndex + ", source=" + sourceWorker +
                            ": previous lastSequence=" + previous + ", new=" + lastSequence);
        }

        state.lastSequenceBySource.put(sourceWorker, lastSequence);
    }


    /**
     * Local target fast path.
     *
     * Work from this source to this same worker never enters Kafka, so there
     * are no local batch sequence numbers to wait for. This method must only be
     * called after this source has finished generating every local-target item
     * for the stage.
     */
    public void acceptLocalSourceDone(int uid, int epoch, String alias, int childIndex, int expectedWorkers, int localWorker) {

        StageState state = state(uid, epoch, alias, childIndex, expectedWorkers);
        Integer previous = state.lastSequenceBySource.get(localWorker);

        if (previous != null && previous != -1) {

            throw new IllegalStateException("Local enrichment source already has non-local sequence metadata." +
                    " uid=" + uid + ", epoch=" + epoch + ", alias=" + alias + ", childIndex=" + childIndex +
                    ", worker=" + localWorker + ", previous=" + previous);
        }

        state.lastSequenceBySource.put(localWorker, -1);
    }


    /**
     * Returns true exactly once when the stage is complete on this destination.
     */
    public boolean markCompleteIfReady(int uid, int epoch, String alias, int childIndex) {

        StageState state = states.get(new StageKey(uid, epoch, alias, childIndex));

        if (state == null || state.completeEmitted) {
            return false;
        }

        for (int source = 0; source < state.expectedWorkers; source++) {

            Integer last = state.lastSequenceBySource.get(source);

            if (last == null) {
                return false;
            }

            if (last >= 0) {
                Set<Integer> received = state.receivedSequences.get(source);

                if (received == null) {
                    return false;
                }

                for (int sequence = 0; sequence <= last; sequence++) {
                    if (!received.contains(sequence)) {
                        return false;
                    }
                }
            }
        }

        state.completeEmitted = true;

        return true;
    }


    public void clearUid(int uid) {
        states.keySet().removeIf(key -> key.uid == uid);
    }


    private StageState state(int uid, int epoch, String alias, int childIndex, int expectedWorkers) {

        StageKey key = new StageKey(uid, epoch, alias, childIndex);
        StageState state = states.get(key);

        if (state == null) {
            state = new StageState(expectedWorkers);
            states.put(key, state);

        } else if (state.expectedWorkers != expectedWorkers) {
            throw new IllegalStateException("Conflicting expectedWorkers for " + key + ": " +
                    state.expectedWorkers + " vs " + expectedWorkers);
        }

        return state;
    }


    private static final class StageState implements Serializable {

        private static final long serialVersionUID = 1L;
        private final int expectedWorkers;
        private final Map<Integer, Set<Integer>> receivedSequences = new HashMap<Integer, Set<Integer>>();
        private final Map<Integer, Integer> lastSequenceBySource = new HashMap<Integer, Integer>();
        private boolean completeEmitted = false;


        private StageState(int expectedWorkers) {
            this.expectedWorkers = expectedWorkers;
        }
    }


    private static final class StageKey implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int uid;
        private final int epoch;
        private final String alias;
        private final int childIndex;


        private StageKey(int uid, int epoch, String alias, int childIndex) {

            this.uid = uid;
            this.epoch = epoch;
            this.alias = alias == null ? "" : alias;
            this.childIndex = childIndex;
        }


        @Override
        public boolean equals(Object other) {

            if (!(other instanceof StageKey)) {
                return false;
            }

            StageKey o = (StageKey) other;
            return uid == o.uid && epoch == o.epoch && childIndex == o.childIndex && Objects.equals(alias, o.alias);
        }


        @Override
        public int hashCode() {
            return Objects.hash(uid, epoch, alias, childIndex);
        }

        @Override
        public String toString() {
            return uid + "|" + epoch + "|" + alias + "|CHILD_" + childIndex;
        }
    }
}