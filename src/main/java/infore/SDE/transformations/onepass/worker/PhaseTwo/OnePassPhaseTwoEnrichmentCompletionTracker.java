package infore.SDE.transformations.onepass.worker.PhaseTwo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Destination-local completion tracker for one Phase-2 root enrichment hop.
 *
 * Stage key:
 *
 *   uid
 *   resultId
 *   rootAlias
 *   childIndex
 *
 * A stage is complete on this destination when every source worker has emitted
 * PHASE2_ROOT_ENRICH_SOURCE_DONE and all declared batch sequences have arrived.
 */
public final class OnePassPhaseTwoEnrichmentCompletionTracker implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<StageKey, StageState> states = new HashMap<StageKey, StageState>();


    public boolean acceptBatch(int uid, String resultId, String rootAlias, int childIndex,
                               int expectedWorkers, int sourceWorker, int sequence) {

        StageState state = state(uid, resultId, rootAlias, childIndex, expectedWorkers);
        Set<Integer> sequences = state.receivedSequences.computeIfAbsent(sourceWorker, k -> new HashSet<Integer>());

        return sequences.add(sequence);
    }


    public void acceptSourceDone(int uid, String resultId, String rootAlias, int childIndex, int expectedWorkers,
                                 int sourceWorker, int lastSequence) {

        StageState state = state(uid, resultId, rootAlias, childIndex, expectedWorkers);

        Integer previous = state.lastSequenceBySource.get(sourceWorker);

        if (previous != null && previous != lastSequence) {

            throw new IllegalStateException("Conflicting Phase-2 SOURCE_DONE." + " uid=" + uid + ", resultId="
                    + resultId + ", root=" + rootAlias + ", childIndex=" + childIndex + ", source=" + sourceWorker +
                    ", previous=" + previous + ", new=" + lastSequence);
        }

        state.lastSequenceBySource.put(sourceWorker, lastSequence);
    }


    /**
     * Local source -> local destination uses the direct fast path and therefore
     * has no Kafka sequence numbers.
     */
    public void acceptLocalSourceDone(int uid, String resultId, String rootAlias, int childIndex,
                                      int expectedWorkers, int localWorker) {

        StageState state = state(uid, resultId, rootAlias, childIndex, expectedWorkers);

        Integer previous = state.lastSequenceBySource.get(localWorker);

        if (previous != null && previous.intValue() != -1) {

            throw new IllegalStateException("Conflicting local Phase-2 completion metadata." + " uid=" + uid +
                    ", resultId=" + resultId + ", root=" + rootAlias + ", childIndex=" + childIndex +
                    ", worker=" + localWorker + ", previous=" + previous);
        }

        state.lastSequenceBySource.put(localWorker, -1);
    }


    public boolean markCompleteIfReady(int uid, String resultId, String rootAlias, int childIndex) {
        StageState state = states.get(new StageKey(uid, resultId, rootAlias, childIndex));
        if (state == null || state.completeEmitted) {
            return false;
        }

        for (int source = 0; source < state.expectedWorkers; source++) {

            Integer lastSequence = state.lastSequenceBySource.get(source);

            if (lastSequence == null) {
                return false;
            }

            if (lastSequence >= 0) {
                Set<Integer> received = state.receivedSequences.get(source);
                if (received == null) {
                    return false;
                }

                for (int sequence = 0; sequence <= lastSequence; sequence++) {
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


    private StageState state(int uid, String resultId, String rootAlias, int childIndex, int expectedWorkers) {

        StageKey key = new StageKey(uid, resultId, rootAlias, childIndex);
        StageState state = states.get(key);

        if (state == null) {
            state = new StageState(expectedWorkers);
            states.put(key, state);
        } else if (state.expectedWorkers != expectedWorkers) {
            throw new IllegalStateException("Conflicting expectedWorkers for Phase-2 stage " + key +
                    ": " + state.expectedWorkers + " vs " + expectedWorkers);
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
        private final String resultId;
        private final String rootAlias;
        private final int childIndex;


        private StageKey(int uid, String resultId, String rootAlias, int childIndex) {

            this.uid = uid;
            this.resultId = resultId == null ? "" : resultId;
            this.rootAlias = rootAlias == null ? "" : rootAlias;
            this.childIndex = childIndex;
        }


        @Override
        public boolean equals(Object other) {

            if (!(other instanceof StageKey)) {
                return false;
            }

            StageKey o = (StageKey) other;

            return uid == o.uid
                    && childIndex == o.childIndex
                    && Objects.equals(resultId, o.resultId)
                    && Objects.equals(rootAlias, o.rootAlias);
        }


        @Override
        public int hashCode() {
            return Objects.hash(uid, resultId, rootAlias, childIndex);
        }


        @Override
        public String toString() {
            return uid + "|" + resultId + "|" + rootAlias + "|PHASE2_CHILD_" + childIndex;
        }
    }
}