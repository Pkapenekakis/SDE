package infore.SDE.transformations.onepass.worker.PhaseThree;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Destination-local, sequence-aware completion tracker for one Phase-3 stage.
 *
 * Stage key:
 *   (uid, resultId, alias, stageIndex)
 *
 * A stage is complete only after every source worker declared SOURCE_DONE and
 * every sequence through that source's declared lastSequence has arrived.
 */
public final class OnePassPhaseThreeEnrichmentCompletionTracker
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<StageKey, StageState> states =
            new HashMap<StageKey, StageState>();

    public boolean acceptBatch(int uid, String resultId, String alias, int stageIndex, int expectedWorkers,
                               int sourceWorker, int sequence) {

        validateWorker(sourceWorker, expectedWorkers, "sourceWorker");
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be >= 0");
        }

        StageState state = state(uid, resultId, alias, stageIndex, expectedWorkers);

        Set<Integer> sequences = state.receivedSequences.computeIfAbsent(sourceWorker, k -> new HashSet<Integer>());

        return sequences.add(sequence);
    }

    public void acceptSourceDone(int uid, String resultId, String alias, int stageIndex, int expectedWorkers,
                                 int sourceWorker, int lastSequence) {

        validateWorker(sourceWorker, expectedWorkers, "sourceWorker");
        if (lastSequence < -1) {
            throw new IllegalArgumentException("lastSequence must be >= -1: " + lastSequence);
        }

        StageState state = state(uid, resultId, alias, stageIndex, expectedWorkers);

        Integer previous = state.lastSequenceBySource.get(sourceWorker);
        if (previous != null && previous != lastSequence) {
            throw new IllegalStateException("Conflicting Phase-3 SOURCE_DONE. uid=" + uid +
                    ", resultId=" + resultId + ", alias=" + alias + ", stageIndex=" + stageIndex +

                    ", source=" + sourceWorker + ", previous=" + previous + ", new=" + lastSequence);
        }

        state.lastSequenceBySource.put(sourceWorker, lastSequence);
    }

    /** Local source -> local destination uses the synchronous fast path. */
    public void acceptLocalSourceDone(int uid, String resultId, String alias, int stageIndex, int expectedWorkers,
                                      int localWorker) {

        validateWorker(localWorker, expectedWorkers, "localWorker");
        StageState state = state(uid, resultId, alias, stageIndex, expectedWorkers);

        Integer previous = state.lastSequenceBySource.get(localWorker);
        if (previous != null && previous != -1) {
            throw new IllegalStateException("Conflicting local Phase-3 completion metadata. uid=" + uid +
                    ", resultId=" + resultId + ", alias=" + alias + ", stageIndex=" + stageIndex +
                    ", worker=" + localWorker + ", previous=" + previous);
        }

        state.lastSequenceBySource.put(localWorker, -1);
    }

    public boolean markCompleteIfReady(int uid, String resultId, String alias, int stageIndex) {

        StageState state = states.get(new StageKey(uid, resultId, alias, stageIndex));

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
        Set<StageKey> keys = new HashSet<StageKey>(states.keySet());
        for (StageKey key : keys) {
            if (key.uid == uid) {
                states.remove(key);
            }
        }
    }

    private StageState state(int uid, String resultId, String alias, int stageIndex, int expectedWorkers) {

        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers must be > 0");
        }
        if (stageIndex < 0) {
            throw new IllegalArgumentException("stageIndex must be >= 0");
        }

        StageKey key = new StageKey(uid, resultId, alias, stageIndex);
        StageState state = states.get(key);

        if (state == null) {
            state = new StageState(expectedWorkers);
            states.put(key, state);
        } else if (state.expectedWorkers != expectedWorkers) {
            throw new IllegalStateException("Conflicting expectedWorkers for Phase-3 stage " + key
                    + ": " + state.expectedWorkers + " vs " + expectedWorkers);
        }

        return state;
    }

    private static void validateWorker(int worker, int expectedWorkers, String label) {

        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers must be > 0");
        }
        if (worker < 0 || worker >= expectedWorkers) {
            throw new IllegalArgumentException(label + " out of range: " + worker + ", expectedWorkers=" + expectedWorkers);
        }
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
        private final String alias;
        private final int stageIndex;

        private StageKey(int uid, String resultId, String alias, int stageIndex) {

            this.uid = uid;
            this.resultId = resultId == null ? "" : resultId.trim();
            this.alias = alias == null ? "" : alias.trim();
            this.stageIndex = stageIndex;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StageKey)) {
                return false;
            }

            StageKey o = (StageKey) other;
            return uid == o.uid && stageIndex == o.stageIndex && Objects.equals(resultId, o.resultId) &&
                    Objects.equals(alias, o.alias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, resultId, alias, stageIndex);
        }

        @Override
        public String toString() {
            return uid + "|" + resultId + "|" + alias + "|PHASE3_STAGE_" + stageIndex;
        }
    }
}