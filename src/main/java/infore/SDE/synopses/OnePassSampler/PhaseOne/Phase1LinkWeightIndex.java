package infore.SDE.synopses.OnePassSampler.PhaseOne;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Hash-table implementation for one link-node partition.
 * Stores aggregated subtree/group weight by join value.
 */
public class Phase1LinkWeightIndex implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String edgeId;
    private final Map<JoinValue, Double> weightsByJoinValue;

    public Phase1LinkWeightIndex(String edgeId) {
        this.edgeId = edgeId;
        this.weightsByJoinValue = new LinkedHashMap<JoinValue, Double>();
    }

    public String getEdgeId() {
        return edgeId;
    }

    public void add(JoinValue key, double delta) {
        if (key == null) {
            throw new IllegalArgumentException("JoinValue key must not be null");
        }
        if (delta == 0.0d) {
            return;
        }
        Double current = weightsByJoinValue.get(key);
        if (current == null) {
            current = 0.0d;
        }
        weightsByJoinValue.put(key, current + delta);
    }

    //Lookup method, if the key exists return the aggregated weight else 0
    public double getOrZero(JoinValue key) {
        Double v = weightsByJoinValue.get(key);
        return v == null ? 0.0d : v;
    }

    public boolean containsKey(JoinValue key) {
        return weightsByJoinValue.containsKey(key);
    }

    public Map<JoinValue, Double> getRawView() {
        return Collections.unmodifiableMap(weightsByJoinValue);
    }

    public Map<String, Double> toDebugMap() {
        Map<String, Double> out = new LinkedHashMap<String, Double>();
        for (Map.Entry<JoinValue, Double> e : weightsByJoinValue.entrySet()) {
            out.put(e.getKey().toString(), e.getValue());
        }
        return out;
    }

    //Used for future distributed implementation, merges key computations from diff workers
    public void mergeFrom(Phase1LinkWeightIndex other) {
        if (other == null) {
            return;
        }
        if (!this.edgeId.equals(other.edgeId)) {
            throw new IllegalArgumentException(
                    "Cannot merge Phase1LinkWeightIndex with different edge ids: "
                            + this.edgeId + " vs " + other.edgeId);
        }
        for (Map.Entry<JoinValue, Double> e : other.weightsByJoinValue.entrySet()) {
            add(e.getKey(), e.getValue());
        }
    }
}