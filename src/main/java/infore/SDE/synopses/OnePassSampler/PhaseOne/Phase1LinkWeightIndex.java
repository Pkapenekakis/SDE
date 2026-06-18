package infore.SDE.synopses.OnePassSampler.PhaseOne;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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

    public int size() {
        return weightsByJoinValue.size();
    }

    public double totalWeight() {
        double total = 0.0d;
        for (Double weight : weightsByJoinValue.values()) {
            if (weight != null) {
                total += weight;
            }
        }
        return total;
    }

    public Phase1LinkWeightIndex copy() {
        Phase1LinkWeightIndex copy = new Phase1LinkWeightIndex(edgeId);
        copy.weightsByJoinValue.putAll(this.weightsByJoinValue);
        return copy;
    }

    public Map<String, Double> toDebugMap() {
        Map<String, Double> out = new LinkedHashMap<String, Double>();
        for (Map.Entry<JoinValue, Double> e : weightsByJoinValue.entrySet()) {
            out.put(e.getKey().toString(), e.getValue());
        }
        return out;
    }

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

    public Map<String, Object> toSummaryMap(int sampleLimit) {
        Map<String, Object> out = new LinkedHashMap<String, Object>();

        out.put("numberOfKeys", size());
        out.put("totalWeight", totalWeight());

        Map<String, Double> sampleEntries = new LinkedHashMap<String, Double>();

        int count = 0;
        for (Map.Entry<JoinValue, Double> entry : weightsByJoinValue.entrySet()) {
            if (sampleLimit >= 0 && count >= sampleLimit) {
                break;
            }

            sampleEntries.put(entry.getKey().toString(), entry.getValue());
            count++;
        }

        out.put("sampleEntries", sampleEntries);

        return out;
    }
}