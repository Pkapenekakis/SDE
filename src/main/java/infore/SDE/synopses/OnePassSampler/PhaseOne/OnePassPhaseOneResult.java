package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializable output of One-pass* Phase 1.
 *
 * This object is the reusable result of the offline side-stream preprocessing pass.
 * Phase 2 will later use it as a read-only lookup structure while processing
 * the main stream online.
 */
public final class OnePassPhaseOneResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CompiledOnePassPlan plan;
    private final Map<String, Phase1LinkWeightIndex> indexByEdgeId;
    private final Map<String, Long> seenTuplesByAlias;

    public OnePassPhaseOneResult(
            CompiledOnePassPlan plan,
            Map<String, Phase1LinkWeightIndex> indexByEdgeId,
            Map<String, Long> seenTuplesByAlias) {

        if (plan == null) {
            throw new IllegalArgumentException("CompiledOnePassPlan must not be null");
        }

        this.plan = plan;
        this.indexByEdgeId = copyIndexes(indexByEdgeId);
        this.seenTuplesByAlias = copySeenTuples(seenTuplesByAlias);
    }

    private static Map<String, Phase1LinkWeightIndex> copyIndexes(
            Map<String, Phase1LinkWeightIndex> source) {

        Map<String, Phase1LinkWeightIndex> copy =
                new LinkedHashMap<String, Phase1LinkWeightIndex>();

        if (source != null) {
            for (Map.Entry<String, Phase1LinkWeightIndex> entry : source.entrySet()) {
                Phase1LinkWeightIndex index = entry.getValue();

                if (index == null) {
                    copy.put(entry.getKey(), new Phase1LinkWeightIndex(entry.getKey()));
                } else {
                    copy.put(entry.getKey(), index.copy());
                }
            }
        }

        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, Long> copySeenTuples(Map<String, Long> source) {
        Map<String, Long> copy = new LinkedHashMap<String, Long>();

        if (source != null) {
            copy.putAll(source);
        }

        return Collections.unmodifiableMap(copy);
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }

    public String getQueryName() {
        return plan.getQueryName();
    }

    public String getRootAlias() {
        return plan.getRootAlias();
    }

    public List<String> getLeafToRootOrder() {
        return plan.getLeafToRootOrder();
    }

    public List<String> getRootToLeafOrder() {
        return plan.getRootToLeafOrder();
    }

    public Map<String, Long> getSeenTuplesByAlias() {
        return seenTuplesByAlias;
    }

    public Map<String, Map<String, Double>> getEdgeIndexes() {
        Map<String, Map<String, Double>> out =
                new LinkedHashMap<String, Map<String, Double>>();

        for (Map.Entry<String, Phase1LinkWeightIndex> entry : indexByEdgeId.entrySet()) {
            out.put(entry.getKey(), entry.getValue().toDebugMap());
        }

        return out;
    }

    public Phase1LinkWeightIndex indexForEdge(String edgeId) {
        Phase1LinkWeightIndex index = indexByEdgeId.get(edgeId);
        return index == null ? null : index.copy();
    }

    public Map<String, Phase1LinkWeightIndex> copyRawIndexesByEdgeId() {
        return copyIndexes(indexByEdgeId);
    }

    /**
     * Used by Phase 2.
     *
     * Given a parent tuple, for example a main/root tuple, and one child edge,
     * this returns the total precomputed subtree weight that can join with that tuple.
     */
    public double lookupChildSubtreeWeight(
            CompiledOnePassPlan.DirectedJoinEdge childEdge,
            OnePassTuple parentTuple) {

        if (childEdge == null) {
            throw new IllegalArgumentException("childEdge must not be null");
        }

        if (parentTuple == null) {
            throw new IllegalArgumentException("parentTuple must not be null");
        }

        Phase1LinkWeightIndex index = indexByEdgeId.get(childEdge.getEdgeId());

        if (index == null) {
            return 0.0d;
        }

        JoinValue parentJoinKey =
                JoinValue.fromTuple(parentTuple, childEdge.getParentFields());

        return index.getOrZero(parentJoinKey);
    }

    public Map<String, Object> toDebugMap() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();

        out.put("queryName", getQueryName());
        out.put("rootAlias", getRootAlias());
        out.put("rootToLeafOrder", getRootToLeafOrder());
        out.put("leafToRootOrder", getLeafToRootOrder());
        out.put("seenTuplesByAlias", getSeenTuplesByAlias());
        out.put("edgeIndexes", getEdgeIndexes());

        return out;
    }

    @Override
    public String toString() {
        return "OnePassPhaseOneResult{" +
                "queryName='" + getQueryName() + '\'' +
                ", rootAlias='" + getRootAlias() + '\'' +
                ", edgeIndexes=" + indexByEdgeId.keySet() +
                ", seenTuplesByAlias=" + seenTuplesByAlias +
                '}';
    }
}