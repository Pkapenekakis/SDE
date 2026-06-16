package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 * Serializable output of One-pass* Phase 1.
 *
 * This is the reusable object that Phase 2 and the later extension phase should
 * consume instead of reading the debugging snapshot. It contains the compiled
 * acyclic join plan and one immutable-by-convention copy of every link-weight
 * index produced during the side-stream preprocessing pass.
 *
 * The public JavaBean getters intentionally expose JSON-safe views so that SDE
 * can return the object through Estimation.toJsonString(). Internal algorithmic
 * consumers should use indexForEdge(...), lookupChildSubtreeWeight(...), or
 * copyRawIndexesByEdgeId().
 */
public final class OnePassPhaseOneResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CompiledOnePassPlan plan;
    private final Map<String, Phase1LinkWeightIndex> indexByEdgeId;
    private final Map<String, Long> seenTuplesByAlias;

    public OnePassPhaseOneResult(CompiledOnePassPlan plan,
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
        Map<String, Phase1LinkWeightIndex> copy = new LinkedHashMap<String, Phase1LinkWeightIndex>();
        if (source != null) {
            for (Map.Entry<String, Phase1LinkWeightIndex> e : source.entrySet()) {
                Phase1LinkWeightIndex index = e.getValue();
                copy.put(e.getKey(), index == null
                        ? new Phase1LinkWeightIndex(e.getKey())
                        : index.copy());
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

    /**
     * JSON-safe representation of the edge indexes.
     */
    public Map<String, Map<String, Double>> getEdgeIndexes() {
        Map<String, Map<String, Double>> out = new LinkedHashMap<String, Map<String, Double>>();
        for (Map.Entry<String, Phase1LinkWeightIndex> e : indexByEdgeId.entrySet()) {
            out.put(e.getKey(), e.getValue().toDebugMap());
        }
        return out;
    }

    /**
     * Internal algorithmic accessor. Returns a defensive copy.
     */
    public Phase1LinkWeightIndex indexForEdge(String edgeId) {
        Phase1LinkWeightIndex index = indexByEdgeId.get(edgeId);
        return index == null ? null : index.copy();
    }

    /**
     * Internal algorithmic accessor for reducers or later phases.
     */
    public Map<String, Phase1LinkWeightIndex> copyRawIndexesByEdgeId() {
        return copyIndexes(indexByEdgeId);
    }

    /**
     * Lookup the aggregated child-subtree weight for a parent tuple and edge.
     *
     * In One-pass*, Phase 2 will use this value when calculating the root group
     * weight:
     *
     *   rootWeight(rootTuple) = ownWeight(rootTuple)
     *                           * product(childSubtreeWeight(edge, rootTuple))
     */
    public double lookupChildSubtreeWeight(CompiledOnePassPlan.DirectedJoinEdge childEdge,
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
        JoinValue parentJoinKey = JoinValue.fromTuple(parentTuple, childEdge.getParentFields());
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
