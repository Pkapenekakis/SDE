package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OnePassPhaseOneState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CompiledOnePassPlan plan;
    private final OnePassWeightEvaluator weightEvaluator;
    private final Map<String, Phase1LinkWeightIndex> indexByEdgeId;
    private final Map<String, Long> seenTuplesByAlias;

    public OnePassPhaseOneState(CompiledOnePassPlan plan,
                                OnePassWeightEvaluator weightEvaluator) {
        if (plan == null) {
            throw new IllegalArgumentException("CompiledOnePassPlan must not be null");
        }
        if (weightEvaluator == null) {
            throw new IllegalArgumentException("OnePassWeightEvaluator must not be null");
        }

        this.plan = plan;
        this.weightEvaluator = weightEvaluator;
        this.indexByEdgeId = new LinkedHashMap<String, Phase1LinkWeightIndex>();
        this.seenTuplesByAlias = new LinkedHashMap<String, Long>();

        for (String alias : plan.getAliases()) {
            seenTuplesByAlias.put(alias, 0L);
            if (!plan.isRoot(alias)) {
                CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);
                if (parentEdge != null && !indexByEdgeId.containsKey(parentEdge.getEdgeId())) {
                    indexByEdgeId.put(parentEdge.getEdgeId(),
                            new Phase1LinkWeightIndex(parentEdge.getEdgeId()));
                }
            }
        }
    }

    public void addTuple(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        final String alias = tuple.getTable();

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException(
                    "Tuple alias/table '" + alias + "' is not part of compiled plan");
        }

        seenTuplesByAlias.put(alias, seenTuplesByAlias.get(alias) + 1L);

        if (plan.isRoot(alias)) {
            return;
        }

        final double ownWeight = weightEvaluator.evaluate(tuple);
        double continuationWeight = 1.0d;

        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);
        for (CompiledOnePassPlan.DirectedJoinEdge childEdge : childEdges) {
            JoinValue currentTupleAsParentKey = JoinValue.fromTuple(tuple, childEdge.getParentFields()); //Build the lookup key e.g ["o_custkey"]
            Phase1LinkWeightIndex childIndex = indexByEdgeId.get(childEdge.getEdgeId()); //Gets the child index e.g. B-C
            if (childIndex == null) {
                throw new IllegalStateException("Missing child index for edge " + childEdge.getEdgeId());
            }
            continuationWeight *= childIndex.getOrZero(currentTupleAsParentKey);
        }

        final double subtreeWeight = ownWeight * continuationWeight;
        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias); //Gets the parent edge of B e.g. A-B
        if (parentEdge == null) {
            throw new IllegalStateException(
                    "Non-root alias '" + alias + "' unexpectedly has no parent edge");
        }

        //Build the key for pushing upward (to parent)
        JoinValue childKey = JoinValue.fromTuple(tuple, parentEdge.getChildFields());
        Phase1LinkWeightIndex parentIndex = indexByEdgeId.get(parentEdge.getEdgeId());
        if (parentIndex == null) {
            throw new IllegalStateException("Missing parent index for edge " + parentEdge.getEdgeId());
        }
        parentIndex.add(childKey, subtreeWeight);
    }

    public Phase1LinkWeightIndex getIndex(String edgeId) {
        return indexByEdgeId.get(edgeId);
    }

    public OnePassPhaseOneResult exportResult() {
        return new OnePassPhaseOneResult(plan, indexByEdgeId, seenTuplesByAlias);
    }

    public Map<String, Object> debugSnapshot() {
        return exportResult().toDebugMap();
    }

    public void mergeFrom(OnePassPhaseOneState other) {
        if (other == null) {
            return;
        }

        if (!this.plan.getRootAlias().equals(other.plan.getRootAlias())
                || this.plan.getAliases().size() != other.plan.getAliases().size()) {
            throw new IllegalArgumentException("Cannot merge incompatible Phase 1 states");
        }

        for (Map.Entry<String, Phase1LinkWeightIndex> e : other.indexByEdgeId.entrySet()) {
            Phase1LinkWeightIndex mine = this.indexByEdgeId.get(e.getKey());
            if (mine == null) {
                mine = new Phase1LinkWeightIndex(e.getKey());
                this.indexByEdgeId.put(e.getKey(), mine);
            }
            mine.mergeFrom(e.getValue());
        }

        for (Map.Entry<String, Long> e : other.seenTuplesByAlias.entrySet()) {
            Long cur = this.seenTuplesByAlias.get(e.getKey());
            if (cur == null) {
                cur = 0L;
            }
            this.seenTuplesByAlias.put(e.getKey(), cur + e.getValue());
        }
    }

    public void replaceWith(OnePassPhaseOneResult result) {
        if (result == null) {
            throw new IllegalArgumentException("result must not be null");
        }

        this.indexByEdgeId.clear();

        Map<String, Phase1LinkWeightIndex> rawIndexes = result.copyRawIndexesByEdgeId();

        for (Map.Entry<String, Phase1LinkWeightIndex> entry : rawIndexes.entrySet()) {
            Phase1LinkWeightIndex index = entry.getValue();

            if (index == null) {
                this.indexByEdgeId.put(entry.getKey(), new Phase1LinkWeightIndex(entry.getKey()));
            } else {
                this.indexByEdgeId.put(entry.getKey(), index.copy());
            }
        }

        /*
         * Keep empty indexes for every non-root edge, even if the current
         * global result has not filled that edge yet.
         */
        for (String alias : plan.getAliases()) {
            if (!plan.isRoot(alias)) {
                CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                        plan.getParentEdge(alias);

                if (parentEdge != null
                        && !this.indexByEdgeId.containsKey(parentEdge.getEdgeId())) {
                    this.indexByEdgeId.put(
                            parentEdge.getEdgeId(),
                            new Phase1LinkWeightIndex(parentEdge.getEdgeId())
                    );
                }
            }
        }

        this.seenTuplesByAlias.clear();
        this.seenTuplesByAlias.putAll(result.getSeenTuplesByAlias());

        for (String alias : plan.getAliases()) {
            if (!this.seenTuplesByAlias.containsKey(alias)) {
                this.seenTuplesByAlias.put(alias, 0L);
            }
        }
    }
}