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
        OnePassPhaseOneContribution contribution = computeContribution(tuple);

        if (contribution != null) {
            applyContribution(contribution);
        }
    }

    /**
     * Calculates the Phase-1 contribution but does not decide where it is stored.
     * Distributed SDE uses this method and then applies the contribution locally
     * or transfers it to the owner through State Topic.
     */
    public OnePassPhaseOneContribution computeContribution(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        final String alias = tuple.getTable();

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException(
                    "Tuple alias/table '" + alias + "' is not part of compiled plan");
        }

        if (plan.isRoot(alias)) {
            throw new IllegalArgumentException(
                    "Root alias '" + alias + "' must not be processed during Phase 1");
        }

        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);

        if (childEdges.size() > 1) {
            throw new UnsupportedOperationException(
                    "Sharded Phase 1 v1 supports at most one child edge per alias. "
                            + "Alias '" + alias + "' has " + childEdges.size() + " child edges."
            );
        }

        seenTuplesByAlias.put(alias, seenTuplesByAlias.get(alias) + 1L);

        final double ownWeight = weightEvaluator.evaluate(tuple);
        double continuationWeight = 1.0d;

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge : childEdges) {
            JoinValue lookupKey = JoinValue.fromTuple(tuple, childEdge.getParentFields());
            Phase1LinkWeightIndex childIndex = indexByEdgeId.get(childEdge.getEdgeId());

            if (childIndex == null) {
                throw new IllegalStateException(
                        "Missing local child index for edge " + childEdge.getEdgeId());
            }

            continuationWeight *= childIndex.getOrZero(lookupKey);
        }

        final double subtreeWeight = ownWeight * continuationWeight;

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);
        if (parentEdge == null) {
            throw new IllegalStateException(
                    "Non-root alias '" + alias + "' unexpectedly has no parent edge");
        }

        JoinValue parentKey = JoinValue.fromTuple(tuple, parentEdge.getChildFields());

        return new OnePassPhaseOneContribution(
                parentEdge.getEdgeId(),
                parentKey,
                subtreeWeight
        );
    }

    public void applyContribution(OnePassPhaseOneContribution contribution) {
        if (contribution == null) {
            return;
        }

        applyContribution(
                contribution.getEdgeId(),
                contribution.getJoinKey(),
                contribution.getDelta()
        );
    }

    public void applyContribution(String edgeId, JoinValue key, double delta) {
        Phase1LinkWeightIndex index = indexByEdgeId.get(edgeId);

        if (index == null) {
            index = new Phase1LinkWeightIndex(edgeId);
            indexByEdgeId.put(edgeId, index);
        }

        index.add(key, delta);
    }

    public Map<String, Object> localEdgeSummary(String edgeId) {
        Phase1LinkWeightIndex index = indexByEdgeId.get(edgeId);

        if (index == null) {
            index = new Phase1LinkWeightIndex(edgeId);
        }

        return index.toSummaryMap(0);
    }

    public long getSeenTupleCount(String alias) {
        Long count = seenTuplesByAlias.get(alias);
        return count == null ? 0L : count.longValue();
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

    /**
     * Export used by distributed Phase 1 merge.
     *
     * Every worker exports its active edge.
     *
     * Exactly one designated worker additionally carries the already-global
     * stable edges so the reducer can construct the complete next global state
     * without P redundant copies of those stable indexes.
     */
    public OnePassPhaseOneResult exportForDistributedMerge(String activeAlias, String activeEdgeId,
                                                           boolean includeStableState) {

        if (activeAlias == null || activeAlias.trim().isEmpty()) {
            throw new IllegalArgumentException("activeAlias must not be blank");
        }

        if (activeEdgeId == null || activeEdgeId.trim().isEmpty()) {
            throw new IllegalArgumentException("activeEdgeId must not be blank");
        }

        Map<String, Phase1LinkWeightIndex> selectedIndexes = new LinkedHashMap<String, Phase1LinkWeightIndex>();

        /*
         * Active edge is required from every worker because it must be summed.
         */
        Phase1LinkWeightIndex activeIndex = indexByEdgeId.get(activeEdgeId);

        if (activeIndex == null) {
            activeIndex = new Phase1LinkWeightIndex(activeEdgeId);
        }

        selectedIndexes.put(activeEdgeId, activeIndex);

        /*
         * Stable global edges need to appear only once in the reducer input.
         */
        if (includeStableState) {
            for (Map.Entry<String, Phase1LinkWeightIndex> entry : indexByEdgeId.entrySet()) {

                if (activeEdgeId.equals(entry.getKey())) {
                    continue;
                }
                selectedIndexes.put(entry.getKey(), entry.getValue());
            }
        }

        Map<String, Long> selectedSeen = new LinkedHashMap<String, Long>();

        Long activeSeen = seenTuplesByAlias.get(activeAlias);

        selectedSeen.put(activeAlias, activeSeen == null ? 0L : activeSeen);
        /*
         * Previous aliases were already globally merged, so include those
         * counters once as stable metadata.
         */
        if (includeStableState) {

            for (Map.Entry<String, Long> entry : seenTuplesByAlias.entrySet()) {

                if (activeAlias.equals(entry.getKey())) {
                    continue;
                }

                selectedSeen.put(entry.getKey(), entry.getValue());
            }
        }

        return new OnePassPhaseOneResult(plan, selectedIndexes, selectedSeen);
    }
}