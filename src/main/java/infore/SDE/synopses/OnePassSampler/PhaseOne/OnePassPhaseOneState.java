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
     * Single-worker/local compatibility path.
     *
     * In a single worker every child index is local, so this can walk every child
     * directly. Distributed sharded execution uses the split methods below.
     */
    public OnePassPhaseOneContribution computeContribution(OnePassTuple tuple) {

        double partialWeight = beginShardedContribution(tuple);
        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(tuple.getTable());

        for (int childIndex = 0; childIndex < childEdges.size(); childIndex++) {
            partialWeight *= lookupChildContinuationWeight(tuple, childIndex);

            if (partialWeight == 0.0d) {
                break;
            }
        }

        return buildParentContribution(tuple, partialWeight);
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

    /**
     * DEBUG / VALIDATION ONLY.
     *
     * Returns a detached copy of the exact Phase-1 indexes physically stored
     * on this worker.
     *
     * IMPORTANT:
     * This method does not rebuild or calculate anything.
     * It directly reads Phase1LinkWeightIndex.getRawView().
     *
     * Output format:
     *
     *   edgeId -> joinKeyText -> weight
     *
     * which is exactly the structure expected under the validator's
     * "edgeIndexes" field.
     */
    public Map<String, Map<String, Double>> debugCopyRawIndexesForValidator() {

        Map<String, Map<String, Double>> result = new LinkedHashMap<String, Map<String, Double>>();

        for (Map.Entry<String, Phase1LinkWeightIndex> edgeEntry : indexByEdgeId.entrySet()) {

            String edgeId = edgeEntry.getKey();
            Phase1LinkWeightIndex index = edgeEntry.getValue();

            Map<String, Double> entries = new LinkedHashMap<String, Double>();

            if (index != null) {

                for (Map.Entry<JoinValue, Double> entry : index.getRawView().entrySet()) {

                    JoinValue joinValue = entry.getKey();

                    if (joinValue == null) {
                        throw new IllegalStateException("DEBUG EXPORT found null JoinValue in edge " + edgeId);
                    }

                    Double weight = entry.getValue();

                    String joinKeyText = debugJoinValueToValidatorText(joinValue);

                    entries.put(joinKeyText, weight == null ? 0.0d : weight.doubleValue());
                }
            }

            result.put(edgeId, entries);
        }

        return result;
    }


    /**
     * Independent debug formatting of JoinValue.
     *
     * We deliberately do not call JoinValue.toString() here so the validation
     * export does not depend on the production/debug string formatter.
     *
     * The Python validator uses:
     *
     *   single field: value
     *   composite:    value1|value2|...
     */
    private static String debugJoinValueToValidatorText(JoinValue joinValue) {

        List<String> parts = joinValue.getParts();

        if (parts == null || parts.isEmpty()) {
            throw new IllegalStateException("DEBUG EXPORT encountered empty JoinValue");
        }

        if (parts.size() == 1) {
            return parts.get(0);
        }

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < parts.size(); i++) {

            if (i > 0) {
                builder.append("|");
            }

            builder.append(parts.get(i));
        }

        return builder.toString();
    }

    /**
     * Distributed Phase-1 tuple start.
     *
     * IMPORTANT:
     * This is the ONLY distributed method that increments seenTuplesByAlias.
     * Enrichment hops must never increment the original-tuple counter again.
     */
    public double beginShardedContribution(OnePassTuple tuple) {

        validatePhaseOneTuple(tuple);
        String alias = tuple.getTable();
        seenTuplesByAlias.compute(alias, (k, current) -> current == null ? 1L : current + 1L);

        return weightEvaluator.evaluate(tuple);
    }


    /**
     * Reads exactly one child continuation entry.
     *
     * The distributed caller must route the work item to the deterministic owner
     * of this child edge/key before calling this method.
     */
    public double lookupChildContinuationWeight(OnePassTuple tuple, int childIndex) {

        validatePhaseOneTuple(tuple);
        String alias = tuple.getTable();

        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);

        if (childIndex < 0 || childIndex >= childEdges.size()) {
            throw new IllegalArgumentException("Invalid childIndex=" + childIndex + " for alias=" + alias +
                    ", childCount=" + childEdges.size());
        }

        CompiledOnePassPlan.DirectedJoinEdge childEdge = childEdges.get(childIndex);
        JoinValue lookupKey = JoinValue.fromTuple(tuple, childEdge.getParentFields());

        Phase1LinkWeightIndex childIndexState = indexByEdgeId.get(childEdge.getEdgeId());

        if (childIndexState == null) {
            throw new IllegalStateException("Missing local child index for edge " + childEdge.getEdgeId() +
                    ", alias=" + alias + ", childIndex=" + childIndex);
        }

        return childIndexState.getOrZero(lookupKey);
    }


    /**
     * Constructs the final contribution to this alias's parent edge.
     *
     * No index is mutated here. SDEcoFlatMap decides the deterministic owner of
     * the resulting (edgeId, joinKey) and either applies it locally or transfers
     * it through the State Topic.
     */
    public OnePassPhaseOneContribution buildParentContribution(OnePassTuple tuple, double subtreeWeight) {

        validatePhaseOneTuple(tuple);
        String alias = tuple.getTable();
        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);

        if (parentEdge == null) {
            throw new IllegalStateException("Non-root alias '" + alias + "' unexpectedly has no parent edge");
        }

        JoinValue parentKey = JoinValue.fromTuple(tuple, parentEdge.getChildFields());

        return new OnePassPhaseOneContribution(parentEdge.getEdgeId(), parentKey, subtreeWeight
        );
    }


    private void validatePhaseOneTuple(OnePassTuple tuple) {

        if (tuple == null) {throw new IllegalArgumentException("tuple must not be null");}

        String alias = tuple.getTable();

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Tuple alias/table '" + alias + "' is not part of compiled plan");
        }

        if (plan.isRoot(alias)) {
            throw new IllegalArgumentException("Root alias '" + alias + "' must not be processed during Phase 1"
            );
        }
    }
}