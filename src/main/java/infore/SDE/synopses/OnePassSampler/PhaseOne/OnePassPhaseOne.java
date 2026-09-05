package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.Synopsis;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Worker-local Phase 1 synopsis facade for One-pass*.
 *
 * In the sharded Phase-1 design this class is no longer a mergeable container
 * for a complete Phase-1 index.
 *
 * The large intermediate index remains partitioned across workers:
 *
 *   raw tuple
 *      -> computeContribution(...)
 *      -> local owner  : applyContribution(...) locally
 *      -> remote owner : batch/transfer through the OnePass State Topic
 *
 * Only small readiness metadata is federated globally.
 *
 * add(...) is kept as a single-worker/local-compatibility entry point:
 * it computes the contribution and immediately applies it to this local state.
 * The distributed SDE path should use computeContribution(...) and decide
 * ownership before applying the result.
 */
public class OnePassPhaseOne extends Synopsis {

    private final CompiledOnePassPlan plan;
    private final OnePassPhaseOneState state;

    public OnePassPhaseOne(int uid,CompiledOnePassPlan plan, WeightSpec weightSpec) {

        super(uid, "-1", "-1", "Queryable");

        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }

        this.plan = plan;
        this.state = new OnePassPhaseOneState(plan, new OnePassWeightEvaluator(weightSpec)
        );
    }

    /**
     * Single-worker compatibility path.
     *
     * For distributed sharded execution, do not use this method as the final
     * state-placement decision. Use computeContribution(...) and then route
     * the calculated contribution to its deterministic owner.
     */
    @Override
    public void add(Object payload) {

        OnePassPhaseOneContribution contribution = computeContribution(payload);

        if (contribution != null) {
            state.applyContribution(contribution);
        }
    }

    /**
     * Computes one already-calculated Phase-1 contribution without deciding
     * where it will be stored.
     *
     * The caller is responsible for:
     *
     *   owner == current worker -> applyContribution(...)
     *   owner != current worker -> OnePassPhaseOneTransferBuffer -> State Topic
     */
    public OnePassPhaseOneContribution computeContribution(Object payload) {

        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        return computeContribution(tuple);
    }

    public OnePassPhaseOneContribution computeContribution(OnePassTuple tuple) {

        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        return state.computeContribution(tuple);
    }

    /**
     * Local fast path for a contribution whose deterministic owner is this
     * worker.
     */
    public void applyContribution(OnePassPhaseOneContribution contribution) {

        state.applyContribution(contribution);
    }

    /**
     * Installation path used both by:
     *
     *   - locally calculated/local-owner contributions, and
     *   - incoming SHARD_BATCH entries from another worker.
     */
    public void applyContribution(String edgeId, JoinValue joinKey, double delta) {

        state.applyContribution(edgeId, joinKey, delta);
    }

    /**
     * Synopsis requires estimate(Object). Under sharded Phase 1 we return only
     * a lightweight LOCAL snapshot; we never export/reconstruct the complete
     * distributed Phase-1 index here.
     */
    @Override
    public Object estimate(Object k) {
        return buildLocalSnapshot();
    }

    /**
     * Same rule as estimate(Object): only worker-local diagnostic metadata is
     * exposed. This is NOT LOCAL_PHASE1_RESULT and must not be used to merge
     * the large Phase-1 index.
     */
    @Override
    public Estimation estimate(Request rq) {

        if (rq == null) {
            throw new IllegalArgumentException("request must not be null");
        }

        return new Estimation(
                rq,
                buildLocalSnapshot(),
                Integer.toString(rq.getUID())
        );
    }

    /**
     * Large Phase-1 synopsis state is deliberately not mergeable anymore.
     *
     * Global Phase-1 synchronization is:
     *
     *   LOCAL_PHASE1_SHARD_READY x P
     *      -> normal SDE federated reduction
     *      -> GLOBAL_PHASE1_ALIAS_READY
     *
     * Merging OnePassPhaseOne instances would re-introduce the large global
     * index bottleneck that the sharded design removes.
     */
    @Override
    public Synopsis merge(Synopsis sk) {

        throw new UnsupportedOperationException("OnePassPhaseOne state must not be globally merged in "
                        + "SHARDED_PHASE1_V1. Merge only LOCAL_PHASE1_SHARD_READY "
                        + "metadata through the SDE federated path.");
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }

    public OnePassPhaseOneState getState() {
        return state;
    }

    public Map<String, Object> getLocalEdgeSummary(String edgeId) {

        if (edgeId == null || edgeId.trim().isEmpty()) {
            throw new IllegalArgumentException("edgeId must not be blank");
        }

        return state.localEdgeSummary(edgeId.trim());
    }

    public long getSeenTupleCount(String alias) {

        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("alias must not be blank");
        }

        return state.getSeenTupleCount(alias.trim());
    }

    /**
     * Lightweight worker-local snapshot for debugging/status only.
     *
     * It contains summaries, never the full join-key -> weight maps.
     */
    public Map<String, Object> exportLocalSnapshot() {
        return buildLocalSnapshot();
    }

    private Map<String, Object> buildLocalSnapshot() {

        Map<String, Object> snapshot = new LinkedHashMap<String, Object>();

        snapshot.put("type", "LOCAL_PHASE1_SHARD_SNAPSHOT"
        );

        snapshot.put("protocol", "SHARDED_PHASE1_V1");

        snapshot.put("rootAlias", plan.getRootAlias());

        Map<String, Object> edgeSummaries = new LinkedHashMap<String, Object>();

        /*
         * Multiple aliases can theoretically refer to the same logical edge
         * while walking metadata, so keep edge IDs unique in the snapshot.
         */
        Set<String> seenEdgeIds = new LinkedHashSet<String>();

        for (String alias : plan.getLeafToRootOrder()) {

            CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);

            if (parentEdge == null) {
                continue;
            }

            String edgeId = parentEdge.getEdgeId();

            if (seenEdgeIds.add(edgeId)) {
                edgeSummaries.put(edgeId, state.localEdgeSummary(edgeId));
            }
        }

        snapshot.put("edgeSummaries", edgeSummaries);

        Map<String, Long> seenTuplesByAlias = new LinkedHashMap<String, Long>();

        for (String alias : plan.getAliases()) {
            seenTuplesByAlias.put(alias, state.getSeenTupleCount(alias));
        }

        snapshot.put("seenTuplesByAlias", seenTuplesByAlias);

        return snapshot;
    }
}
