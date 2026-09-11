package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.PhaseOne.*;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeResult;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeState;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassShardedPhaseThreeState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassPhaseTwoState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassShardedPhaseTwoState;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassShardOwnership;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

import java.io.Serializable;
import java.util.Map;

/**
 * One-pass* lifecycle coordinator.
 * This is the high-level One-pass* object that owns the query lifecycle:
 *
 *   PHASE_1:
 *       Read side relations in leaf-to-root order and build Phase 1 indexes.
 *   PHASE_2:
 *       Read the root relation once and sample root groups.
 *
 *   PHASE_3:
 *       Not implemented yet. Later this will replay side relations and extend
 *       sampled roots into full join samples.
 *
 * For now this class is intentionally testable without Kafka/SDE plumbing.
 * Later, the SDE-facing synopsis wrapper can call the same methods:
 *
 *   add(...)
 *   finishPhaseOne()
 *   finishPhaseTwo()
 *   getPhaseOneResult()
 *   getPhaseTwoResult()
 */
public final class OnePassSamplerSynopsis implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Phase {
        PHASE_1,
        PHASE_2,
        PHASE_3,
        DONE
    }

    private final CompiledOnePassPlan plan;
    private final String rootAlias;

    private Phase phase;

    private final OnePassPhaseOneState phaseOneState;
    private OnePassPhaseOneResult phaseOneResult;

    private OnePassPhaseTwoState phaseTwoState;
    private OnePassRootSampleResult phaseTwoResult;
    private OnePassShardedPhaseTwoState shardedPhaseTwoState;
    private boolean shardedPhaseTwoComplete;

    private OnePassPhaseThreeState phaseThreeState;
    private OnePassPhaseThreeResult phaseThreeResult;

    private OnePassShardedPhaseThreeState shardedPhaseThreeState;
    private boolean shardedPhaseThreeComplete;

    public OnePassSamplerSynopsis(CompiledOnePassPlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }

        if (plan.getRootAlias() == null || plan.getRootAlias().trim().isEmpty()) {
            throw new IllegalArgumentException("plan root alias must not be blank");
        }

        this.plan = plan;
        this.rootAlias = plan.getRootAlias();

        this.phase = Phase.PHASE_1;

        this.phaseOneState =
                new OnePassPhaseOneState(
                        plan,
                        new OnePassWeightEvaluator(plan.getWeightSpec())
                );

        this.phaseOneResult = null;
        this.phaseTwoState = null;
        this.phaseTwoResult = null;
        this.shardedPhaseTwoState = null;
        this.shardedPhaseTwoComplete = false;
        this.phaseThreeState = null;
        this.phaseThreeResult = null;
        this.shardedPhaseThreeState = null;
        this.shardedPhaseThreeComplete = false;
    }

    /**
     * Adds one payload to the currently active phase.
     *
     * In PHASE_1, only non-root aliases are accepted.
     * In PHASE_2, only the root alias is accepted.
     */
    public void add(Object payload) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        addTuple(tuple);
    }

    public void addTuple(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (phase == Phase.PHASE_1) {
            addPhaseOneTuple(tuple);
            return;
        }

        if (phase == Phase.PHASE_2) {
            addPhaseTwoTuple(tuple);
            return;
        }

        if (phase == Phase.PHASE_3) {
            addPhaseThreeTuple(tuple);
            return;
        }

        throw new IllegalStateException(
                "Cannot add tuple while OnePassSamplerSynopsis is in phase " + phase);
    }

    private void addPhaseOneTuple(OnePassTuple tuple) {
        String alias = tuple.getTable();

        if (rootAlias.equals(alias)) {
            throw new IllegalArgumentException("Received root alias '" + rootAlias + "' during PHASE_1. "
                    + "Root tuples must be processed only during PHASE_2.");
        }

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Unknown alias during PHASE_1: " + alias);
        }

        phaseOneState.addTuple(tuple);
    }

    private void addPhaseTwoTuple(OnePassTuple tuple) {

        String alias = tuple.getTable();

        if (!rootAlias.equals(alias)) {

            throw new IllegalArgumentException("Received non-root alias '" + alias +
                    "' during PHASE_2. " + "Expected root alias '" + rootAlias + "'.");
        }

        if (shardedPhaseTwoState != null) {
            throw new IllegalStateException(
                    "Generic lifecycle.add() must not be used for sharded Phase 2."
                            + " Use beginShardedPhaseTwoRootTuple(), "
                            + "lookupShardedPhaseTwoRootChildWeight(), and "
                            + "acceptShardedPhaseTwoRootCandidate()."
            );
        }

        if (phaseTwoState == null) {
            throw new IllegalStateException("Replicated Phase-2 state is null");
        }

        phaseTwoState.addTuple(tuple);
    }

    private void addPhaseThreeTuple(OnePassTuple tuple) {
        if (shardedPhaseThreeState != null) {
            throw new IllegalStateException("Generic lifecycle.add() must not be used for sharded Phase 3. "
                    + "Use beginShardedPhaseThreeCandidate(), "
                    + "lookupShardedPhaseThreeChildWeight(), and "
                    + "acceptShardedPhaseThreeCandidate().");
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException("Phase 3 state has not been initialized");
        }

        phaseThreeState.addTuple(tuple);
    }

    /**
     * Completes Phase 1 and initializes Phase 2.
     * This is the single-worker version of the Phase 1 barrier.
     * Later, for multiworker execution, this method should correspond to:
     *   all local Phase 1 states complete
     *   -> merge Phase 1 indexes globally
     *   -> distribute final Phase 1 result
     *   -> start Phase 2
     */
    public OnePassPhaseOneResult finishPhaseOne() {
        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException("finishPhaseOne() is only valid during PHASE_1. Current phase: " + phase);
        }

        //this.phaseOneResult = phaseOneState.exportResult();
        this.phaseTwoState = new OnePassPhaseTwoState(phaseOneResult);
        this.phase = Phase.PHASE_2;

        return phaseOneResult;
    }

    public OnePassPhaseOneResult installGlobalPhaseOneResult(
            OnePassPhaseOneResult globalPhaseOneResult) {

        return installGlobalPhaseOneResult(globalPhaseOneResult, true);
    }

    public OnePassPhaseOneResult installGlobalPhaseOneResult(
            OnePassPhaseOneResult globalPhaseOneResult,
            boolean phaseOneComplete) {

        if (globalPhaseOneResult == null) {
            throw new IllegalArgumentException("globalPhaseOneResult must not be null");
        }

        if (phase != Phase.PHASE_1 && phase != Phase.PHASE_2) {
            throw new IllegalStateException(
                    "installGlobalPhaseOneResult() is only valid during PHASE_1 or PHASE_2. Current phase: "
                            + phase
            );
        }

        /*
         * Important for multi-alias Phase 1:
         *
         * The installed global result must become the Phase 1 working state.
         * Example:
         *   after l is merged globally, every worker must use global l<->o
         *   while processing o.
         */
        this.phaseOneState.replaceWith(globalPhaseOneResult);
        this.phaseOneResult = globalPhaseOneResult;
        this.phaseTwoResult = null;

        if (phaseOneComplete) {
            this.phaseTwoState = new OnePassPhaseTwoState(globalPhaseOneResult);
            this.phase = Phase.PHASE_2;
        } else {
            this.phaseTwoState = null;
            this.phase = Phase.PHASE_1;
        }

        return this.phaseOneResult;
    }


    //Distributed sharded Phase-1 start Increments the original tuple count exactly once and returns ownWeight.
    public double beginShardedPhaseOneTuple(Object payload) {

        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException("beginShardedPhaseOneTuple() is only valid during PHASE_1. Current phase: " + phase);
        }

        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        return phaseOneState.beginShardedContribution(tuple);
    }


    /**
     * Reads one already-built child continuation entry.
     * The SDE sharded worker must have routed this work item to the owner of (childEdge, joinKey) before calling this.
     */
    public double lookupShardedPhaseOneChildWeight(Object payload, int childIndex) {

        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException("lookupShardedPhaseOneChildWeight() is only valid during PHASE_1. Current phase: " + phase);
        }

        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        return phaseOneState.lookupChildContinuationWeight(tuple, childIndex);
    }


    /**
     * Constructs the final parent-edge contribution after all child continuation
     * weights have been multiplied.
     */
    public OnePassPhaseOneContribution buildShardedPhaseOneParentContribution(Object payload, double subtreeWeight) {

        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException("buildShardedPhaseOneParentContribution() is only valid during PHASE_1. Current phase: " + phase);
        }

        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        return phaseOneState.buildParentContribution(tuple, subtreeWeight);
    }

    /**
     * Activates Phase 2 without constructing a replicated OnePassPhaseOneResult.
     * The local Phase-1 indexes remain inside phaseOneState.
     */
    public void startShardedPhaseTwo(int workerId) {

        if (phase == Phase.PHASE_2 && shardedPhaseTwoState != null) {
            return;
        }

        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException("startShardedPhaseTwo() is only valid while leaving PHASE_1." +
                    " Current phase=" + phase);
        }

        this.phaseTwoState = null;
        this.phaseTwoResult = null;
        this.shardedPhaseTwoState = new OnePassShardedPhaseTwoState(plan, workerId);
        this.shardedPhaseTwoComplete = false;
        this.phase = Phase.PHASE_2;
    }

    public double beginShardedPhaseTwoRootTuple(Object payload) {

        requireShardedPhaseTwoActive();
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        return shardedPhaseTwoState.beginRootTuple(tuple);
    }

    public double lookupShardedPhaseTwoRootChildWeight(Object payload, int childIndex) {

        requireShardedPhaseTwoActive();
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        return phaseOneState.lookupRootChildContinuationWeight(tuple, childIndex);
    }

    public void acceptShardedPhaseTwoRootCandidate(Object payload, double rootGroupWeight) {

        requireShardedPhaseTwoActive();
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        shardedPhaseTwoState.acceptCompletedRootTuple(tuple, rootGroupWeight);
    }

    private void requireShardedPhaseTwoActive() {
        if (phase != Phase.PHASE_2 || shardedPhaseTwoState == null) {
            throw new IllegalStateException("Sharded Phase 2 is not active." + " phase=" + phase +
                    ", state=" + shardedPhaseTwoState
            );
        }
    }

    /**
     * Compatibility overload. It preserves the old sharded Phase-2 endpoint
     * for direct callers that do not provide physical worker metadata.
     */
    public OnePassRootSampleResult installGlobalShardedPhaseTwoRootSampleResult(OnePassRootSampleResult globalPhaseTwoResult) {

        if (globalPhaseTwoResult == null) {
            throw new IllegalArgumentException("globalPhaseTwoResult must not be null");
        }

        requireShardedPhaseTwoActive();
        this.phaseTwoResult = globalPhaseTwoResult;
        this.shardedPhaseTwoComplete = true;
        return phaseTwoResult;
    }

    /**
     * Sharded Phase-2 installation path.
     * The replicated O(K) root sample is converted into bounded replicated
     * partial samples. No complete OnePassPhaseOneResult is created.
     * The lifecycle deliberately remains in PHASE_2 until the first
     * START_PHASE_3_ALIAS request is consumed. Request 86 is the distributed
     * installation barrier that triggers that stateless transition.
     */
    public OnePassRootSampleResult installGlobalShardedPhaseTwoRootSampleResult(
            OnePassRootSampleResult globalPhaseTwoResult, int workerId, int expectedWorkers) {

        installGlobalShardedPhaseTwoRootSampleResult(globalPhaseTwoResult);

        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers must be > 0");
        }
        if (workerId < 0 || workerId >= expectedWorkers) {
            throw new IllegalArgumentException("Invalid workerId=" + workerId + ", expectedWorkers=" + expectedWorkers);
        }

        this.shardedPhaseThreeState = new OnePassShardedPhaseThreeState(plan, phaseTwoResult, plan.getDatasetSeed(),
                workerId, expectedWorkers);
        this.shardedPhaseThreeComplete = false;
        this.phaseThreeState = null;
        this.phaseThreeResult = null;

        return phaseTwoResult;
    }

    public OnePassRootSampleResult finishPhaseTwo() {
        if (phase != Phase.PHASE_2) {
            throw new IllegalStateException(
                    "finishPhaseTwo() is only valid during PHASE_2. Current phase: "+ phase);
        }

        this.phaseTwoResult = phaseTwoState.exportResult();

        this.phaseThreeState = new OnePassPhaseThreeState(phaseOneResult, phaseTwoResult, plan.getDatasetSeed());
        this.phase = Phase.PHASE_3;

        return phaseTwoResult;
    }

    public void startShardedPhaseThreeAlias(String alias) {
        if (!shardedPhaseTwoComplete || phaseTwoResult == null) {
            throw new IllegalStateException("Cannot start sharded Phase 3 before the global Phase-2 " +
                    "root sample has been installed");
        }

        if (shardedPhaseThreeState == null) {
            throw new IllegalStateException("Sharded Phase-3 state has not been initialized");
        }

        if (phase != Phase.PHASE_2 && phase != Phase.PHASE_3) {
            throw new IllegalStateException("startShardedPhaseThreeAlias() requires PHASE_2 or PHASE_3. " +
                    "Current phase=" + phase);
        }

        if (shardedPhaseThreeComplete) {
            throw new IllegalStateException("Sharded Phase 3 is already complete");
        }

        shardedPhaseThreeState.startAlias(alias);
        this.phase = Phase.PHASE_3;
    }

    public double beginShardedPhaseThreeCandidate(Object payload) {
        requireShardedPhaseThreeActive();
        return shardedPhaseThreeState.beginCandidate(
                OnePassTupleExtractor.extract(payload));
    }

    /**
     * Local Phase-1 continuation lookup for one child edge. The caller must
     * already have routed the candidate to the owner of this edge/key.
     */
    public double lookupShardedPhaseThreeChildWeight(Object payload, int childIndex) {

        requireShardedPhaseThreeActive();
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        java.util.List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(tuple.getTable());
        if (childIndex < 0 || childIndex >= childEdges.size()) {
            throw new IllegalArgumentException("Invalid sharded Phase-3 childIndex=" + childIndex +
                    " for alias=" + tuple.getTable());
        }

        CompiledOnePassPlan.DirectedJoinEdge childEdge = childEdges.get(childIndex);
        JoinValue key = JoinValue.fromTuple(tuple, childEdge.getParentFields());

        int expectedOwner = OnePassShardOwnership.ownerForEdgeKey(childEdge.getEdgeId(), key,
                shardedPhaseThreeState.getExpectedWorkers());

        if (expectedOwner != shardedPhaseThreeState.getWorkerId()) {
            throw new IllegalStateException(
                    "Phase-3 continuation lookup attempted on wrong worker. alias="
                            + tuple.getTable()
                            + ", childIndex=" + childIndex
                            + ", edge=" + childEdge.getEdgeId()
                            + ", expectedOwner=" + expectedOwner
                            + ", actualWorker="
                            + shardedPhaseThreeState.getWorkerId());
        }

        return phaseOneState.lookupPhaseThreeChildContinuationWeight(tuple, childIndex);
    }

    public void acceptShardedPhaseThreeCandidate(Object payload, double candidateWeight) {
        requireShardedPhaseThreeActive();
        shardedPhaseThreeState.acceptCompletedCandidate(OnePassTupleExtractor.extract(payload), candidateWeight);
    }

    public java.util.List<java.util.Map<String, Object>> exportShardedPhaseThreeOwnedSelections() {
        requireShardedPhaseThreeActive();
        return shardedPhaseThreeState.exportOwnedSelections();
    }

    public void installGlobalShardedPhaseThreeAliasSelections(String alias, JsonNode selectionsNode) {

        if (phase != Phase.PHASE_3 || shardedPhaseThreeState == null) {
            throw new IllegalStateException("Sharded Phase 3 is not active. phase=" + phase);
        }

        shardedPhaseThreeState.installGlobalAliasSelections(alias, selectionsNode);

        if (shardedPhaseThreeState.areAllSamplesComplete()) {
            this.phaseThreeResult = shardedPhaseThreeState.buildResultIfComplete();
            this.shardedPhaseThreeComplete = true;
            this.phase = Phase.DONE;
        }
    }

    private void requireShardedPhaseThreeActive() {
        if (phase != Phase.PHASE_3 || shardedPhaseThreeState == null || !shardedPhaseThreeState.isAliasActive()) {
            throw new IllegalStateException(
                    "Sharded Phase 3 alias is not active. phase=" + phase
                            + ", state=" + shardedPhaseThreeState
                            + ", activeAlias="
                            + (shardedPhaseThreeState == null
                            ? null
                            : shardedPhaseThreeState.getActiveAlias()));
        }
    }

    public void startPhaseThreeAlias(String alias) {
        if (phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "startPhaseThreeAlias() is only valid during PHASE_3. Current phase: " + phase
            );
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException("Phase 3 state has not been initialized");
        }

        phaseThreeState.startAlias(alias);
    }

    public void finishPhaseThreeAlias() {
        if (phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "finishPhaseThreeAlias() is only valid during PHASE_3. Current phase: "
                            + phase
            );
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException(
                    "Phase 3 state has not been initialized"
            );
        }

        phaseThreeState.finishAlias();
    }

    public OnePassPhaseThreeResult finishPhaseThree() {
        if (phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "finishPhaseThree() is only valid during PHASE_3. Current phase: "
                            + phase
            );
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException(
                    "Phase 3 state has not been initialized"
            );
        }

        if (phaseThreeState.isAliasActive()) {
            throw new IllegalStateException(
                    "Cannot finish Phase 3 while alias '"
                            + phaseThreeState.getActiveAlias()
                            + "' is still active. Call finishPhaseThreeAlias() first."
            );
        }

        this.phaseThreeResult =
                phaseThreeState.finish();

        this.phase =
                Phase.DONE;

        return phaseThreeResult;
    }

    public double computeRootGroupWeight(Object payload) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        return computeRootGroupWeight(tuple);
    }

    public double computeRootGroupWeight(OnePassTuple tuple) {
        if (phase != Phase.PHASE_2) {
            throw new IllegalStateException(
                    "computeRootGroupWeight() is only valid during PHASE_2. Current phase: "
                            + phase
            );
        }

        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (!rootAlias.equals(tuple.getTable())) {
            throw new IllegalArgumentException(
                    "Expected root alias '"
                            + rootAlias
                            + "' but received alias '"
                            + tuple.getTable()
                            + "'"
            );
        }

        return phaseTwoState.computeRootGroupWeight(tuple);
    }

    public Phase getPhase() {
        return phase;
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public OnePassPhaseOneResult getPhaseOneResult() {
        return phaseOneResult;
    }

    public OnePassRootSampleResult getPhaseTwoResult() {
        return phaseTwoResult;
    }

    public boolean isPhaseOneComplete() {
        return phaseOneResult != null;
    }

    public boolean isPhaseTwoComplete() {
        return phaseTwoResult != null;
    }

    public OnePassPhaseThreeResult getPhaseThreeResult() {
        return phaseThreeResult;
    }

    public boolean isPhaseThreeComplete() {
        return phaseThreeResult != null || shardedPhaseThreeComplete;
    }

    public boolean isPhaseThreeAliasActive() {
        if (shardedPhaseThreeState != null) {
            return shardedPhaseThreeState.isAliasActive();
        }
        return phaseThreeState != null && phaseThreeState.isAliasActive();
    }

    public String getPhaseThreeActiveAlias() {
        if (shardedPhaseThreeState != null) {
            return shardedPhaseThreeState.getActiveAlias();
        }
        return phaseThreeState == null ? null : phaseThreeState.getActiveAlias();
    }

    public OnePassShardedPhaseThreeState getShardedPhaseThreeState() {
        return shardedPhaseThreeState;
    }

    public boolean isShardedPhaseThreeActive() {
        return phase == Phase.PHASE_3 && shardedPhaseThreeState != null && shardedPhaseThreeState.isAliasActive();
    }

    public boolean isShardedPhaseThreeComplete() {
        return shardedPhaseThreeComplete;
    }

    public OnePassShardedPhaseTwoState getShardedPhaseTwoState() {
        return shardedPhaseTwoState;
    }

    public boolean isShardedPhaseTwoActive() {
        return phase == Phase.PHASE_2
                && shardedPhaseTwoState != null;
    }

    public boolean isShardedPhaseTwoComplete() {
        return shardedPhaseTwoComplete;
    }

    public OnePassPhaseTwoState getPhaseTwoState() {
        return phaseTwoState;
    }

    public String getDatasetSeed() {
        return plan.getDatasetSeed();
    }

    @Override
    public String toString() {
        return "OnePassSamplerSynopsis{" +
                "queryName='" + plan.getQueryName() + '\'' +
                ", rootAlias='" + rootAlias + '\'' +
                ", phase=" + phase +
                ", leafToRootOrder=" + plan.getLeafToRootOrder() +
                ", rootToLeafOrder=" + plan.getRootToLeafOrder() +
                '}';
    }

    public OnePassRootSampleResult installGlobalPhaseTwoRootSampleResult(
            OnePassRootSampleResult globalPhaseTwoResult) {

        if (globalPhaseTwoResult == null) {
            throw new IllegalArgumentException("globalPhaseTwoResult must not be null");
        }

        if (phase != Phase.PHASE_2 && phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "installGlobalPhaseTwoRootSampleResult() is only valid during PHASE_2 or PHASE_3. Current phase: "
                            + phase
            );
        }

        if (phaseOneResult == null) {
            throw new IllegalStateException("Cannot install Phase 2 root sample before Phase 1 result is available.");
        }

        this.phaseTwoResult = globalPhaseTwoResult;
        this.phaseThreeState = new OnePassPhaseThreeState(
                phaseOneResult,
                phaseTwoResult,
                plan.getDatasetSeed()
        );
        this.phase = Phase.PHASE_3;

        return this.phaseTwoResult;
    }

    public Map<String, Object> exportPhaseThreeActiveAliasLocalChoices() {
        if (phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "exportPhaseThreeActiveAliasLocalChoices() is only valid during PHASE_3. Current phase: "
                            + phase);
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException("Phase 3 state has not been initialized");
        }

        return phaseThreeState.exportActiveAliasLocalChoices();
    }

    public void installGlobalPhaseThreeAliasSelections(String alias, JsonNode selectionsNode) {
        if (phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "installGlobalPhaseThreeAliasSelections() is only valid during PHASE_3. Current phase: "
                            + phase);
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException("Phase 3 state has not been initialized");
        }

        phaseThreeState.installGlobalAliasSelections(alias, selectionsNode);
    }

    public OnePassPhaseOneResult exportLocalP1ResultForDistMerge(String activeAlias, String activeEdgeId,
                                                                 boolean includeStableState) {

        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException("exportLocalPhaseOneResultForDistributedMerge() "
                    + "is only valid during PHASE_1. Current phase: " + phase);
        }

        return phaseOneState.exportForDistributedMerge(activeAlias, activeEdgeId, includeStableState);
    }

    public OnePassPhaseOneContribution computePhaseOneContribution(Object payload) {
        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException(
                    "computePhaseOneContribution() is only valid during PHASE_1. Current phase: " + phase);
        }

        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        return phaseOneState.computeContribution(tuple);
    }

    public void applyPhaseOneContribution(String edgeId, JoinValue joinKey, double delta) {
        if (phase != Phase.PHASE_1) {
            throw new IllegalStateException(
                    "applyPhaseOneContribution() is only valid during PHASE_1. Current phase: " + phase);
        }

        phaseOneState.applyContribution(edgeId, joinKey, delta);
    }

    public Map<String, Object> getLocalPhaseOneEdgeSummary(String edgeId) {
        return phaseOneState.localEdgeSummary(edgeId);
    }

    public long getLocalPhaseOneSeenTupleCount(String alias) {
        return phaseOneState.getSeenTupleCount(alias);
    }

    /**
     * DEBUG / VALIDATION ONLY.
     */
    public Map<String, Map<String, Double>> debugCopyPhaseOneRawIndexesForValidator() {
        return phaseOneState.debugCopyRawIndexesForValidator();
    }
}