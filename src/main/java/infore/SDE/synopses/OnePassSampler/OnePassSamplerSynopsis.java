package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.PhaseOne.*;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeResult;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassPhaseTwoState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
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

    private OnePassPhaseThreeState phaseThreeState;
    private OnePassPhaseThreeResult phaseThreeResult;

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
        this.phaseThreeState = null;
        this.phaseThreeResult = null;
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
            throw new IllegalArgumentException(
                    "Received non-root alias '" + alias + "' during PHASE_2. " +
                            "Expected root alias '" + rootAlias + "'.");
        }

        phaseTwoState.addTuple(tuple);
    }

    private void addPhaseThreeTuple(OnePassTuple tuple) {
        if (phaseThreeState == null) {
            throw new IllegalStateException("Phase 3 state has not been initialized");
        }

        phaseThreeState.addTuple(tuple);
    }

    /**
     * Completes Phase 1 and initializes Phase 2.
     *
     * This is the single-worker version of the Phase 1 barrier.
     *
     * Later, for multiworker execution, this method should correspond to:
     *
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
     * Completes Phase 2.
     *
     * For now this ends the lifecycle because Phase 3 is not implemented yet.
     *
     * Later, this should initialize Phase 3 using:
     *
     *   phaseOneResult
     *   phaseTwoResult
     */
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

    public void startPhaseThreeAlias(String alias) {
        if (phase != Phase.PHASE_3) {
            throw new IllegalStateException(
                    "startPhaseThreeAlias() is only valid during PHASE_3. Current phase: "
                            + phase
            );
        }

        if (phaseThreeState == null) {
            throw new IllegalStateException(
                    "Phase 3 state has not been initialized"
            );
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
        return phaseThreeResult != null;
    }

    public boolean isPhaseThreeAliasActive() {
        return phaseThreeState != null && phaseThreeState.isAliasActive();
    }

    public String getPhaseThreeActiveAlias() {
        return phaseThreeState == null ? null : phaseThreeState.getActiveAlias();
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