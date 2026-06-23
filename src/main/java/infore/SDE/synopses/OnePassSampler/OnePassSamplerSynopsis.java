package infore.SDE.synopses.OnePassSampler;

import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneState;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassPhaseTwoState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

import java.io.Serializable;

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

        throw new IllegalStateException(
                "Cannot add tuple while OnePassSamplerSynopsis is in phase "
                        + phase
        );
    }

    private void addPhaseOneTuple(OnePassTuple tuple) {
        String alias = tuple.getTable();

        if (rootAlias.equals(alias)) {
            throw new IllegalArgumentException(
                    "Received root alias '"
                            + rootAlias
                            + "' during PHASE_1. "
                            + "Root tuples must be processed only during PHASE_2."
            );
        }

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException(
                    "Unknown alias during PHASE_1: " + alias
            );
        }

        phaseOneState.addTuple(tuple);
    }

    private void addPhaseTwoTuple(OnePassTuple tuple) {
        String alias = tuple.getTable();

        if (!rootAlias.equals(alias)) {
            throw new IllegalArgumentException(
                    "Received non-root alias '"
                            + alias
                            + "' during PHASE_2. "
                            + "Expected root alias '" + rootAlias + "'."
            );
        }

        phaseTwoState.addTuple(tuple);
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
            throw new IllegalStateException(
                    "finishPhaseOne() is only valid during PHASE_1. Current phase: "
                            + phase
            );
        }

        this.phaseOneResult = phaseOneState.exportResult();
        this.phaseTwoState = new OnePassPhaseTwoState(phaseOneResult);
        this.phase = Phase.PHASE_2;

        return phaseOneResult;
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
                    "finishPhaseTwo() is only valid during PHASE_2. Current phase: "
                            + phase
            );
        }

        this.phaseTwoResult = phaseTwoState.exportResult();

        /*
         * Later this should become:
         *
         *   phaseThreeState = new OnePassPhaseThreeState(...);
         *   phase = Phase.PHASE_3;
         *
         * For now Phase 3 is not implemented, so the lifecycle stops here.
         */
        this.phase = Phase.DONE;

        return phaseTwoResult;
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
}