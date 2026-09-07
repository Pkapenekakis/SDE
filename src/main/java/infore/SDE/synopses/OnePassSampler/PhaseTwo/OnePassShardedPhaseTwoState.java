package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.List;

/**
 * Worker-local state for SHARDED Phase 2.
 *
 * This class deliberately does NOT own a complete OnePassPhaseOneResult.
 *
 * Phase-1 continuation indexes remain partitioned across the SDE workers.
 * SDEcoFlatMap moves each root computation through the workers that own the
 * required child continuation entries.
 *
 * Once all child weights for a root tuple have been accumulated, the final
 * weighted root candidate is inserted into this worker's local ES reservoir.
 */
public final class OnePassShardedPhaseTwoState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CompiledOnePassPlan plan;
    private final String rootAlias;
    private final int workerId;

    private final OnePassWeightEvaluator weightEvaluator;

    /*
     * Local ES reservoir only.
     *
     * Important:
     * use a worker-specific deterministic seed so that different workers do
     * not replay the same Random sequence.
     */
    private final OnlineMultinomialSampler<OnePassRootSampleCandidate> sampler;

    private long nextCandidateId;
    private long rootTuplesSeen;


    public OnePassShardedPhaseTwoState(CompiledOnePassPlan plan, int workerId) {

        if (plan == null) {throw new IllegalArgumentException("plan must not be null");
        }

        if (workerId < 0) {throw new IllegalArgumentException("workerId must be >= 0");
        }

        this.plan = plan;
        this.rootAlias = plan.getRootAlias();
        this.workerId = workerId;
        this.weightEvaluator = new OnePassWeightEvaluator(plan.getWeightSpec());

        String localReservoirSeed = safe(plan.getDatasetSeed()) + "|" + safe(plan.getQueryName()) +
                "|SHARDED_PHASE2|worker=" + workerId;

        this.sampler = new OnlineMultinomialSampler<OnePassRootSampleCandidate>(plan.getSampleSize(), localReservoirSeed);
        this.nextCandidateId = 0L;
        this.rootTuplesSeen = 0L;
    }


    /**
     * Called exactly once for each ORIGINAL root tuple.
     *
     * Returns root own weight.
     *
     * The root tuple may later move through several Phase-1 index owners, but
     * those enrichment hops must never increment rootTuplesSeen again.
     */
    public double beginRootTuple(OnePassTuple tuple) {

        validateRootTuple(tuple);
        rootTuplesSeen++;

        double ownWeight = weightEvaluator.evaluate(tuple);
        validateNonNegativeFinite(ownWeight, "rootOwnWeight");

        return ownWeight;
    }


    /**
     * Called after the root tuple has visited ALL child continuation indexes.
     *
     * weight == 0 is ignored.
     *
     * Positive root tuples enter the local mergeable ES reservoir.
     */
    public void acceptCompletedRootTuple(OnePassTuple tuple, double rootGroupWeight) {

        validateRootTuple(tuple);
        validateNonNegativeFinite(rootGroupWeight, "rootGroupWeight");

        if (rootGroupWeight == 0.0d) {
            return;
        }

        OnePassRootSampleCandidate candidate = new OnePassRootSampleCandidate(nextCandidateId++, rootAlias,
                tuple.getRawJson(), rootGroupWeight);

        sampler.add(candidate, rootGroupWeight);
    }


    public List<WeightedReservoirEntry<OnePassRootSampleCandidate>> getOrderedReservoir() {
        return sampler.getOrderedReservoir();
    }

    public long getRootTuplesSeen() {
        return rootTuplesSeen;
    }

    public long getPositiveRootCandidatesSeen() {
        return sampler.getPositiveItemsSeen();
    }

    public double getTotalRootGroupWeight() {
        return sampler.getTotalWeight();
    }

    public int getSampleSize() {
        return plan.getSampleSize();
    }

    public int getWorkerId() {
        return workerId;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }


    private void validateRootTuple(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("root tuple must not be null");
        }

        if (!rootAlias.equals(tuple.getTable())) {
            throw new IllegalArgumentException("Sharded Phase 2 expected root alias '" +
                    rootAlias + "' but received alias '" + tuple.getTable() + "'");
        }
    }


    public static double checkedMultiply(double left, double right, String label) {

        validateNonNegativeFinite(left, label + ".left");
        validateNonNegativeFinite(right, label + ".right");
        double result = left * right;

        validateNonNegativeFinite(result, label);

        return result;
    }


    private static void validateNonNegativeFinite(double value, String label) {

        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException(label + " must be finite: " + value);
        }

        if (value < 0.0d) {
            throw new IllegalArgumentException(label + " must be non-negative: " + value);
        }
    }


    private static String safe(String value) {
        return value == null ? "" : value;
    }
}