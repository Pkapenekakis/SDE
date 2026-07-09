package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 2 root sampler.
 *
 * This class is now responsible for computing the full root-group weight:
 *
 *     rootGroupWeight(root)
 *       =
 *     rootOwnWeight(root)
 *       *
 *     product(child subtree weights from Phase 1)
 *
 * It then feeds the root candidate into OnlineMultinomialSampler.
 *
 * Important:
 * This class samples root groups only. It does not materialize full joined
 * output tuples. Phase 3 will later extend the sampled roots.
 */
public final class OnePassRootSampler implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OnePassPhaseOneResult phaseOneResult;
    private final CompiledOnePassPlan plan;
    private final OnePassWeightEvaluator weightEvaluator;

    private final String rootAlias;
    private final int sampleSize;
    private final OnlineMultinomialSampler<OnePassRootSampleCandidate> sampler;

    private long nextCandidateId;
    private long rootTuplesSeen;

    public OnePassRootSampler(OnePassPhaseOneResult phaseOneResult,
                              int sampleSize,
                              String seed) {
        if (phaseOneResult == null) {
            throw new IllegalArgumentException("phaseOneResult must not be null");
        }

        if (phaseOneResult.getPlan() == null) {
            throw new IllegalArgumentException("phaseOneResult.getPlan() must not be null");
        }

        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be positive");
        }

        this.phaseOneResult = phaseOneResult;
        this.plan = phaseOneResult.getPlan();
        this.rootAlias = plan.getRootAlias();
        this.sampleSize = sampleSize;
        this.weightEvaluator =
                new OnePassWeightEvaluator(plan.getWeightSpec());

        this.sampler =
                new OnlineMultinomialSampler<OnePassRootSampleCandidate>(
                        sampleSize,
                        seed
                );

        this.nextCandidateId = 0L;
        this.rootTuplesSeen = 0L;
    }

    /**
     * Adds one root tuple payload.
     *
     * The payload must be extractable by OnePassTupleExtractor and must belong
     * to the compiled root alias.
     */
    public void addRootTuple(Object payload) {
        OnePassTuple rootTuple = OnePassTupleExtractor.extract(payload);
        addRootTuple(rootTuple);
    }

    /**
     * Adds one already-extracted root tuple.
     */
    public void addRootTuple(OnePassTuple rootTuple) {
        if (rootTuple == null) {
            throw new IllegalArgumentException("rootTuple must not be null");
        }

        validateRootTuple(rootTuple);

        rootTuplesSeen++;

        double rootGroupWeight = computeRootGroupWeight(rootTuple);

        validateNonNegativeFiniteWeight(rootGroupWeight, "rootGroupWeight");

        /*
         * If a root tuple has no valid continuation in Phase 1, its group
         * weight is zero and it must not become a candidate.
         */
        if (rootGroupWeight == 0.0d) {
            return;
        }

        OnePassRootSampleCandidate candidate =
                new OnePassRootSampleCandidate(
                        nextCandidateId++,
                        rootAlias,
                        rootTuple.getRawJson(),
                        rootGroupWeight
                );

        sampler.add(candidate, rootGroupWeight);
    }

    /**
     * Deterministic Phase 2 weight computation.
     *
     * This method should be tested before testing the randomness of the
     * sampler.
     */
    public double computeRootGroupWeight(Object payload) {
        OnePassTuple rootTuple = OnePassTupleExtractor.extract(payload);
        return computeRootGroupWeight(rootTuple);
    }

    /**
     * Computes:
     *
     *     rootOwnWeight * product(child subtree weights)
     *
     * For example, for wq3_alias rooted at c:
     *
     *     rootGroupWeight(c)
     *       =
     *     weight(c) * index[c<->o][c.c_custkey]
     */
    public double computeRootGroupWeight(OnePassTuple rootTuple) {
        if (rootTuple == null) {
            throw new IllegalArgumentException("rootTuple must not be null");
        }

        validateRootTuple(rootTuple);

        double rootOwnWeight = weightEvaluator.evaluate(rootTuple);

        validateNonNegativeFiniteWeight(rootOwnWeight, "rootOwnWeight");

        if (rootOwnWeight == 0.0d) {
            return 0.0d;
        }

        double continuationWeight = 1.0d;

        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges =
                plan.getChildEdges(rootAlias);

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge : childEdges) {
            double childSubtreeWeight =
                    phaseOneResult.lookupChildSubtreeWeight(
                            childEdge,
                            rootTuple
                    );

            validateNonNegativeFiniteWeight(
                    childSubtreeWeight,
                    "childSubtreeWeight for edge " + childEdge.getEdgeId()
            );

            if (childSubtreeWeight == 0.0d) {
                return 0.0d;
            }

            continuationWeight =
                    checkedMultiply(
                            continuationWeight,
                            childSubtreeWeight,
                            "continuationWeight"
                    );
        }

        return checkedMultiply(
                rootOwnWeight,
                continuationWeight,
                "rootGroupWeight"
        );
    }

    public OnePassRootSampleResult finish() {
        OnlineMultinomialSample<OnePassRootSampleCandidate> sample =
                sampler.finish();

        List<OnePassRootSampleInstance> instances =
                new ArrayList<OnePassRootSampleInstance>(
                        sample.getSamples().size()
                );

        long sampleInstanceId = 0L;

        for (OnePassRootSampleCandidate candidate : sample.getSamples()) {
            instances.add(
                    new OnePassRootSampleInstance(
                            sampleInstanceId++,
                            candidate
                    )
            );
        }

        return new OnePassRootSampleResult(
                rootAlias,
                sampleSize,
                rootTuplesSeen,
                sample.getPositiveItemsSeen(),
                sample.getTotalWeight(),
                instances
        );
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public long getRootTuplesSeen() {
        return rootTuplesSeen;
    }

    private void validateRootTuple(OnePassTuple rootTuple) {
        String tupleAlias = rootTuple.getTable();

        if (!rootAlias.equals(tupleAlias)) {
            throw new IllegalArgumentException(
                    "Phase 2 expected root alias '"
                            + rootAlias
                            + "' but received tuple for alias '"
                            + tupleAlias
                            + "': "
                            + rootTuple
            );
        }
    }

    private static void validateNonNegativeFiniteWeight(double weight,
                                                        String label) {
        if (Double.isNaN(weight) || Double.isInfinite(weight)) {
            throw new IllegalArgumentException(label + " must be finite: " + weight);
        }

        if (weight < 0.0d) {
            throw new IllegalArgumentException(label + " must be non-negative: " + weight);
        }
    }

    private static double checkedMultiply(double left,
                                          double right,
                                          String label) {
        double result = left * right;

        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new IllegalArgumentException(
                    label + " overflow/invalid multiplication: "
                            + left + " * " + right + " = " + result
            );
        }

        if (result < 0.0d) {
            throw new IllegalArgumentException(
                    label + " became negative: "
                            + left + " * " + right + " = " + result
            );
        }

        return result;
    }

    public List<WeightedReservoirEntry<OnePassRootSampleCandidate>> getOrderedReservoir() {
        return sampler.getOrderedReservoir();
    }

    public long getPositiveRootCandidatesSeen() {
        return sampler.getPositiveItemsSeen();
    }

    public double getTotalRootGroupWeight() {
        return sampler.getTotalWeight();
    }
}