package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

import java.io.Serializable;
import java.util.List;

/**
 * Phase 2 state wrapper.
 *
 * This class turns OnePassRootSampler into a proper Phase 2 module:
 *   - receives the completed Phase 1 result,
 *   - accepts only root tuples,
 *   - computes root group weights through OnePassRootSampler,
 *   - produces OnePassRootSampleResult.
 */
public final class OnePassPhaseTwoState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OnePassPhaseOneResult phaseOneResult;
    private final CompiledOnePassPlan plan;
    private final String rootAlias;
    private final OnePassRootSampler rootSampler;

    public OnePassPhaseTwoState(OnePassPhaseOneResult phaseOneResult) {
        if (phaseOneResult == null) {
            throw new IllegalArgumentException("phaseOneResult must not be null");
        }

        if (phaseOneResult.getPlan() == null) {
            throw new IllegalArgumentException("phaseOneResult.getPlan() must not be null");
        }

        this.phaseOneResult = phaseOneResult;
        this.plan = phaseOneResult.getPlan();
        this.rootAlias = plan.getRootAlias();

        this.rootSampler =
                new OnePassRootSampler(
                        phaseOneResult,
                        plan.getSampleSize(),
                        plan.getDatasetSeed()
                );
    }

    public void add(Object payload) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        addTuple(tuple);
    }

    public void addTuple(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (!rootAlias.equals(tuple.getTable())) {
            throw new IllegalArgumentException(
                    "Phase 2 expected root alias '"
                            + rootAlias
                            + "' but received alias '"
                            + tuple.getTable()
                            + "'"
            );
        }

        rootSampler.addRootTuple(tuple);
    }

    public double computeRootGroupWeight(Object payload) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        return computeRootGroupWeight(tuple);
    }

    public double computeRootGroupWeight(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (!rootAlias.equals(tuple.getTable())) {
            throw new IllegalArgumentException(
                    "Phase 2 expected root alias '"
                            + rootAlias
                            + "' but received alias '"
                            + tuple.getTable()
                            + "'"
            );
        }

        return rootSampler.computeRootGroupWeight(tuple);
    }

    public OnePassRootSampleResult finish() {
        return rootSampler.finish();
    }

    public OnePassRootSampleResult exportResult() {
        return finish();
    }

    public OnePassPhaseOneResult getPhaseOneResult() {
        return phaseOneResult;
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public long getRootTuplesSeen() {
        return rootSampler.getRootTuplesSeen();
    }

    public List<WeightedReservoirEntry<OnePassRootSampleCandidate>> getOrderedReservoir() {
        return rootSampler.getOrderedReservoir();
    }

    public long getPositiveRootCandidatesSeen() {
        return rootSampler.getPositiveRootCandidatesSeen();
    }

    public double getTotalRootGroupWeight() {
        return rootSampler.getTotalRootGroupWeight();
    }

    public int getSampleSize() {
        return rootSampler.getSampleSize();
    }
}