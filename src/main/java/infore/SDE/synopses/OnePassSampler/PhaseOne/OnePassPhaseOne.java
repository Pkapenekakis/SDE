package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.Synopsis;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;

/**
 * Phase 1 synopsis for One-pass*.
 *
 * Receives side-stream tuples and builds the link-value hash indexes
 * described by the group-weight algorithm.
 *
 * For now:
 * - single worker execution
 * - acyclic plans only
 * - assumes preprocessing replay is leaf-to-root on non-root aliases
 */
public class OnePassPhaseOne extends Synopsis {

    private final CompiledOnePassPlan plan;
    private final OnePassPhaseOneState state;

    public OnePassPhaseOne(int uid,
                           CompiledOnePassPlan plan,
                           WeightSpec weightSpec) {
        super(uid, "-1", "-1", "Queryable");
        this.plan = plan;
        this.state = new OnePassPhaseOneState(plan, new OnePassWeightEvaluator(weightSpec));
    }

    @Override
    public void add(Object payload) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
        state.addTuple(tuple);
    }

    @Override
    public Object estimate(Object k) {
        return state.exportResult();
    }

    @Override
    public Estimation estimate(Request rq) {
        return new Estimation(rq, state.exportResult(), Integer.toString(rq.getUID()));
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        if (!(sk instanceof OnePassPhaseOne)) {
            throw new IllegalArgumentException("Cannot merge non-OnePassPhaseOne synopsis");
        }
        OnePassPhaseOne other = (OnePassPhaseOne) sk;
        this.state.mergeFrom(other.state);
        return this;
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }

    public OnePassPhaseOneState getState() {
        return state;
    }

    public OnePassPhaseOneResult exportResult() {
        return state.exportResult();
    }
}