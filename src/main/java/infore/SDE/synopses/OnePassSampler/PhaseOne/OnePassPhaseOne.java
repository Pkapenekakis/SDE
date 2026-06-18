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
        OnePassPhaseOneResult result = state.exportResult();

        if (wantsSummary(rq)) {
            int sampleLimit = summarySampleLimit(rq);
            return new Estimation(
                    rq,
                    result.toSummaryMap(sampleLimit),
                    Integer.toString(rq.getUID())
            );
        }

        return new Estimation(
                rq,
                result.toDebugMap(),
                Integer.toString(rq.getUID())
        );
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

    private boolean wantsSummary(Request rq) {
        if (rq == null || rq.getParam() == null) {
            return false;
        }

        for (String p : rq.getParam()) {
            if (p == null) {
                continue;
            }

            String value = p.trim();

            if ("summary".equalsIgnoreCase(value)
                    || "phase1-summary".equalsIgnoreCase(value)
                    || "stats".equalsIgnoreCase(value)) {
                return true;
            }
        }

        return false;
    }

    private int summarySampleLimit(Request rq) {
        int defaultLimit = 5;

        if (rq == null || rq.getParam() == null) {
            return defaultLimit;
        }

        for (String p : rq.getParam()) {
            if (p == null) {
                continue;
            }

            String value = p.trim();

            if (value.startsWith("sample=")) {
                return parsePositiveInt(value.substring("sample=".length()), defaultLimit);
            }

            if (value.startsWith("summarySample=")) {
                return parsePositiveInt(value.substring("summarySample=".length()), defaultLimit);
            }
        }

        return defaultLimit;
    }

    private int parsePositiveInt(String value, int fallback) {
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed < 0 ? fallback : parsed;
        } catch (Exception e) {
            return fallback;
        }
    }
}