package infore.SDE.synopses.OnePassSampler.PhaseThree;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleInstance;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Phase 3 extension state.
 *
 * First implementation target:
 *   - noOfP = 1
 *   - in-memory tests first
 *   - replay one side alias at a time in root-to-leaf order
 *
 * Phase 3 takes:
 *   - Phase 1 indexes
 *   - Phase 2 explicit root sample instances
 *
 * and extends each sampled root into one complete joined sample.
 */
public final class OnePassPhaseThreeState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OnePassPhaseOneResult phaseOneResult;
    private final OnePassRootSampleResult phaseTwoResult;
    private final CompiledOnePassPlan plan;
    private final OnePassWeightEvaluator weightEvaluator;

    private final String rootAlias;
    private final Random random;

    private final Map<Long, OnePassPartialSample> partialSamplesById;

    private String activeAlias;
    private CompiledOnePassPlan.DirectedJoinEdge activeParentEdge;
    private Map<Long, OnePassExtensionChoice> activeChoicesBySampleId;

    public OnePassPhaseThreeState(OnePassPhaseOneResult phaseOneResult,
                                  OnePassRootSampleResult phaseTwoResult,
                                  String seed) {
        if (phaseOneResult == null) {
            throw new IllegalArgumentException("phaseOneResult must not be null");
        }

        if (phaseTwoResult == null) {
            throw new IllegalArgumentException("phaseTwoResult must not be null");
        }

        this.phaseOneResult = phaseOneResult;
        this.phaseTwoResult = phaseTwoResult;
        this.plan = phaseOneResult.getPlan();
        this.weightEvaluator = new OnePassWeightEvaluator(plan.getWeightSpec());

        this.rootAlias = plan.getRootAlias();
        this.random = new Random(stableSeed(seed) + 3001L);

        this.partialSamplesById = new LinkedHashMap<Long, OnePassPartialSample>();
        this.activeAlias = null;
        this.activeParentEdge = null;
        this.activeChoicesBySampleId = null;

        initializeFromPhaseTwoRoots();
    }

    private void initializeFromPhaseTwoRoots() {
        if (!rootAlias.equals(phaseTwoResult.getRootAlias())) {
            throw new IllegalArgumentException("Phase 2 root alias '" + phaseTwoResult.getRootAlias() +
                    "' does not match plan root alias '" + rootAlias + "'");
        }

        for (OnePassRootSampleInstance rootInstance : phaseTwoResult.getSampleInstances()) {

            OnePassPartialSample partial = new OnePassPartialSample(rootInstance.getSampleInstanceId(),
                            rootInstance.getSourceCandidateId());

            JsonNode rootJson = rootInstance.getRootTuple();

            OnePassTuple rootTuple = new OnePassTuple(rootAlias, rootJson);

            partial.putTuple(rootAlias, rootTuple);

            partialSamplesById.put(partial.getSampleInstanceId(), partial);
        }
    }

    /**
     * Replay one side alias and extend all partial samples that need this alias.
     *
     * The caller should use root-to-leaf order and skip the root alias.
     *
     * Example for WQ3 rooted at c:
     *
     *   extendAlias("o", ordersTuples)
     *   extendAlias("l", lineitemTuples)
     */
    /*
    public void extendAlias(String alias, List<OnePassTuple> replayedTuples) {
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("alias must not be blank");
        }

        if (replayedTuples == null) {
            throw new IllegalArgumentException("replayedTuples must not be null");
        }

        if (plan.isRoot(alias)) {
            throw new IllegalArgumentException("Phase 3 should not replay root alias '" + alias +
                    "'. Root tuples already come from Phase 2.");
        }

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Unknown alias: " + alias);
        }

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);

        if (parentEdge == null) {
            throw new IllegalStateException("Alias '" + alias + "' has no parent edge");
        }

        Map<Long, OnePassExtensionChoice> choicesBySampleId = initializeChoicesForAlias(alias, parentEdge);

        for (OnePassTuple childTuple : replayedTuples) {
            if (childTuple == null) {
                continue;
            }

            if (!alias.equals(childTuple.getTable())) {
                throw new IllegalArgumentException(
                        "extendAlias(" + alias + ") received tuple from alias " + childTuple.getTable());
            }

            double candidateWeight = computeTupleSubtreeWeight(childTuple);

            if (candidateWeight == 0.0d) {
                continue;
            }

            validatePositiveFinite(candidateWeight, "candidateWeight");

            JoinValue childSideKey = JoinValue.fromTuple(childTuple, parentEdge.getChildFields());

            for (OnePassPartialSample partial : partialSamplesById.values()) {

                if (partial.hasAlias(alias)) {
                    continue;
                }

                OnePassTuple parentTuple = partial.getTuple(parentEdge.getParentAlias());

                if (parentTuple == null) {
                     //This can happen only if caller replays aliases in the
                     //wrong order. We detect it after the replay as well.
                    continue;
                }

                JoinValue parentSideKey = JoinValue.fromTuple(parentTuple, parentEdge.getParentFields());

                if (!parentSideKey.equals(childSideKey)) {
                    continue;
                }

                OnePassExtensionChoice choice = choicesBySampleId.get(partial.getSampleInstanceId());

                if (choice == null) {
                    throw new IllegalStateException("Missing extension choice for sample " +
                            partial.getSampleInstanceId() + " and alias " + alias);
                }

                choice.consider(childTuple, candidateWeight, random);
            }
        }

        commitAliasSelections(alias, choicesBySampleId);
    } */

    public void extendAlias(String alias, List<OnePassTuple> replayedTuples) {
        if (replayedTuples == null) {
            throw new IllegalArgumentException("replayedTuples must not be null");
        }

        startAlias(alias);

        for (OnePassTuple tuple : replayedTuples) {
            addTuple(tuple);
        }

        finishAlias();
    }

    public void startAlias(String alias) {
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("alias must not be blank");
        }

        if (activeAlias != null) {
            throw new IllegalStateException("Cannot start Phase 3 alias '" + alias + "' because alias '" +
                    activeAlias + "' is already active. " + "Call finishAlias() first."
            );
        }

        if (plan.isRoot(alias)) {
            throw new IllegalArgumentException("Phase 3 should not replay root alias '" +
                    alias + "'. Root tuples already come from Phase 2.");
        }

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Unknown alias: " + alias);
        }

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);

        if (parentEdge == null) {
            throw new IllegalStateException("Alias '" + alias + "' has no parent edge");
        }

        this.activeAlias = alias;
        this.activeParentEdge = parentEdge;
        this.activeChoicesBySampleId = initializeChoicesForAlias(alias, parentEdge);
    }

    public void addTuple(OnePassTuple childTuple) {
        if (activeAlias == null) {
            throw new IllegalStateException("No active Phase 3 alias. "
                    + "Call startAlias(alias) before addTuple(...).");
        }

        if (childTuple == null) {
            return;
        }

        if (!activeAlias.equals(childTuple.getTable())) {
            throw new IllegalArgumentException("Active Phase 3 alias is '" + activeAlias +
                    "' but received tuple from alias '" + childTuple.getTable() + "'");
        }

        double candidateWeight = computeTupleSubtreeWeight(childTuple);

        if (candidateWeight == 0.0d) {
            return;
        }

        validatePositiveFinite(candidateWeight, "candidateWeight");

        JoinValue childSideKey = JoinValue.fromTuple(childTuple, activeParentEdge.getChildFields());

        for (OnePassPartialSample partial : partialSamplesById.values()) {

            if (partial.hasAlias(activeAlias)) {
                continue;
            }

            OnePassTuple parentTuple = partial.getTuple(activeParentEdge.getParentAlias());

            if (parentTuple == null) {
                /*
                 * This means the caller is replaying aliases in the wrong order.
                 * startAlias(...) usually catches this earlier through
                 * initializeChoicesForAlias(...)
                 */
                continue;
            }

            JoinValue parentSideKey = JoinValue.fromTuple(parentTuple, activeParentEdge.getParentFields());

            if (!parentSideKey.equals(childSideKey)) {
                continue;
            }

            OnePassExtensionChoice choice = activeChoicesBySampleId.get(partial.getSampleInstanceId());

            if (choice == null) {
                throw new IllegalStateException("Missing extension choice for sample " +
                        partial.getSampleInstanceId() + " and alias " + activeAlias);
            }

            choice.consider(childTuple, candidateWeight, random);
        }
    }

    public void finishAlias() {
        if (activeAlias == null) {
            throw new IllegalStateException("No active Phase 3 alias to finish.");
        }

        String aliasToCommit = activeAlias;

        Map<Long, OnePassExtensionChoice> choicesToCommit = activeChoicesBySampleId;

        commitAliasSelections(aliasToCommit, choicesToCommit);

        clearActiveAlias();
    }

    private Map<Long, OnePassExtensionChoice> initializeChoicesForAlias(String alias,
            CompiledOnePassPlan.DirectedJoinEdge parentEdge) {

        Map<Long, OnePassExtensionChoice> choicesBySampleId = new LinkedHashMap<Long, OnePassExtensionChoice>();

        for (OnePassPartialSample partial : partialSamplesById.values()) {
            if (partial.hasAlias(alias)) {
                continue;
            }

            OnePassTuple parentTuple = partial.getTuple(parentEdge.getParentAlias());

            if (parentTuple == null) {
                throw new IllegalStateException(
                        "Cannot extend alias '"
                                + alias
                                + "' for sample "
                                + partial.getSampleInstanceId()
                                + " because parent alias '"
                                + parentEdge.getParentAlias()
                                + "' has not been selected yet. "
                                + "Replay aliases in root-to-leaf order."
                );
            }

            double denominator = phaseOneResult.lookupChildSubtreeWeight(parentEdge, parentTuple);

            validateNonNegativeFinite(denominator, "Phase 3 denominator for edge "+ parentEdge.getEdgeId());

            if (denominator == 0.0d) {
                throw new IllegalStateException(
                        "Cannot extend sample "
                                + partial.getSampleInstanceId()
                                + " through edge "
                                + parentEdge.getEdgeId()
                                + " because Phase 1 denominator is zero. "
                                + "This should not happen for a positive Phase 2 sample."
                );
            }

            choicesBySampleId.put(partial.getSampleInstanceId(), new OnePassExtensionChoice());
        }

        return choicesBySampleId;
    }

    private void commitAliasSelections(
            String alias,
            Map<Long, OnePassExtensionChoice> choicesBySampleId) {

        for (Map.Entry<Long, OnePassExtensionChoice> entry : choicesBySampleId.entrySet()) {

            Long sampleId = entry.getKey();

            OnePassExtensionChoice choice = entry.getValue();

            if (!choice.hasSelection()) {
                throw new IllegalStateException("Could not extend sample " + sampleId
                                + " with alias " + alias + ". No matching positive child tuple was found.");
            }

            OnePassPartialSample partial = partialSamplesById.get(sampleId);

            if (partial == null) {
                throw new IllegalStateException("Missing partial sample " + sampleId);
            }

            partial.putTuple(alias, choice.getSelectedTuple());
        }
    }

    /**
     * Computes subtree weight for any tuple using the same formula as Phase 1:
     *
     *   ownWeight(tuple) * product(child continuation weights)
     */
    public double computeTupleSubtreeWeight(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        String alias = tuple.getTable();

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Unknown tuple alias: " + alias);
        }

        double ownWeight = weightEvaluator.evaluate(tuple);

        validateNonNegativeFinite(ownWeight, "ownWeight");

        if (ownWeight == 0.0d) {
            return 0.0d;
        }

        double continuationWeight = 1.0d;

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge : plan.getChildEdges(alias)) {

            double childSubtreeWeight = phaseOneResult.lookupChildSubtreeWeight(childEdge, tuple);

            validateNonNegativeFinite(childSubtreeWeight, "childSubtreeWeight for edge " + childEdge.getEdgeId());

            if (childSubtreeWeight == 0.0d) {
                return 0.0d;
            }

            continuationWeight = checkedMultiply(continuationWeight, childSubtreeWeight, "continuationWeight");
        }

        return checkedMultiply(ownWeight, continuationWeight, "tupleSubtreeWeight");
    }

    /**
     * Convenience method for in-memory tests.
     *
     * Replays all non-root aliases in root-to-leaf order.
     */
    public OnePassPhaseThreeResult extendAll(
            Map<String, List<OnePassTuple>> tuplesByAlias) {

        if (tuplesByAlias == null) {
            throw new IllegalArgumentException("tuplesByAlias must not be null");
        }

        for (String alias : plan.getRootToLeafOrder()) {
            if (plan.isRoot(alias)) {
                continue;
            }

            List<OnePassTuple> tuples = tuplesByAlias.get(alias);

            if (tuples == null) {
                throw new IllegalArgumentException("Missing replay tuples for alias " + alias);
            }

            extendAlias(alias, tuples);
        }

        return finish();
    }

    public OnePassPhaseThreeResult finish() {
        List<OnePassCompletedSample> completed = new ArrayList<OnePassCompletedSample>();

        for (OnePassPartialSample partial : partialSamplesById.values()) {

            if (!partial.isComplete(plan)) {
                throw new IllegalStateException("Sample " + partial.getSampleInstanceId() + " is not complete: "
                                + partial);
            }

            completed.add(new OnePassCompletedSample(partial.getSampleInstanceId(),
                            partial.getSourceRootCandidateId(),
                            partial.getSelectedTuplesByAlias()));
        }

        return new OnePassPhaseThreeResult(plan.getQueryName(), rootAlias,
                phaseTwoResult.getRequestedSampleSize(), completed);
    }

    public Map<Long, OnePassPartialSample> copyPartialSamplesById() {
        Map<Long, OnePassPartialSample> copy = new LinkedHashMap<Long, OnePassPartialSample>();

        for (Map.Entry<Long, OnePassPartialSample> entry : partialSamplesById.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }

        return copy;
    }

    private static void validateNonNegativeFinite(double value, String label) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException(label + " must be finite: " + value);
        }

        if (value < 0.0d) {
            throw new IllegalArgumentException(label + " must be non-negative: " + value);
        }
    }

    private static void validatePositiveFinite(double value, String label) {
        validateNonNegativeFinite(value, label);

        if (value <= 0.0d) {
            throw new IllegalArgumentException(label + " must be positive: " + value);
        }
    }

    private static double checkedMultiply(double left, double right, String label) {
        double result = left * right;

        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new IllegalArgumentException(label + " overflow/invalid multiplication: "
                    + left + " * " + right + " = " + result);
        }

        if (result < 0.0d) {
            throw new IllegalArgumentException(label + " became negative: " + left + " * " + right + " = "
                            + result);
        }

        return result;
    }

    private static long stableSeed(String seed) {
        String s = seed == null ? "" : seed;

        long h = 1125899906842597L;

        for (int i = 0; i < s.length(); i++) {
            h = 31L * h + s.charAt(i);
        }

        return h;
    }

    public boolean isAliasActive() {
        return activeAlias != null;
    }

    public String getActiveAlias() {
        return activeAlias;
    }

    private void clearActiveAlias() {
        this.activeAlias = null;
        this.activeParentEdge = null;
        this.activeChoicesBySampleId = null;
    }
}