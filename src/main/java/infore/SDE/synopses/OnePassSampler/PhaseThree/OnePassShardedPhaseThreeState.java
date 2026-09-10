package infore.SDE.synopses.OnePassSampler.PhaseThree;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleInstance;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassShardOwnership;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Bounded worker-local state for coordinator-free, sharded OnePass Phase 3.
 *
 * The Phase-1 continuation indexes are NOT copied here. Candidate child
 * continuation lookups remain in the physically local OnePassPhaseOneState.
 *
 * Replicated state:
 *   - the bounded Phase-2 root sample;
 *   - one OnePassPartialSample per explicit sampleInstanceId.
 *
 * Sharded active state:
 *   - only choices whose parent-edge join key belongs to this worker.
 */
public final class OnePassShardedPhaseThreeState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CompiledOnePassPlan plan;
    private final OnePassWeightEvaluator weightEvaluator;
    private final OnePassRootSampleResult phaseTwoResult;
    private final int workerId;
    private final int expectedWorkers;
    private final String datasetSeed;

    private final Map<Long, OnePassPartialSample> partialSamplesById;

    private String activeAlias;
    private CompiledOnePassPlan.DirectedJoinEdge activeParentEdge;
    private Map<JoinValue, List<Long>> ownedSampleIdsByParentKey;
    private Map<Long, OnePassExtensionChoice> ownedChoicesBySampleId;
    private Map<Long, Random> randomBySampleId;

    public OnePassShardedPhaseThreeState(CompiledOnePassPlan plan, OnePassRootSampleResult phaseTwoResult,
                                         String datasetSeed, int workerId, int expectedWorkers) {

        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }
        if (phaseTwoResult == null) {
            throw new IllegalArgumentException("phaseTwoResult must not be null");
        }
        if (workerId < 0) {
            throw new IllegalArgumentException("workerId must be >= 0");
        }
        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers must be > 0");
        }
        if (workerId >= expectedWorkers) {
            throw new IllegalArgumentException(
                    "workerId must be < expectedWorkers. workerId=" + workerId
                            + ", expectedWorkers=" + expectedWorkers);
        }
        if (!plan.getRootAlias().equals(phaseTwoResult.getRootAlias())) {
            throw new IllegalArgumentException(
                    "Phase-2 root alias mismatch. plan=" + plan.getRootAlias()
                            + ", phaseTwo=" + phaseTwoResult.getRootAlias());
        }
        if (!phaseTwoResult.hasFullSample()) {
            throw new IllegalStateException(
                    "Cannot initialize Phase 3 from an incomplete Phase-2 sample. requested="
                            + phaseTwoResult.getRequestedSampleSize()
                            + ", actual=" + phaseTwoResult.getSampleInstances().size());
        }

        this.plan = plan;
        this.weightEvaluator = new OnePassWeightEvaluator(plan.getWeightSpec());
        this.phaseTwoResult = phaseTwoResult;
        this.workerId = workerId;
        this.expectedWorkers = expectedWorkers;
        this.datasetSeed = datasetSeed == null ? "" : datasetSeed;
        this.partialSamplesById = new LinkedHashMap<Long, OnePassPartialSample>();

        this.activeAlias = null;
        this.activeParentEdge = null;
        this.ownedSampleIdsByParentKey = null;
        this.ownedChoicesBySampleId = null;
        this.randomBySampleId = null;

        initializePartialSamples();
    }

    private void initializePartialSamples() {
        String rootAlias = plan.getRootAlias();

        for (OnePassRootSampleInstance rootInstance : phaseTwoResult.getSampleInstances()) {
            long sampleInstanceId = rootInstance.getSampleInstanceId();

            if (partialSamplesById.containsKey(sampleInstanceId)) {
                throw new IllegalStateException(
                        "Duplicate Phase-2 sampleInstanceId: " + sampleInstanceId);
            }

            JsonNode rootJson = rootInstance.getRootTuple();
            if (rootJson == null || rootJson.isNull()) {
                throw new IllegalStateException(
                        "Phase-2 sample " + sampleInstanceId + " has no root tuple");
            }

            OnePassTuple rootTuple = new OnePassTuple(rootAlias, rootJson);
            OnePassPartialSample partial = new OnePassPartialSample(
                    sampleInstanceId,
                    rootInstance.getSourceCandidateId());
            partial.putTuple(rootAlias, rootTuple);
            partialSamplesById.put(sampleInstanceId, partial);
        }

        if (partialSamplesById.size() != phaseTwoResult.getRequestedSampleSize()) {
            throw new IllegalStateException(
                    "Phase-3 partial sample count mismatch. expected="
                            + phaseTwoResult.getRequestedSampleSize()
                            + ", actual=" + partialSamplesById.size());
        }
    }

    /**
     * Activates one non-root alias. Choice state is created only for sample
     * instances whose selected parent key belongs to this worker.
     */
    public void startAlias(String alias) {
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("alias must not be blank");
        }
        alias = alias.trim();

        if (activeAlias != null) {
            throw new IllegalStateException(
                    "Phase-3 alias already active: " + activeAlias);
        }
        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Unknown Phase-3 alias: " + alias);
        }
        if (plan.isRoot(alias)) {
            throw new IllegalArgumentException(
                    "Phase 3 must not replay root alias " + alias);
        }

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);
        if (parentEdge == null) {
            throw new IllegalStateException(
                    "Non-root Phase-3 alias has no parent edge: " + alias);
        }

        Map<JoinValue, List<Long>> byParentKey =
                new LinkedHashMap<JoinValue, List<Long>>();
        Map<Long, OnePassExtensionChoice> choices =
                new LinkedHashMap<Long, OnePassExtensionChoice>();
        Map<Long, Random> randoms = new LinkedHashMap<Long, Random>();

        for (OnePassPartialSample partial : partialSamplesById.values()) {
            if (partial.hasAlias(alias)) {
                throw new IllegalStateException(
                        "Sample " + partial.getSampleInstanceId()
                                + " already contains Phase-3 alias " + alias);
            }

            OnePassTuple parentTuple = partial.getTuple(parentEdge.getParentAlias());
            if (parentTuple == null) {
                throw new IllegalStateException(
                        "Sample " + partial.getSampleInstanceId()
                                + " has no selected parent alias "
                                + parentEdge.getParentAlias()
                                + " before starting child alias " + alias);
            }

            JoinValue parentKey = JoinValue.fromTuple(
                    parentTuple,
                    parentEdge.getParentFields());

            int owner = OnePassShardOwnership.ownerForEdgeKey(
                    parentEdge.getEdgeId(),
                    parentKey,
                    expectedWorkers);

            if (owner != workerId) {
                continue;
            }

            long sampleId = partial.getSampleInstanceId();
            List<Long> ids = byParentKey.computeIfAbsent(parentKey, k -> new ArrayList<Long>());
            ids.add(sampleId);

            choices.put(sampleId, new OnePassExtensionChoice());
            randoms.put(sampleId,
                    new Random(stableSeed(datasetSeed, alias, sampleId)));
        }

        this.activeAlias = alias;
        this.activeParentEdge = parentEdge;
        this.ownedSampleIdsByParentKey = byParentKey;
        this.ownedChoicesBySampleId = choices;
        this.randomBySampleId = randoms;
    }

    /** Evaluate only the tuple's own local weight. */
    public double beginCandidate(OnePassTuple tuple) {
        requireActiveAlias();
        requireActiveTuple(tuple);

        double ownWeight = weightEvaluator.evaluate(tuple);
        validateNonNegativeFinite(ownWeight, "Phase-3 ownWeight");
        return ownWeight;
    }

    /**
     * Accepts a candidate only after every child continuation weight has been
     * multiplied. This method must execute on the parent-edge selection owner.
     */
    public void acceptCompletedCandidate(
            OnePassTuple tuple,
            double candidateWeight) {

        requireActiveAlias();
        requireActiveTuple(tuple);
        validateNonNegativeFinite(candidateWeight, "Phase-3 candidateWeight");

        if (candidateWeight == 0.0d) {
            return;
        }

        JoinValue candidateKey = JoinValue.fromTuple(
                tuple,
                activeParentEdge.getChildFields());

        int candidateOwner = OnePassShardOwnership.ownerForEdgeKey(
                activeParentEdge.getEdgeId(),
                candidateKey,
                expectedWorkers);

        if (candidateOwner != workerId) {
            throw new IllegalStateException(
                    "Phase-3 candidate reached the wrong selection owner. alias="
                            + activeAlias + ", edge=" + activeParentEdge.getEdgeId()
                            + ", expectedOwner=" + candidateOwner
                            + ", actualWorker=" + workerId);
        }

        List<Long> sampleIds = ownedSampleIdsByParentKey.get(candidateKey);
        if (sampleIds == null || sampleIds.isEmpty()) {
            return;
        }

        for (Long sampleId : sampleIds) {
            OnePassExtensionChoice choice = ownedChoicesBySampleId.get(sampleId);
            Random random = randomBySampleId.get(sampleId);

            if (choice == null || random == null) {
                throw new IllegalStateException(
                        "Missing owned Phase-3 choice state for sample " + sampleId);
            }

            choice.consider(tuple, candidateWeight, random);
        }
    }

    /**
     * Exports only this worker's disjoint sample-instance selections.
     * Important current-source adaptation: transport the selected tuple's raw
     * JSON, not the OnePassTuple wrapper. Serializing OnePassTuple itself would
     * nest the original fields under rawJson/fields and would break subsequent
     * JoinValue lookups after installation.
     */
    public List<Map<String, Object>> exportOwnedSelections() {
        requireActiveAlias();

        List<Long> sampleIds = new ArrayList<Long>(ownedChoicesBySampleId.keySet());
        Collections.sort(sampleIds);

        List<Map<String, Object>> out =
                new ArrayList<Map<String, Object>>(sampleIds.size());

        for (Long sampleId : sampleIds) {
            OnePassExtensionChoice choice = ownedChoicesBySampleId.get(sampleId);

            if (choice == null || !choice.hasSelection()) {
                throw new IllegalStateException(
                        "No Phase-3 selection for owned sample " + sampleId
                                + ", alias=" + activeAlias
                                + ", worker=" + workerId);
            }

            OnePassTuple selectedTuple = choice.getSelectedTuple();
            JsonNode rawTuple = selectedTuple.getRawJson();
            if (rawTuple == null || rawTuple.isNull()) {
                throw new IllegalStateException(
                        "Selected Phase-3 tuple has no raw JSON. sample=" + sampleId);
            }

            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("sampleInstanceId", sampleId);
            item.put("alias", activeAlias);
            item.put("ownerWorkerId", workerId);
            item.put("candidatesSeen", choice.getCandidatesSeen());
            item.put("cumulativeWeight", choice.getCumulativeWeight());
            item.put("selectedWeight", choice.getSelectedWeight());
            item.put("tuple", rawTuple.deepCopy());
            out.add(item);
        }

        return out;
    }

    /**
     * Installs the strict-union result replicated from request 88.
     * Validation is completed before mutating any partial sample.
     */
    public void installGlobalAliasSelections(String alias, JsonNode entries) {
        requireActiveAlias();

        if (alias == null || !activeAlias.equals(alias.trim())) {
            throw new IllegalStateException(
                    "Phase-3 alias install mismatch. active=" + activeAlias
                            + ", received=" + alias);
        }
        if (entries == null || !entries.isArray()) {
            throw new IllegalArgumentException(
                    "Global Phase-3 selections must be an array");
        }
        if (entries.size() != partialSamplesById.size()) {
            throw new IllegalStateException(
                    "Global Phase-3 selection count mismatch. expected="
                            + partialSamplesById.size()
                            + ", actual=" + entries.size());
        }

        Map<Long, OnePassTuple> tupleBySampleId =
                new LinkedHashMap<Long, OnePassTuple>();
        Set<Long> seen = new HashSet<Long>();

        for (JsonNode entry : entries) {
            long sampleId = longField(entry, "sampleInstanceId", -1L);
            if (sampleId < 0L) {
                throw new IllegalStateException(
                        "Phase-3 selection has invalid sampleInstanceId: " + entry);
            }
            if (!seen.add(sampleId)) {
                throw new IllegalStateException(
                        "Duplicate Phase-3 sampleInstanceId " + sampleId);
            }

            OnePassPartialSample partial = partialSamplesById.get(sampleId);
            if (partial == null) {
                throw new IllegalStateException(
                        "Unknown Phase-3 sampleInstanceId " + sampleId);
            }
            if (partial.hasAlias(activeAlias)) {
                throw new IllegalStateException(
                        "Sample " + sampleId + " already contains alias " + activeAlias);
            }

            JsonNode entryAlias = entry.get("alias");
            if (entryAlias != null && !entryAlias.isNull()
                    && !activeAlias.equals(entryAlias.asText(""))) {
                throw new IllegalStateException(
                        "Selection alias mismatch for sample " + sampleId
                                + ": expected=" + activeAlias
                                + ", actual=" + entryAlias.asText(""));
            }

            JsonNode tupleNode = entry.get("tuple");
            if (tupleNode == null || tupleNode.isNull()) {
                throw new IllegalStateException(
                        "Phase-3 selection has no tuple for sample " + sampleId);
            }

            OnePassTuple tuple = OnePassTupleExtractor.extract(tupleNode);
            if (!activeAlias.equals(tuple.getTable())) {
                throw new IllegalStateException(
                        "Selected tuple alias mismatch for sample " + sampleId
                                + ": expected=" + activeAlias
                                + ", actual=" + tuple.getTable());
            }

            tupleBySampleId.put(sampleId, tuple);
        }

        if (seen.size() != partialSamplesById.size()) {
            throw new IllegalStateException(
                    "Global Phase-3 selection set is incomplete. expected="
                            + partialSamplesById.size() + ", actual=" + seen.size());
        }

        for (Map.Entry<Long, OnePassTuple> entry : tupleBySampleId.entrySet()) {
            partialSamplesById.get(entry.getKey()).putTuple(activeAlias, entry.getValue());
        }

        clearActiveAlias();
    }

    public boolean areAllSamplesComplete() {
        if (activeAlias != null) {
            return false;
        }

        for (OnePassPartialSample partial : partialSamplesById.values()) {
            if (!partial.isComplete(plan)) {
                return false;
            }
        }
        return true;
    }

    public OnePassPhaseThreeResult buildResultIfComplete() {
        if (activeAlias != null) {
            throw new IllegalStateException(
                    "Cannot build Phase-3 result while alias is active: " + activeAlias);
        }

        List<OnePassPartialSample> partials =
                new ArrayList<OnePassPartialSample>(partialSamplesById.values());
        partials.sort(new Comparator<OnePassPartialSample>() {
            @Override
            public int compare(OnePassPartialSample left, OnePassPartialSample right) {
                return Long.compare(
                        left.getSampleInstanceId(),
                        right.getSampleInstanceId());
            }
        });

        List<OnePassCompletedSample> completed =
                new ArrayList<OnePassCompletedSample>(partials.size());

        for (OnePassPartialSample partial : partials) {
            if (!partial.isComplete(plan)) {
                throw new IllegalStateException(
                        "Sample " + partial.getSampleInstanceId()
                                + " is not complete: " + partial);
            }

            completed.add(new OnePassCompletedSample(
                    partial.getSampleInstanceId(),
                    partial.getSourceRootCandidateId(),
                    partial.getSelectedTuplesByAlias()));
        }

        if (completed.size() != phaseTwoResult.getRequestedSampleSize()) {
            throw new IllegalStateException(
                    "Completed Phase-3 sample count mismatch. expected="
                            + phaseTwoResult.getRequestedSampleSize()
                            + ", actual=" + completed.size());
        }

        return new OnePassPhaseThreeResult(
                plan.getQueryName(),
                plan.getRootAlias(),
                phaseTwoResult.getRequestedSampleSize(),
                completed);
    }

    public boolean isAliasActive() {
        return activeAlias != null;
    }

    public String getActiveAlias() {
        return activeAlias;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getExpectedWorkers() {
        return expectedWorkers;
    }

    public int getPartialSampleCount() {
        return partialSamplesById.size();
    }

    public int getOwnedActiveChoiceCount() {
        return ownedChoicesBySampleId == null ? 0 : ownedChoicesBySampleId.size();
    }

    public Map<Long, OnePassPartialSample> copyPartialSamplesById() {
        Map<Long, OnePassPartialSample> copy =
                new LinkedHashMap<Long, OnePassPartialSample>();

        for (Map.Entry<Long, OnePassPartialSample> entry : partialSamplesById.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }
        return copy;
    }

    private void requireActiveAlias() {
        if (activeAlias == null
                || activeParentEdge == null
                || ownedSampleIdsByParentKey == null
                || ownedChoicesBySampleId == null
                || randomBySampleId == null) {
            throw new IllegalStateException("No active sharded Phase-3 alias");
        }
    }

    private void requireActiveTuple(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }
        if (!activeAlias.equals(tuple.getTable())) {
            throw new IllegalArgumentException(
                    "Expected Phase-3 alias " + activeAlias
                            + " but received " + tuple.getTable());
        }
    }

    private void clearActiveAlias() {
        this.activeAlias = null;
        this.activeParentEdge = null;
        this.ownedSampleIdsByParentKey = null;
        this.ownedChoicesBySampleId = null;
        this.randomBySampleId = null;
    }

    private static long stableSeed(
            String datasetSeed,
            String alias,
            long sampleInstanceId) {

        String value = (datasetSeed == null ? "" : datasetSeed)
                + "|SHARDED_PHASE3|" + alias
                + "|sample=" + sampleInstanceId;

        long hash = 1125899906842597L;
        for (int i = 0; i < value.length(); i++) {
            hash = 31L * hash + value.charAt(i);
        }
        return hash;
    }

    private static void validateNonNegativeFinite(double value, String label) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException(label + " must be finite: " + value);
        }
        if (value < 0.0d) {
            throw new IllegalArgumentException(label + " must be non-negative: " + value);
        }
    }

    private static long longField(JsonNode node, String field, long defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong(defaultValue);
    }
}