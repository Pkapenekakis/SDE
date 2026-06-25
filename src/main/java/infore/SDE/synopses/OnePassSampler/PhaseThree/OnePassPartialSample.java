package infore.SDE.synopses.OnePassSampler.PhaseThree;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mutable partial joined sample used during Phase 3.
 *
 * One object corresponds to one explicit Phase 2 root sample instance.
 * Duplicates from Phase 2 remain separate because each partial sample has
 * its own sampleInstanceId.
 */
public final class OnePassPartialSample implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long sampleInstanceId;
    private final long sourceRootCandidateId;

    private final Map<String, OnePassTuple> selectedTuplesByAlias;

    public OnePassPartialSample(long sampleInstanceId, long sourceRootCandidateId) {
        this.sampleInstanceId = sampleInstanceId;
        this.sourceRootCandidateId = sourceRootCandidateId;
        this.selectedTuplesByAlias = new LinkedHashMap<String, OnePassTuple>();
    }

    public long getSampleInstanceId() {
        return sampleInstanceId;
    }

    public long getSourceRootCandidateId() {
        return sourceRootCandidateId;
    }

    public boolean hasAlias(String alias) {
        return selectedTuplesByAlias.containsKey(alias);
    }

    public OnePassTuple getTuple(String alias) {
        return selectedTuplesByAlias.get(alias);
    }

    public void putTuple(String alias, OnePassTuple tuple) {
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("alias must not be blank");
        }

        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (!alias.equals(tuple.getTable())) {
            throw new IllegalArgumentException("Alias mismatch. Expected alias '" + alias +
                    "' but tuple belongs to alias '" + tuple.getTable() + "'");
        }

        if (selectedTuplesByAlias.containsKey(alias)) {
            throw new IllegalStateException("Sample " + sampleInstanceId + " already contains alias '" + alias + "'");
        }

        selectedTuplesByAlias.put(alias, tuple);
    }

    public Map<String, OnePassTuple> getSelectedTuplesByAlias() {
        return Collections.unmodifiableMap(selectedTuplesByAlias);
    }

    public boolean isComplete(CompiledOnePassPlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }

        for (String alias : plan.getAliases()) {
            if (!selectedTuplesByAlias.containsKey(alias)) {
                return false;
            }
        }

        return true;
    }

    public OnePassPartialSample copy() {
        OnePassPartialSample copy = new OnePassPartialSample(sampleInstanceId, sourceRootCandidateId);

        copy.selectedTuplesByAlias.putAll(selectedTuplesByAlias);

        return copy;
    }

    @Override
    public String toString() {
        return "OnePassPartialSample{" +
                "sampleInstanceId=" + sampleInstanceId +
                ", sourceRootCandidateId=" + sourceRootCandidateId +
                ", aliases=" + selectedTuplesByAlias.keySet() +
                '}';
    }
}