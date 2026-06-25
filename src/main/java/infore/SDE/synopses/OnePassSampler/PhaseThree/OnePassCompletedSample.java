package infore.SDE.synopses.OnePassSampler.PhaseThree;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One complete joined sample after Phase 3.
 *
 * This still stores full tuples by alias.
 * TODO: Projection should be applied later, after the algorithmic extension is validated.
 */
public final class OnePassCompletedSample implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long sampleInstanceId;
    private final long sourceRootCandidateId;
    private final Map<String, OnePassTuple> tuplesByAlias;

    public OnePassCompletedSample(long sampleInstanceId, long sourceRootCandidateId,
                                  Map<String, OnePassTuple> tuplesByAlias) {

        if (tuplesByAlias == null || tuplesByAlias.isEmpty()) {
            throw new IllegalArgumentException("tuplesByAlias must not be empty");
        }

        this.sampleInstanceId = sampleInstanceId;
        this.sourceRootCandidateId = sourceRootCandidateId;
        this.tuplesByAlias = Collections.unmodifiableMap( new LinkedHashMap<String, OnePassTuple>(tuplesByAlias));
    }

    public long getSampleInstanceId() {
        return sampleInstanceId;
    }

    public long getSourceRootCandidateId() {
        return sourceRootCandidateId;
    }

    public Map<String, OnePassTuple> getTuplesByAlias() {
        return tuplesByAlias;
    }

    public OnePassTuple getTuple(String alias) {
        return tuplesByAlias.get(alias);
    }

    @Override
    public String toString() {
        return "OnePassCompletedSample{" +
                "sampleInstanceId=" + sampleInstanceId +
                ", sourceRootCandidateId=" + sourceRootCandidateId +
                ", aliases=" + tuplesByAlias.keySet() +
                '}';
    }
}