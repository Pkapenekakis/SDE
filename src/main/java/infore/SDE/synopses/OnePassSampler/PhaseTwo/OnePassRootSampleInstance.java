package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * One explicit multinomial root sample instance.
 *
 * Multiple sample instances may reference the same immutable source
 * candidate because Phase 2 samples with replacement.
 *
 * Sample instances remain distinct because Phase 3 may extend duplicate
 * roots differently.
 */
public final class OnePassRootSampleInstance implements Serializable {

    private static final long serialVersionUID = 1L;
    private final long sampleInstanceId;

    /*
     * Shared immutable root candidate.
     * One candidate may be referenced by multiple sample instances.
     */
    private final OnePassRootSampleCandidate candidate;

    public OnePassRootSampleInstance(long sampleInstanceId, OnePassRootSampleCandidate candidate) {
        if (candidate == null) {
            throw new IllegalArgumentException("candidate must not be null");
        }

        this.sampleInstanceId = sampleInstanceId;
        this.candidate = candidate;
    }

    public long getSampleInstanceId() {
        return sampleInstanceId;
    }

    public long getSourceCandidateId() {
        return candidate.getCandidateId();
    }

    public String getRootAlias() {
        return candidate.getRootAlias();
    }

    public JsonNode getRootTuple() {
        return candidate.getRootTuple();
    }

    public double getRootGroupWeight() {
        return candidate.getRootGroupWeight();
    }

    @Override
    public String toString() {
        return "OnePassRootSampleInstance{" + "sampleInstanceId=" +
                sampleInstanceId + ", sourceCandidateId=" + getSourceCandidateId() +
                ", rootAlias='" + getRootAlias() + '\'' + ", rootGroupWeight=" + getRootGroupWeight() + '}';
    }
}