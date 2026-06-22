package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * One explicit multinomial root sample instance.
 *
 * Duplicates are allowed:
 *
 * sampleInstanceId=1 -> root candidate A
 * sampleInstanceId=2 -> root candidate A
 *
 * These must remain separate because Phase 3 may extend them differently.
 */
public final class OnePassRootSampleInstance implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long sampleInstanceId;
    private final long sourceCandidateId;
    private final String rootAlias;
    private final JsonNode rootTuple;
    private final double rootGroupWeight;

    public OnePassRootSampleInstance(
            long sampleInstanceId,
            OnePassRootSampleCandidate candidate) {

        if (candidate == null) {
            throw new IllegalArgumentException("candidate must not be null");
        }

        this.sampleInstanceId = sampleInstanceId;
        this.sourceCandidateId = candidate.getCandidateId();
        this.rootAlias = candidate.getRootAlias();
        this.rootTuple = candidate.getRootTuple();
        this.rootGroupWeight = candidate.getRootGroupWeight();
    }

    public long getSampleInstanceId() {
        return sampleInstanceId;
    }

    public long getSourceCandidateId() {
        return sourceCandidateId;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public JsonNode getRootTuple() {
        return rootTuple.deepCopy();
    }

    public double getRootGroupWeight() {
        return rootGroupWeight;
    }

    @Override
    public String toString() {
        return "OnePassRootSampleInstance{" +
                "sampleInstanceId=" + sampleInstanceId +
                ", sourceCandidateId=" + sourceCandidateId +
                ", rootAlias='" + rootAlias + '\'' +
                ", rootGroupWeight=" + rootGroupWeight +
                '}';
    }
}