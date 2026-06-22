package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * A positive-weight root tuple candidate seen during Phase 2.
 *
 * This is not yet a final sample instance.
 * The same candidate may appear multiple times in the final multinomial output.
 */
public final class OnePassRootSampleCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long candidateId;
    private final String rootAlias;
    private final JsonNode rootTuple;
    private final double rootGroupWeight;

    public OnePassRootSampleCandidate(
            long candidateId,
            String rootAlias,
            JsonNode rootTuple,
            double rootGroupWeight) {

        if (rootAlias == null || rootAlias.trim().isEmpty()) {
            throw new IllegalArgumentException("rootAlias must not be empty");
        }

        if (rootTuple == null) {
            throw new IllegalArgumentException("rootTuple must not be null");
        }

        validatePositiveFiniteWeight(rootGroupWeight);

        this.candidateId = candidateId;
        this.rootAlias = rootAlias;
        this.rootTuple = rootTuple.deepCopy();
        this.rootGroupWeight = rootGroupWeight;
    }

    public long getCandidateId() {
        return candidateId;
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

    private static void validatePositiveFiniteWeight(double weight) {
        if (Double.isNaN(weight) || Double.isInfinite(weight)) {
            throw new IllegalArgumentException("rootGroupWeight must be finite");
        }

        if (weight <= 0.0d) {
            throw new IllegalArgumentException("rootGroupWeight must be positive");
        }
    }

    @Override
    public String toString() {
        return "OnePassRootSampleCandidate{" +
                "candidateId=" + candidateId +
                ", rootAlias='" + rootAlias + '\'' +
                ", rootGroupWeight=" + rootGroupWeight +
                '}';
    }
}