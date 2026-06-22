package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 2 root sampler.
 *
 * This class does not compute rootGroupWeight yet.
 * It receives root tuples whose rootGroupWeight has already been computed,
 * then feeds them into OnlineMultinomialSampler.
 */
public final class OnePassRootSampler implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String rootAlias;
    private final int sampleSize;
    private final OnlineMultinomialSampler<OnePassRootSampleCandidate> sampler;

    private long nextCandidateId;
    private long rootTuplesSeen;

    public OnePassRootSampler(String rootAlias, int sampleSize, String seed) {
        if (rootAlias == null || rootAlias.trim().isEmpty()) {
            throw new IllegalArgumentException("rootAlias must not be empty");
        }

        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be positive");
        }

        this.rootAlias = rootAlias;
        this.sampleSize = sampleSize;
        this.sampler =
                new OnlineMultinomialSampler<OnePassRootSampleCandidate>(
                        sampleSize,
                        seed
                );
    }

    /**
     * Adds one root tuple after its full rootGroupWeight has been computed.
     *
     * This method deliberately does not use the root tuple's own/base weight.
     * The caller must pass the full rootGroupWeight.
     */
    public void addRootTuple(JsonNode rootTuple, double rootGroupWeight) {
        rootTuplesSeen++;

        validateRootGroupWeight(rootGroupWeight);

        if (rootGroupWeight == 0.0d) {
            return;
        }

        OnePassRootSampleCandidate candidate =
                new OnePassRootSampleCandidate(
                        nextCandidateId++,
                        rootAlias,
                        rootTuple,
                        rootGroupWeight
                );

        sampler.add(candidate, rootGroupWeight);
    }

    public OnePassRootSampleResult finish() {
        OnlineMultinomialSample<OnePassRootSampleCandidate> sample =
                sampler.finish();

        List<OnePassRootSampleInstance> instances =
                new ArrayList<OnePassRootSampleInstance>(
                        sample.getSamples().size()
                );

        long sampleInstanceId = 0L;

        for (OnePassRootSampleCandidate candidate : sample.getSamples()) {
            instances.add(
                    new OnePassRootSampleInstance(
                            sampleInstanceId++,
                            candidate
                    )
            );
        }

        return new OnePassRootSampleResult(
                rootAlias,
                sampleSize,
                rootTuplesSeen,
                sample.getPositiveItemsSeen(),
                sample.getTotalWeight(),
                instances
        );
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public long getRootTuplesSeen() {
        return rootTuplesSeen;
    }

    private static void validateRootGroupWeight(double weight) {
        if (Double.isNaN(weight) || Double.isInfinite(weight)) {
            throw new IllegalArgumentException("rootGroupWeight must be finite");
        }

        if (weight < 0.0d) {
            throw new IllegalArgumentException("rootGroupWeight must be non-negative");
        }
    }
}