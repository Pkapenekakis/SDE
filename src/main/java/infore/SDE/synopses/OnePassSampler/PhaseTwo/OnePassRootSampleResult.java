package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Final Phase 2 root-sampling result.
 *
 * This result contains explicit sample instances, not a deduplicated list.
 */
public final class OnePassRootSampleResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String rootAlias;
    private final int requestedSampleSize;
    private final long rootTuplesSeen;
    private final long positiveRootCandidatesSeen;
    private final double totalRootGroupWeight;
    private final List<OnePassRootSampleInstance> sampleInstances;

    public OnePassRootSampleResult(
            String rootAlias,
            int requestedSampleSize,
            long rootTuplesSeen,
            long positiveRootCandidatesSeen,
            double totalRootGroupWeight,
            List<OnePassRootSampleInstance> sampleInstances) {

        this.rootAlias = rootAlias;
        this.requestedSampleSize = requestedSampleSize;
        this.rootTuplesSeen = rootTuplesSeen;
        this.positiveRootCandidatesSeen = positiveRootCandidatesSeen;
        this.totalRootGroupWeight = totalRootGroupWeight;
        this.sampleInstances = Collections.unmodifiableList(
                new ArrayList<OnePassRootSampleInstance>(sampleInstances)
        );
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public int getRequestedSampleSize() {
        return requestedSampleSize;
    }

    public long getRootTuplesSeen() {
        return rootTuplesSeen;
    }

    public long getPositiveRootCandidatesSeen() {
        return positiveRootCandidatesSeen;
    }

    public double getTotalRootGroupWeight() {
        return totalRootGroupWeight;
    }

    public List<OnePassRootSampleInstance> getSampleInstances() {
        return sampleInstances;
    }

    public boolean hasFullSample() {
        return sampleInstances.size() == requestedSampleSize;
    }

    @Override
    public String toString() {
        return "OnePassRootSampleResult{" +
                "rootAlias='" + rootAlias + '\'' +
                ", requestedSampleSize=" + requestedSampleSize +
                ", rootTuplesSeen=" + rootTuplesSeen +
                ", positiveRootCandidatesSeen=" + positiveRootCandidatesSeen +
                ", totalRootGroupWeight=" + totalRootGroupWeight +
                ", sampleInstanceCount=" + sampleInstances.size() +
                '}';
    }
}