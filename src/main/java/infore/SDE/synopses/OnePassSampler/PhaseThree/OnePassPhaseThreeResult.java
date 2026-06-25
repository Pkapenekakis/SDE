package infore.SDE.synopses.OnePassSampler.PhaseThree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Final Phase 3 result for the current noOfP = 1 implementation.
 */
public final class OnePassPhaseThreeResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String queryName;
    private final String rootAlias;
    private final int requestedSampleSize;
    private final List<OnePassCompletedSample> completedSamples;

    public OnePassPhaseThreeResult(String queryName, String rootAlias, int requestedSampleSize,
                                   List<OnePassCompletedSample> completedSamples) {
        this.queryName = queryName;
        this.rootAlias = rootAlias;
        this.requestedSampleSize = requestedSampleSize;
        this.completedSamples = Collections.unmodifiableList(new ArrayList<OnePassCompletedSample>(completedSamples));
    }

    public String getQueryName() {
        return queryName;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public int getRequestedSampleSize() {
        return requestedSampleSize;
    }

    public List<OnePassCompletedSample> getCompletedSamples() {
        return completedSamples;
    }

    public boolean hasFullSample() {
        return completedSamples.size() == requestedSampleSize;
    }

    @Override
    public String toString() {
        return "OnePassPhaseThreeResult{" +
                "queryName='" + queryName + '\'' +
                ", rootAlias='" + rootAlias + '\'' +
                ", requestedSampleSize=" + requestedSampleSize +
                ", completedSampleCount=" + completedSamples.size() +
                '}';
    }
}