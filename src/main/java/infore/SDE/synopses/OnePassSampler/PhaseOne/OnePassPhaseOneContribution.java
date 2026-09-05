package infore.SDE.synopses.OnePassSampler.PhaseOne;

import java.io.Serializable;

/** One already-calculated contribution to one Phase-1 continuation index. */
public final class OnePassPhaseOneContribution implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String edgeId;
    private final JoinValue joinKey;
    private final double delta;

    public OnePassPhaseOneContribution(String edgeId, JoinValue joinKey, double delta) {
        if (edgeId == null || edgeId.trim().isEmpty()) {
            throw new IllegalArgumentException("edgeId must not be blank");
        }
        if (joinKey == null) {
            throw new IllegalArgumentException("joinKey must not be null");
        }

        this.edgeId = edgeId.trim();
        this.joinKey = joinKey;
        this.delta = delta;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public JoinValue getJoinKey() {
        return joinKey;
    }

    public double getDelta() {
        return delta;
    }
}