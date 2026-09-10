package infore.SDE.transformations.onepass;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Single source of truth for OnePass sharded-state ownership.
 * IMPORTANT: both the data router and worker-side state readers/writers must use this class.
 * Otherwise, the worker selected to read continuation state and
 * the worker that physically owns that state can diverge.
 */
public final class OnePassShardOwnership {

    private OnePassShardOwnership() {}

    public static int ownerForEdgeKey(String edgeId, JoinValue joinValue, int parallelism) {

        if (parallelism <= 1) {
            return 0;
        }
        if (edgeId == null || edgeId.trim().isEmpty()) {
            throw new IllegalArgumentException("edgeId must not be blank");
        }
        if (joinValue == null) {
            throw new IllegalArgumentException("joinValue must not be null");
        }

        StringBuilder canonical = new StringBuilder();
        canonical.append(edgeId.length()).append(':').append(edgeId).append('|');

        for (String part : joinValue.getParts()) {
            String value = part == null ? "" : part;
            canonical.append(value.length()).append(':').append(value).append('|');
        }

        return stableWorkerHash(canonical.toString(), parallelism);
    }

    /**
     * Phase-1 input owner.
     * Internal alias -> first child continuation owner.
     * Leaf alias     -> final parent-edge index owner.
     */
    public static int ownerForPhaseOneInputTuple(OnePassTuple tuple, CompiledOnePassPlan plan, int parallelism) {

        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }

        String alias = tuple.getTable();
        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);

        if (!childEdges.isEmpty()) {
            CompiledOnePassPlan.DirectedJoinEdge firstChild = childEdges.get(0);
            JoinValue lookupKey = JoinValue.fromTuple(tuple, firstChild.getParentFields());
            return ownerForEdgeKey(firstChild.getEdgeId(), lookupKey, parallelism);
        }

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);
        if (parentEdge == null) {
            throw new IllegalStateException(
                    "Non-root Phase-1 alias '" + alias + "' has no parent edge");
        }

        JoinValue writeKey = JoinValue.fromTuple(tuple, parentEdge.getChildFields());
        return ownerForEdgeKey(parentEdge.getEdgeId(), writeKey, parallelism);
    }

    /**
     * Initial Phase-3 input owner.
     * Internal alias -> child-edge-0 continuation owner.
     * Leaf alias     -> parent-edge selection owner.
     */
    public static int ownerForPhaseThreeInputTuple(OnePassTuple tuple, CompiledOnePassPlan plan, int parallelism) {

        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }

        String alias = tuple.getTable();
        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException("Unknown Phase-3 alias '" + alias + "'");
        }
        if (plan.isRoot(alias)) {
            throw new IllegalArgumentException("Phase-3 input cannot be root alias '" + alias + "'");
        }

        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);

        if (!childEdges.isEmpty()) {
            CompiledOnePassPlan.DirectedJoinEdge firstChild = childEdges.get(0);
            JoinValue lookupKey = JoinValue.fromTuple(tuple, firstChild.getParentFields());

            return ownerForEdgeKey(firstChild.getEdgeId(), lookupKey, parallelism);
        }

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);
        if (parentEdge == null) {
            throw new IllegalStateException("Non-root Phase-3 alias '" + alias + "' has no parent edge");
        }

        JoinValue selectionKey = JoinValue.fromTuple(tuple, parentEdge.getChildFields());

        return ownerForEdgeKey(parentEdge.getEdgeId(), selectionKey, parallelism);
    }

    public static String workerKey(String baseKey, int parallelism, int workerId) {
        return baseKey + "_" + parallelism + "_KEYED_" + workerId;
    }

    public static String baseKeyFromWorkerKey(String workerKey, int parallelism, int workerId) {

        if (workerKey == null) {
            return "";
        }

        String suffix = "_" + parallelism + "_KEYED_" + workerId;
        return workerKey.endsWith(suffix) ? workerKey.substring(0, workerKey.length() - suffix.length()) : workerKey;
    }

    //Initial Phase-2 root owner: first child continuation owner.
    public static int ownerForPhaseTwoRootTuple(OnePassTuple tuple, CompiledOnePassPlan plan, int parallelism) {

        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }

        String alias = tuple.getTable();
        if (!plan.isRoot(alias)) {
            throw new IllegalArgumentException("Phase-2 input must be root alias '" + plan.getRootAlias() +
                    "' but received '" + alias + "'");
        }

        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);

        if (childEdges.isEmpty()) {
            return 0;
        }

        CompiledOnePassPlan.DirectedJoinEdge firstChild = childEdges.get(0);
        JoinValue lookupKey = JoinValue.fromTuple(tuple, firstChild.getParentFields());

        return ownerForEdgeKey(firstChild.getEdgeId(), lookupKey, parallelism);
    }

    // deterministic FNV-1a 64-bit hash, folded to int
    private static int stableWorkerHash(String routingKey, int parallelism) {
        byte[] bytes = routingKey.getBytes(StandardCharsets.UTF_8);

        long hash = -3750763034362895579L;
        final long prime = 1099511628211L;

        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= prime;
        }

        int folded = Long.hashCode(hash);
        return Math.floorMod(folded, parallelism);
    }
}