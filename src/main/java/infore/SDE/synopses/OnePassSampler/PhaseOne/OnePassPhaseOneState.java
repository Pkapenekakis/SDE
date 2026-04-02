package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds Phase 1 preprocessing state:
 * - one hash index per logical parent-child edge
 * - tuple counts per alias
 *
 * Assumption for the first implementation:
 * the preprocessing replay feeds non-root aliases in leaf-to-root order.
 */
public class OnePassPhaseOneState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CompiledOnePassPlan plan;
    private final OnePassWeightEvaluator weightEvaluator;

    /*
    * IndexByEdgeId is the stored output of Phase1
    * For each logical edge keeps one Phase1LinkWeightIndex
    * edge B-C -> index
    * edge C-D -> index
    * */
    private final Map<String, Phase1LinkWeightIndex> indexByEdgeId;

    private final Map<String, Long> seenTuplesByAlias;

    public OnePassPhaseOneState(CompiledOnePassPlan plan,
                                OnePassWeightEvaluator weightEvaluator) {
        if (plan == null) {
            throw new IllegalArgumentException("CompiledOnePassPlan must not be null");
        }
        if (weightEvaluator == null) {
            throw new IllegalArgumentException("OnePassWeightEvaluator must not be null");
        }

        this.plan = plan;
        this.weightEvaluator = weightEvaluator;
        this.indexByEdgeId = new LinkedHashMap<String, Phase1LinkWeightIndex>();
        this.seenTuplesByAlias = new LinkedHashMap<String, Long>(); //Used for debugging

        //Create one empty Phase1LinkWeightIndex for each non-root parent edge
        for (String alias : plan.getAliases()) {
            seenTuplesByAlias.put(alias, 0L);
            if (!plan.isRoot(alias)) {
                CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);
                if (parentEdge != null && !indexByEdgeId.containsKey(parentEdge.getEdgeId())) {
                    indexByEdgeId.put(parentEdge.getEdgeId(),
                            new Phase1LinkWeightIndex(parentEdge.getEdgeId()));
                }
            }
        }
    }

    public void addTuple(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        final String alias = tuple.getTable();

        if (!plan.containsAlias(alias)) {
            throw new IllegalArgumentException(
                    "Tuple alias/table '" + alias + "' is not part of compiled plan");
        }

        seenTuplesByAlias.put(alias, seenTuplesByAlias.get(alias) + 1L);

        if (plan.isRoot(alias)) {
            // Phase 1 does not preprocess the main/root stream.
            return;
        }

        final double ownWeight = weightEvaluator.evaluate(tuple);

        double continuationWeight = 1.0d; //Leafs (no children) have a subTreeWeight of 1

        /*
        * For each child edge:
        *   Treat the current tuple as its parent
        *   Extract the parent-side join key from the Tuple
        *   Look that key up in the child edge Index
        *   Multiply all such values
        * */
        List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = plan.getChildEdges(alias);

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge : childEdges) {
            JoinValue currentTupleAsParentKey =
                    JoinValue.fromTuple(tuple, childEdge.getParentFields()); //Build the lookup key e.g ["o_custkey"]

            Phase1LinkWeightIndex childIndex = indexByEdgeId.get(childEdge.getEdgeId()); //Gets the child index e.g. B-C
            if (childIndex == null) {
                throw new IllegalStateException(
                        "Missing child index for edge " + childEdge.getEdgeId());
            }

            continuationWeight *= childIndex.getOrZero(currentTupleAsParentKey);
        }

        final double subtreeWeight = ownWeight * continuationWeight;

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias); //Gets the parent edge of B e.g. A-B
        if (parentEdge == null) {
            throw new IllegalStateException(
                    "Non-root alias '" + alias + "' unexpectedly has no parent edge");
        }

        //Build the key for pushing upward (to parent)
        JoinValue currentTupleAsChildKey =
                JoinValue.fromTuple(tuple, parentEdge.getChildFields());

        //Add the subtree weight to the parent index
        Phase1LinkWeightIndex parentIndex = indexByEdgeId.get(parentEdge.getEdgeId());
        if (parentIndex == null) {
            throw new IllegalStateException(
                    "Missing parent index for edge " + parentEdge.getEdgeId());
        }

        parentIndex.add(currentTupleAsChildKey, subtreeWeight);
    }

    public Phase1LinkWeightIndex getIndex(String edgeId) {
        return indexByEdgeId.get(edgeId);
    }

    //Used for debugging
    public Map<String, Object> debugSnapshot() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("rootAlias", plan.getRootAlias());
        out.put("leafToRootOrder", plan.getLeafToRootOrder());
        out.put("seenTuplesByAlias", new LinkedHashMap<String, Long>(seenTuplesByAlias));

        Map<String, Object> indexes = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Phase1LinkWeightIndex> e : indexByEdgeId.entrySet()) {
            indexes.put(e.getKey(), e.getValue().toDebugMap());
        }
        out.put("edgeIndexes", indexes);
        return out;
    }

    //Used for future distributed implementation, merges edge indexes edge-by-edge, adds tuple counters for debugging
    public void mergeFrom(OnePassPhaseOneState other) {
        if (other == null) {
            return;
        }

        if (!this.plan.getRootAlias().equals(other.plan.getRootAlias())
                || this.plan.getAliases().size() != other.plan.getAliases().size()) {
            throw new IllegalArgumentException("Cannot merge incompatible Phase 1 states");
        }

        for (Map.Entry<String, Phase1LinkWeightIndex> e : other.indexByEdgeId.entrySet()) {
            Phase1LinkWeightIndex mine = this.indexByEdgeId.get(e.getKey());
            if (mine == null) {
                mine = new Phase1LinkWeightIndex(e.getKey());
                this.indexByEdgeId.put(e.getKey(), mine);
            }
            mine.mergeFrom(e.getValue());
        }

        for (Map.Entry<String, Long> e : other.seenTuplesByAlias.entrySet()) {
            Long cur = this.seenTuplesByAlias.get(e.getKey());
            if (cur == null) {
                cur = 0L;
            }
            this.seenTuplesByAlias.put(e.getKey(), cur + e.getValue());
        }
    }
}