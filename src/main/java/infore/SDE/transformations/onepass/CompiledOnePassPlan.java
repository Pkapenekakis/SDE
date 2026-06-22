package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Onepass.DatasetConfig;
import infore.SDE.messages.Onepass.JoinEdgeSpec;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.messages.Onepass.OnePassRequestValidator;
import infore.SDE.messages.Onepass.OutputSpec;
import infore.SDE.messages.Onepass.RelationSpec;
import infore.SDE.messages.Onepass.WeightSpec;

import java.io.Serializable;
import java.util.*;

/**
 * Compiles the raw OnePass JSON request into an execution plan.
 *
 * Responsibilities:
 * 1) Normalize the schema into typed relation / join metadata.
 * 2) Group multiple field-pairs between the same aliases into one logical edge
 *    (so composite join keys do not appear as graph cycles).
 * 3) Root the join graph at mainTable.
 * 4) Validate that the resulting alias graph is connected and acyclic.
 * 5) Pre-compute traversal orders needed by Phase 1 and later extension phases.
 *
 * This class is intentionally immutable and Serializable so it can be safely
 * attached to operator state / functions in SDE.
 */
public final class CompiledOnePassPlan implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String queryName;
    private final String rootAlias;

    private final String datasetName;
    private final String datasetDbConfig;
    private final int datasetScaleFactor;
    private final String datasetSeed;

    /*
     * Full compiled weight specification.
     *
     * This is required by Phase 2 because rootOwnWeight must be evaluated from
     * the same alias-local weight rules used by Phase 1.
     */
    private final WeightSpec weightSpec;

    /*
     * Kept as convenience/backwards-compatible accessors.
     */
    private final String weightExpression;
    private final List<String> weightVariables;
    private final Map<String, String> weightsByAlias;

    private final int sampleSize;
    private final List<String> projection;

    private final Map<String, RelationNode> relationsByAlias;
    private final Map<String, LogicalJoinEdge> logicalEdgesById;

    private final Map<String, String> parentByAlias;
    private final Map<String, List<String>> childrenByAlias;
    private final Map<String, Integer> depthByAlias;

    private final List<String> rootToLeafOrder;
    private final List<String> leafToRootOrder;
    private final Set<String> leafAliases;

    private CompiledOnePassPlan(
            String queryName,
            String rootAlias,
            String datasetName,
            String datasetDbConfig,
            int datasetScaleFactor,
            String datasetSeed,
            WeightSpec weightSpec,
            int sampleSize,
            List<String> projection,
            Map<String, RelationNode> relationsByAlias,
            Map<String, LogicalJoinEdge> logicalEdgesById,
            Map<String, String> parentByAlias,
            Map<String, List<String>> childrenByAlias,
            Map<String, Integer> depthByAlias,
            List<String> rootToLeafOrder,
            List<String> leafToRootOrder,
            Set<String> leafAliases) {

        this.queryName = queryName;
        this.rootAlias = rootAlias;

        this.datasetName = datasetName;
        this.datasetDbConfig = datasetDbConfig;
        this.datasetScaleFactor = datasetScaleFactor;
        this.datasetSeed = datasetSeed;

        this.weightSpec = copyWeightSpec(weightSpec);
        this.weightExpression = this.weightSpec.getExpression();
        this.weightVariables = Collections.unmodifiableList(
                toStringList(this.weightSpec.getVariables())
        );
        this.weightsByAlias = Collections.unmodifiableMap(
                copyStringMap(this.weightSpec.getWeightsByAlias())
        );

        this.sampleSize = sampleSize;
        this.projection = Collections.unmodifiableList(new ArrayList<String>(projection));

        this.relationsByAlias = Collections.unmodifiableMap(new LinkedHashMap<String, RelationNode>(relationsByAlias));
        this.logicalEdgesById = Collections.unmodifiableMap(new LinkedHashMap<String, LogicalJoinEdge>(logicalEdgesById));

        this.parentByAlias = Collections.unmodifiableMap(new LinkedHashMap<String, String>(parentByAlias));

        Map<String, List<String>> immutableChildren = new LinkedHashMap<String, List<String>>();
        for (Map.Entry<String, List<String>> e : childrenByAlias.entrySet()) {
            immutableChildren.put(e.getKey(), Collections.unmodifiableList(new ArrayList<String>(e.getValue())));
        }
        this.childrenByAlias = Collections.unmodifiableMap(immutableChildren);

        this.depthByAlias = Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(depthByAlias));
        this.rootToLeafOrder = Collections.unmodifiableList(new ArrayList<String>(rootToLeafOrder));
        this.leafToRootOrder = Collections.unmodifiableList(new ArrayList<String>(leafToRootOrder));
        this.leafAliases = Collections.unmodifiableSet(new LinkedHashSet<String>(leafAliases));
    }

    public static CompiledOnePassPlan from(OnePassParams params) {
        OnePassRequestValidator.validate(params);

        final Map<String, RelationNode> relationsByAlias = compileRelations(params);
        final Map<String, LogicalJoinEdge> logicalEdgesById = compileLogicalEdges(params);

        if (!relationsByAlias.containsKey(params.getMainTable())) {
            throw new IllegalArgumentException(
                    "mainTable/root alias '" + params.getMainTable() + "' does not exist in relations");
        }

        final String rootAlias = params.getMainTable();

        // Undirected adjacency over aliases using logical edges.
        final Map<String, Set<String>> adjacency = new LinkedHashMap<String, Set<String>>();
        for (String alias : relationsByAlias.keySet()) {
            adjacency.put(alias, new TreeSet<String>());
        }

        for (LogicalJoinEdge edge : logicalEdgesById.values()) {
            adjacency.get(edge.getAliasA()).add(edge.getAliasB());
            adjacency.get(edge.getAliasB()).add(edge.getAliasA());
        }

        // Check connectedness from the chosen root and compute BFS tree.
        final Map<String, String> parentByAlias = new LinkedHashMap<String, String>();
        final Map<String, Integer> depthByAlias = new LinkedHashMap<String, Integer>();
        final List<String> rootToLeafOrder = new ArrayList<String>();

        Queue<String> queue = new ArrayDeque<String>();
        queue.add(rootAlias);
        parentByAlias.put(rootAlias, null);
        depthByAlias.put(rootAlias, 0);

        while (!queue.isEmpty()) {
            String current = queue.remove();
            rootToLeafOrder.add(current);

            for (String neighbor : adjacency.get(current)) {
                if (!depthByAlias.containsKey(neighbor)) {
                    parentByAlias.put(neighbor, current);
                    depthByAlias.put(neighbor, depthByAlias.get(current) + 1);
                    queue.add(neighbor);
                }
            }
        }

        if (depthByAlias.size() != relationsByAlias.size()) {
            List<String> missing = new ArrayList<String>();
            for (String alias : relationsByAlias.keySet()) {
                if (!depthByAlias.containsKey(alias)) {
                    missing.add(alias);
                }
            }
            throw new IllegalArgumentException(
                    "Join graph is disconnected from root '" + rootAlias + "'. Unreachable aliases: " + missing);
        }

        // For a connected simple graph, edges == nodes - 1 <=> acyclic.
        if (logicalEdgesById.size() != relationsByAlias.size() - 1) {
            throw new IllegalArgumentException(
                    "One-pass* currently supports only connected acyclic join graphs. " +
                            "After grouping composite-key joins, found " + relationsByAlias.size() +
                            " aliases and " + logicalEdgesById.size() + " logical edges.");
        }

        final Map<String, List<String>> childrenByAlias = new LinkedHashMap<String, List<String>>();
        for (String alias : relationsByAlias.keySet()) {
            childrenByAlias.put(alias, new ArrayList<String>());
        }
        for (Map.Entry<String, String> e : parentByAlias.entrySet()) {
            String child = e.getKey();
            String parent = e.getValue();
            if (parent != null) {
                childrenByAlias.get(parent).add(child);
            }
        }
        for (List<String> children : childrenByAlias.values()) {
            Collections.sort(children);
        }

        final Set<String> leafAliases = new LinkedHashSet<String>();
        for (String alias : relationsByAlias.keySet()) {
            if (!alias.equals(rootAlias) && childrenByAlias.get(alias).isEmpty()) {
                leafAliases.add(alias);
            }
        }

        final List<String> leafToRootOrder = new ArrayList<String>();
        for (String alias : relationsByAlias.keySet()) {
            if (!alias.equals(rootAlias)) {
                leafToRootOrder.add(alias);
            }
        }
        Collections.sort(leafToRootOrder, new Comparator<String>() {
            @Override
            public int compare(String a, String b) {
                int depthCmp = Integer.compare(depthByAlias.get(b), depthByAlias.get(a)); // deeper first
                if (depthCmp != 0) {
                    return depthCmp;
                }
                return a.compareTo(b);
            }
        });

        DatasetConfig dataset = params.getDataset();
        WeightSpec weight = params.getWeight();
        OutputSpec output = params.getOutput();

        return new CompiledOnePassPlan(
                params.getQueryName(),
                rootAlias,
                dataset != null ? dataset.getName() : null,
                dataset != null ? dataset.getDbConfig() : null,
                dataset != null ? dataset.getScaleFactor() : 1,
                dataset != null ? dataset.getSeed() : null,
                weight,
                output != null ? output.getSampleSize() : 10,
                toStringList(output != null ? output.getProjection() : null),
                relationsByAlias,
                logicalEdgesById,
                parentByAlias,
                childrenByAlias,
                depthByAlias,
                rootToLeafOrder,
                leafToRootOrder,
                leafAliases
        );
    }

    private static Map<String, RelationNode> compileRelations(OnePassParams params) {
        Map<String, RelationNode> relationsByAlias = new LinkedHashMap<String, RelationNode>();

        if (params.getRelations() == null || params.getRelations().isEmpty()) {
            throw new IllegalArgumentException("relations must not be empty");
        }

        for (Object raw : params.getRelations()) {
            RelationSpec relation = coerceRelationSpec(raw);

            String alias = requireNonBlank(relation.getAlias(), "relation.alias");
            String table = requireNonBlank(relation.getTable(), "relation.table");

            if (relationsByAlias.containsKey(alias)) {
                throw new IllegalArgumentException("Duplicate relation alias: " + alias);
            }

            relationsByAlias.put(alias, new RelationNode(alias, table));
        }

        return relationsByAlias;
    }

    private static Map<String, LogicalJoinEdge> compileLogicalEdges(OnePassParams params) {
        Map<String, LogicalJoinEdgeBuilder> builders = new LinkedHashMap<String, LogicalJoinEdgeBuilder>();

        if (params.getJoins() == null || params.getJoins().isEmpty()) {
            throw new IllegalArgumentException("joins must not be empty");
        }

        for (Object raw : params.getJoins()) {
            JoinEdgeSpec join = coerceJoinEdgeSpec(raw);

            String leftAlias = requireNonBlank(join.getLeftAlias(), "join.leftAlias");
            String rightAlias = requireNonBlank(join.getRightAlias(), "join.rightAlias");
            String leftField = requireNonBlank(join.getLeftField(), "join.leftField");
            String rightField = requireNonBlank(join.getRightField(), "join.rightField");

            if (leftAlias.equals(rightAlias)) {
                throw new IllegalArgumentException(
                        "Self-join edge on the same alias is not allowed: " + leftAlias +
                                ". Use two aliases if you want a self-join.");
            }

            String aliasA;
            String aliasB;

            if (leftAlias.compareTo(rightAlias) <= 0) {
                aliasA = leftAlias;
                aliasB = rightAlias;
            } else {
                aliasA = rightAlias;
                aliasB = leftAlias;
            }

            String edgeId = edgeId(aliasA, aliasB);
            LogicalJoinEdgeBuilder builder = builders.get(edgeId);
            if (builder == null) {
                builder = new LogicalJoinEdgeBuilder(aliasA, aliasB);
                builders.put(edgeId, builder);
            }

            builder.add(leftAlias, leftField, rightAlias, rightField);
        }

        Map<String, LogicalJoinEdge> compiled = new LinkedHashMap<String, LogicalJoinEdge>();
        for (Map.Entry<String, LogicalJoinEdgeBuilder> e : builders.entrySet()) {
            compiled.put(e.getKey(), e.getValue().build());
        }
        return compiled;
    }

    private static RelationSpec coerceRelationSpec(Object raw) {
        if (raw instanceof RelationSpec) {
            return (RelationSpec) raw;
        }
        return MAPPER.convertValue(raw, RelationSpec.class);
    }

    private static JoinEdgeSpec coerceJoinEdgeSpec(Object raw) {
        if (raw instanceof JoinEdgeSpec) {
            return (JoinEdgeSpec) raw;
        }
        return MAPPER.convertValue(raw, JoinEdgeSpec.class);
    }

    private static List<String> toStringList(List raw) {
        List<String> out = new ArrayList<String>();
        if (raw == null) {
            return out;
        }
        for (Object o : raw) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    private static Map<String, String> copyStringMap(Map raw) {
        Map<String, String> out = new LinkedHashMap<String, String>();

        if (raw == null) {
            return out;
        }

        for (Object rawEntry : raw.entrySet()) {
            Map.Entry entry = (Map.Entry) rawEntry;

            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }

            String key = String.valueOf(entry.getKey()).trim();
            String value = String.valueOf(entry.getValue()).trim();

            if (!key.isEmpty() && !value.isEmpty()) {
                out.put(key, value);
            }
        }

        return out;
    }

    private static WeightSpec copyWeightSpec(WeightSpec source) {
        WeightSpec copy = new WeightSpec();

        if (source == null) {
            copy.setExpression("1");
            copy.setVariables(new ArrayList<String>());
            copy.setWeightsByAlias(new LinkedHashMap<String, String>());
            return copy;
        }

        String expression = source.getExpression();

        if (expression == null || expression.trim().isEmpty()) {
            expression = "1";
        }

        copy.setExpression(expression);
        copy.setVariables(toStringList(source.getVariables()));
        copy.setWeightsByAlias(copyStringMap(source.getWeightsByAlias()));

        return copy;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    private static String edgeId(String aliasA, String aliasB) {
        return aliasA + "<->" + aliasB;
    }

    // ---------- Accessors ----------

    public String getQueryName() {
        return queryName;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getDatasetDbConfig() {
        return datasetDbConfig;
    }

    public int getDatasetScaleFactor() {
        return datasetScaleFactor;
    }

    public String getDatasetSeed() {
        return datasetSeed;
    }

    /**
     * Returns a defensive copy of the full compiled WeightSpec.
     *
     * Phase 1 and Phase 2 should use this instead of only getWeightExpression(),
     * because WEIGHTED BY is now represented through weightsByAlias.
     */
    public WeightSpec getWeightSpec() {
        return copyWeightSpec(weightSpec);
    }

    public String getWeightExpression() {
        return weightExpression;
    }

    public List<String> getWeightVariables() {
        return weightVariables;
    }

    public Map<String, String> getWeightsByAlias() {
        return weightsByAlias;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public List<String> getProjection() {
        return projection;
    }

    public boolean containsAlias(String alias) {
        return relationsByAlias.containsKey(alias);
    }

    public RelationNode getRelation(String alias) {
        return relationsByAlias.get(alias);
    }

    public Collection<RelationNode> getRelations() {
        return relationsByAlias.values();
    }

    public Set<String> getAliases() {
        return relationsByAlias.keySet();
    }

    public String getParentAlias(String alias) {
        return parentByAlias.get(alias);
    }

    public List<String> getChildrenAliases(String alias) {
        List<String> children = childrenByAlias.get(alias);
        return children == null ? Collections.<String>emptyList() : children;
    }

    public int getDepth(String alias) {
        Integer depth = depthByAlias.get(alias);
        if (depth == null) {
            throw new IllegalArgumentException("Unknown alias: " + alias);
        }
        return depth;
    }

    public boolean isRoot(String alias) {
        return rootAlias.equals(alias);
    }

    public boolean isLeaf(String alias) {
        return leafAliases.contains(alias);
    }

    /**
     * Phase 1 preprocessing order:
     * deepest aliases first, root excluded.
     */
    public List<String> getLeafToRootOrder() {
        return leafToRootOrder;
    }

    /**
     * Useful later for sample extension / traversal away from the root.
     */
    public List<String> getRootToLeafOrder() {
        return rootToLeafOrder;
    }

    public Set<String> getLeafAliases() {
        return leafAliases;
    }

    public LogicalJoinEdge getLogicalEdge(String alias1, String alias2) {
        String a = alias1.compareTo(alias2) <= 0 ? alias1 : alias2;
        String b = alias1.compareTo(alias2) <= 0 ? alias2 : alias1;
        return logicalEdgesById.get(edgeId(a, b));
    }

    public DirectedJoinEdge getDirectedEdge(String parentAlias, String childAlias) {
        LogicalJoinEdge edge = getLogicalEdge(parentAlias, childAlias);
        if (edge == null) {
            throw new IllegalArgumentException(
                    "No logical edge exists between aliases '" + parentAlias + "' and '" + childAlias + "'");
        }
        return edge.asDirected(parentAlias, childAlias);
    }

    public DirectedJoinEdge getParentEdge(String childAlias) {
        String parent = getParentAlias(childAlias);
        if (parent == null) {
            return null;
        }
        return getDirectedEdge(parent, childAlias);
    }

    public List<DirectedJoinEdge> getChildEdges(String parentAlias) {
        List<DirectedJoinEdge> out = new ArrayList<DirectedJoinEdge>();
        for (String child : getChildrenAliases(parentAlias)) {
            out.add(getDirectedEdge(parentAlias, child));
        }
        return Collections.unmodifiableList(out);
    }

    @Override
    public String toString() {
        return "CompiledOnePassPlan{" +
                "queryName='" + queryName + '\'' +
                ", rootAlias='" + rootAlias + '\'' +
                ", aliases=" + relationsByAlias.keySet() +
                ", rootToLeafOrder=" + rootToLeafOrder +
                ", leafToRootOrder=" + leafToRootOrder +
                ", weightsByAlias=" + weightsByAlias +
                '}';
    }

    // ---------- Nested model classes ----------

    public static final class RelationNode implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String alias;
        private final String table;

        public RelationNode(String alias, String table) {
            this.alias = alias;
            this.table = table;
        }

        public String getAlias() {
            return alias;
        }

        public String getTable() {
            return table;
        }

        @Override
        public String toString() {
            return alias + "(" + table + ")";
        }
    }

    public static final class LogicalJoinEdge implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String edgeId;
        private final String aliasA;
        private final String aliasB;
        private final List<FieldPair> fieldPairs;

        public LogicalJoinEdge(String edgeId, String aliasA, String aliasB, List<FieldPair> fieldPairs) {
            this.edgeId = edgeId;
            this.aliasA = aliasA;
            this.aliasB = aliasB;
            this.fieldPairs = Collections.unmodifiableList(new ArrayList<FieldPair>(fieldPairs));
        }

        public String getEdgeId() {
            return edgeId;
        }

        public String getAliasA() {
            return aliasA;
        }

        public String getAliasB() {
            return aliasB;
        }

        public List<FieldPair> getFieldPairs() {
            return fieldPairs;
        }

        public boolean connects(String x, String y) {
            return (aliasA.equals(x) && aliasB.equals(y)) || (aliasA.equals(y) && aliasB.equals(x));
        }

        public DirectedJoinEdge asDirected(String parentAlias, String childAlias) {
            if (!connects(parentAlias, childAlias)) {
                throw new IllegalArgumentException(
                        "Edge " + edgeId + " does not connect " + parentAlias + " and " + childAlias);
            }

            List<String> parentFields = new ArrayList<String>();
            List<String> childFields = new ArrayList<String>();

            boolean forward = aliasA.equals(parentAlias) && aliasB.equals(childAlias);

            for (FieldPair pair : fieldPairs) {
                if (forward) {
                    parentFields.add(pair.getFieldA());
                    childFields.add(pair.getFieldB());
                } else {
                    parentFields.add(pair.getFieldB());
                    childFields.add(pair.getFieldA());
                }
            }

            return new DirectedJoinEdge(edgeId, parentAlias, childAlias, parentFields, childFields);
        }

        @Override
        public String toString() {
            return edgeId + fieldPairs;
        }
    }

    public static final class FieldPair implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String fieldA;
        private final String fieldB;

        public FieldPair(String fieldA, String fieldB) {
            this.fieldA = fieldA;
            this.fieldB = fieldB;
        }

        public String getFieldA() {
            return fieldA;
        }

        public String getFieldB() {
            return fieldB;
        }

        @Override
        public String toString() {
            return "(" + fieldA + "=" + fieldB + ")";
        }
    }

    public static final class DirectedJoinEdge implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String edgeId;
        private final String parentAlias;
        private final String childAlias;
        private final List<String> parentFields;
        private final List<String> childFields;

        public DirectedJoinEdge(
                String edgeId,
                String parentAlias,
                String childAlias,
                List<String> parentFields,
                List<String> childFields) {

            this.edgeId = edgeId;
            this.parentAlias = parentAlias;
            this.childAlias = childAlias;
            this.parentFields = Collections.unmodifiableList(new ArrayList<String>(parentFields));
            this.childFields = Collections.unmodifiableList(new ArrayList<String>(childFields));
        }

        public String getEdgeId() {
            return edgeId;
        }

        public String getParentAlias() {
            return parentAlias;
        }

        public String getChildAlias() {
            return childAlias;
        }

        public List<String> getParentFields() {
            return parentFields;
        }

        public List<String> getChildFields() {
            return childFields;
        }

        public boolean isCompositeKey() {
            return parentFields.size() > 1;
        }

        @Override
        public String toString() {
            return "DirectedJoinEdge{" +
                    "edgeId='" + edgeId + '\'' +
                    ", parentAlias='" + parentAlias + '\'' +
                    ", childAlias='" + childAlias + '\'' +
                    ", parentFields=" + parentFields +
                    ", childFields=" + childFields +
                    '}';
        }
    }

    private static final class LogicalJoinEdgeBuilder {
        private final String aliasA;
        private final String aliasB;
        private final List<FieldPair> fieldPairs = new ArrayList<FieldPair>();
        private final Set<String> signatures = new LinkedHashSet<String>();

        private LogicalJoinEdgeBuilder(String aliasA, String aliasB) {
            this.aliasA = aliasA;
            this.aliasB = aliasB;
        }

        private void add(String leftAlias, String leftField, String rightAlias, String rightField) {
            String fieldA;
            String fieldB;

            if (aliasA.equals(leftAlias) && aliasB.equals(rightAlias)) {
                fieldA = leftField;
                fieldB = rightField;
            } else if (aliasA.equals(rightAlias) && aliasB.equals(leftAlias)) {
                fieldA = rightField;
                fieldB = leftField;
            } else {
                throw new IllegalArgumentException(
                        "Join pair does not match builder endpoints: " +
                                leftAlias + "." + leftField + " = " + rightAlias + "." + rightField);
            }

            String signature = fieldA + "=" + fieldB;
            if (!signatures.add(signature)) {
                throw new IllegalArgumentException(
                        "Duplicate field-pair on logical edge " + aliasA + "<->" + aliasB + ": " + signature);
            }

            fieldPairs.add(new FieldPair(fieldA, fieldB));
        }

        private LogicalJoinEdge build() {
            return new LogicalJoinEdge(edgeId(aliasA, aliasB), aliasA, aliasB, fieldPairs);
        }
    }
}