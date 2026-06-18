package infore.SDE.Experiments.Onepass;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.DatasetConfig;
import infore.SDE.messages.Onepass.JoinEdgeSpec;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.messages.Onepass.OutputSpec;
import infore.SDE.messages.Onepass.RelationSpec;
import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOne;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;

import java.util.Arrays;
import java.util.Map;

public class PhaseOneDirectTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        OnePassParams params = buildPlan();
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassPhaseOne synopsis =
                new OnePassPhaseOne(31002, plan, params.getWeight());

        /*
         * Join tree:
         *
         *          A
         *          |
         *          B
         *        /   \
         *       C     E
         *
         * A is the main/root relation.
         * B, C, E are side relations for Phase 1.
         *
         * Phase 1 must process side relations from leaves to root:
         *
         *      C first
         *      E second
         *      B last
         */

        // Leaf child C.
        synopsis.add(tupleC("k1", 5.0));
        synopsis.add(tupleC("k1", 8.0));
        synopsis.add(tupleC("k2", 1.0));

        // Leaf child E.
        synopsis.add(tupleE("z1", 4.0));
        synopsis.add(tupleE("z1", 6.0));
        synopsis.add(tupleE("z2", 3.0));

        // Parent side tuple B.
        synopsis.add(tupleB("x1", "k1", "z1", 2.0));

        OnePassPhaseOneResult result = synopsis.exportResult();

        System.out.println("Phase 1 result:");
        System.out.println(result.toDebugMap());

        validate(result);

        System.out.println();
        System.out.println("SUCCESS: Phase 1 two-child-edges test passed.");
    }

    private static OnePassParams buildPlan() {
        OnePassParams p = new OnePassParams();

        p.setQueryName("test-a-b-c-e");
        p.setMainTable("A");

        RelationSpec a = relation("A", "A");
        RelationSpec b = relation("B", "B");
        RelationSpec c = relation("C", "C");
        RelationSpec e = relation("E", "E");

        p.setRelations(Arrays.asList(a, b, c, e));

        /*
         * Root-to-leaf join structure:
         *
         * A.ab_key = B.ab_key
         * B.bc_key = C.bc_key
         * B.be_key = E.be_key
         */
        JoinEdgeSpec ab = join("A", "ab_key", "B", "ab_key");
        JoinEdgeSpec bc = join("B", "bc_key", "C", "bc_key");
        JoinEdgeSpec be = join("B", "be_key", "E", "be_key");

        p.setJoins(Arrays.asList(ab, bc, be));

        WeightSpec weight = new WeightSpec();
        weight.setExpression("weight");
        weight.setVariables(Arrays.asList("weight"));
        p.setWeight(weight);

        OutputSpec output = new OutputSpec();
        output.setSampleSize(10);
        output.setProjection(Arrays.asList("A", "B", "C", "E"));
        p.setOutput(output);

        DatasetConfig dataset = new DatasetConfig();
        dataset.setName("synthetic");
        dataset.setScaleFactor(1);
        dataset.setSeed("1");
        p.setDataset(dataset);

        return p;
    }

    private static RelationSpec relation(String alias, String table) {
        RelationSpec r = new RelationSpec();
        r.setAlias(alias);
        r.setTable(table);
        return r;
    }

    private static JoinEdgeSpec join(
            String leftAlias,
            String leftField,
            String rightAlias,
            String rightField) {

        JoinEdgeSpec j = new JoinEdgeSpec();

        j.setLeftAlias(leftAlias);
        j.setLeftField(leftField);
        j.setRightAlias(rightAlias);
        j.setRightField(rightField);

        return j;
    }

    private static ObjectNode tupleC(String bcKey, double weight) {
        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "C");
        tuple.put("bc_key", bcKey);
        tuple.put("weight", weight);

        return tuple;
    }

    private static ObjectNode tupleE(String beKey, double weight) {
        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "E");
        tuple.put("be_key", beKey);
        tuple.put("weight", weight);

        return tuple;
    }

    private static ObjectNode tupleB(
            String abKey,
            String bcKey,
            String beKey,
            double weight) {

        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "B");
        tuple.put("ab_key", abKey);
        tuple.put("bc_key", bcKey);
        tuple.put("be_key", beKey);
        tuple.put("weight", weight);

        return tuple;
    }

    private static void validate(OnePassPhaseOneResult result) {
        Map<String, Map<String, Double>> edgeIndexes = result.getEdgeIndexes();

        /*
         * C tuples:
         *
         * c1: bc_key = k1, weight = 5
         * c2: bc_key = k1, weight = 8
         * c3: bc_key = k2, weight = 1
         *
         * Therefore:
         *
         * B<->C[k1] = 5 + 8 = 13
         * B<->C[k2] = 1
         */
        assertWeight(edgeIndexes, "B<->C", "k1", 13.0);
        assertWeight(edgeIndexes, "B<->C", "k2", 1.0);

        /*
         * E tuples:
         *
         * e1: be_key = z1, weight = 4
         * e2: be_key = z1, weight = 6
         * e3: be_key = z2, weight = 3
         *
         * Therefore:
         *
         * B<->E[z1] = 4 + 6 = 10
         * B<->E[z2] = 3
         */
        assertWeight(edgeIndexes, "B<->E", "z1", 10.0);
        assertWeight(edgeIndexes, "B<->E", "z2", 3.0);

        /*
         * B tuple:
         *
         * b1:
         *      weight = 2
         *      ab_key = x1
         *      bc_key = k1
         *      be_key = z1
         *
         * continuationWeight(b1) =
         *      B<->C[k1] * B<->E[z1]
         *    = 13 * 10
         *    = 130
         *
         * subtreeWeight(b1) =
         *      weight(b1) * continuationWeight(b1)
         *    = 2 * 130
         *    = 260
         *
         * Therefore:
         *
         * A<->B[x1] = 260
         */
        assertWeight(edgeIndexes, "A<->B", "x1", 260.0);

        assertSeen(result, "A", 0L);
        assertSeen(result, "B", 1L);
        assertSeen(result, "C", 3L);
        assertSeen(result, "E", 3L);
    }

    private static void assertWeight(
            Map<String, Map<String, Double>> edgeIndexes,
            String edgeId,
            String key,
            double expected) {

        Map<String, Double> index = edgeIndexes.get(edgeId);

        if (index == null) {
            throw new IllegalStateException("Missing edge index: " + edgeId);
        }

        Double actual = index.get(key);

        if (actual == null) {
            throw new IllegalStateException(
                    "Missing key '" + key + "' in edge index " + edgeId
            );
        }

        double tolerance = 0.000001d;

        if (Math.abs(expected - actual) > tolerance) {
            throw new IllegalStateException(
                    edgeId + "[" + key + "] expected " + expected + " but got " + actual
            );
        }
    }

    private static void assertSeen(
            OnePassPhaseOneResult result,
            String alias,
            long expected) {

        Long actual = result.getSeenTuplesByAlias().get(alias);

        if (actual == null) {
            throw new IllegalStateException("Missing seen counter for alias " + alias);
        }

        if (actual != expected) {
            throw new IllegalStateException(
                    "Seen counter for " + alias + " expected " + expected + " but got " + actual
            );
        }
    }
}