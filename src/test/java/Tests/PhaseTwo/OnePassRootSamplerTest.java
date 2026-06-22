package Tests.PhaseTwo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneState;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampler;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
import infore.SDE.transformations.onepass.sql.OnePassCatalog;
import infore.SDE.transformations.onepass.sql.OnePassQueryCatalogLoader;
import infore.SDE.transformations.onepass.sql.OnePassSqlCompiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

/**
 * Direct in-memory Phase 2 test.
 *
 * This test verifies:
 *
 * 1. Phase 1 indexes are built in leaf-to-root order.
 * 2. OnePassRootSampler computes rootGroupWeight itself.
 * 3. rootGroupWeight equals:
 *
 *      rootOwnWeight * product(child subtree weights from Phase 1)
 *
 * 4. The sampler produces explicit root sample instances.
 *
 * This test deliberately avoids Kafka/SDE request plumbing.
 */
public class OnePassRootSamplerTest {

    private static final String TEST_TPCH_DIR =
            "/home/vboxuser/Desktop/Thesis/tpch-data/sf1";

    private static final String TEST_SQL =
            "SELECT * " +
                    "FROM wq3_alias ROOT c " +
                    "WEIGHTED BY (" +
                    "o.o_totalprice * " +
                    "(l.l_extendedprice * (1 - l.l_discount))" +
                    ") " +
                    "LIMIT 100 " +
                    "/* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    /*
     * Keep this equal to the row limit you want to test.
     * Use small values first while debugging.
     * -1L for full TPCH dataset
     */
    private static final long TEST_ROW_LIMIT = 1000;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("SQL:");
        System.out.println(TEST_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_SQL);
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassCatalog catalog =
                OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Root-to-leaf order: " + plan.getRootToLeafOrder());
        System.out.println("Weights by alias: " + plan.getWeightsByAlias());
        System.out.println();

        OnePassWeightEvaluator weightEvaluator =
                new OnePassWeightEvaluator(plan.getWeightSpec());

        OnePassPhaseOneState phaseOneState =
                new OnePassPhaseOneState(plan, weightEvaluator);

        System.out.println("1. Building Phase 1 indexes...");

        for (String alias : plan.getLeafToRootOrder()) {
            long count =
                    replayAliasIntoPhaseOne(
                            phaseOneState,
                            catalog,
                            plan,
                            alias,
                            TEST_ROW_LIMIT
                    );

            System.out.println("Phase 1 alias " + alias + " rows: " + count);
        }

        OnePassPhaseOneResult phaseOneResult = phaseOneState.exportResult();

        System.out.println();
        System.out.println("2. Testing Phase 2 rootGroupWeight computation...");

        OnePassRootSampler rootSampler =
                new OnePassRootSampler(
                        phaseOneResult,
                        params.getOutput().getSampleSize(),
                        params.getDataset().getSeed()
                );

        RootReplayStats rootStats =
                replayRootAliasIntoSampler(
                        rootSampler,
                        phaseOneResult,
                        weightEvaluator,
                        catalog,
                        plan,
                        TEST_ROW_LIMIT
                );

        System.out.println();
        System.out.println("Root replay stats:");
        System.out.println("  rootRowsSeen: " + rootStats.rootRowsSeen);
        System.out.println("  positiveRootRows: " + rootStats.positiveRootRows);
        System.out.println("  expectedTotalRootGroupWeight: " + rootStats.totalRootGroupWeight);

        OnePassRootSampleResult result = rootSampler.finish();

        System.out.println();
        System.out.println("3. Phase 2 result:");
        System.out.println(result);
        System.out.println("Sample instances:");
        for (int i = 0; i < result.getSampleInstances().size(); i++) {
            System.out.println("  " + result.getSampleInstances().get(i));
        }

        assertEquals(
                "rootTuplesSeen",
                rootStats.rootRowsSeen,
                result.getRootTuplesSeen()
        );

        assertEquals(
                "positiveRootCandidatesSeen",
                rootStats.positiveRootRows,
                result.getPositiveRootCandidatesSeen()
        );

        assertClose(
                "totalRootGroupWeight",
                rootStats.totalRootGroupWeight,
                result.getTotalRootGroupWeight()
        );

        if (rootStats.positiveRootRows > 0
                && result.getSampleInstances().isEmpty()) {
            throw new IllegalStateException(
                    "Expected non-empty sampleInstances because positive roots exist"
            );
        }

        if (rootStats.positiveRootRows >= params.getOutput().getSampleSize()
                && result.getSampleInstances().size() != params.getOutput().getSampleSize()) {
            throw new IllegalStateException(
                    "Expected full sample of size "
                            + params.getOutput().getSampleSize()
                            + " but got "
                            + result.getSampleInstances().size()
            );
        }

        System.out.println();
        System.out.println("SUCCESS: OnePassRootSampler Phase 2 test passed.");
    }

    private static long replayAliasIntoPhaseOne(OnePassPhaseOneState phaseOneState,
                                                OnePassCatalog catalog,
                                                CompiledOnePassPlan plan,
                                                String alias,
                                                long maxRows) throws Exception {
        File tableFile = tableFileForAlias(catalog, plan, alias);
        List<String> columns = columnsForAlias(catalog, plan, alias);
        String separator = separatorForAlias(catalog, plan, alias);

        long count = 0L;

        BufferedReader br = new BufferedReader(new FileReader(tableFile));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && count >= maxRows) {
                    break;
                }

                ObjectNode tupleJson =
                        tupleJsonFromLine(alias, columns, separator, line);

                OnePassTuple tuple = OnePassTupleExtractor.extract(tupleJson);

                phaseOneState.addTuple(tuple);

                count++;
            }
        } finally {
            br.close();
        }

        return count;
    }

    private static RootReplayStats replayRootAliasIntoSampler(OnePassRootSampler rootSampler,
                                                              OnePassPhaseOneResult phaseOneResult,
                                                              OnePassWeightEvaluator weightEvaluator,
                                                              OnePassCatalog catalog,
                                                              CompiledOnePassPlan plan,
                                                              long maxRows) throws Exception {
        String rootAlias = plan.getRootAlias();

        File tableFile = tableFileForAlias(catalog, plan, rootAlias);
        List<String> columns = columnsForAlias(catalog, plan, rootAlias);
        String separator = separatorForAlias(catalog, plan, rootAlias);

        RootReplayStats stats = new RootReplayStats();

        BufferedReader br = new BufferedReader(new FileReader(tableFile));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && stats.rootRowsSeen >= maxRows) {
                    break;
                }

                ObjectNode tupleJson =
                        tupleJsonFromLine(rootAlias, columns, separator, line);

                OnePassTuple rootTuple = OnePassTupleExtractor.extract(tupleJson);

                double expectedRootGroupWeight =
                        computeExpectedRootGroupWeight(
                                phaseOneResult,
                                weightEvaluator,
                                plan,
                                rootTuple
                        );

                double actualRootGroupWeight =
                        rootSampler.computeRootGroupWeight(rootTuple);

                assertClose(
                        "rootGroupWeight for root tuple " + stats.rootRowsSeen,
                        expectedRootGroupWeight,
                        actualRootGroupWeight
                );

                rootSampler.addRootTuple(rootTuple);

                stats.rootRowsSeen++;

                if (expectedRootGroupWeight > 0.0d) {
                    stats.positiveRootRows++;
                    stats.totalRootGroupWeight += expectedRootGroupWeight;
                }
            }
        } finally {
            br.close();
        }

        return stats;
    }

    private static double computeExpectedRootGroupWeight(OnePassPhaseOneResult phaseOneResult,
                                                         OnePassWeightEvaluator weightEvaluator,
                                                         CompiledOnePassPlan plan,
                                                         OnePassTuple rootTuple) {
        double rootOwnWeight = weightEvaluator.evaluate(rootTuple);

        if (rootOwnWeight == 0.0d) {
            return 0.0d;
        }

        double continuationWeight = 1.0d;

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge :
                plan.getChildEdges(plan.getRootAlias())) {

            double childSubtreeWeight =
                    phaseOneResult.lookupChildSubtreeWeight(
                            childEdge,
                            rootTuple
                    );

            if (childSubtreeWeight == 0.0d) {
                return 0.0d;
            }

            continuationWeight *= childSubtreeWeight;
        }

        return rootOwnWeight * continuationWeight;
    }

    private static File tableFileForAlias(OnePassCatalog catalog,
                                          CompiledOnePassPlan plan,
                                          String alias) {
        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        if (relation == null) {
            throw new IllegalStateException("Unknown alias in plan: " + alias);
        }

        OnePassCatalog.CatalogTable table =
                catalog.getDataset().getTables().get(relation.getTable());

        if (table == null) {
            throw new IllegalStateException(
                    "Catalog does not define table '"
                            + relation.getTable()
                            + "' for alias '"
                            + alias
                            + "'"
            );
        }

        File file = new File(TEST_TPCH_DIR, table.getFile());

        if (!file.exists()) {
            throw new IllegalStateException(
                    "Missing TPC-H file for alias '"
                            + alias
                            + "': "
                            + file.getAbsolutePath()
            );
        }

        return file;
    }

    private static List<String> columnsForAlias(OnePassCatalog catalog,
                                                CompiledOnePassPlan plan,
                                                String alias) {
        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        OnePassCatalog.CatalogTable table =
                catalog.getDataset().getTables().get(relation.getTable());

        List<String> columns = table.getColumns();

        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException(
                    "Catalog table '"
                            + relation.getTable()
                            + "' has no columns"
            );
        }

        return columns;
    }

    private static String separatorForAlias(OnePassCatalog catalog,
                                            CompiledOnePassPlan plan,
                                            String alias) {
        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        OnePassCatalog.CatalogTable table =
                catalog.getDataset().getTables().get(relation.getTable());

        String separator = table.getSeparator();

        if (separator == null || separator.length() == 0) {
            return "|";
        }

        return separator;
    }

    private static ObjectNode tupleJsonFromLine(String alias,
                                                List<String> columns,
                                                String separator,
                                                String line) {
        String[] parts = line.split("\\Q" + separator + "\\E", -1);

        ObjectNode tuple = MAPPER.createObjectNode();
        tuple.put("alias", alias);

        int limit = Math.min(columns.size(), parts.length);

        for (int i = 0; i < limit; i++) {
            putTypedValue(tuple, columns.get(i), parts[i]);
        }

        return tuple;
    }

    private static void putTypedValue(ObjectNode tuple,
                                      String fieldName,
                                      String rawValue) {
        if (fieldName == null || fieldName.trim().isEmpty()) {
            return;
        }

        if (rawValue == null) {
            tuple.put(fieldName, "");
            return;
        }

        String value = rawValue.trim();

        if (value.length() == 0) {
            tuple.put(fieldName, "");
            return;
        }

        Long asLong = tryParseLong(value);

        if (asLong != null) {
            tuple.put(fieldName, asLong.longValue());
            return;
        }

        Double asDouble = tryParseDouble(value);

        if (asDouble != null) {
            tuple.put(fieldName, asDouble.doubleValue());
            return;
        }

        tuple.put(fieldName, value);
    }

    private static Long tryParseLong(String value) {
        try {
            if (value.indexOf('.') >= 0) {
                return null;
            }

            return Long.parseLong(value);
        } catch (Exception e) {
            return null;
        }
    }

    private static Double tryParseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (Exception e) {
            return null;
        }
    }

    private static void assertEquals(String label,
                                     long expected,
                                     long actual) {
        if (expected != actual) {
            throw new IllegalStateException(
                    label
                            + " mismatch. Expected "
                            + expected
                            + " but got "
                            + actual
            );
        }
    }

    private static void assertClose(String label,
                                    double expected,
                                    double actual) {
        double absoluteTolerance = 0.000001d;
        double relativeTolerance = 0.000000001d;

        double diff = Math.abs(expected - actual);
        double allowed =
                Math.max(absoluteTolerance, Math.abs(expected) * relativeTolerance);

        if (diff > allowed) {
            throw new IllegalStateException(
                    label
                            + " mismatch. Expected "
                            + expected
                            + " but got "
                            + actual
                            + ". Difference "
                            + diff
                            + ", allowed "
                            + allowed
            );
        }
    }

    private static final class RootReplayStats {
        private long rootRowsSeen;
        private long positiveRootRows;
        private double totalRootGroupWeight;
    }
}