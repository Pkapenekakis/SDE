package Tests.PhaseThree;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneState;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassCompletedSample;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeResult;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleInstance;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Direct in-memory Phase 3 test.
 *
 * This test verifies the first Phase 3 implementation before wiring it into SDE.
 *
 * Flow:
 *
 *   1. Compile OnePass SQL.
 *   2. Build Phase 1 indexes from side aliases.
 *   3. Run Phase 2 root sampling from the root alias.
 *   4. Create OnePassPhaseThreeState from Phase 1 + Phase 2 results.
 *   5. Replay side aliases in root-to-leaf order.
 *   6. Validate that every completed sample:
 *        - contains all aliases,
 *        - preserves sampleInstanceId from Phase 2,
 *        - satisfies every parent-child join condition,
 *        - has positive subtree weight for every selected tuple.
 *
 * This deliberately avoids Kafka/SDE plumbing.
 */
public class OnePassPhaseThreeTest {

    private static final String TEST_TPCH_DIR =
            "/home/vboxuser/Desktop/Thesis/tpch-data/sf1";

    private static final String TEST_SQL =
            "SELECT * " +
                    "FROM wq3_alias ROOT c " +
                    "WEIGHTED BY (" +
                    "o.o_totalprice * " + "(l.l_extendedprice * (1 - l.l_discount))) " +
                    "LIMIT 50 " +
                    "/* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    /*
     * Keep the same row limit for:
     *   - Phase 1 side aliases
     *   - Phase 2 root alias
     *   - Phase 3 side-alias replay
     *
     * This is important because Phase 3 can only select tuples that are replayed.
     */
    private static final long TEST_ROW_LIMIT = 1000L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("=== OnePassPhaseThreeTest ===");
        System.out.println("SQL:");
        System.out.println(TEST_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_SQL);

        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassCatalog catalog = OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Root-to-leaf order: " + plan.getRootToLeafOrder());
        System.out.println("Weights by alias: " + plan.getWeightsByAlias());
        System.out.println("Sample size: " + params.getOutput().getSampleSize());
        System.out.println();

        OnePassWeightEvaluator weightEvaluator = new OnePassWeightEvaluator(plan.getWeightSpec());

        /*
         * Phase 1.
         */
        System.out.println("1. Building Phase 1 indexes...");

        OnePassPhaseOneState phaseOneState = new OnePassPhaseOneState(plan, weightEvaluator);

        for (String alias : plan.getLeafToRootOrder()) {
            List<OnePassTuple> tuples = readTuplesForAlias(catalog, plan, alias, TEST_ROW_LIMIT);

            for (OnePassTuple tuple : tuples) {
                phaseOneState.addTuple(tuple);
            }

            System.out.println("  Phase 1 alias " + alias + " rows: " + tuples.size());
        }

        OnePassPhaseOneResult phaseOneResult = phaseOneState.exportResult();

        System.out.println();
        System.out.println("Phase 1 result:");
        System.out.println(phaseOneResult);
        System.out.println();

        /*
         * Phase 2.
         */
        System.out.println("2. Running Phase 2 root sampler...");

        OnePassRootSampler rootSampler = new OnePassRootSampler(phaseOneResult,
                params.getOutput().getSampleSize(), params.getDataset().getSeed());

        List<OnePassTuple> rootTuples = readTuplesForAlias(catalog, plan, plan.getRootAlias(), TEST_ROW_LIMIT);

        long positiveRootRows = 0L;

        double expectedTotalRootGroupWeight = 0.0d;

        for (OnePassTuple rootTuple : rootTuples) {
            double rootGroupWeight = rootSampler.computeRootGroupWeight(rootTuple);

            if (rootGroupWeight > 0.0d) {
                positiveRootRows++;
                expectedTotalRootGroupWeight += rootGroupWeight;
            }

            rootSampler.addRootTuple(rootTuple);
        }

        OnePassRootSampleResult phaseTwoResult =
                rootSampler.finish();

        System.out.println("Root rows replayed: " + rootTuples.size());
        System.out.println("Expected positive root rows: " + positiveRootRows);
        System.out.println("Expected total root group weight: " + expectedTotalRootGroupWeight);
        System.out.println("Phase 2 result:");
        System.out.println(phaseTwoResult);
        System.out.println();

        validatePhaseTwoResult(
                phaseTwoResult,
                rootTuples.size(),
                positiveRootRows,
                expectedTotalRootGroupWeight,
                params.getOutput().getSampleSize()
        );

        /*
         * Phase 3.
         */
        System.out.println("3. Running Phase 3 extension...");

        OnePassPhaseThreeState phaseThreeState = new OnePassPhaseThreeState(phaseOneResult, phaseTwoResult,
                        params.getDataset().getSeed());

        Map<String, List<OnePassTuple>> sideReplayTuplesByAlias = new LinkedHashMap<String, List<OnePassTuple>>();

        for (String alias : plan.getRootToLeafOrder()) {
            if (plan.isRoot(alias)) {
                continue;
            }

            List<OnePassTuple> tuples = readTuplesForAlias(catalog, plan, alias, TEST_ROW_LIMIT);

            sideReplayTuplesByAlias.put(alias, tuples);

            System.out.println("  Phase 3 replay alias " + alias + " rows: " + tuples.size());
        }

        OnePassPhaseThreeResult phaseThreeResult = phaseThreeState.extendAll(sideReplayTuplesByAlias);

        System.out.println();
        System.out.println("Phase 3 result:");
        System.out.println(phaseThreeResult);

        for (OnePassCompletedSample sample : phaseThreeResult.getCompletedSamples()) {
            System.out.println("  " + sample);
        }

        validatePhaseThreeResult(phaseThreeResult, phaseTwoResult, phaseThreeState, plan);

        System.out.println();
        System.out.println("SUCCESS: OnePassPhaseThree in-memory test passed.");
    }

    private static void validatePhaseTwoResult(
            OnePassRootSampleResult result,
            long expectedRootRows,
            long expectedPositiveRootRows,
            double expectedTotalRootGroupWeight,
            int expectedSampleSize) {

        assertEquals("rootTuplesSeen", expectedRootRows, result.getRootTuplesSeen());

        assertEquals("positiveRootCandidatesSeen", expectedPositiveRootRows,
                result.getPositiveRootCandidatesSeen());

        assertClose("totalRootGroupWeight", expectedTotalRootGroupWeight, result.getTotalRootGroupWeight());

        if (expectedPositiveRootRows > 0) {
            assertEquals("sampleInstances.size", expectedSampleSize, result.getSampleInstances().size());
        }

        if (expectedPositiveRootRows == 0L && !result.getSampleInstances().isEmpty()) {
            throw new IllegalStateException("Expected empty Phase 2 sample because no positive root candidates exist");
        }

        for (OnePassRootSampleInstance instance : result.getSampleInstances()) {

            if (instance.getRootGroupWeight() <= 0.0d || Double.isNaN(instance.getRootGroupWeight()) ||
                    Double.isInfinite(instance.getRootGroupWeight())) {
                throw new IllegalStateException("Invalid rootGroupWeight in Phase 2 sample instance: " + instance);
            }
        }

        System.out.println("Phase 2 validation passed.");
    }

    private static void validatePhaseThreeResult(
            OnePassPhaseThreeResult phaseThreeResult,
            OnePassRootSampleResult phaseTwoResult,
            OnePassPhaseThreeState phaseThreeState,
            CompiledOnePassPlan plan) {

        assertEquals("completedSamples.size", phaseTwoResult.getSampleInstances().size(),
                phaseThreeResult.getCompletedSamples().size());

        assertEquals("requestedSampleSize", phaseTwoResult.getRequestedSampleSize(),
                phaseThreeResult.getRequestedSampleSize());

        if (phaseTwoResult.hasFullSample() && !phaseThreeResult.hasFullSample()) {
            throw new IllegalStateException("Phase 2 had a full sample, but Phase 3 result is not full");
        }

        Set<Long> phaseTwoSampleIds = new LinkedHashSet<Long>();

        for (OnePassRootSampleInstance instance : phaseTwoResult.getSampleInstances()) {
            phaseTwoSampleIds.add(instance.getSampleInstanceId());
        }

        Set<Long> phaseThreeSampleIds = new LinkedHashSet<Long>();

        for (OnePassCompletedSample sample : phaseThreeResult.getCompletedSamples()) {

            phaseThreeSampleIds.add(sample.getSampleInstanceId());

            validateSampleContainsAllAliases(sample, plan);
            validateSampleJoinConditions(sample, plan);
            validateSampleSubtreeWeights(sample, phaseThreeState, plan);
        }

        if (!phaseTwoSampleIds.equals(phaseThreeSampleIds)) {
            throw new IllegalStateException("Phase 3 sample ids do not match Phase 2 sample ids. "
                            + "Phase 2 ids=" + phaseTwoSampleIds + ", Phase 3 ids=" + phaseThreeSampleIds);
        }

        System.out.println("Phase 3 validation passed.");
    }

    private static void validateSampleContainsAllAliases(
            OnePassCompletedSample sample,
            CompiledOnePassPlan plan) {

        for (String alias : plan.getAliases()) {
            OnePassTuple tuple = sample.getTuple(alias);

            if (tuple == null) {
                throw new IllegalStateException("Sample " + sample.getSampleInstanceId() +
                        " is missing alias " + alias);
            }

            if (!alias.equals(tuple.getTable())) {
                throw new IllegalStateException("Sample " + sample.getSampleInstanceId()
                                + " has tuple alias mismatch. Expected " + alias + " but got " + tuple.getTable());
            }
        }
    }

    private static void validateSampleJoinConditions(
            OnePassCompletedSample sample,
            CompiledOnePassPlan plan) {

        for (String alias : plan.getRootToLeafOrder()) {
            if (plan.isRoot(alias)) {
                continue;
            }

            CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                    plan.getParentEdge(alias);

            if (parentEdge == null) {
                throw new IllegalStateException("Non-root alias has no parent edge: " + alias);
            }

            OnePassTuple parentTuple = sample.getTuple(parentEdge.getParentAlias());

            OnePassTuple childTuple = sample.getTuple(parentEdge.getChildAlias());

            if (parentTuple == null || childTuple == null) {
                throw new IllegalStateException("Sample " + sample.getSampleInstanceId() +
                        " is missing parent/child tuple for edge " + parentEdge);
            }

            JoinValue parentKey = JoinValue.fromTuple(parentTuple, parentEdge.getParentFields());

            JoinValue childKey = JoinValue.fromTuple(childTuple, parentEdge.getChildFields());

            if (!parentKey.equals(childKey)) {
                throw new IllegalStateException("Join mismatch in sample " + sample.getSampleInstanceId() +
                        " on edge " + parentEdge + ". parentKey=" + parentKey + ", childKey=" + childKey);
            }
        }
    }

    private static void validateSampleSubtreeWeights(
            OnePassCompletedSample sample,
            OnePassPhaseThreeState phaseThreeState,
            CompiledOnePassPlan plan) {

        for (String alias : plan.getAliases()) {
            OnePassTuple tuple =
                    sample.getTuple(alias);

            double subtreeWeight =
                    phaseThreeState.computeTupleSubtreeWeight(tuple);

            if (subtreeWeight <= 0.0d
                    || Double.isNaN(subtreeWeight)
                    || Double.isInfinite(subtreeWeight)) {
                throw new IllegalStateException(
                        "Selected tuple has invalid subtree weight. "
                                + "sampleInstanceId="
                                + sample.getSampleInstanceId()
                                + ", alias="
                                + alias
                                + ", subtreeWeight="
                                + subtreeWeight
                                + ", tuple="
                                + tuple
                );
            }
        }
    }

    private static List<OnePassTuple> readTuplesForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias,
            long maxRows) throws Exception {

        File tableFile = tableFileForAlias(catalog, plan, alias);

        List<String> columns = columnsForAlias(catalog, plan, alias);

        String separator = separatorForAlias(catalog, plan, alias);

        List<OnePassTuple> out = new ArrayList<OnePassTuple>();

        BufferedReader br = new BufferedReader(new FileReader(tableFile));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && out.size() >= maxRows) {
                    break;
                }

                ObjectNode tupleJson = tupleJsonFromLine(alias,columns, separator, line);

                out.add(OnePassTupleExtractor.extract(tupleJson));
            }
        } finally {
            br.close();
        }

        return out;
    }

    private static File tableFileForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias) {

        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(alias);

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

        File file =
                new File(TEST_TPCH_DIR, table.getFile());

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

    private static List<String> columnsForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias) {

        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(alias);

        OnePassCatalog.CatalogTable table =
                catalog.getDataset().getTables().get(relation.getTable());

        List<String> columns =
                table.getColumns();

        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException(
                    "Catalog table '"
                            + relation.getTable()
                            + "' has no columns"
            );
        }

        return columns;
    }

    private static String separatorForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias) {

        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(alias);

        OnePassCatalog.CatalogTable table =
                catalog.getDataset().getTables().get(relation.getTable());

        String separator =
                table.getSeparator();

        if (separator == null || separator.length() == 0) {
            return "|";
        }

        return separator;
    }

    private static ObjectNode tupleJsonFromLine(
            String alias,
            List<String> columns,
            String separator,
            String line) {

        String[] parts =
                line.split("\\Q" + separator + "\\E", -1);

        ObjectNode tuple =
                MAPPER.createObjectNode();

        tuple.put("alias", alias);

        int limit =
                Math.min(columns.size(), parts.length);

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

        String value =
                rawValue.trim();

        if (value.length() == 0) {
            tuple.put(fieldName, "");
            return;
        }

        Long asLong =
                tryParseLong(value);

        if (asLong != null) {
            tuple.put(fieldName, asLong.longValue());
            return;
        }

        Double asDouble =
                tryParseDouble(value);

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

            return Long.valueOf(Long.parseLong(value));
        } catch (Exception e) {
            return null;
        }
    }

    private static Double tryParseDouble(String value) {
        try {
            return Double.valueOf(Double.parseDouble(value));
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
        double absoluteTolerance =
                0.000001d;

        double relativeTolerance =
                0.000000001d;

        double diff =
                Math.abs(expected - actual);

        double allowed =
                Math.max(
                        absoluteTolerance,
                        Math.abs(expected) * relativeTolerance
                );

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
}