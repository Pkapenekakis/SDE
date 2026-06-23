package Tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.synopses.OnePassSampler.OnePassSamplerSynopsis;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
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
 * Direct lifecycle test for OnePassSamplerSynopsis.
 *
 * No Kafka.
 * No SDE runtime.
 *
 * This test validates the high-level lifecycle:
 *
 *   1. Compile SQL.
 *   2. Create OnePassSamplerSynopsis.
 *   3. Feed side aliases in leaf-to-root order.
 *   4. finishPhaseOne().
 *   5. Feed root alias.
 *   6. finishPhaseTwo().
 *   7. Validate Phase 2 counters.
 *
 * This should pass before wiring OnePassSamplerSynopsis into SDE.
 */
public final class OnePassSamplerSynopsisDirectTest {

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
     * Use 5000 first while debugging.
     * Use -1 for full TPC-H files.
     */
    private static final long TEST_ROW_LIMIT = 8000;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassSamplerSynopsisDirectTest() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== OnePassSamplerSynopsisDirectTest ===");
        System.out.println("SQL:");
        System.out.println(TEST_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_SQL);
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassCatalog catalog =
                OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        OnePassSamplerSynopsis synopsis =
                new OnePassSamplerSynopsis(plan);

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println();

        System.out.println("Synopsis:");
        System.out.println(synopsis);
        System.out.println();

        System.out.println("1. Feeding PHASE_1 side aliases...");

        for (String alias : plan.getLeafToRootOrder()) {
            long count =
                    replayAliasIntoSynopsis(
                            synopsis,
                            catalog,
                            plan,
                            alias,
                            TEST_ROW_LIMIT
                    );

            System.out.println("  PHASE_1 alias " + alias + " rows: " + count);
        }

        System.out.println();
        System.out.println("2. Finishing PHASE_1...");

        synopsis.finishPhaseOne();

        if (synopsis.getPhase() != OnePassSamplerSynopsis.Phase.PHASE_2) {
            throw new IllegalStateException(
                    "Expected synopsis to move to PHASE_2, but current phase is "
                            + synopsis.getPhase()
            );
        }

        if (synopsis.getPhaseOneResult() == null) {
            throw new IllegalStateException("phaseOneResult must not be null after finishPhaseOne()");
        }

        System.out.println("  PHASE_1 complete.");
        System.out.println("  Current phase: " + synopsis.getPhase());
        System.out.println();

        System.out.println("3. Feeding PHASE_2 root alias...");

        RootReplayStats rootStats =
                replayRootAliasIntoSynopsis(
                        synopsis,
                        catalog,
                        plan,
                        TEST_ROW_LIMIT
                );

        System.out.println("  Root rows seen: " + rootStats.rootRowsSeen);
        System.out.println("  Positive root rows: " + rootStats.positiveRootRows);
        System.out.println("  Total root group weight: " + rootStats.totalRootGroupWeight);
        System.out.println();

        System.out.println("4. Finishing PHASE_2...");

        OnePassRootSampleResult result =
                synopsis.finishPhaseTwo();

        if (synopsis.getPhase() != OnePassSamplerSynopsis.Phase.DONE) {
            throw new IllegalStateException(
                    "Expected synopsis to move to DONE after finishPhaseTwo(), but current phase is "
                            + synopsis.getPhase()
            );
        }

        if (result == null) {
            throw new IllegalStateException("Phase 2 result must not be null");
        }

        validatePhaseTwoResult(rootStats, result, plan.getSampleSize());

        System.out.println();
        System.out.println("5. Phase 2 result:");
        System.out.println(result);

        System.out.println();
        System.out.println("Sample instances:");
        for (int i = 0; i < result.getSampleInstances().size(); i++) {
            System.out.println("  " + result.getSampleInstances().get(i));
        }

        System.out.println();
        System.out.println("SUCCESS: OnePassSamplerSynopsis lifecycle test passed.");
    }

    private static long replayAliasIntoSynopsis(OnePassSamplerSynopsis synopsis,
                                                OnePassCatalog catalog,
                                                CompiledOnePassPlan plan,
                                                String alias,
                                                long maxRows) throws Exception {
        File file = tableFileForAlias(catalog, plan, alias);
        List<String> columns = columnsForAlias(catalog, plan, alias);
        String separator = separatorForAlias(catalog, plan, alias);

        long count = 0L;

        BufferedReader br = new BufferedReader(new FileReader(file));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && count >= maxRows) {
                    break;
                }

                ObjectNode tupleJson =
                        tupleJsonFromLine(alias, columns, separator, line);

                synopsis.add(tupleJson);

                count++;
            }
        } finally {
            br.close();
        }

        return count;
    }

    private static RootReplayStats replayRootAliasIntoSynopsis(OnePassSamplerSynopsis synopsis,
                                                               OnePassCatalog catalog,
                                                               CompiledOnePassPlan plan,
                                                               long maxRows) throws Exception {
        String rootAlias = plan.getRootAlias();

        File file = tableFileForAlias(catalog, plan, rootAlias);
        List<String> columns = columnsForAlias(catalog, plan, rootAlias);
        String separator = separatorForAlias(catalog, plan, rootAlias);

        RootReplayStats stats = new RootReplayStats();

        BufferedReader br = new BufferedReader(new FileReader(file));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && stats.rootRowsSeen >= maxRows) {
                    break;
                }

                ObjectNode tupleJson =
                        tupleJsonFromLine(rootAlias, columns, separator, line);

                OnePassTuple rootTuple =
                        OnePassTupleExtractor.extract(tupleJson);

                double rootGroupWeight =
                        synopsis.computeRootGroupWeight(rootTuple);

                synopsis.addTuple(rootTuple);

                stats.rootRowsSeen++;

                if (rootGroupWeight > 0.0d) {
                    stats.positiveRootRows++;
                    stats.totalRootGroupWeight += rootGroupWeight;
                }
            }
        } finally {
            br.close();
        }

        return stats;
    }

    private static void validatePhaseTwoResult(RootReplayStats expected,
                                               OnePassRootSampleResult actual,
                                               int sampleSize) {
        assertEquals(
                "rootTuplesSeen",
                expected.rootRowsSeen,
                actual.getRootTuplesSeen()
        );

        assertEquals(
                "positiveRootCandidatesSeen",
                expected.positiveRootRows,
                actual.getPositiveRootCandidatesSeen()
        );

        assertClose(
                "totalRootGroupWeight",
                expected.totalRootGroupWeight,
                actual.getTotalRootGroupWeight()
        );

        if (expected.positiveRootRows > 0
                && actual.getSampleInstances().isEmpty()) {
            throw new IllegalStateException(
                    "Expected non-empty sampleInstances because positive root candidates exist"
            );
        }

        if (expected.positiveRootRows >= sampleSize
                && actual.getSampleInstances().size() != sampleSize) {
            throw new IllegalStateException(
                    "Expected sampleInstances.size() = "
                            + sampleSize
                            + " but got "
                            + actual.getSampleInstances().size()
            );
        }
    }

    private static File tableFileForAlias(OnePassCatalog catalog,
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

        if (table.getColumns() == null || table.getColumns().isEmpty()) {
            throw new IllegalStateException(
                    "Catalog table '"
                            + relation.getTable()
                            + "' has no columns"
            );
        }

        return table.getColumns();
    }

    private static String separatorForAlias(OnePassCatalog catalog,
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
        double absoluteTolerance = 0.000001d;
        double relativeTolerance = 0.000000001d;

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

    private static final class RootReplayStats {
        private long rootRowsSeen;
        private long positiveRootRows;
        private double totalRootGroupWeight;
    }
}