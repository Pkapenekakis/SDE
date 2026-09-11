package infore.SDE.Experiments.Onepass.MultiWorker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.sql.OnePassCatalog;
import infore.SDE.transformations.onepass.sql.OnePassQueryCatalogLoader;
import infore.SDE.transformations.onepass.sql.OnePassSqlCompiler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Local integration / benchmark test for the SHARDED OnePass* Phase 1 + Phase 2 + Phase 3 design.
 * <p>
 * Timing semantics:
 * <p>
 * Phase 1:
 * - TPC-H parsing + Kafka producer.send() happen outside phase1_algorithm_total.
 * - Each non-root alias is prepared in an open Kafka transaction.
 * - commitTransaction() releases one alias at a time and IS inside the Phase-1 timer.
 * - the phase ends when START_PHASE_2 is observed on RequestTopic.
 * <p>
 * Phase 2:
 * - the root relation is parsed + written into an open Kafka transaction
 * AFTER Phase 1 completes and BEFORE phase2_algorithm_total starts.
 * - commitTransaction() is the Phase-2 release/start signal and IS inside
 * phase2_algorithm_total.
 * - the measured phase ends when:
 * <p>
 * GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED
 * <p>
 * is observed on the SDE output topic.
 * <p>
 * Under the coordinator-free StateTopic architecture this event is the
 * global installation barrier: every worker has received and installed the
 * globally merged Phase-2 root sample.
 * <p>
 * Phase 3:
 * - request 86 automatically starts the first root-to-leaf replay alias.
 * - every non-root relation is parsed + written into an open Kafka transaction
 *   before phase3_algorithm_total starts.
 * - each commitTransaction() releases exactly one replay alias and IS inside
 *   the Phase-3 timer.
 * - for non-final aliases, the measured alias ends when the next
 *   START_PHASE_3_ALIAS transition is observed on RequestTopic.
 * - the final alias ends when GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED with
 *   phaseThreeComplete=true is observed on the SDE output topic.
 * <p>
 * This keeps expensive local file parsing / JSON creation / producer.send()
 * outside all algorithm timers while preserving the actual Kafka visibility
 * boundary (read_committed + transaction commit) inside each measured phase.
 * <p>
 * The Phase-3 assertions validate the complete coordinator-free lifecycle:
 * request 86 -> first replay, request 87/88 strict global selections,
 * request 90/91 global installation, next-alias transitions, and the final
 * all-worker installation barrier. The production protocol does not currently
 * expose the completed sample tuples on OUTPUT_TOPIC, so this class validates
 * Phase-3 transport/lifecycle metadata and selection cardinality rather than
 * independently re-computing the selected sample contents.
 */
public final class OnePassSamplerSdeCoordinatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String LOCAL_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOFTNET_BOOTSTRAP_SERVERS = "clu02.softnet.tuc.gr:6667," + "clu03.softnet.tuc.gr:6667,"
            + "clu04.softnet.tuc.gr:6667," + "clu06.softnet.tuc.gr:6667";

    // ---------------------------------------------------------------------
    // LOCAL TEST SETTINGS
    // ---------------------------------------------------------------------

    private static String BOOTSTRAP_SERVERS = LOCAL_BOOTSTRAP_SERVERS;
    private static final String DATA_TOPIC = System.getProperty("onepass.dataTopic",
            "dataTopic");
    private static final String REQUEST_TOPIC = System.getProperty("onepass.requestTopic",
            "requestTopic");
    private static final String OUTPUT_TOPIC = System.getProperty("onepass.outputTopic",
            "estimationTopic");

    private static final String STATE_TOPIC = System.getProperty("onepass.stateTopic",
            "onepassStateTopic");

    private static final String TEST_TPCH_DIR = System.getProperty("onepass.tpchDir",
            "/home/vboxuser/Desktop/Thesis/tpch-data/sf1");

    private static final String DEFAULT_PHASE1_BENCHMARK_CSV_PATH =
            "/home/vboxuser/Desktop/Thesis/onepass_multiworker_phase1_sharded_local.csv";

    private static final String DEFAULT_PHASE2_BENCHMARK_CSV_PATH =
            "/home/vboxuser/Desktop/Thesis/onepass_multiworker_phase2_sharded_local.csv";

    private static final String DEFAULT_COMBINED_BENCHMARK_CSV_PATH =
            "/home/vboxuser/Desktop/Thesis/onepass_all_phases_local.csv";

    // =========================
    // SOFTNET
    // Uncomment these and comment the LOCAL definitions above.
    // =========================

    /*
    private static String BOOTSTRAP_SERVERS =SOFTNET_BOOTSTRAP_SERVERS;
    private static final String DATA_TOPIC = "pkapenekakis-dataTopic";
    private static final String REQUEST_TOPIC = "pkapenekakis-requestTopic";
    private static final String OUTPUT_TOPIC = "pkapenekakis-estimationTopic";
    private static final String STATE_TOPIC = "pkapenekakis-onepassStateTopic";

    private static final String TEST_TPCH_DIR = System.getProperty("onepass.tpchDir",
            "/home/pkapenekakis/onepass/tpch-data/sf1");

    private static final String DEFAULT_PHASE1_BENCHMARK_CSV_PATH =
            "/home/pkapenekakis/onepass/results/onepass_phase1_softnet.csv";

    private static final String DEFAULT_PHASE2_BENCHMARK_CSV_PATH =
            "/home/pkapenekakis/onepass/results/onepass_phase2_softnet.csv";

    private static final String DEFAULT_COMBINED_BENCHMARK_CSV_PATH =
            "/home/pkapenekakis/onepass/results/onepass_all_phases_softnet.csv";
*/
    // ---------------------------------------------------------------------
    // TEST CONFIGURATION
    // ---------------------------------------------------------------------

    private static final String PHASE1_BENCHMARK_CSV_PATH =
            System.getProperty("onepass.phase1Csv", DEFAULT_PHASE1_BENCHMARK_CSV_PATH);

    private static final String PHASE2_BENCHMARK_CSV_PATH =
            System.getProperty("onepass.phase2Csv", DEFAULT_PHASE2_BENCHMARK_CSV_PATH);

    private static final String COMBINED_BENCHMARK_CSV_PATH =
            System.getProperty("onepass.combinedCsv", DEFAULT_COMBINED_BENCHMARK_CSV_PATH);


    private static final String TEST_ONEPASS_SQL = "SELECT * FROM wq3_alias WEIGHTED BY " +
            "(" + "o.o_totalprice * (l.l_extendedprice * (1 - l.l_discount))) " +
            "LIMIT 10000 /* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

//    private static final String TEST_ONEPASS_SQL = "SELECT * FROM w_branch_supplier WEIGHTED BY " +
//            "(" + "l1.l_extendedprice * l2.l_extendedprice" + ") " +
//            "LIMIT 1000 " + "/* catalog='tpch-onepass-catalog.json', " + "seed='branch-test-123', scalefactor=1 */";

    //Use -1 for the full TPC-H relation.
    private static final long TEST_ROW_LIMIT = Long.parseLong(System.getProperty("onepass.testRowLimit", "100000"));
    private static final int EXPECTED_WORKERS = Integer.parseInt(System.getProperty("onepass.workers", "4"));

    private static final long TIMEOUT_MS = Long.parseLong(System.getProperty("onepass.timeoutMs",
            Long.toString(30L * 60L * 1000L)));

    /*
     * Kafka transaction timeout.
     *
     * Keep this <= the broker's transaction.max.timeout.ms.
     * Kafka's common default broker maximum is 15 minutes; 10 minutes is used
     * here for the local first pass.
     */
    private static final int TRANSACTION_TIMEOUT_MS =
            Integer.parseInt(System.getProperty("onepass.transactionTimeoutMs", "600000"));

    private static final boolean ENABLE_REQUIRED_FIELD_PRUNING = true;

    /*
     * Primary benchmark output.
     * One completed Phase-1 + Phase-2 + optional Phase-3 run appends exactly
     * one row to the combined CSV. This is the file intended for P=2 / P=4 /
     * P=8 comparisons.
     */
    private static final boolean WRITE_COMBINED_BENCHMARK_CSV = true;

    /*
     * false:
     *   Only the compact comparison fields are populated.
     * true:
     *   The detailed Phase-1 / Phase-2 / Phase-3 metrics used by the graphing
     *   workflow are populated as extra columns in THE SAME combined CSV.
     * The CSV schema is stable in both modes; detailed columns are simply left
     * empty when this flag is false.
     */
    private static final boolean WRITE_DETAILED_BENCHMARK_DATA = false;

    /*
     * Legacy compatibility only.
     * The old per-phase CSV writer methods are intentionally kept in this
     * class. Leave this false for the new one-file workflow. Set it to true
     * only if you explicitly want the historical separate Phase-1 / Phase-2
     * CSV files in addition to the combined file.
     */
    private static final boolean WRITE_LEGACY_SEPARATE_BENCHMARK_CSV = false;

    private static final boolean RUN_PHASE_2 = Boolean.parseBoolean(System.getProperty("onepass.runPhase2", "true"));
    private static final boolean RUN_PHASE_3 = Boolean.parseBoolean(System.getProperty("onepass.runPhase3", "true"));

    private static final int SYNOPSIS_ID = 30;
    private static final int REQUEST_ADD = 1;
    private static final int REQUEST_UPDATE = 7;


    /*
     * Timing maps deliberately contain only algorithm timings.
     * Kafka/TPC-H preload is tracked separately and never added to
     * phase1_algorithm_total, phase2_algorithm_total or phase3_algorithm_total.
     */
    private static final Map<String, Long> benchmarkNanos = new LinkedHashMap<String, Long>();
    private static final Map<String, Long> benchmarkCounts = new LinkedHashMap<String, Long>();

    /*
     * DEBUG / CORRECTNESS VALIDATION ONLY. !!!!!!DO NOT RUN AS TRUE ON THE SOFTNET CLUSTER!!!!!!!
     *
     * Set this to true when you want the test to ask every OnePass worker to
     * dump its final Phase-1 shard after the measured Phase-1 algorithm has
     * completed. The test then unions the worker files into the exact
     * {"edgeIndexes": ...} JSON format expected by
     * validate_onepass_catalog_phase1.py.
     *
     * IMPORTANT: the dump/merge happens AFTER phase1_algorithm_total stops,
     * so enabling this flag does not pollute the benchmark timing.
     */
    private static final int REQUEST_DEBUG_EXPORT_PHASE1 = 79;
    private static final boolean EXPORT_PHASE1_INDEXES = false;

    private static final String PHASE1_INDEX_EXPORT_DIR = System.getProperty("onepass.phase1IndexExportDir",
            "/tmp/onepass-phase1-validator");
    private static final String PHASE1_VALIDATOR_JSON_PATH = System.getProperty("onepass.phase1ValidatorJson",
            "/tmp/onepass_wq3_alias_phase1_full_indexes.json");
    private static final long PHASE1_INDEX_EXPORT_TIMEOUT_MS = Long.
            parseLong(System.getProperty("onepass.phase1IndexExportTimeoutMs", "120000"));

    /*
     * DEBUG / CORRECTNESS VALIDATION ONLY.
     *
     * This is the ONLY switch needed for local Phase-2 validation.
     *
     * true:
     *   - after phase2_algorithm_total has stopped, the test sends
     *     DEBUG_VALIDATE_PHASE2_ROOT_SAMPLE (request 89);
     *   - each worker exports a checksum of its already-installed root sample;
     *   - the test compares the worker artifacts and writes the consolidated
     *     validation JSON for the independent Python validator.
     *
     * false:
     *   - request 89 is never sent;
     *   - no Phase-2 checksum/export work is performed;
     *   - normal/benchmark execution is unaffected.
     */
    private static final int REQUEST_DEBUG_VALIDATE_PHASE2 = 89;
    private static final boolean VALIDATE_PHASE2 = false;

    private static final String PHASE2_VALIDATION_DIR =
            System.getProperty("onepass.phase2ValidationDir", "/tmp/onepass-phase2-validator");

    private static final String PHASE2_VALIDATION_JSON_PATH =
            System.getProperty("onepass.phase2ValidatorJson", "/tmp/onepass_phase2_validation.json");

    private static final long PHASE2_VALIDATION_TIMEOUT_MS =
            Long.parseLong(System.getProperty("onepass.phase2ValidationTimeoutMs", "120000"));

    private static final String PHASE2_CHECKSUM_VERSION =
            "ONEPASS_PHASE2_ROOT_SAMPLE_SHA256_V1";

    //Get the query output at console
    private static final int REQUEST_ESTIMATE = 3;
    private static final int FINAL_RESULT_PREVIEW_LIMIT = 4;
    private static final boolean PRINT_FINAL_RESULTS = Boolean.
            parseBoolean(System.getProperty("onepass.printFinalResults", "false"));

    private OnePassSamplerSdeCoordinatorTest() {}

    public static void main(String[] args) throws Exception {

        configureRuntimeArguments(args);

        int uid = UUID.randomUUID().toString().hashCode() & 0x7fffffff;

        benchmarkNanos.clear();
        benchmarkCounts.clear();

        if (RUN_PHASE_3 && !RUN_PHASE_2) {
            throw new IllegalStateException(
                    "RUN_PHASE_3=true requires RUN_PHASE_2=true because Phase 3 extends the installed Phase-2 root sample.");
        }

        String streamId = "onepass-sharded-phase123-local-test";

        String baseKey = "onepass-phase123-" + uid;

        System.out.println("=======================================================");
        System.out.println(" OnePass* SHARDED PHASE 1 + PHASE 2 + PHASE 3 - LOCAL TEST");
        System.out.println("=======================================================");
        System.out.println("uid              = " + uid);
        System.out.println("baseKey          = " + baseKey);
        System.out.println("workers          = " + EXPECTED_WORKERS);
        System.out.println("bootstrap        = " + BOOTSTRAP_SERVERS);
        System.out.println("dataTopic        = " + DATA_TOPIC);
        System.out.println("requestTopic     = " + REQUEST_TOPIC);
        System.out.println("outputTopic      = " + OUTPUT_TOPIC);
        System.out.println("stateTopic(SDE)  = " + STATE_TOPIC);
        System.out.println("TPC-H dir        = " + TEST_TPCH_DIR);
        System.out.println("TEST_ROW_LIMIT   = " + TEST_ROW_LIMIT);
        System.out.println("transactionTimeoutMs = " + TRANSACTION_TIMEOUT_MS);
        System.out.println("RUN_PHASE_2      = " + RUN_PHASE_2);
        System.out.println("RUN_PHASE_3      = " + RUN_PHASE_3);
        System.out.println("EXPORT_PHASE1_INDEXES = " + EXPORT_PHASE1_INDEXES);
        System.out.println("VALIDATE_PHASE2  = " + VALIDATE_PHASE2);
        System.out.println("combinedBenchmarkCsv = " + COMBINED_BENCHMARK_CSV_PATH);
        System.out.println("WRITE_COMBINED_BENCHMARK_CSV = " + WRITE_COMBINED_BENCHMARK_CSV);
        System.out.println("WRITE_DETAILED_BENCHMARK_DATA = " + WRITE_DETAILED_BENCHMARK_DATA);
        System.out.println("WRITE_LEGACY_SEPARATE_BENCHMARK_CSV = " + WRITE_LEGACY_SEPARATE_BENCHMARK_CSV);

        if (EXPORT_PHASE1_INDEXES) {
            System.out.println("phase1IndexExportDir = " + PHASE1_INDEX_EXPORT_DIR);
            System.out.println("phase1ValidatorJson  = " + PHASE1_VALIDATOR_JSON_PATH);
        }

        if (VALIDATE_PHASE2) {
            System.out.println("phase2ValidationDir  = " + PHASE2_VALIDATION_DIR);
            System.out.println("phase2ValidatorJson  = " + PHASE2_VALIDATION_JSON_PATH);
            System.out.println("phase2ChecksumVersion= " + PHASE2_CHECKSUM_VERSION);
        }

        if (WRITE_COMBINED_BENCHMARK_CSV) {
            validateCombinedBenchmarkCsvSchema();
        }

        System.out.println("SQL:");
        System.out.println(TEST_ONEPASS_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_ONEPASS_SQL);

        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassCatalog catalog = OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        validatePlanForShardedOnePassV1(plan);

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Root child edges: " + plan.getChildEdges(plan.getRootAlias()));
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Root-to-leaf order: " + plan.getRootToLeafOrder());
        System.out.println("Phase-3 replay order: " + phaseThreeAliasOrder(plan));
        System.out.println("Required fields by alias: " + plan.getRequiredFieldsByAlias());
        System.out.println();

        KafkaProducer<String, String> controlProducer = createProducer();
        KafkaConsumer<String, String> phaseOneFeedbackConsumer = createObserverConsumer();
        KafkaConsumer<String, String> phaseTwoOutputConsumer = null;
        List<PreparedAliasTransaction> preparedPhaseOne = new ArrayList<PreparedAliasTransaction>();
        PreparedAliasTransaction preparedPhaseTwoRoot = null;
        List<PreparedAliasTransaction> preparedPhaseThree = new ArrayList<PreparedAliasTransaction>();

        try {

            /*
             * Position the Phase-1 RequestTopic observer BEFORE ADD so no
             * transition produced for this UID can be missed.
             */
            initializeObserver(phaseOneFeedbackConsumer, REQUEST_TOPIC);

            // =============================================================
            // PHASE 1 PRELOAD
            // =============================================================

            System.out.println();
            System.out.println("Preloading all Phase-1 aliases into Kafka transactions " + "BEFORE starting the measured Phase-1 algorithm...");
            System.out.println();

            long phaseOnePreloadStartNanos = tic();

            preparedPhaseOne = preparePhaseOneTransactions(uid, baseKey, streamId, catalog, plan);

            long phaseOnePreloadNanos = System.nanoTime() - phaseOnePreloadStartNanos;

            System.out.printf("Phase-1 Kafka preload completed OUTSIDE algorithm timer: %.3f s%n", phaseOnePreloadNanos / 1_000_000_000.0d);

            long totalPreparedPhaseOneRows = 0L;

            for (PreparedAliasTransaction prepared : preparedPhaseOne) {

                totalPreparedPhaseOneRows += prepared.rows;

                System.out.println("  PREPARED PHASE1 alias=" + prepared.alias + ", epoch=" + prepared.epoch + ", rows=" + prepared.rows + ", committed=" + prepared.committed);
            }

            if (totalPreparedPhaseOneRows <= 0L) {

                throw new IllegalStateException("No Phase-1 rows were preloaded.");
            }

            // =============================================================
            // ADD
            // =============================================================

            System.out.println();
            System.out.println("Sending ADD OnePass request with noOfP=" + EXPECTED_WORKERS + "...");

            ObjectNode addRequest = buildOnePassAddRequest(baseKey, streamId, uid, EXPECTED_WORKERS);

            sendJson(controlProducer, REQUEST_TOPIC, baseKey, addRequest);

            controlProducer.flush();

            /*
             * Existing temporary ADD synchronization.
             * Setup time; deliberately outside both algorithm timers.
             */
            Thread.sleep(3000L);

            // =============================================================
            // MEASURED PHASE 1
            // =============================================================

            System.out.println();
            System.out.println("=======================================================");
            System.out.println(" STARTING MEASURED ONEPASS* SHARDED PHASE 1");
            System.out.println(" TPC-H parsing + Kafka sends are already complete.");
            System.out.println("=======================================================");
            System.out.println();

            long phaseOneTotalStartNanos = tic();

            for (PreparedAliasTransaction prepared : preparedPhaseOne) {

                String alias = prepared.alias;

                int epoch = prepared.epoch;

                long aliasStartNanos = tic();

                String resultId = "PHASE1_" + alias + "_" + uid;

                boolean last = epoch == plan.getLeafToRootOrder().size();

                String expectedNextCommand = last ? "START_PHASE_2" : "START_NEXT_ALIAS";

                String expectedNextAlias = last ? plan.getRootAlias() : plan.getLeafToRootOrder().get(epoch);

                System.out.println();
                System.out.println("-------------------------------------------------------");
                System.out.println("Releasing Phase-1 alias=" + alias + ", epoch=" + epoch + ", rows=" + prepared.rows);

                System.out.println("Expected transition: " + expectedNextCommand + " -> " + expectedNextAlias);
                System.out.println("-------------------------------------------------------");

                /*
                 * The commit is the algorithm release signal and is therefore
                 * intentionally inside the measured interval.
                 */
                prepared.producer.commitTransaction();
                prepared.committed = true;

                System.out.println("Kafka transaction committed. " + alias + " is now visible to the read_committed SDE source.");

                JsonNode transition = waitForShardedPhaseOneTransition(phaseOneFeedbackConsumer, uid, epoch, alias, resultId, expectedNextCommand, expectedNextAlias, TIMEOUT_MS);

                long globalSeen = longField(transition, "globalSeenTuples", -1L);

                long globalKeyCount = longField(transition, "globalKeyCount", -1L);

                double globalTotalWeight = doubleField(transition, "globalTotalWeight", 0.0d);

                if (globalSeen != prepared.rows) {

                    throw new IllegalStateException("Phase-1 seen-tuple mismatch for alias=" + alias + ": expected=" + prepared.rows + ", globalSeen=" + globalSeen + ". Transition=" + transition);
                }

                if (globalKeyCount <= 0L) {

                    throw new IllegalStateException("Invalid global shard key count after alias=" + alias + ": " + globalKeyCount + ". Transition=" + transition);
                }

                if (globalTotalWeight <= 0.0d) {

                    throw new IllegalStateException("Invalid global shard total weight after alias=" + alias + ": " + globalTotalWeight + ". Transition=" + transition);
                }

                recordCount("phase1_rows_processed", prepared.rows);

                recordCount("phase1_alias_" + alias + "_rows_processed", prepared.rows);

                recordDuration("phase1_alias_" + alias + "_algorithm", aliasStartNanos);

                System.out.println("Alias complete: alias=" + alias + ", epoch=" + epoch + ", globalSeenTuples=" + globalSeen + ", globalKeyCount=" + globalKeyCount + ", globalTotalWeight=" + globalTotalWeight);
            }

            recordDuration("phase1_algorithm_total", phaseOneTotalStartNanos);

            System.out.println();
            System.out.println("=======================================================");
            System.out.println(" SHARDED PHASE 1 COMPLETE");
            System.out.println(" START_PHASE_2 has been observed.");
            System.out.println("=======================================================");

            /*
             * Debug export is deliberately outside phase1_algorithm_total.
             *
             * START_PHASE_2 may already have activated the Phase-2 lifecycle,
             * but Phase-1 shard state still exists and the debug exporter reads
             * that physical state directly.
             */
            if (EXPORT_PHASE1_INDEXES) {

                exportPhaseOneIndexesForValidator(controlProducer, uid, baseKey, streamId);
            }

            printPhaseOneBenchmarkSummary(plan, phaseOnePreloadNanos);

            /*
             * Historical per-phase CSV writer. The method is retained, but
             * it is a no-op unless WRITE_LEGACY_SEPARATE_BENCHMARK_CSV=true.
             */
            writePhaseOneBenchmarkCsv(
                    plan,
                    phaseOnePreloadNanos,
                    "SDE_KAFKA_MULTIWORKER_SHARDED_PHASE1_LOCAL"
            );

            // =============================================================
            // PHASE 2
            // =============================================================

            if (RUN_PHASE_2) {

                /*
                 * Prepare the ROOT transaction now.
                 *
                 * This is AFTER Phase 1 and BEFORE the Phase-2 timer. It avoids
                 * keeping another Kafka transaction open throughout the entire
                 * Phase-1 run, while still excluding TPC-H parsing / JSON
                 * building / producer.send() from phase2_algorithm_total.
                 */
                System.out.println();
                System.out.println("=======================================================");
                System.out.println(" PREPARING PHASE-2 ROOT TRANSACTION OUTSIDE TIMER");
                System.out.println(" rootAlias=" + plan.getRootAlias());
                System.out.println("=======================================================");

                long phaseTwoPreloadStartNanos = tic();

                preparedPhaseTwoRoot = preparePhaseTwoRootTransaction(uid, baseKey, streamId, catalog, plan);

                long phaseTwoPreloadNanos = System.nanoTime() - phaseTwoPreloadStartNanos;

                System.out.printf("Phase-2 root Kafka preload completed OUTSIDE algorithm timer: %.3f s%n", phaseTwoPreloadNanos / 1_000_000_000.0d);

                System.out.println("  PREPARED PHASE2 rootAlias=" + preparedPhaseTwoRoot.alias + ", epoch=" + preparedPhaseTwoRoot.epoch + ", rows=" + preparedPhaseTwoRoot.rows + ", committed=" + preparedPhaseTwoRoot.committed);

                /*
                 * OUT observer is initialized immediately before Phase 2.
                 * Seeking to end here discards unrelated/old Phase-1 output but
                 * cannot miss Phase-2 completion because root data is still
                 * hidden inside the uncommitted transaction.
                 */
                phaseTwoOutputConsumer = createObserverConsumer();

                initializeObserver(phaseTwoOutputConsumer, OUTPUT_TOPIC);

                String phaseTwoResultId = phaseTwoResultId(uid);

                System.out.println();
                System.out.println("=======================================================");
                System.out.println(" STARTING MEASURED ONEPASS* SHARDED PHASE 2");
                System.out.println(" rootAlias=" + plan.getRootAlias());
                System.out.println(" rootRows=" + preparedPhaseTwoRoot.rows);
                System.out.println(" resultId=" + phaseTwoResultId);
                System.out.println("=======================================================");

                long phaseTwoStartNanos = tic();

                preparedPhaseTwoRoot.producer.commitTransaction();
                preparedPhaseTwoRoot.committed = true;

                System.out.println("Kafka transaction committed. Phase-2 root " + preparedPhaseTwoRoot.alias + " is now visible to the read_committed SDE source.");

                PhaseTwoCompletion phaseTwoCompletion = waitForShardedPhaseTwoCompletion(phaseTwoOutputConsumer, uid, phaseTwoResultId, TIMEOUT_MS);

                recordDuration("phase2_algorithm_total", phaseTwoStartNanos);

                recordCount("phase2_root_rows_processed", preparedPhaseTwoRoot.rows);

                JsonNode ready = phaseTwoCompletion.readyPayload;

                JsonNode installed = phaseTwoCompletion.installedPayload;

                long rootTuplesSeen = longField(ready, "rootTuplesSeen", -1L);

                long positiveRootCandidatesSeen = longField(ready, "positiveRootCandidatesSeen", -1L);

                double totalRootGroupWeight = doubleField(ready, "totalRootGroupWeight", 0.0d);

                int sampleSize = intField(ready, "sampleSize", -1);

                int sampleInstanceCount = intField(ready, "sampleInstanceCount", -1);

                int installedWorkerCount = intField(installed, "installedWorkerCount", -1);

                if (rootTuplesSeen != preparedPhaseTwoRoot.rows) {

                    throw new IllegalStateException("Phase-2 root tuple count mismatch." + " expected=" + preparedPhaseTwoRoot.rows + ", actual=" + rootTuplesSeen + ", ready=" + ready);
                }

                if (positiveRootCandidatesSeen <= 0L || positiveRootCandidatesSeen > rootTuplesSeen) {

                    throw new IllegalStateException("Invalid Phase-2 positiveRootCandidatesSeen=" + positiveRootCandidatesSeen + ", rootTuplesSeen=" + rootTuplesSeen + ", ready=" + ready);
                }

                if (totalRootGroupWeight <= 0.0d || Double.isNaN(totalRootGroupWeight) || Double.isInfinite(totalRootGroupWeight)) {

                    throw new IllegalStateException("Invalid Phase-2 totalRootGroupWeight=" + totalRootGroupWeight + ". Ready=" + ready);
                }

                if (sampleSize != plan.getSampleSize()) {

                    throw new IllegalStateException("Phase-2 sampleSize mismatch." + " plan=" + plan.getSampleSize() + ", global=" + sampleSize + ", ready=" + ready);
                }

                if (sampleInstanceCount != plan.getSampleSize()) {

                    throw new IllegalStateException("Phase-2 sampleInstanceCount mismatch." + " expected=" + plan.getSampleSize() + ", actual=" + sampleInstanceCount + ", ready=" + ready);
                }

                if (installedWorkerCount != EXPECTED_WORKERS) {

                    throw new IllegalStateException("Phase-2 installed worker count mismatch." + " expected=" + EXPECTED_WORKERS + ", actual=" + installedWorkerCount + ", installed=" + installed);
                }

                List<String> phaseThreeOrder = phaseThreeAliasOrder(plan);

                int phaseThreeAliasCount = intField(installed, "phaseThreeAliasCount", -1);

                String firstPhaseThreeAlias = textField(installed, "firstPhaseThreeAlias", "");

                if (phaseThreeAliasCount != phaseThreeOrder.size()) {
                    throw new IllegalStateException(
                            "Phase-2 -> Phase-3 alias-count mismatch. planOrder=" + phaseThreeOrder
                                    + ", payloadCount=" + phaseThreeAliasCount
                                    + ", installed=" + installed);
                }

                if (phaseThreeOrder.isEmpty()) {
                    if (!firstPhaseThreeAlias.isEmpty()) {
                        throw new IllegalStateException(
                                "Degenerate Phase-3 plan unexpectedly declares firstPhaseThreeAlias="
                                        + firstPhaseThreeAlias + ". Installed=" + installed);
                    }
                } else if (!phaseThreeOrder.get(0).equals(firstPhaseThreeAlias)) {
                    throw new IllegalStateException(
                            "Phase-2 -> Phase-3 first-alias mismatch. expected="
                                    + phaseThreeOrder.get(0)
                                    + ", actual=" + firstPhaseThreeAlias
                                    + ", installed=" + installed);
                }

                printPhaseTwoBenchmarkSummary(plan, preparedPhaseTwoRoot.rows, phaseTwoPreloadNanos, ready, installed);

                /*
                 * Historical per-phase CSV writer. The method is retained, but
                 * it is a no-op unless WRITE_LEGACY_SEPARATE_BENCHMARK_CSV=true.
                 */
                writePhaseTwoBenchmarkCsv(
                        plan,
                        preparedPhaseTwoRoot.rows,
                        phaseTwoPreloadNanos,
                        ready,
                        installed,
                        "SDE_KAFKA_MULTIWORKER_SHARDED_PHASE2_LOCAL"
                );

                /*
                 * TEST-ONLY correctness validation.
                 *
                 * IMPORTANT:
                 * phase2_algorithm_total has already stopped above. Therefore
                 * checksum construction, worker JSON exports, file polling, and
                 * independent-validator artifact generation are excluded from
                 * the measured Phase-2 runtime.
                 */
                if (VALIDATE_PHASE2) {

                    PhaseTwoValidationResult validation = validateInstalledPhaseTwoRootSamples(controlProducer, uid, baseKey, streamId, plan, ready, installed);

                    System.out.println();
                    System.out.println("Phase-2 installed-root validation PASSED.");
                    System.out.println("  checksumVersion = " + validation.checksumVersion);
                    System.out.println("  checksum        = " + validation.checksum);
                    System.out.println("  workerChecksums = " + validation.workerChecksums);
                    System.out.println("  validatorJson   = " + PHASE2_VALIDATION_JSON_PATH);
                }

                System.out.println();
                System.out.println("=======================================================");
                System.out.println(" SHARDED PHASE 2 COMPLETE");
                System.out.println("=======================================================");
                System.out.println("Global root sample is installed on all " + EXPECTED_WORKERS + " workers.");

                // =========================================================
                // PHASE 3
                // =========================================================

                if (RUN_PHASE_3) {

                    if (phaseThreeOrder.isEmpty()) {
                        throw new IllegalStateException(
                                "RUN_PHASE_3=true but the compiled plan has no non-root aliases.");
                    }

                    /*
                     * request 86 is fed into the stateless Phase-3 transition
                     * mapper. Before any replay data is released, prove that
                     * the first START_PHASE_3_ALIAS request reached RequestTopic
                     * with the same traversal metadata used by the workers.
                     */
                    JsonNode firstPhaseThreeStart = waitForShardedPhaseThreeStartTransition(
                            phaseOneFeedbackConsumer,
                            uid,
                            baseKey,
                            phaseThreeOrder.get(0),
                            0,
                            phaseThreeResultId(uid, phaseThreeOrder.get(0)),
                            phaseThreeOrder.size(),
                            TIMEOUT_MS
                    );

                    String phaseTwoStateRef = textField(installed, "stateRef", "");
                    String firstTriggerStateRef = textField(firstPhaseThreeStart, "triggerStateRef", "");

                    if (phaseTwoStateRef.isEmpty() || !phaseTwoStateRef.equals(firstTriggerStateRef)) {
                        throw new IllegalStateException(
                                "First Phase-3 transition does not reference the installed Phase-2 state."
                                        + " phase2StateRef=" + phaseTwoStateRef
                                        + ", triggerStateRef=" + firstTriggerStateRef
                                        + ", transition=" + firstPhaseThreeStart);
                    }

                    /*
                     * As in Phase 1, parse and producer.send() every Phase-3
                     * replay relation before the measured algorithm starts.
                     * The transactions remain open, so read_committed SDE
                     * workers cannot observe any replay tuple yet.
                     */
                    System.out.println();
                    System.out.println("=======================================================");
                    System.out.println(" PREPARING PHASE-3 REPLAY TRANSACTIONS OUTSIDE TIMER");
                    System.out.println(" replayOrder=" + phaseThreeOrder);
                    System.out.println("=======================================================");

                    long phaseThreePreloadStartNanos = tic();

                    preparedPhaseThree = preparePhaseThreeTransactions(
                            uid,
                            baseKey,
                            streamId,
                            catalog,
                            plan
                    );

                    long phaseThreePreloadNanos =
                            System.nanoTime() - phaseThreePreloadStartNanos;

                    long totalPreparedPhaseThreeRows = 0L;

                    for (int i = 0; i < preparedPhaseThree.size(); i++) {
                        PreparedAliasTransaction prepared = preparedPhaseThree.get(i);

                        totalPreparedPhaseThreeRows += prepared.rows;

                        System.out.println(
                                "  PREPARED PHASE3 alias=" + prepared.alias
                                        + ", aliasIndex=" + i
                                        + ", epoch=" + prepared.epoch
                                        + ", rows=" + prepared.rows
                                        + ", committed=" + prepared.committed);
                    }

                    if (preparedPhaseThree.size() != phaseThreeOrder.size()) {
                        throw new IllegalStateException(
                                "Prepared Phase-3 transaction count mismatch. expected="
                                        + phaseThreeOrder.size()
                                        + ", actual=" + preparedPhaseThree.size());
                    }

                    if (totalPreparedPhaseThreeRows <= 0L) {
                        throw new IllegalStateException("No Phase-3 replay rows were preloaded.");
                    }

                    System.out.printf(
                            "Phase-3 Kafka preload completed OUTSIDE algorithm timer: %.3f s%n",
                            phaseThreePreloadNanos / 1_000_000_000.0d
                    );

                    System.out.println();
                    System.out.println("=======================================================");
                    System.out.println(" STARTING MEASURED ONEPASS* SHARDED PHASE 3");
                    System.out.println(" replayOrder=" + phaseThreeOrder);
                    System.out.println("=======================================================");

                    long phaseThreeTotalStartNanos = tic();

                    JsonNode finalPhaseThreeCompletion = null;

                    for (int aliasIndex = 0; aliasIndex < preparedPhaseThree.size(); aliasIndex++) {

                        PreparedAliasTransaction prepared = preparedPhaseThree.get(aliasIndex);

                        String alias = prepared.alias;

                        String expectedAlias = phaseThreeOrder.get(aliasIndex);

                        if (!expectedAlias.equals(alias)) {
                            throw new IllegalStateException(
                                    "Prepared Phase-3 order mismatch. aliasIndex=" + aliasIndex
                                            + ", expected=" + expectedAlias
                                            + ", actual=" + alias);
                        }

                        String resultId = phaseThreeResultId(uid, alias);

                        boolean last = aliasIndex == phaseThreeOrder.size() - 1;

                        long aliasStartNanos = tic();

                        System.out.println();
                        System.out.println("-------------------------------------------------------");
                        System.out.println(
                                "Releasing Phase-3 alias=" + alias
                                        + ", aliasIndex=" + aliasIndex
                                        + ", rows=" + prepared.rows
                                        + ", resultId=" + resultId);
                        System.out.println("-------------------------------------------------------");

                        /*
                         * The transaction commit is the Phase-3 replay release
                         * signal and is intentionally inside the algorithm timer.
                         */
                        prepared.producer.commitTransaction();
                        prepared.committed = true;

                        System.out.println(
                                "Kafka transaction committed. Phase-3 replay alias "
                                        + alias
                                        + " is now visible to the read_committed SDE source.");

                        if (last) {

                            finalPhaseThreeCompletion = waitForShardedPhaseThreeCompletion(
                                    phaseTwoOutputConsumer,
                                    uid,
                                    baseKey,
                                    alias,
                                    aliasIndex,
                                    resultId,
                                    phaseThreeOrder.size(),
                                    plan.getSampleSize(),
                                    TIMEOUT_MS
                            );

                        } else {

                            String nextAlias = phaseThreeOrder.get(aliasIndex + 1);

                            JsonNode nextTransition = waitForShardedPhaseThreeStartTransition(
                                    phaseOneFeedbackConsumer,
                                    uid,
                                    baseKey,
                                    nextAlias,
                                    aliasIndex + 1,
                                    phaseThreeResultId(uid, nextAlias),
                                    phaseThreeOrder.size(),
                                    TIMEOUT_MS
                            );

                            String triggerStateRef = textField(nextTransition, "triggerStateRef", "");

                            if (triggerStateRef.isEmpty()) {
                                throw new IllegalStateException(
                                        "Non-final Phase-3 transition has no triggerStateRef."
                                                + " completedAlias=" + alias
                                                + ", nextAlias=" + nextAlias
                                                + ", transition=" + nextTransition);
                            }
                        }

                        recordCount("phase3_rows_replayed", prepared.rows);

                        recordCount(
                                "phase3_alias_" + alias + "_rows_replayed",
                                prepared.rows
                        );

                        recordDuration(
                                "phase3_alias_" + alias + "_algorithm",
                                aliasStartNanos
                        );

                        System.out.println(
                                "Phase-3 alias complete: alias=" + alias
                                        + ", aliasIndex=" + aliasIndex
                                        + ", rows=" + prepared.rows
                                        + ", finalAlias=" + last);
                    }

                    recordDuration("phase3_algorithm_total", phaseThreeTotalStartNanos);

                    if (finalPhaseThreeCompletion == null) {
                        throw new IllegalStateException(
                                "Phase-3 loop completed without the final GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED barrier.");
                    }

                    if (PRINT_FINAL_RESULTS) {
                        JsonNode finalQueryResult = requestFinalQueryResult(controlProducer,
                                phaseTwoOutputConsumer, uid, baseKey, streamId, TIMEOUT_MS);
                        printFinalQueryResultPreview(finalQueryResult, FINAL_RESULT_PREVIEW_LIMIT);
                    }


                    printPhaseThreeBenchmarkSummary(plan, phaseThreeOrder, phaseThreePreloadNanos, finalPhaseThreeCompletion);

                    writeCombinedBenchmarkCsv(
                            plan,
                            phaseOnePreloadNanos,
                            preparedPhaseTwoRoot.rows,
                            phaseTwoPreloadNanos,
                            ready,
                            installed,
                            phaseThreePreloadNanos,
                            finalPhaseThreeCompletion,
                            "SDE_KAFKA_MULTIWORKER_SHARDED_ONEPASS_PHASE123"
                    );

                    System.out.println();
                    System.out.println("=======================================================");
                    System.out.println(" SHARDED PHASE 3 COMPLETE");
                    System.out.println("=======================================================");
                    System.out.println(
                            "SUCCESS: Phase 1 + Phase 2 + Phase 3 completed locally.");
                    System.out.println(
                            "Final global Phase-3 selections were installed on all "
                                    + EXPECTED_WORKERS
                                    + " workers.");

                } else {

                    /*
                     * Production request 86 still activates the first Phase-3
                     * alias automatically. RUN_PHASE_3=false simply means this
                     * integration test does not replay its tuples and stops its
                     * assertions at the Phase-2 barrier.
                     */
                    writeCombinedBenchmarkCsv(
                            plan,
                            phaseOnePreloadNanos,
                            preparedPhaseTwoRoot.rows,
                            phaseTwoPreloadNanos,
                            ready,
                            installed,
                            0L,
                            null,
                            "SDE_KAFKA_MULTIWORKER_SHARDED_ONEPASS_PHASE12_ONLY"
                    );

                    System.out.println();
                    System.out.println(
                            "SUCCESS: Phase 1 + sharded Phase 2 completed locally. "
                                    + "RUN_PHASE_3=false, so Phase-3 replay data was not released.");
                }

            } else {

                System.out.println();
                System.out.println("SUCCESS: sharded Phase 1 completed locally. " + "RUN_PHASE_2=false, so the test stops at START_PHASE_2.");
            }

        } finally {

            // =============================================================
            // ONEPASS CLEANUP
            // =============================================================

            try {

                System.out.println();
                System.out.println("Removing OnePass synopsis uid=" + uid);

                ObjectNode removeRequest = buildOnePassRemoveRequest(baseKey, streamId, uid, EXPECTED_WORKERS);

                sendJson(controlProducer, REQUEST_TOPIC, baseKey, removeRequest);

                controlProducer.flush();

            } catch (Exception cleanupError) {

                System.err.println("WARNING: OnePass cleanup request failed for uid=" + uid);

                cleanupError.printStackTrace();
            }

            /*
             * Abort any Phase-1 transaction that did not reach its release
             * point, then close every transactional producer.
             */
            for (PreparedAliasTransaction prepared : preparedPhaseOne) {

                closePreparedTransaction(prepared, "Phase-1");
            }

            /*
             * Same cleanup for the root transaction if Phase 2 preparation
             * succeeded but its release/completion failed.
             */
            closePreparedTransaction(preparedPhaseTwoRoot, "Phase-2 root");

            /*
             * Abort any Phase-3 replay transaction that did not reach its
             * release point, then close every transactional producer.
             */
            for (PreparedAliasTransaction prepared : preparedPhaseThree) {
                closePreparedTransaction(prepared, "Phase-3 replay");
            }

            try {

                phaseOneFeedbackConsumer.close();

            } catch (Exception ignored) {
            }

            if (phaseTwoOutputConsumer != null) {

                try {

                    phaseTwoOutputConsumer.close();

                } catch (Exception ignored) {
                }
            }

            try {

                controlProducer.close();

            } catch (Exception ignored) {
            }
        }
    }


    private static void closePreparedTransaction(PreparedAliasTransaction prepared, String label) {

        if (prepared == null || prepared.producer == null) {

            return;
        }

        try {

            if (!prepared.committed) {

                System.err.println("Aborting uncommitted " + label + " transaction: alias=" + prepared.alias + ", epoch=" + prepared.epoch);

                prepared.producer.abortTransaction();
            }

        } catch (Exception abortError) {

            System.err.println("WARNING: could not abort " + label + " transaction for alias=" + prepared.alias);

            abortError.printStackTrace();
        }

        try {

            prepared.producer.close();

        } catch (Exception closeError) {

            System.err.println("WARNING: could not close " + label + " transactional producer for alias=" + prepared.alias);

            closeError.printStackTrace();
        }
    }

    // =====================================================================
    // PHASE-1 PRELOAD
    // =====================================================================

    private static List<PreparedAliasTransaction> preparePhaseOneTransactions(int uid, String baseKey, String streamId, OnePassCatalog catalog, CompiledOnePassPlan plan) throws Exception {

        List<PreparedAliasTransaction> prepared = new ArrayList<PreparedAliasTransaction>();

        int position = 0;

        for (String alias : plan.getLeafToRootOrder()) {

            position++;

            int epoch = position;

            boolean last = position == plan.getLeafToRootOrder().size();

            String resultId = "PHASE1_" + alias + "_" + uid;

            String nextCommand = last ? "START_PHASE_2" : "START_NEXT_ALIAS";

            String nextAlias = last ? plan.getRootAlias() : plan.getLeafToRootOrder().get(position);

            String transactionalId = "onepass-p1-" + uid + "-" + epoch + "-" + alias + "-" + Long.toHexString(System.nanoTime());

            KafkaProducer<String, String> aliasProducer = createTransactionalProducer(transactionalId);

            boolean success = false;

            try {
                System.out.println("Preparing Kafka transaction for alias=" + alias + ", epoch=" + epoch + "...");

                long rows = streamAlias(aliasProducer, DATA_TOPIC, baseKey, streamId, catalog, plan, alias, TEST_ROW_LIMIT, plan.getRequiredFieldsByAlias());

                if (rows <= 0L) {
                    throw new IllegalStateException("No rows were read for Phase-1 alias " + alias);
                }

                ObjectNode endAlias = buildEndAliasDatapoint(baseKey, streamId, uid, alias, epoch, resultId, EXPECTED_WORKERS, nextCommand, nextAlias);

                /*
                 * END_ALIAS is in the SAME transaction and uses the SAME Kafka
                 * key as the alias tuples. Therefore it becomes visible only
                 * after all tuple records for this alias.
                 */
                sendJsonAsync(aliasProducer, DATA_TOPIC, baseKey, endAlias);

                aliasProducer.flush();

                /*
                 * IMPORTANT: do NOT commit here.
                 * The transaction remains open until the measured algorithm
                 * reaches this alias.
                 */
                prepared.add(new PreparedAliasTransaction(alias, epoch, rows, aliasProducer));

                success = true;

                System.out.println("Prepared UNCOMMITTED transaction: alias=" + alias + ", epoch=" + epoch + ", rows=" + rows);

            } finally {

                /*
                 * If preparation itself fails before ownership of the producer
                 * moves into the prepared list, abort/close immediately.
                 */
                if (!success) {
                    try {
                        aliasProducer.abortTransaction();
                    } catch (Exception ignored) {
                    }

                    try {
                        aliasProducer.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        return prepared;
    }


    // =====================================================================
    // PHASE-2 ROOT PRELOAD
    // =====================================================================

    private static PreparedAliasTransaction preparePhaseTwoRootTransaction(int uid, String baseKey, String streamId, OnePassCatalog catalog, CompiledOnePassPlan plan) throws Exception {

        String rootAlias = plan.getRootAlias();

        int epoch = plan.getLeafToRootOrder().size() + 1;

        String resultId = phaseTwoResultId(uid);

        String transactionalId = "onepass-p2-" + uid + "-" + epoch + "-" + rootAlias + "-" + Long.toHexString(System.nanoTime());

        KafkaProducer<String, String> rootProducer = createTransactionalProducer(transactionalId);

        boolean success = false;

        try {

            System.out.println("Preparing Kafka transaction for Phase-2 root alias=" + rootAlias + ", epoch=" + epoch + "...");

            long rows = streamAlias(rootProducer, DATA_TOPIC, baseKey, streamId, catalog, plan, rootAlias, TEST_ROW_LIMIT, plan.getRequiredFieldsByAlias());

            if (rows <= 0L) {

                throw new IllegalStateException("No rows were read for Phase-2 root alias " + rootAlias);
            }

            ObjectNode endRoot = buildPhaseTwoEndAliasDatapoint(baseKey, streamId, uid, rootAlias, epoch, resultId, EXPECTED_WORKERS);

            /*
             * Same transaction + same Kafka key as the root tuples.
             * With read_committed, END_ALIAS(root) cannot be observed before
             * all earlier root rows in this transaction become visible.
             */
            sendJsonAsync(rootProducer, DATA_TOPIC, baseKey, endRoot);

            rootProducer.flush();

            PreparedAliasTransaction prepared = new PreparedAliasTransaction(rootAlias, epoch, rows, rootProducer);

            success = true;

            System.out.println("Prepared UNCOMMITTED Phase-2 root transaction:" + " alias=" + rootAlias + ", epoch=" + epoch + ", rows=" + rows + ", resultId=" + resultId);

            return prepared;

        } finally {

            if (!success) {

                try {

                    rootProducer.abortTransaction();

                } catch (Exception ignored) {
                }

                try {

                    rootProducer.close();

                } catch (Exception ignored) {
                }
            }
        }
    }



    // =====================================================================
    // PHASE-3 REPLAY PRELOAD
    // =====================================================================

    /**
     * Prepares one read_committed Kafka transaction per Phase-3 replay alias.
     *
     * The replay order deliberately mirrors the production
     * SDEcoFlatMap.phaseThreeAliasOrder(...): plan.getRootToLeafOrder() with
     * the root itself removed.
     */
    private static List<PreparedAliasTransaction> preparePhaseThreeTransactions(
            int uid,
            String baseKey,
            String streamId,
            OnePassCatalog catalog,
            CompiledOnePassPlan plan) throws Exception {

        List<String> order = phaseThreeAliasOrder(plan);

        List<PreparedAliasTransaction> prepared =
                new ArrayList<PreparedAliasTransaction>();

        for (int aliasIndex = 0; aliasIndex < order.size(); aliasIndex++) {

            String alias = order.get(aliasIndex);

            /*
             * Epoch is diagnostic only for PHASE3 END_ALIAS today, but keeping
             * it monotonic makes Kafka traces easier to read.
             */
            int epoch =
                    plan.getLeafToRootOrder().size()
                            + 2
                            + aliasIndex;

            String resultId =
                    phaseThreeResultId(
                            uid,
                            alias
                    );

            String transactionalId =
                    "onepass-p3-"
                            + uid
                            + "-"
                            + aliasIndex
                            + "-"
                            + alias
                            + "-"
                            + Long.toHexString(
                            System.nanoTime()
                    );

            KafkaProducer<String, String> aliasProducer =
                    createTransactionalProducer(
                            transactionalId
                    );

            boolean success =
                    false;

            try {

                System.out.println(
                        "Preparing Kafka transaction for Phase-3 replay alias="
                                + alias
                                + ", aliasIndex="
                                + aliasIndex
                                + ", epoch="
                                + epoch
                                + "..."
                );

                long rows =
                        streamAlias(
                                aliasProducer,
                                DATA_TOPIC,
                                baseKey,
                                streamId,
                                catalog,
                                plan,
                                alias,
                                TEST_ROW_LIMIT,
                                plan.getRequiredFieldsByAlias()
                        );

                if (rows <= 0L) {

                    throw new IllegalStateException(
                            "No rows were read for Phase-3 replay alias "
                                    + alias
                    );
                }

                ObjectNode endAlias =
                        buildPhaseThreeEndAliasDatapoint(
                                baseKey,
                                streamId,
                                uid,
                                alias,
                                epoch,
                                aliasIndex,
                                order.size(),
                                resultId,
                                EXPECTED_WORKERS,
                                aliasIndex == order.size() - 1
                                        ? ""
                                        : order.get(aliasIndex + 1)
                        );

                /*
                 * END_ALIAS is in the same transaction and uses the same base
                 * Kafka key as every replay tuple. The OnePass router broadcasts
                 * the marker to all logical workers after commit.
                 */
                sendJsonAsync(
                        aliasProducer,
                        DATA_TOPIC,
                        baseKey,
                        endAlias
                );

                aliasProducer.flush();

                prepared.add(
                        new PreparedAliasTransaction(
                                alias,
                                epoch,
                                rows,
                                aliasProducer
                        )
                );

                success =
                        true;

            } finally {

                if (!success) {

                    try {
                        aliasProducer.abortTransaction();
                    } catch (Exception ignored) {
                    }

                    try {
                        aliasProducer.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        return prepared;
    }


    private static String phaseThreeResultId(
            int uid,
            String alias) {

        return "PHASE3_"
                + alias
                + "_"
                + uid;
    }


    /**
     * Exact production Phase-3 traversal order.
     *
     * CompiledOnePassPlan.rootToLeafOrder includes the root relation, while
     * Phase 3 replays only non-root relations.
     */
    private static List<String> phaseThreeAliasOrder(
            CompiledOnePassPlan plan) {

        List<String> order =
                new ArrayList<String>();

        for (String alias : plan.getRootToLeafOrder()) {

            if (!plan.isRoot(alias)) {

                order.add(alias);
            }
        }

        return order;
    }


    private static String phaseTwoResultId(int uid) {

        return "PHASE2_RESULT_" + uid;
    }


    private static KafkaProducer<String, String> createTransactionalProducer(String transactionalId) {

        Properties props = baseProducerProperties();

        props.put("enable.idempotence", "true");

        props.put("transactional.id", transactionalId);

        props.put("transaction.timeout.ms", Integer.toString(TRANSACTION_TIMEOUT_MS));

        /*
         * Larger batch than the small control producer because this producer
         * is used to preload relation data.
         */
        props.put("batch.size", "65536");

        props.put("linger.ms", "5");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        producer.initTransactions();
        producer.beginTransaction();

        return producer;
    }

    private static final class PreparedAliasTransaction {

        private final String alias;
        private final int epoch;
        private final long rows;
        private final KafkaProducer<String, String> producer;

        private boolean committed;

        private PreparedAliasTransaction(String alias, int epoch, long rows, KafkaProducer<String, String> producer) {

            this.alias = alias;
            this.epoch = epoch;
            this.rows = rows;
            this.producer = producer;
            this.committed = false;
        }
    }

    // =====================================================================
    // SHARDED PHASE-1 REQUEST-TOPIC OBSERVER
    // =====================================================================

    private static JsonNode waitForShardedPhaseOneTransition(KafkaConsumer<String, String> consumer, int uid, int completedEpoch, String completedAlias, String expectedResultId, String expectedType, String expectedNextAlias, long timeoutMs) throws Exception {

        long deadline = System.currentTimeMillis() + timeoutMs;

        int recordsSeen = 0;

        while (System.currentTimeMillis() < deadline) {

            ConsumerRecords<String, String> records = consumer.poll(1000L);

            for (ConsumerRecord<String, String> record : records) {

                recordsSeen++;

                String value = record.value();

                if (value == null || value.trim().isEmpty()) {
                    continue;
                }

                JsonNode request;

                try {
                    request = MAPPER.readTree(value);
                } catch (Exception ignored) {
                    continue;
                }

                if (intField(request, "uid", -1) != uid) {
                    continue;
                }

                if (intField(request, "synopsisID", -1) != SYNOPSIS_ID) {
                    continue;
                }

                if (intField(request, "requestID", -1) != REQUEST_UPDATE) {
                    continue;
                }

                JsonNode payload = request.get("parameters");

                if (payload == null || !payload.isObject()) {
                    continue;
                }

                if (!"SHARDED_PHASE1_V1".equals(textField(payload, "protocol", ""))) {
                    continue;
                }

                if (!expectedType.equals(textField(payload, "type", ""))) {
                    continue;
                }

                if (intField(payload, "completedEpoch", -1) != completedEpoch) {
                    continue;
                }

                if (!completedAlias.equals(textField(payload, "completedAlias", ""))) {
                    continue;
                }

                if (!expectedResultId.equals(textField(payload, "resultId", ""))) {
                    continue;
                }

                if (!expectedNextAlias.equals(textField(payload, "nextAlias", ""))) {
                    continue;
                }

                int nextEpoch = intField(payload, "epoch", -1);

                if (nextEpoch != completedEpoch + 1) {
                    throw new IllegalStateException("Transition epoch mismatch. completedEpoch=" + completedEpoch + ", expected next epoch=" + (completedEpoch + 1) + ", actual=" + nextEpoch + ". Payload=" + payload);
                }

                int expectedWorkers = intField(payload, "expectedWorkers", -1);

                if (expectedWorkers != EXPECTED_WORKERS) {
                    throw new IllegalStateException("Transition expectedWorkers mismatch. configured=" + EXPECTED_WORKERS + ", payload=" + expectedWorkers + ". Payload=" + payload);
                }

                System.out.println("Observed sharded Phase-1 transition: " + expectedType + ", completedAlias=" + completedAlias + ", completedEpoch=" + completedEpoch + ", nextAlias=" + expectedNextAlias + ", globalSeenTuples=" + longField(payload, "globalSeenTuples", -1L) + ", globalKeyCount=" + longField(payload, "globalKeyCount", -1L));

                return payload;
            }
        }

        throw new IllegalStateException("Timed out waiting for sharded Phase-1 transition. " + "uid=" + uid + ", epoch=" + completedEpoch + ", alias=" + completedAlias + ", resultId=" + expectedResultId + ", expectedType=" + expectedType + ", nextAlias=" + expectedNextAlias + ", recordsSeen=" + recordsSeen);
    }


    // =====================================================================
    // SHARDED PHASE-2 OUTPUT-TOPIC OBSERVER
    // =====================================================================

    /**
     * Waits for the single global Phase-2 completion barrier:
     * <p>
     * GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED
     * <p>
     * Under the coordinator-free StateTopic architecture, this event is
     * emitted only after every worker has installed the same globally merged
     * Phase-2 root sample.
     * <p>
     * The payload also carries the global Phase-2 counters and sample metadata
     * used by the benchmark and correctness validator.
     */
    private static PhaseTwoCompletion waitForShardedPhaseTwoCompletion(
            KafkaConsumer<String, String> consumer,
            int uid,
            String expectedResultId,
            long timeoutMs) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + timeoutMs;

        int recordsSeen =
                0;

        while (System.currentTimeMillis()
                < deadline) {

            ConsumerRecords<String, String> records =
                    consumer.poll(
                            1000L
                    );

            for (ConsumerRecord<String, String> record
                    : records) {

                recordsSeen++;

                JsonNode envelope;

                try {

                    envelope =
                            MAPPER.readTree(
                                    record.value()
                            );

                } catch (Exception ignored) {

                    continue;
                }

                JsonNode payload =
                        unwrapEstimationPayload(
                                envelope
                        );

                if (payload == null
                        || payload.isNull()
                        || !payload.isObject()) {

                    continue;
                }

                if (intField(
                        payload,
                        "uid",
                        -1
                ) != uid) {

                    continue;
                }

                String type =
                        textField(
                                payload,
                                "type",
                                ""
                        );

                /*
                 * The old coordinator-era request-84 READY event no longer
                 * exists.
                 *
                 * Request 86 / GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED is now the
                 * global Phase-2 completion barrier.
                 */
                if (!"GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED"
                        .equals(type)) {

                    continue;
                }

                String resultId =
                        textField(
                                payload,
                                "resultId",
                                ""
                        );

                if (!expectedResultId.equals(
                        resultId
                )) {

                    continue;
                }

                String stateRef =
                        textField(
                                payload,
                                "stateRef",
                                ""
                        );

                if (stateRef == null
                        || stateRef.trim().isEmpty()) {

                    throw new IllegalStateException(
                            "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED "
                                    + "has no stateRef. Payload="
                                    + payload
                    );
                }

                int expectedWorkers =
                        intField(
                                payload,
                                "expectedWorkers",
                                -1
                        );

                if (expectedWorkers
                        != EXPECTED_WORKERS) {

                    throw new IllegalStateException(
                            "Phase-2 expectedWorkers mismatch."
                                    + " configured="
                                    + EXPECTED_WORKERS
                                    + ", payload="
                                    + expectedWorkers
                                    + ". Payload="
                                    + payload
                    );
                }

                int installedWorkerCount =
                        intField(
                                payload,
                                "installedWorkerCount",
                                -1
                        );

                if (installedWorkerCount
                        != EXPECTED_WORKERS) {

                    throw new IllegalStateException(
                            "Phase-2 installation barrier completed "
                                    + "with unexpected worker count."
                                    + " expected="
                                    + EXPECTED_WORKERS
                                    + ", installed="
                                    + installedWorkerCount
                                    + ". Payload="
                                    + payload
                    );
                }

                System.out.println(
                        "Observed GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED:"
                                + " resultId="
                                + resultId
                                + ", stateRef="
                                + stateRef
                                + ", rootTuplesSeen="
                                + longField(
                                payload,
                                "rootTuplesSeen",
                                -1L
                        )
                                + ", positiveRootCandidatesSeen="
                                + longField(
                                payload,
                                "positiveRootCandidatesSeen",
                                -1L
                        )
                                + ", totalRootGroupWeight="
                                + doubleField(
                                payload,
                                "totalRootGroupWeight",
                                0.0d
                        )
                                + ", sampleInstanceCount="
                                + intField(
                                payload,
                                "sampleInstanceCount",
                                -1
                        )
                                + ", installedWorkerCount="
                                + installedWorkerCount
                                + ", expectedWorkers="
                                + expectedWorkers
                );

                /*
                 * Compatibility with the existing benchmark and validator.
                 *
                 * The rest of this test still uses the historical names:
                 *
                 *     readyPayload
                 *     installedPayload
                 *
                 * Request 86 now contains all information that used to be
                 * split between the old READY and INSTALLED events.
                 *
                 * Return copies of the same request-86 payload in both fields
                 * so benchmark and validation code can remain unchanged for
                 * this migration.
                 */
                JsonNode completion =
                        payload.deepCopy();

                return new PhaseTwoCompletion(
                        completion,
                        completion.deepCopy()
                );
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for "
                        + "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED."
                        + " uid="
                        + uid
                        + ", resultId="
                        + expectedResultId
                        + ", recordsSeen="
                        + recordsSeen
        );
    }


    /**
     * Estimation.toKafkaJson() wraps the actual OnePass payload in the
     * Estimation.estimation field. That field is normally a JSON string.
     * <p>
     * This helper also accepts a raw payload object so the observer remains
     * tolerant if the output serializer changes representation later.
     */
    private static JsonNode unwrapEstimationPayload(JsonNode envelope) {

        if (envelope == null || envelope.isNull()) {

            return null;
        }

        JsonNode estimation = envelope.get("estimation");

        if (estimation == null || estimation.isNull()) {

            /*
             * Raw-payload fallback.
             */
            if (envelope.has("type")) {

                return envelope;
            }

            return null;
        }

        if (estimation.isObject() || estimation.isArray()) {

            return estimation;
        }

        if (estimation.isTextual()) {

            String value = estimation.asText();

            if (value == null || value.trim().isEmpty()) {

                return null;
            }

            try {

                return MAPPER.readTree(value);

            } catch (Exception ignored) {

                return null;
            }
        }

        return null;
    }


    private static final class PhaseTwoCompletion {

        private final JsonNode readyPayload;
        private final JsonNode installedPayload;


        private PhaseTwoCompletion(JsonNode readyPayload, JsonNode installedPayload) {

            this.readyPayload = readyPayload;

            this.installedPayload = installedPayload;
        }
    }


    // =====================================================================
    // SHARDED PHASE-3 REQUEST/OUTPUT OBSERVERS
    // =====================================================================

    /**
     * Waits for one stateless request-7 START_PHASE_3_ALIAS transition.
     *
     * request 86 produces aliasIndex 0. Every non-final request 91 produces
     * the next alias. Observing this request is therefore an end-to-end proof
     * that the previous alias reached the all-worker installation barrier.
     */
    private static JsonNode waitForShardedPhaseThreeStartTransition(
            KafkaConsumer<String, String> consumer,
            int uid,
            String expectedBaseKey,
            String expectedAlias,
            int expectedAliasIndex,
            String expectedResultId,
            int expectedAliasCount,
            long timeoutMs) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + timeoutMs;

        int recordsSeen =
                0;

        while (System.currentTimeMillis()
                < deadline) {

            ConsumerRecords<String, String> records =
                    consumer.poll(
                            1000L
                    );

            for (ConsumerRecord<String, String> record
                    : records) {

                recordsSeen++;

                String value =
                        record.value();

                if (value == null
                        || value.trim().isEmpty()) {

                    continue;
                }

                JsonNode request;

                try {

                    request =
                            MAPPER.readTree(
                                    value
                            );

                } catch (Exception ignored) {

                    continue;
                }

                if (intField(
                        request,
                        "uid",
                        -1
                ) != uid) {

                    continue;
                }

                if (intField(
                        request,
                        "synopsisID",
                        -1
                ) != SYNOPSIS_ID) {

                    continue;
                }

                if (intField(
                        request,
                        "requestID",
                        -1
                ) != REQUEST_UPDATE) {

                    continue;
                }

                JsonNode payload =
                        request.get(
                                "parameters"
                        );

                if (payload == null
                        || !payload.isObject()) {

                    continue;
                }

                if (!"SHARDED_PHASE3_V1".equals(
                        textField(
                                payload,
                                "protocol",
                                ""
                        )
                )) {

                    continue;
                }

                if (!"START_PHASE_3_ALIAS".equals(
                        textField(
                                payload,
                                "type",
                                ""
                        )
                )) {

                    continue;
                }

                String alias =
                        textField(
                                payload,
                                "phaseThreeAlias",
                                textField(
                                        payload,
                                        "alias",
                                        ""
                                )
                        );

                int aliasIndex =
                        intField(
                                payload,
                                "aliasIndex",
                                -1
                        );

                /*
                 * Skip an older Phase-3 transition if the observer happens to
                 * encounter one while waiting for a later alias.
                 */
                if (!expectedAlias.equals(alias)
                        || aliasIndex != expectedAliasIndex) {

                    continue;
                }

                if (!"START_PHASE_3_ALIAS".equals(
                        textField(
                                payload,
                                "onePassCommand",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Phase-3 transition has invalid onePassCommand. Payload="
                                    + payload
                    );
                }

                if (!"PHASE3".equalsIgnoreCase(
                        textField(
                                payload,
                                "phase",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Phase-3 transition has invalid phase. Payload="
                                    + payload
                    );
                }

                if (!expectedResultId.equals(
                        textField(
                                payload,
                                "resultId",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Phase-3 transition resultId mismatch."
                                    + " expected="
                                    + expectedResultId
                                    + ", actual="
                                    + textField(
                                    payload,
                                    "resultId",
                                    ""
                            )
                                    + ". Payload="
                                    + payload
                    );
                }

                if (!expectedBaseKey.equals(
                        textField(
                                payload,
                                "baseKey",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Phase-3 transition baseKey mismatch."
                                    + " expected="
                                    + expectedBaseKey
                                    + ", actual="
                                    + textField(
                                    payload,
                                    "baseKey",
                                    ""
                            )
                                    + ". Payload="
                                    + payload
                    );
                }

                int expectedWorkers =
                        intField(
                                payload,
                                "expectedWorkers",
                                -1
                        );

                if (expectedWorkers
                        != EXPECTED_WORKERS) {

                    throw new IllegalStateException(
                            "Phase-3 transition expectedWorkers mismatch."
                                    + " configured="
                                    + EXPECTED_WORKERS
                                    + ", payload="
                                    + expectedWorkers
                                    + ". Payload="
                                    + payload
                    );
                }

                int aliasCount =
                        intField(
                                payload,
                                "phaseThreeAliasCount",
                                -1
                        );

                if (aliasCount
                        != expectedAliasCount) {

                    throw new IllegalStateException(
                            "Phase-3 transition alias-count mismatch."
                                    + " expected="
                                    + expectedAliasCount
                                    + ", actual="
                                    + aliasCount
                                    + ". Payload="
                                    + payload
                    );
                }

                System.out.println(
                        "Observed START_PHASE_3_ALIAS:"
                                + " alias="
                                + alias
                                + ", aliasIndex="
                                + aliasIndex
                                + ", resultId="
                                + expectedResultId
                                + ", triggerStateRef="
                                + textField(
                                payload,
                                "triggerStateRef",
                                ""
                        )
                );

                return payload.deepCopy();
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for START_PHASE_3_ALIAS."
                        + " uid="
                        + uid
                        + ", alias="
                        + expectedAlias
                        + ", aliasIndex="
                        + expectedAliasIndex
                        + ", resultId="
                        + expectedResultId
                        + ", recordsSeen="
                        + recordsSeen
        );
    }


    /**
     * Waits for the final request-91 Phase-3 installation barrier exposed on
     * OUTPUT_TOPIC by RunOnepass.
     *
     * Non-final request-91 records remain internal and are observed indirectly
     * through the next START_PHASE_3_ALIAS transition above. The final record
     * is external only when phaseThreeComplete=true.
     */
    private static JsonNode waitForShardedPhaseThreeCompletion(
            KafkaConsumer<String, String> consumer,
            int uid,
            String expectedBaseKey,
            String expectedAlias,
            int expectedAliasIndex,
            String expectedResultId,
            int expectedAliasCount,
            int expectedSampleSize,
            long timeoutMs) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + timeoutMs;

        int recordsSeen =
                0;

        while (System.currentTimeMillis()
                < deadline) {

            ConsumerRecords<String, String> records =
                    consumer.poll(
                            1000L
                    );

            for (ConsumerRecord<String, String> record
                    : records) {

                recordsSeen++;

                JsonNode envelope;

                try {

                    envelope =
                            MAPPER.readTree(
                                    record.value()
                            );

                } catch (Exception ignored) {

                    continue;
                }

                JsonNode payload =
                        unwrapEstimationPayload(
                                envelope
                        );

                if (payload == null
                        || payload.isNull()
                        || !payload.isObject()) {

                    continue;
                }

                if (intField(
                        payload,
                        "uid",
                        -1
                ) != uid) {

                    continue;
                }

                if (!"GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED".equals(
                        textField(
                                payload,
                                "type",
                                ""
                        )
                )) {

                    continue;
                }

                if (!expectedResultId.equals(
                        textField(
                                payload,
                                "resultId",
                                ""
                        )
                )) {

                    continue;
                }

                if (!"SHARDED_PHASE3_V1".equals(
                        textField(
                                payload,
                                "protocol",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Final Phase-3 completion has unexpected protocol. Payload="
                                    + payload
                    );
                }

                if (!"PHASE3".equalsIgnoreCase(
                        textField(
                                payload,
                                "phase",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Final Phase-3 completion has unexpected phase. Payload="
                                    + payload
                    );
                }

                String alias =
                        textField(
                                payload,
                                "phaseThreeAlias",
                                textField(
                                        payload,
                                        "alias",
                                        ""
                                )
                        );

                if (!expectedAlias.equals(
                        alias
                )) {

                    throw new IllegalStateException(
                            "Final Phase-3 alias mismatch."
                                    + " expected="
                                    + expectedAlias
                                    + ", actual="
                                    + alias
                                    + ". Payload="
                                    + payload
                    );
                }

                if (!expectedBaseKey.equals(
                        textField(
                                payload,
                                "baseKey",
                                ""
                        )
                )) {

                    throw new IllegalStateException(
                            "Final Phase-3 baseKey mismatch."
                                    + " expected="
                                    + expectedBaseKey
                                    + ", actual="
                                    + textField(
                                    payload,
                                    "baseKey",
                                    ""
                            )
                                    + ". Payload="
                                    + payload
                    );
                }

                int aliasIndex =
                        intField(
                                payload,
                                "aliasIndex",
                                -1
                        );

                if (aliasIndex
                        != expectedAliasIndex) {

                    throw new IllegalStateException(
                            "Final Phase-3 aliasIndex mismatch."
                                    + " expected="
                                    + expectedAliasIndex
                                    + ", actual="
                                    + aliasIndex
                                    + ". Payload="
                                    + payload
                    );
                }

                int aliasCount =
                        intField(
                                payload,
                                "phaseThreeAliasCount",
                                -1
                        );

                if (aliasCount
                        != expectedAliasCount) {

                    throw new IllegalStateException(
                            "Final Phase-3 alias-count mismatch."
                                    + " expected="
                                    + expectedAliasCount
                                    + ", actual="
                                    + aliasCount
                                    + ". Payload="
                                    + payload
                    );
                }

                int sampleSize =
                        intField(
                                payload,
                                "sampleSize",
                                -1
                        );

                int selectionCount =
                        intField(
                                payload,
                                "selectionCount",
                                -1
                        );

                if (sampleSize != expectedSampleSize
                        || selectionCount != expectedSampleSize) {

                    throw new IllegalStateException(
                            "Final Phase-3 selection cardinality mismatch."
                                    + " expectedSampleSize="
                                    + expectedSampleSize
                                    + ", sampleSize="
                                    + sampleSize
                                    + ", selectionCount="
                                    + selectionCount
                                    + ". Payload="
                                    + payload
                    );
                }

                if (!booleanField(
                        payload,
                        "isLastAlias",
                        false
                )) {

                    throw new IllegalStateException(
                            "Final Phase-3 completion is not marked isLastAlias=true. Payload="
                                    + payload
                    );
                }

                if (!booleanField(
                        payload,
                        "phaseThreeComplete",
                        false
                )) {

                    throw new IllegalStateException(
                            "Final Phase-3 completion is not marked phaseThreeComplete=true. Payload="
                                    + payload
                    );
                }

                String nextAlias =
                        textField(
                                payload,
                                "nextAlias",
                                ""
                        );

                if (!nextAlias.isEmpty()) {

                    throw new IllegalStateException(
                            "Final Phase-3 completion unexpectedly declares nextAlias="
                                    + nextAlias
                                    + ". Payload="
                                    + payload
                    );
                }

                int expectedWorkers =
                        intField(
                                payload,
                                "expectedWorkers",
                                -1
                        );

                int installedWorkerCount =
                        intField(
                                payload,
                                "installedWorkerCount",
                                -1
                        );

                if (expectedWorkers != EXPECTED_WORKERS
                        || installedWorkerCount != EXPECTED_WORKERS) {

                    throw new IllegalStateException(
                            "Final Phase-3 installation barrier worker-count mismatch."
                                    + " configured="
                                    + EXPECTED_WORKERS
                                    + ", expectedWorkers="
                                    + expectedWorkers
                                    + ", installedWorkerCount="
                                    + installedWorkerCount
                                    + ". Payload="
                                    + payload
                    );
                }

                String stateRef =
                        textField(
                                payload,
                                "stateRef",
                                ""
                        );

                if (stateRef.isEmpty()) {

                    throw new IllegalStateException(
                            "Final Phase-3 completion has no stateRef. Payload="
                                    + payload
                    );
                }

                JsonNode receivedWorkers =
                        payload.get(
                                "receivedWorkers"
                        );

                if (receivedWorkers != null
                        && !receivedWorkers.isNull()) {

                    if (!receivedWorkers.isArray()
                            || receivedWorkers.size() != EXPECTED_WORKERS) {

                        throw new IllegalStateException(
                                "Final Phase-3 receivedWorkers metadata is incomplete. Payload="
                                        + payload
                        );
                    }
                }

                System.out.println(
                        "Observed GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED:"
                                + " alias="
                                + alias
                                + ", aliasIndex="
                                + aliasIndex
                                + ", resultId="
                                + expectedResultId
                                + ", stateRef="
                                + stateRef
                                + ", selectionCount="
                                + selectionCount
                                + ", installedWorkerCount="
                                + installedWorkerCount
                                + ", phaseThreeComplete=true"
                );

                return payload.deepCopy();
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for final GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED."
                        + " uid="
                        + uid
                        + ", alias="
                        + expectedAlias
                        + ", aliasIndex="
                        + expectedAliasIndex
                        + ", resultId="
                        + expectedResultId
                        + ", recordsSeen="
                        + recordsSeen
        );
    }


    // =====================================================================
    // DEBUG PHASE-1 INDEX EXPORT
    // =====================================================================

    /**
     * DEBUG / VALIDATION ONLY.
     * <p>
     * This runs strictly after phase1_algorithm_total has stopped. It asks
     * every worker to snapshot its already-computed local Phase-1 index shard,
     * waits for worker-N.json files, and performs a dumb set-union into the
     * exact JSON format expected by validate_onepass_catalog_phase1.py.
     * <p>
     * The merger intentionally does NOT know or use OnePass ownership, Kafka
     * state transfer, weight computation, or reducer logic. A duplicate
     * (edgeId, joinKey) across workers is treated as a validation failure.
     */
    private static void exportPhaseOneIndexesForValidator(KafkaProducer<String, String> controlProducer, int uid, String baseKey, String streamId) throws Exception {

        System.out.println();
        System.out.println("=======================================================");
        System.out.println(" DEBUG: EXPORTING FINAL PHASE-1 INDEX SHARDS");
        System.out.println("=======================================================");

        File runDirectory = new File(PHASE1_INDEX_EXPORT_DIR, "uid-" + uid);

        if (runDirectory.exists()) {
            deleteRecursively(runDirectory);
        }

        if (!runDirectory.mkdirs() && !runDirectory.isDirectory()) {
            throw new IllegalStateException("Could not create debug shard directory: " + runDirectory.getAbsolutePath());
        }

        ObjectNode debugRequest = buildPhaseOneDebugExportRequest(baseKey, streamId, uid, EXPECTED_WORKERS, PHASE1_INDEX_EXPORT_DIR);

        sendJson(controlProducer, REQUEST_TOPIC, baseKey, debugRequest);

        controlProducer.flush();

        System.out.println("DEBUG_EXPORT_PHASE1_INDEXES request sent. Waiting for " + EXPECTED_WORKERS + " worker shard files in " + runDirectory.getAbsolutePath());

        waitForPhaseOneWorkerShardFiles(runDirectory, EXPECTED_WORKERS, PHASE1_INDEX_EXPORT_TIMEOUT_MS);

        File validatorOutput = new File(PHASE1_VALIDATOR_JSON_PATH);

        mergePhaseOneWorkerShardsForValidator(runDirectory, EXPECTED_WORKERS, uid, validatorOutput);

        System.out.println();
        System.out.println("Phase-1 validator JSON written to: " + validatorOutput.getAbsolutePath());
        System.out.println("Use this file as --sde-json with " + "validate_onepass_catalog_phase1.py");
        System.out.println();
    }

    private static ObjectNode buildPhaseOneDebugExportRequest(String datasetKey, String streamId, int uid, int noOfP, String outputDirectory) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_DEBUG_EXPORT_PHASE1);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();

        param.add("DEBUG_EXPORT_PHASE1_INDEXES");
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();

        parameters.put("onePassCommand", "DEBUG_EXPORT_PHASE1_INDEXES");

        parameters.put("debugOutputDirectory", outputDirectory);

        request.set("parameters", parameters);

        return request;
    }

    private static void waitForPhaseOneWorkerShardFiles(File runDirectory, int expectedWorkers, long timeoutMs) throws Exception {

        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {

            boolean allPresent = true;

            for (int workerId = 0; workerId < expectedWorkers; workerId++) {

                File file = new File(runDirectory, "worker-" + workerId + ".json");

                if (!file.isFile() || file.length() <= 0L) {
                    allPresent = false;
                    break;
                }
            }

            if (allPresent) {
                /*
                 * Give the final writer a brief moment to close/flush the
                 * file before the independent merger opens it.
                 */
                Thread.sleep(250L);
                return;
            }

            Thread.sleep(100L);
        }

        StringBuilder missing = new StringBuilder();

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {

            File file = new File(runDirectory, "worker-" + workerId + ".json");

            if (!file.isFile() || file.length() <= 0L) {
                if (missing.length() > 0) {
                    missing.append(", ");
                }
                missing.append(file.getName());
            }
        }

        throw new IllegalStateException("Timed out waiting for Phase-1 debug worker shards. " + "directory=" + runDirectory.getAbsolutePath() + ", missing=[" + missing + "]");
    }

    /**
     * Independent debug merger.
     * <p>
     * Expected worker shard format:
     * <p>
     * {
     * "uid": ...,
     * "workerId": ...,
     * "expectedWorkers": ...,
     * "edgeIndexes": {
     * "edgeId": { "joinKey": weight }
     * }
     * }
     * <p>
     * Final validator format:
     * <p>
     * {
     * "edgeIndexes": { ... }
     * }
     */
    private static void mergePhaseOneWorkerShardsForValidator(File runDirectory, int expectedWorkers, int expectedUid, File outputFile) throws Exception {

        Map<String, Map<String, Double>> merged = new LinkedHashMap<String, Map<String, Double>>();

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {

            File shardFile = new File(runDirectory, "worker-" + workerId + ".json");

            JsonNode root = MAPPER.readTree(shardFile);

            int fileUid = intField(root, "uid", -1);

            if (fileUid != expectedUid) {
                throw new IllegalStateException("Debug shard UID mismatch in " + shardFile.getAbsolutePath() + ": expected=" + expectedUid + ", actual=" + fileUid);
            }

            int fileWorkerId = intField(root, "workerId", -1);

            if (fileWorkerId != workerId) {
                throw new IllegalStateException("Debug shard workerId mismatch in " + shardFile.getAbsolutePath() + ": expected=" + workerId + ", actual=" + fileWorkerId);
            }

            int fileExpectedWorkers = intField(root, "expectedWorkers", -1);

            if (fileExpectedWorkers != expectedWorkers) {
                throw new IllegalStateException("Debug shard expectedWorkers mismatch in " + shardFile.getAbsolutePath() + ": expected=" + expectedWorkers + ", actual=" + fileExpectedWorkers);
            }

            JsonNode edgeIndexes = root.get("edgeIndexes");

            if (edgeIndexes == null || !edgeIndexes.isObject()) {

                throw new IllegalStateException("Debug shard has no edgeIndexes object: " + shardFile.getAbsolutePath());
            }

            Iterator<Map.Entry<String, JsonNode>> edges = edgeIndexes.fields();

            while (edges.hasNext()) {

                Map.Entry<String, JsonNode> edgeEntry = edges.next();

                String edgeId = edgeEntry.getKey();

                JsonNode entriesNode = edgeEntry.getValue();

                if (entriesNode == null || !entriesNode.isObject()) {

                    throw new IllegalStateException("Debug shard edgeIndexes['" + edgeId + "'] is not an object in " + shardFile.getAbsolutePath());
                }

                Map<String, Double> mergedEdge = merged.get(edgeId);

                if (mergedEdge == null) {
                    mergedEdge = new LinkedHashMap<String, Double>();

                    merged.put(edgeId, mergedEdge);
                }

                Iterator<Map.Entry<String, JsonNode>> entries = entriesNode.fields();

                while (entries.hasNext()) {

                    Map.Entry<String, JsonNode> entry = entries.next();

                    String joinKey = entry.getKey();

                    double weight = entry.getValue().asDouble();

                    /*
                     * Strict sharding invariant:
                     * one final (edgeId, joinKey) may exist on exactly one
                     * worker. Do NOT sum duplicates here; duplicates mean the
                     * sharding itself is wrong.
                     */
                    if (mergedEdge.containsKey(joinKey)) {
                        throw new IllegalStateException("Duplicate Phase-1 sharded key found across workers. " + "edgeId=" + edgeId + ", joinKey=" + joinKey + ", secondWorker=" + workerId + ", firstWeight=" + mergedEdge.get(joinKey) + ", secondWeight=" + weight);
                    }

                    mergedEdge.put(joinKey, weight);
                }
            }
        }

        ObjectNode validatorJson = MAPPER.createObjectNode();

        validatorJson.set("edgeIndexes", MAPPER.valueToTree(merged));

        File parent = outputFile.getParentFile();

        if (parent != null && !parent.exists() && !parent.mkdirs()) {

            throw new IllegalStateException("Could not create validator output directory: " + parent.getAbsolutePath());
        }

        MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputFile, validatorJson);
    }

    private static void deleteRecursively(File file) {

        if (file == null || !file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] children = file.listFiles();

            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }

        if (!file.delete()) {
            throw new IllegalStateException("Could not delete stale debug path: " + file.getAbsolutePath());
        }
    }


    // =====================================================================
    // DEBUG PHASE-2 INSTALLED-ROOT VALIDATION
    // =====================================================================

    /**
     * DEBUG / VALIDATION ONLY.
     * <p>
     * This method is called only when VALIDATE_PHASE2=true and only
     * after GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED has been observed and
     * phase2_algorithm_total has stopped.
     * <p>
     * The normal OnePass Phase-2 path is therefore not modified or slowed by
     * this validation.
     */
    private static PhaseTwoValidationResult validateInstalledPhaseTwoRootSamples(KafkaProducer<String, String> controlProducer, int uid, String baseKey, String streamId, CompiledOnePassPlan plan, JsonNode ready, JsonNode installed) throws Exception {

        System.out.println();
        System.out.println("=======================================================");
        System.out.println(" DEBUG: VALIDATING INSTALLED PHASE-2 ROOT SAMPLE");
        System.out.println(" This work is OUTSIDE phase2_algorithm_total.");
        System.out.println("=======================================================");

        File runDirectory = new File(PHASE2_VALIDATION_DIR, "uid-" + uid);

        /*
         * The UID is random per run, so collisions are unlikely, but deleting
         * any stale directory makes the validation deterministic and prevents
         * accidentally accepting files from a previous failed run.
         */
        if (runDirectory.exists()) {
            deleteRecursively(runDirectory);
        }

        ObjectNode request = buildPhaseTwoDebugValidationRequest(baseKey, streamId, uid, EXPECTED_WORKERS, PHASE2_VALIDATION_DIR);

        sendJson(controlProducer, REQUEST_TOPIC, baseKey, request);

        controlProducer.flush();

        System.out.println("DEBUG_VALIDATE_PHASE2_ROOT_SAMPLE request sent. Waiting for " + EXPECTED_WORKERS + " worker validation files in " + runDirectory.getAbsolutePath());

        Map<Integer, JsonNode> workerArtifacts = waitForPhaseTwoValidationFiles(runDirectory, uid, EXPECTED_WORKERS, PHASE2_VALIDATION_TIMEOUT_MS);

        String referenceChecksum = null;

        String referenceChecksumVersion = null;

        Map<Integer, String> workerChecksums = new LinkedHashMap<Integer, String>();

        long globalRootTuplesSeen = longField(ready, "rootTuplesSeen", -1L);

        long globalPositiveRootCandidatesSeen = longField(ready, "positiveRootCandidatesSeen", -1L);

        double globalTotalRootGroupWeight = doubleField(ready, "totalRootGroupWeight", Double.NaN);

        int globalSampleInstanceCount = intField(ready, "sampleInstanceCount", -1);

        int globalSampleSize = intField(ready, "sampleSize", -1);

        for (int workerId = 0; workerId < EXPECTED_WORKERS; workerId++) {

            JsonNode worker = workerArtifacts.get(workerId);

            if (worker == null) {

                throw new IllegalStateException("Missing Phase-2 validation artifact for worker=" + workerId);
            }

            int artifactUid = intField(worker, "uid", -1);

            if (artifactUid != uid) {

                throw new IllegalStateException("Phase-2 validation UID mismatch." + " worker=" + workerId + ", expected=" + uid + ", actual=" + artifactUid + ", artifact=" + worker);
            }

            int artifactWorkerId = intField(worker, "workerId", -1);

            if (artifactWorkerId != workerId) {

                throw new IllegalStateException("Phase-2 validation workerId mismatch." + " expected=" + workerId + ", actual=" + artifactWorkerId + ", artifact=" + worker);
            }

            int artifactExpectedWorkers = intField(worker, "expectedWorkers", -1);

            if (artifactExpectedWorkers != EXPECTED_WORKERS) {

                throw new IllegalStateException("Phase-2 validation expectedWorkers mismatch." + " worker=" + workerId + ", expected=" + EXPECTED_WORKERS + ", actual=" + artifactExpectedWorkers);
            }

            String rootAlias = textField(worker, "rootAlias", "");

            if (!plan.getRootAlias().equals(rootAlias)) {

                throw new IllegalStateException("Phase-2 validation rootAlias mismatch." + " worker=" + workerId + ", expected=" + plan.getRootAlias() + ", actual=" + rootAlias);
            }

            int requestedSampleSize = intField(worker, "requestedSampleSize", -1);

            if (requestedSampleSize != globalSampleSize || requestedSampleSize != plan.getSampleSize()) {

                throw new IllegalStateException("Phase-2 validation requestedSampleSize mismatch." + " worker=" + workerId + ", plan=" + plan.getSampleSize() + ", global=" + globalSampleSize + ", workerValue=" + requestedSampleSize);
            }

            int sampleInstanceCount = intField(worker, "sampleInstanceCount", -1);

            if (sampleInstanceCount != globalSampleInstanceCount) {

                throw new IllegalStateException("Phase-2 validation sampleInstanceCount mismatch." + " worker=" + workerId + ", expected=" + globalSampleInstanceCount + ", actual=" + sampleInstanceCount);
            }

            long rootTuplesSeen = longField(worker, "rootTuplesSeen", -1L);

            if (rootTuplesSeen != globalRootTuplesSeen) {

                throw new IllegalStateException("Phase-2 validation rootTuplesSeen mismatch." + " worker=" + workerId + ", expected=" + globalRootTuplesSeen + ", actual=" + rootTuplesSeen);
            }

            long positiveRootCandidatesSeen = longField(worker, "positiveRootCandidatesSeen", -1L);

            if (positiveRootCandidatesSeen != globalPositiveRootCandidatesSeen) {

                throw new IllegalStateException("Phase-2 validation positiveRootCandidatesSeen mismatch." + " worker=" + workerId + ", expected=" + globalPositiveRootCandidatesSeen + ", actual=" + positiveRootCandidatesSeen);
            }

            double totalRootGroupWeight = doubleField(worker, "totalRootGroupWeight", Double.NaN);

            /*
             * Worker validation files are written from the installed
             * OnePassRootSampleResult, which was reconstructed from the same
             * global state. Exact IEEE-754 equality is therefore expected here.
             */
            if (Double.doubleToLongBits(totalRootGroupWeight) != Double.doubleToLongBits(globalTotalRootGroupWeight)) {

                throw new IllegalStateException("Phase-2 validation totalRootGroupWeight mismatch." + " worker=" + workerId + ", expected=" + globalTotalRootGroupWeight + ", actual=" + totalRootGroupWeight);
            }

            String checksumVersion = textField(worker, "checksumVersion", "");

            String checksum = textField(worker, "checksum", "");

            if (!PHASE2_CHECKSUM_VERSION.equals(checksumVersion)) {

                throw new IllegalStateException("Unexpected Phase-2 checksum version." + " worker=" + workerId + ", expected=" + PHASE2_CHECKSUM_VERSION + ", actual=" + checksumVersion);
            }

            if (checksum == null || checksum.trim().isEmpty()) {

                throw new IllegalStateException("Worker " + workerId + " exported an empty Phase-2 checksum.");
            }

            if (referenceChecksum == null) {

                referenceChecksum = checksum;

                referenceChecksumVersion = checksumVersion;

            } else {

                if (!referenceChecksum.equals(checksum)) {

                    throw new IllegalStateException("Installed Phase-2 root sample differs across workers." + " referenceChecksum=" + referenceChecksum + ", worker=" + workerId + ", workerChecksum=" + checksum);
                }

                if (!referenceChecksumVersion.equals(checksumVersion)) {

                    throw new IllegalStateException("Phase-2 checksum version differs across workers." + " reference=" + referenceChecksumVersion + ", worker=" + workerId + ", workerVersion=" + checksumVersion);
                }
            }

            workerChecksums.put(workerId, checksum);
        }

        if (referenceChecksum == null || referenceChecksum.trim().isEmpty()) {

            throw new IllegalStateException("Phase-2 validation completed without a reference checksum.");
        }

        writePhaseTwoValidationJson(plan, ready, installed, referenceChecksumVersion, referenceChecksum, workerArtifacts);

        return new PhaseTwoValidationResult(referenceChecksumVersion, referenceChecksum, workerChecksums);
    }


    private static ObjectNode buildPhaseTwoDebugValidationRequest(String datasetKey, String streamId, int uid, int noOfP, String outputDirectory) {

        /*
         * Keep the same request JSON shape used by the existing Phase-1 debug
         * request so it follows the normal SDE request-routing conventions.
         */
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);

        request.put("key", datasetKey);

        request.put("requestID", REQUEST_DEBUG_VALIDATE_PHASE2);

        request.put("synopsisID", SYNOPSIS_ID);

        request.put("uid", uid);

        request.put("streamID", streamId);

        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();

        param.add("DEBUG_VALIDATE_PHASE2_ROOT_SAMPLE");

        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();

        parameters.put("onePassCommand", "DEBUG_VALIDATE_PHASE2_ROOT_SAMPLE");

        parameters.put("outputDirectory", outputDirectory);

        request.set("parameters", parameters);

        return request;
    }


    private static Map<Integer, JsonNode> waitForPhaseTwoValidationFiles(File runDirectory, int expectedUid, int expectedWorkers, long timeoutMs) throws Exception {

        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {

            Map<Integer, JsonNode> artifacts = new LinkedHashMap<Integer, JsonNode>();

            for (int workerId = 0; workerId < expectedWorkers; workerId++) {

                File file = new File(runDirectory, "worker-" + workerId + ".json");

                if (!file.isFile() || file.length() <= 0L) {

                    continue;
                }

                try {

                    JsonNode node = MAPPER.readTree(file);

                    int artifactUid = intField(node, "uid", -1);

                    if (artifactUid != expectedUid) {

                        continue;
                    }

                    artifacts.put(workerId, node);

                } catch (Exception ignored) {

                    /*
                     * The worker may still be completing the file write.
                     * Retry on the next polling iteration.
                     */
                }
            }

            if (artifacts.size() == expectedWorkers) {

                /*
                 * Give the last writer a tiny amount of time to close the
                 * underlying file before the caller performs all assertions.
                 */
                Thread.sleep(100L);

                return artifacts;
            }

            Thread.sleep(100L);
        }

        StringBuilder missing = new StringBuilder();

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {

            File file = new File(runDirectory, "worker-" + workerId + ".json");

            if (!file.isFile() || file.length() <= 0L) {

                if (missing.length() > 0) {

                    missing.append(", ");
                }

                missing.append(file.getName());
            }
        }

        throw new IllegalStateException("Timed out waiting for Phase-2 validation worker files." + " directory=" + runDirectory.getAbsolutePath() + ", missing=[" + missing + "]");
    }


    private static void writePhaseTwoValidationJson(CompiledOnePassPlan plan, JsonNode ready, JsonNode installed, String checksumVersion, String checksum, Map<Integer, JsonNode> workerArtifacts) throws Exception {

        ObjectNode output = MAPPER.createObjectNode();

        output.put("type", "ONEPASS_PHASE2_VALIDATION");

        output.put("queryName", plan.getQueryName());

        output.put("rootAlias", plan.getRootAlias());

        output.put("testRowLimit", TEST_ROW_LIMIT);

        output.put("workerCount", EXPECTED_WORKERS);

        output.put("installedRootSamplesIdentical", true);

        output.put("checksumVersion", checksumVersion);

        output.put("checksum", checksum);

        ObjectNode workers = output.putObject("workers");

        for (Map.Entry<Integer, JsonNode> entry : workerArtifacts.entrySet()) {

            workers.set(Integer.toString(entry.getKey()), entry.getValue().deepCopy());
        }

        output.set("ready", ready == null ? MAPPER.getNodeFactory().nullNode() : ready.deepCopy());

        output.set("installed", installed == null ? MAPPER.getNodeFactory().nullNode() : installed.deepCopy());

        File outputFile = new File(PHASE2_VALIDATION_JSON_PATH);

        File parent = outputFile.getParentFile();

        if (parent != null && !parent.exists() && !parent.mkdirs()) {

            throw new IllegalStateException("Could not create Phase-2 validator output directory: " + parent.getAbsolutePath());
        }

        MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputFile, output);

        System.out.println("Consolidated Phase-2 validation JSON written to: " + outputFile.getAbsolutePath());
    }


    private static final class PhaseTwoValidationResult {

        private final String checksumVersion;

        private final String checksum;

        private final Map<Integer, String> workerChecksums;


        private PhaseTwoValidationResult(String checksumVersion, String checksum, Map<Integer, String> workerChecksums) {

            this.checksumVersion = checksumVersion;

            this.checksum = checksum;

            this.workerChecksums = new LinkedHashMap<Integer, String>(workerChecksums);
        }
    }


    // =====================================================================
    // REQUEST BUILDERS
    // =====================================================================

    private static ObjectNode buildOnePassAddRequest(String datasetKey, String streamId, int uid, int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);

        request.put("key", datasetKey);

        request.put("requestID", REQUEST_ADD);

        request.put("synopsisID", SYNOPSIS_ID);

        request.put("uid", uid);

        request.put("streamID", streamId);

        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();

        param.add("ONEPASS_SQL");

        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();

        parameters.put("onePassSql", TEST_ONEPASS_SQL);

        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildOnePassRemoveRequest(String datasetKey, String streamId, int uid, int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);

        request.put("key", datasetKey);

        request.put("requestID", 2);

        request.put("synopsisID", SYNOPSIS_ID);

        request.put("uid", uid);

        request.put("streamID", streamId);

        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();

        param.add("REMOVE");

        request.set("param", param);

        return request;
    }

    private static ObjectNode buildEndAliasDatapoint(String datasetKey, String streamId, int uid, String alias, int epoch, String resultId, int expectedWorkers, String nextCommand, String nextAlias) {

        ObjectNode marker = MAPPER.createObjectNode();

        marker.put("type", "END_ALIAS");

        marker.put("synopsisID", SYNOPSIS_ID);

        marker.put("uid", uid);

        marker.put("phase", "PHASE1");

        marker.put("alias", alias);

        marker.put("epoch", epoch);

        marker.put("resultId", resultId);

        marker.put("expectedWorkers", expectedWorkers);

        marker.put("nextCommand", nextCommand);

        marker.put("nextAlias", nextAlias);

        ObjectNode datapoint = MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);

        datapoint.put("streamID", streamId);

        datapoint.set("values", marker);

        return datapoint;
    }


    private static ObjectNode buildPhaseTwoEndAliasDatapoint(String datasetKey, String streamId, int uid, String rootAlias, int epoch, String resultId, int expectedWorkers) {

        ObjectNode marker = MAPPER.createObjectNode();

        marker.put("type", "END_ALIAS");

        marker.put("synopsisID", SYNOPSIS_ID);

        marker.put("uid", uid);

        marker.put("phase", "PHASE2");

        marker.put("alias", rootAlias);

        marker.put("epoch", epoch);

        marker.put("resultId", resultId);

        marker.put("expectedWorkers", expectedWorkers);

        ObjectNode datapoint = MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);

        datapoint.put("streamID", streamId);

        datapoint.set("values", marker);

        return datapoint;
    }


    /**
     * Phase-3 END_ALIAS marker.
     *
     * The production handler requires phase, alias, uid, resultId and
     * expectedWorkers. The traversal metadata below is also included so the
     * Kafka trace is self-describing and can be inspected independently.
     */
    private static ObjectNode buildPhaseThreeEndAliasDatapoint(
            String datasetKey,
            String streamId,
            int uid,
            String alias,
            int epoch,
            int aliasIndex,
            int phaseThreeAliasCount,
            String resultId,
            int expectedWorkers,
            String nextAlias) {

        ObjectNode marker =
                MAPPER.createObjectNode();

        marker.put(
                "type",
                "END_ALIAS"
        );

        marker.put(
                "synopsisID",
                SYNOPSIS_ID
        );

        marker.put(
                "uid",
                uid
        );

        marker.put(
                "phase",
                "PHASE3"
        );

        marker.put(
                "protocol",
                "SHARDED_PHASE3_V1"
        );

        marker.put(
                "alias",
                alias
        );

        marker.put(
                "phaseThreeAlias",
                alias
        );

        marker.put(
                "epoch",
                epoch
        );

        marker.put(
                "aliasIndex",
                aliasIndex
        );

        marker.put(
                "phaseThreeAliasCount",
                phaseThreeAliasCount
        );

        marker.put(
                "isLastAlias",
                aliasIndex == phaseThreeAliasCount - 1
        );

        marker.put(
                "nextAlias",
                nextAlias == null
                        ? ""
                        : nextAlias
        );

        marker.put(
                "resultId",
                resultId
        );

        marker.put(
                "expectedWorkers",
                expectedWorkers
        );

        ObjectNode datapoint =
                MAPPER.createObjectNode();

        datapoint.put(
                "dataSetkey",
                datasetKey
        );

        datapoint.put(
                "streamID",
                streamId
        );

        datapoint.set(
                "values",
                marker
        );

        return datapoint;
    }


    // =====================================================================
    // TPC-H -> DATAPOINT PRELOAD
    // =====================================================================

    private static long streamAlias(KafkaProducer<String, String> producer, String topic, String datasetKey, String streamId, OnePassCatalog catalog, CompiledOnePassPlan plan, String alias, long maxRows, Map<String, Set<String>> requiredFieldsByAlias) throws Exception {

        File file = tableFileForAlias(catalog, plan, alias);

        List<String> columns = columnsForAlias(catalog, plan, alias);

        String separator = separatorForAlias(catalog, plan, alias);

        Set<String> requiredFields = requiredFieldsByAlias == null ? null : requiredFieldsByAlias.get(alias);

        if (ENABLE_REQUIRED_FIELD_PRUNING && requiredFields == null) {

            throw new IllegalStateException("Required-field pruning is enabled, " + "but plan has no required fields for alias: " + alias);
        }

        System.out.println("  file: " + file.getAbsolutePath());

        System.out.println("  required fields: " + requiredFields);

        long count = 0L;

        BufferedReader br = new BufferedReader(new FileReader(file));

        try {
            String line;

            while ((line = br.readLine()) != null) {

                if (maxRows >= 0L && count >= maxRows) {
                    break;
                }

                ObjectNode tuple = tupleJsonFromLine(alias, columns, separator, line, requiredFields);

                ObjectNode datapoint = wrapTupleAsDatapoint(datasetKey, streamId, tuple);

                sendJsonAsync(producer, topic, datasetKey, datapoint);

                count++;

                if (count % 50000L == 0L) {
                    System.out.println("    prepared " + count + " rows for alias " + alias);
                }
            }

        } finally {
            br.close();
        }

        return count;
    }

    private static File tableFileForAlias(OnePassCatalog catalog, CompiledOnePassPlan plan, String alias) {

        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        if (relation == null) {
            throw new IllegalStateException("Unknown alias in plan: " + alias);
        }

        OnePassCatalog.CatalogTable table = catalog.getDataset().getTables().get(relation.getTable());

        if (table == null) {
            throw new IllegalStateException("Catalog does not define table '" + relation.getTable() + "' for alias '" + alias + "'");
        }

        File file = new File(TEST_TPCH_DIR, table.getFile());

        if (!file.exists()) {
            throw new IllegalStateException("Missing TPC-H file for alias '" + alias + "': " + file.getAbsolutePath());
        }

        return file;
    }

    private static List<String> columnsForAlias(OnePassCatalog catalog, CompiledOnePassPlan plan, String alias) {

        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        OnePassCatalog.CatalogTable table = catalog.getDataset().getTables().get(relation.getTable());

        List<String> columns = table.getColumns();

        if (columns == null || columns.isEmpty()) {

            throw new IllegalStateException("Catalog table '" + relation.getTable() + "' has no columns");
        }

        return columns;
    }

    private static String separatorForAlias(OnePassCatalog catalog, CompiledOnePassPlan plan, String alias) {

        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        OnePassCatalog.CatalogTable table = catalog.getDataset().getTables().get(relation.getTable());

        String separator = table.getSeparator();

        if (separator == null || separator.length() == 0) {
            return "|";
        }

        return separator;
    }

    private static ObjectNode tupleJsonFromLine(String alias, List<String> columns, String separator, String line, Set<String> requiredFields) {

        String[] parts = line.split("\\Q" + separator + "\\E", -1);

        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", alias);

        int limit = Math.min(columns.size(), parts.length);

        for (int i = 0; i < limit; i++) {

            String fieldName = columns.get(i);

            if (ENABLE_REQUIRED_FIELD_PRUNING && requiredFields != null && !requiredFields.contains("*") && !requiredFields.contains(fieldName)) {

                continue;
            }

            putTypedValue(tuple, fieldName, parts[i]);
        }

        return tuple;
    }

    private static void putTypedValue(ObjectNode tuple, String fieldName, String rawValue) {

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

    private static ObjectNode wrapTupleAsDatapoint(String datasetKey, String streamId, ObjectNode tuple) {

        ObjectNode datapoint = MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);

        datapoint.put("streamID", streamId);

        datapoint.set("values", tuple.deepCopy());

        return datapoint;
    }

    // =====================================================================
    // KAFKA
    // =====================================================================

    private static Properties baseProducerProperties() {

        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        props.put("acks", "all");

        props.put("retries", "3");

        props.put("buffer.memory", "268435456");

        props.put("max.request.size", "104857600");

        props.put("delivery.timeout.ms", "900000");

        props.put("request.timeout.ms", "300000");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    private static KafkaProducer<String, String> createProducer() {

        Properties props = baseProducerProperties();

        props.put("batch.size", "16384");

        props.put("linger.ms", "1");

        return new KafkaProducer<String, String>(props);
    }

    private static KafkaConsumer<String, String> createObserverConsumer() {

        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        /*
         * This consumer is a passive observer with explicit partition
         * assignment. It does not participate in the SDE consumer group.
         */
        props.put("enable.auto.commit", "false");

        props.put("auto.offset.reset", "latest");

        props.put("request.timeout.ms", "300000");

        props.put("fetch.max.bytes", "104857600");

        props.put("max.partition.fetch.bytes", "104857600");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

    private static void initializeObserver(KafkaConsumer<String, String> consumer, String topic) {

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new IllegalStateException("Kafka topic has no discoverable partitions: " + topic);
        }

        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        for (PartitionInfo info : partitionInfos) {
            partitions.add(new TopicPartition(topic, info.partition()));
        }

        consumer.assign(partitions);
        Map<TopicPartition, Long> startingOffsets = consumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            Long offset = startingOffsets.get(partition);

            if (offset == null) {
                throw new IllegalStateException("Could not resolve end offset for Kafka observer." + " topic=" + topic + ", partition=" + partition);
            }

            consumer.seek(partition, offset);
        }

        /*
         * Force KafkaConsumer's local position to be materialized before returning.
         *
         * At this point no Phase-2 root data has been committed yet, so every
         * Phase-2 output produced after this method returns must be visible to the
         * observer.
         */
        for (TopicPartition partition : partitions) {
            long position = consumer.position(partition);
            System.out.println("Kafka observer start position:" + " topic=" + topic + ", partition=" + partition.partition() + ", offset=" + position);
        }

        System.out.println("Kafka observer READY on " + topic + ", partitions=" + partitions + ", startingOffsets=" + startingOffsets);
    }

    private static void sendJsonAsync(KafkaProducer<String, String> producer, String topic, String key, JsonNode json) {

        producer.send(new ProducerRecord<String, String>(topic, key, json.toString()));
    }

    private static void sendJson(KafkaProducer<String, String> producer, String topic, String key, JsonNode json) throws Exception {

        producer.send(new ProducerRecord<String, String>(topic, key, json.toString())).get();
    }

    // =====================================================================
    // PLAN VALIDATION
    // =====================================================================

    private static void validatePlanForShardedOnePassV1(CompiledOnePassPlan plan) {

        if (plan == null) {
            throw new IllegalArgumentException("Compiled plan must not be null");
        }

        if (plan.getLeafToRootOrder() == null || plan.getLeafToRootOrder().isEmpty()) {

            throw new IllegalStateException("Compiled plan has empty leafToRootOrder: " + plan);
        }

        if (plan.getRootToLeafOrder() == null || plan.getRootToLeafOrder().isEmpty()) {

            throw new IllegalStateException("Compiled plan has empty rootToLeafOrder: " + plan);
        }

        /*
         * Every alias processed during Phase 1 and replayed during Phase 3 is
         * non-root and must therefore have exactly one parent edge in the
         * rooted join tree.
         *
         * Multiple CHILD edges are supported by both sharded enrichment paths.
         */
        for (String alias : plan.getLeafToRootOrder()) {

            if (plan.getParentEdge(alias) == null) {

                throw new IllegalStateException(
                        "Non-root alias has no parent edge: "
                                + alias
                );
            }
        }

        List<String> phaseThreeOrder =
                phaseThreeAliasOrder(
                        plan
                );

        if (phaseThreeOrder.size() != plan.getLeafToRootOrder().size()) {

            throw new IllegalStateException(
                    "Phase-1/Phase-3 non-root alias-count mismatch."
                            + " leafToRoot="
                            + plan.getLeafToRootOrder()
                            + ", phaseThreeOrder="
                            + phaseThreeOrder
            );
        }

        for (String alias : plan.getLeafToRootOrder()) {

            if (!phaseThreeOrder.contains(alias)) {

                throw new IllegalStateException(
                        "Phase-3 replay order does not contain Phase-1 alias "
                                + alias
                                + ". phaseThreeOrder="
                                + phaseThreeOrder
                );
            }
        }

        if (RUN_PHASE_3 && EXPECTED_WORKERS <= 1) {

            throw new IllegalStateException(
                    "The production SHARDED_PHASE3_V1 path requires expectedWorkers > 1."
                            + " Configured workers="
                            + EXPECTED_WORKERS
            );
        }
    }

    // =====================================================================
    // BENCHMARK
    // =====================================================================

    private static long tic() {
        return System.nanoTime();
    }

    private static void recordDuration(String label, long startNanos) {

        long elapsed = System.nanoTime() - startNanos;

        benchmarkNanos.compute(label, (k, current) -> current == null ? elapsed : current + elapsed);
    }

    private static void recordCount(String label, long value) {

        Long current = benchmarkCounts.get(label);

        benchmarkCounts.put(label, current == null ? value : current.longValue() + value);
    }

    private static long nanosFor(String label) {

        Long value = benchmarkNanos.get(label);

        return value == null ? 0L : value.longValue();
    }

    private static double secondsFor(String label) {

        return nanosFor(label) / 1_000_000_000.0d;
    }

    private static long countFor(String label) {

        Long value = benchmarkCounts.get(label);

        return value == null ? 0L : value.longValue();
    }

    private static double rowsPerSecond(long rows, double seconds) {

        if (seconds <= 0.0d) {
            return 0.0d;
        }

        return rows / seconds;
    }

    private static void printPhaseOneBenchmarkSummary(CompiledOnePassPlan plan, long preloadNanos) {

        double preloadSeconds = preloadNanos / 1_000_000_000.0d;

        double algorithmSeconds = secondsFor("phase1_algorithm_total");

        long rows = countFor("phase1_rows_processed");

        System.out.println();
        System.out.println("=== Sharded OnePass* Phase 1 benchmark ===");

        System.out.printf("%-42s %12.3f s  [OUTSIDE TIMER]%n", "phase1_kafka_preload", preloadSeconds);

        System.out.printf("%-42s %12.3f s%n", "phase1_algorithm_total", algorithmSeconds);

        System.out.printf("%-42s %12d%n", "phase1_rows_processed", rows);

        System.out.printf("%-42s %12.3f rows/s%n", "phase1_algorithm_rows_per_sec", rowsPerSecond(rows, algorithmSeconds));

        System.out.println();
        System.out.println("Per-alias algorithm timings:");

        for (String alias : plan.getLeafToRootOrder()) {

            long aliasRows = countFor("phase1_alias_" + alias + "_rows_processed");

            double aliasAlgorithm = secondsFor("phase1_alias_" + alias + "_algorithm");

            System.out.println("  alias=" + alias + ", rows=" + aliasRows + ", algorithm_s=" + aliasAlgorithm + ", rows_per_s=" + rowsPerSecond(aliasRows, aliasAlgorithm));
        }

        System.out.println("===========================================");
        System.out.println();
    }

    private static void writePhaseOneBenchmarkCsv(CompiledOnePassPlan plan, long preloadNanos, String implementation) throws Exception {

        if (!WRITE_LEGACY_SEPARATE_BENCHMARK_CSV) {
            return;
        }

        File csvFile = new File(PHASE1_BENCHMARK_CSV_PATH);

        File parent = csvFile.getParentFile();

        if (parent != null && !parent.exists()) {

            /*
             * Do not fail simply because the local results directory has not
             * been created yet.
             */
            parent.mkdirs();
        }

        boolean writeHeader = !csvFile.exists() || csvFile.length() == 0L;

        double preloadSeconds = preloadNanos / 1_000_000_000.0d;

        double algorithmSeconds = secondsFor("phase1_algorithm_total");

        long rows = countFor("phase1_rows_processed");

        FileWriter writer = new FileWriter(csvFile, true);

        try {

            if (writeHeader) {
                writer.write("timestamp_ms," + "implementation," + "workers," + "query_name," + "seed," + "test_row_limit," + "sample_size_limit," + "root_alias," + "leaf_to_root_order," + "phase1_rows_processed," + "phase1_kafka_preload_s," + "phase1_algorithm_total_s," + "phase1_algorithm_rows_per_sec," + "phase1_alias_rows_processed," + "phase1_alias_algorithm_s" + System.lineSeparator());
            }

            writer.write(Long.toString(System.currentTimeMillis()) + "," + csv(implementation) + "," + EXPECTED_WORKERS + "," + csv(plan.getQueryName()) + "," + csv(plan.getDatasetSeed()) + "," + csv(formatRowLimit(TEST_ROW_LIMIT)) + "," + plan.getSampleSize() + "," + csv(plan.getRootAlias()) + "," + csv(String.valueOf(plan.getLeafToRootOrder())) + "," + rows + "," + preloadSeconds + "," + algorithmSeconds + "," + rowsPerSecond(rows, algorithmSeconds) + "," + csv(String.valueOf(phaseOneAliasRowsMap(plan))) + "," + csv(String.valueOf(phaseOneAliasAlgorithmSecondsMap(plan))) + System.lineSeparator());

        } finally {
            writer.close();
        }

        System.out.println("Phase-1 benchmark CSV appended to: " + csvFile.getAbsolutePath());
    }

    private static Map<String, Long> phaseOneAliasRowsMap(CompiledOnePassPlan plan) {

        Map<String, Long> out = new LinkedHashMap<String, Long>();

        for (String alias : plan.getLeafToRootOrder()) {

            out.put(alias, countFor("phase1_alias_" + alias + "_rows_processed"));
        }

        return out;
    }

    private static Map<String, Double> phaseOneAliasAlgorithmSecondsMap(CompiledOnePassPlan plan) {

        Map<String, Double> out = new LinkedHashMap<String, Double>();

        for (String alias : plan.getLeafToRootOrder()) {

            out.put(alias, secondsFor("phase1_alias_" + alias + "_algorithm"));
        }

        return out;
    }


    private static void printPhaseTwoBenchmarkSummary(CompiledOnePassPlan plan, long rootRows, long preloadNanos, JsonNode ready, JsonNode installed) {

        double preloadSeconds = preloadNanos / 1_000_000_000.0d;

        double algorithmSeconds = secondsFor("phase2_algorithm_total");

        long rootTuplesSeen = longField(ready, "rootTuplesSeen", -1L);

        long positiveRootCandidatesSeen = longField(ready, "positiveRootCandidatesSeen", -1L);

        double totalRootGroupWeight = doubleField(ready, "totalRootGroupWeight", 0.0d);

        int sampleInstanceCount = intField(ready, "sampleInstanceCount", -1);

        int installedWorkerCount = intField(installed, "installedWorkerCount", -1);

        System.out.println();
        System.out.println("=== Sharded OnePass* Phase 2 benchmark ===");

        System.out.printf("%-42s %12.3f s  [OUTSIDE TIMER]%n", "phase2_root_kafka_preload", preloadSeconds);

        System.out.printf("%-42s %12.3f s%n", "phase2_algorithm_total", algorithmSeconds);

        System.out.printf("%-42s %12d%n", "phase2_root_rows_processed", rootRows);

        System.out.printf("%-42s %12.3f rows/s%n", "phase2_algorithm_rows_per_sec", rowsPerSecond(rootRows, algorithmSeconds));

        System.out.printf("%-42s %12d%n", "phase2_root_tuples_seen", rootTuplesSeen);

        System.out.printf("%-42s %12d%n", "phase2_positive_root_candidates", positiveRootCandidatesSeen);

        System.out.printf("%-42s %12.6e%n", "phase2_total_root_group_weight", totalRootGroupWeight);

        System.out.printf("%-42s %12d%n", "phase2_sample_instance_count", sampleInstanceCount);

        System.out.printf("%-42s %12d%n", "phase2_installed_worker_count", installedWorkerCount);

        System.out.println("rootAlias=" + plan.getRootAlias() + ", rootChildEdges=" + plan.getChildEdges(plan.getRootAlias()).size() + ", stateRef=" + textField(ready, "stateRef", ""));

        System.out.println("===========================================");
        System.out.println();
    }


    private static void printPhaseThreeBenchmarkSummary(
            CompiledOnePassPlan plan,
            List<String> phaseThreeOrder,
            long preloadNanos,
            JsonNode finalCompletion) {

        double preloadSeconds =
                preloadNanos / 1_000_000_000.0d;

        double algorithmSeconds =
                secondsFor(
                        "phase3_algorithm_total"
                );

        long rows =
                countFor(
                        "phase3_rows_replayed"
                );

        int selectionCount =
                intField(
                        finalCompletion,
                        "selectionCount",
                        -1
                );

        int installedWorkerCount =
                intField(
                        finalCompletion,
                        "installedWorkerCount",
                        -1
                );

        System.out.println();
        System.out.println("=== Sharded OnePass* Phase 3 benchmark ===");

        System.out.printf(
                "%-42s %12.3f s  [OUTSIDE TIMER]%n",
                "phase3_kafka_preload",
                preloadSeconds
        );

        System.out.printf(
                "%-42s %12.3f s%n",
                "phase3_algorithm_total",
                algorithmSeconds
        );

        System.out.printf(
                "%-42s %12d%n",
                "phase3_rows_replayed",
                rows
        );

        System.out.printf(
                "%-42s %12.3f rows/s%n",
                "phase3_algorithm_rows_per_sec",
                rowsPerSecond(
                        rows,
                        algorithmSeconds
                )
        );

        System.out.printf(
                "%-42s %12d%n",
                "phase3_selection_count",
                selectionCount
        );

        System.out.printf(
                "%-42s %12d%n",
                "phase3_installed_worker_count",
                installedWorkerCount
        );

        System.out.println();
        System.out.println("Per-alias Phase-3 algorithm timings:");

        for (String alias : phaseThreeOrder) {

            long aliasRows =
                    countFor(
                            "phase3_alias_"
                                    + alias
                                    + "_rows_replayed"
                    );

            double aliasAlgorithm =
                    secondsFor(
                            "phase3_alias_"
                                    + alias
                                    + "_algorithm"
                    );

            System.out.println(
                    "  alias="
                            + alias
                            + ", rows="
                            + aliasRows
                            + ", algorithm_s="
                            + aliasAlgorithm
                            + ", rows_per_s="
                            + rowsPerSecond(
                            aliasRows,
                            aliasAlgorithm
                    )
            );
        }

        System.out.println(
                "finalAlias="
                        + textField(
                        finalCompletion,
                        "alias",
                        ""
                )
                        + ", stateRef="
                        + textField(
                        finalCompletion,
                        "stateRef",
                        ""
                )
                        + ", phaseThreeComplete="
                        + booleanField(
                        finalCompletion,
                        "phaseThreeComplete",
                        false
                )
        );

        System.out.println("===========================================");
        System.out.println();
    }


    private static Map<String, Long> phaseThreeAliasRowsMap(
            CompiledOnePassPlan plan) {

        Map<String, Long> out =
                new LinkedHashMap<String, Long>();

        for (String alias : phaseThreeAliasOrder(plan)) {

            out.put(
                    alias,
                    countFor(
                            "phase3_alias_"
                                    + alias
                                    + "_rows_replayed"
                    )
            );
        }

        return out;
    }


    private static Map<String, Double> phaseThreeAliasAlgorithmSecondsMap(
            CompiledOnePassPlan plan) {

        Map<String, Double> out =
                new LinkedHashMap<String, Double>();

        for (String alias : phaseThreeAliasOrder(plan)) {

            out.put(
                    alias,
                    secondsFor(
                            "phase3_alias_"
                                    + alias
                                    + "_algorithm"
                    )
            );
        }

        return out;
    }


    private static void writePhaseTwoBenchmarkCsv(CompiledOnePassPlan plan, long rootRows, long preloadNanos, JsonNode ready, JsonNode installed, String implementation) throws Exception {

        if (!WRITE_LEGACY_SEPARATE_BENCHMARK_CSV) {

            return;
        }

        File csvFile = new File(PHASE2_BENCHMARK_CSV_PATH);

        File parent = csvFile.getParentFile();

        if (parent != null && !parent.exists()) {

            parent.mkdirs();
        }

        boolean writeHeader = !csvFile.exists() || csvFile.length() == 0L;

        double preloadSeconds = preloadNanos / 1_000_000_000.0d;

        double algorithmSeconds = secondsFor("phase2_algorithm_total");

        long rootTuplesSeen = longField(ready, "rootTuplesSeen", -1L);

        long positiveRootCandidatesSeen = longField(ready, "positiveRootCandidatesSeen", -1L);

        double totalRootGroupWeight = doubleField(ready, "totalRootGroupWeight", 0.0d);

        int sampleInstanceCount = intField(ready, "sampleInstanceCount", -1);

        int installedWorkerCount = intField(installed, "installedWorkerCount", -1);

        String stateRef = textField(ready, "stateRef", "");

        FileWriter writer = new FileWriter(csvFile, true);

        try {

            if (writeHeader) {

                writer.write("timestamp_ms," + "implementation," + "workers," + "query_name," + "seed," + "test_row_limit," + "sample_size_limit," + "root_alias," + "root_child_edge_count," + "phase2_root_rows_processed," + "phase2_root_tuples_seen," + "phase2_positive_root_candidates_seen," + "phase2_total_root_group_weight," + "phase2_sample_instance_count," + "phase2_installed_worker_count," + "phase2_root_kafka_preload_s," + "phase2_algorithm_total_s," + "phase2_algorithm_rows_per_sec," + "state_ref" + System.lineSeparator());
            }

            writer.write(Long.toString(System.currentTimeMillis()) + "," + csv(implementation) + "," + EXPECTED_WORKERS + "," + csv(plan.getQueryName()) + "," + csv(plan.getDatasetSeed()) + "," + csv(formatRowLimit(TEST_ROW_LIMIT)) + "," + plan.getSampleSize() + "," + csv(plan.getRootAlias()) + "," + plan.getChildEdges(plan.getRootAlias()).size() + "," + rootRows + "," + rootTuplesSeen + "," + positiveRootCandidatesSeen + "," + totalRootGroupWeight + "," + sampleInstanceCount + "," + installedWorkerCount + "," + preloadSeconds + "," + algorithmSeconds + "," + rowsPerSecond(rootRows, algorithmSeconds) + "," + csv(stateRef) + System.lineSeparator());

        } finally {

            writer.close();
        }

        System.out.println("Phase-2 benchmark CSV appended to: " + csvFile.getAbsolutePath());
    }



    private static String combinedBenchmarkHeader() {

        return "workers,"
                + "query,"
                + "test_row_limit,"
                + "sample_size,"
                + "phase1_algorithm_total_s,"
                + "phase1_alias_algorithm_s,"
                + "phase2_algorithm_total_s,"
                + "phase3_algorithm_total_s,"
                + "phase3_alias_algorithm_s,"
                + "full_algorithm_time_s,"
                + "timestamp_ms,"
                + "implementation,"
                + "seed,"
                + "root_alias,"
                + "leaf_to_root_order,"
                + "phase3_root_to_leaf_order,"
                + "phase1_rows_processed,"
                + "phase1_kafka_preload_s,"
                + "phase1_algorithm_rows_per_sec,"
                + "phase1_alias_rows_processed,"
                + "root_child_edge_count,"
                + "phase2_root_rows_processed,"
                + "phase2_root_tuples_seen,"
                + "phase2_positive_root_candidates_seen,"
                + "phase2_total_root_group_weight,"
                + "phase2_sample_instance_count,"
                + "phase2_installed_worker_count,"
                + "phase2_root_kafka_preload_s,"
                + "phase2_algorithm_rows_per_sec,"
                + "phase2_state_ref,"
                + "phase3_rows_replayed,"
                + "phase3_kafka_preload_s,"
                + "phase3_algorithm_rows_per_sec,"
                + "phase3_alias_rows_replayed,"
                + "phase3_final_alias,"
                + "phase3_final_state_ref,"
                + "phase3_selection_count,"
                + "phase3_installed_worker_count";
    }


    /**
     * Prevents silently appending the new Phase-3 benchmark row shape below an
     * older Phase-1 + Phase-2-only CSV header.
     */
    private static void validateCombinedBenchmarkCsvSchema() throws Exception {

        File csvFile =
                new File(
                        COMBINED_BENCHMARK_CSV_PATH
                );

        if (!csvFile.isFile()
                || csvFile.length() == 0L) {

            return;
        }

        BufferedReader reader =
                new BufferedReader(
                        new FileReader(
                                csvFile
                        )
                );

        String existingHeader;

        try {

            existingHeader =
                    reader.readLine();

        } finally {

            reader.close();
        }

        String expectedHeader =
                combinedBenchmarkHeader();

        if (!expectedHeader.equals(existingHeader)) {

            throw new IllegalStateException(
                    "Existing combined benchmark CSV has a different schema: "
                            + csvFile.getAbsolutePath()
                            + ". The Phase-3-enabled test adds Phase-3 timing and "
                            + "completion columns. Move/delete the old CSV or use "
                            + "-Donepass.combinedCsv=<new-path> before running."
            );
        }
    }


    /**
     * Writes exactly one row for a completed OnePass run.
     *
     * Main comparison fields are always populated:
     *
     * workers
     * query
     * test_row_limit
     * sample_size
     * phase1_algorithm_total_s
     * phase1_alias_algorithm_s
     * phase2_algorithm_total_s
     * phase3_algorithm_total_s
     * phase3_alias_algorithm_s
     * full_algorithm_time_s
     *
     * full_algorithm_time_s is deliberately the sum of measured algorithm
     * phases only. Kafka/TPC-H preload work remains excluded.
     *
     * When RUN_PHASE_3=false the Phase-3 timings are zero / empty and the row
     * represents a Phase-1 + Phase-2-only run.
     */
    private static void writeCombinedBenchmarkCsv(
            CompiledOnePassPlan plan,
            long phaseOnePreloadNanos,
            long phaseTwoRootRows,
            long phaseTwoPreloadNanos,
            JsonNode ready,
            JsonNode installed,
            long phaseThreePreloadNanos,
            JsonNode finalPhaseThreeCompletion,
            String implementation) throws Exception {

        if (!WRITE_COMBINED_BENCHMARK_CSV) {
            return;
        }

        File csvFile =
                new File(
                        COMBINED_BENCHMARK_CSV_PATH
                );

        File parent =
                csvFile.getParentFile();

        if (parent != null
                && !parent.exists()
                && !parent.mkdirs()) {

            throw new IllegalStateException(
                    "Could not create combined benchmark directory: "
                            + parent.getAbsolutePath()
            );
        }

        boolean writeHeader =
                !csvFile.exists()
                        || csvFile.length() == 0L;

        // -----------------------------------------------------------------
        // Main comparison metrics.
        // -----------------------------------------------------------------

        double phaseOneAlgorithmSeconds =
                secondsFor(
                        "phase1_algorithm_total"
                );

        double phaseTwoAlgorithmSeconds =
                secondsFor(
                        "phase2_algorithm_total"
                );

        boolean phaseThreeCompleted =
                finalPhaseThreeCompletion != null
                        && !finalPhaseThreeCompletion.isNull();

        double phaseThreeAlgorithmSeconds =
                phaseThreeCompleted
                        ? secondsFor(
                        "phase3_algorithm_total"
                )
                        : 0.0d;

        double fullAlgorithmSeconds =
                phaseOneAlgorithmSeconds
                        + phaseTwoAlgorithmSeconds
                        + phaseThreeAlgorithmSeconds;

        String phaseOneAliasAlgorithmSeconds =
                String.valueOf(
                        phaseOneAliasAlgorithmSecondsMap(
                                plan
                        )
                );

        String phaseThreeAliasAlgorithmSeconds =
                phaseThreeCompleted
                        ? String.valueOf(
                        phaseThreeAliasAlgorithmSecondsMap(
                                plan
                        )
                )
                        : "{}";

        // -----------------------------------------------------------------
        // Detailed Phase-1 metrics.
        // -----------------------------------------------------------------

        double phaseOnePreloadSeconds =
                phaseOnePreloadNanos
                        / 1_000_000_000.0d;

        long phaseOneRows =
                countFor(
                        "phase1_rows_processed"
                );

        double phaseOneRowsPerSecond =
                rowsPerSecond(
                        phaseOneRows,
                        phaseOneAlgorithmSeconds
                );

        String phaseOneAliasRows =
                String.valueOf(
                        phaseOneAliasRowsMap(
                                plan
                        )
                );

        // -----------------------------------------------------------------
        // Detailed Phase-2 metrics.
        // -----------------------------------------------------------------

        double phaseTwoPreloadSeconds =
                phaseTwoPreloadNanos
                        / 1_000_000_000.0d;

        long rootTuplesSeen =
                longField(
                        ready,
                        "rootTuplesSeen",
                        -1L
                );

        long positiveRootCandidatesSeen =
                longField(
                        ready,
                        "positiveRootCandidatesSeen",
                        -1L
                );

        double totalRootGroupWeight =
                doubleField(
                        ready,
                        "totalRootGroupWeight",
                        0.0d
                );

        int sampleInstanceCount =
                intField(
                        ready,
                        "sampleInstanceCount",
                        -1
                );

        int phaseTwoInstalledWorkerCount =
                intField(
                        installed,
                        "installedWorkerCount",
                        -1
                );

        String phaseTwoStateRef =
                textField(
                        ready,
                        "stateRef",
                        ""
                );

        double phaseTwoRowsPerSecond =
                rowsPerSecond(
                        phaseTwoRootRows,
                        phaseTwoAlgorithmSeconds
                );

        // -----------------------------------------------------------------
        // Detailed Phase-3 metrics.
        // -----------------------------------------------------------------

        double phaseThreePreloadSeconds =
                phaseThreeCompleted
                        ? phaseThreePreloadNanos
                        / 1_000_000_000.0d
                        : 0.0d;

        long phaseThreeRows =
                phaseThreeCompleted
                        ? countFor(
                        "phase3_rows_replayed"
                )
                        : 0L;

        double phaseThreeRowsPerSecond =
                phaseThreeCompleted
                        ? rowsPerSecond(
                        phaseThreeRows,
                        phaseThreeAlgorithmSeconds
                )
                        : 0.0d;

        String phaseThreeAliasRows =
                phaseThreeCompleted
                        ? String.valueOf(
                        phaseThreeAliasRowsMap(
                                plan
                        )
                )
                        : "{}";

        String finalPhaseThreeAlias =
                phaseThreeCompleted
                        ? textField(
                        finalPhaseThreeCompletion,
                        "alias",
                        ""
                )
                        : "";

        String finalPhaseThreeStateRef =
                phaseThreeCompleted
                        ? textField(
                        finalPhaseThreeCompletion,
                        "stateRef",
                        ""
                )
                        : "";

        int finalPhaseThreeSelectionCount =
                phaseThreeCompleted
                        ? intField(
                        finalPhaseThreeCompletion,
                        "selectionCount",
                        -1
                )
                        : -1;

        int finalPhaseThreeInstalledWorkerCount =
                phaseThreeCompleted
                        ? intField(
                        finalPhaseThreeCompletion,
                        "installedWorkerCount",
                        -1
                )
                        : -1;

        FileWriter writer =
                new FileWriter(
                        csvFile,
                        true
                );

        try {

            if (writeHeader) {

                writer.write(
                        combinedBenchmarkHeader()
                                + System.lineSeparator()
                );
            }

            List<String> fields =
                    new ArrayList<String>();

            // -------------------------------------------------------------
            // Main fields - always populated.
            // -------------------------------------------------------------

            fields.add(
                    Integer.toString(
                            EXPECTED_WORKERS
                    )
            );

            fields.add(
                    csv(
                            plan.getQueryName()
                    )
            );

            fields.add(
                    csv(
                            formatRowLimit(
                                    TEST_ROW_LIMIT
                            )
                    )
            );

            fields.add(
                    Integer.toString(
                            plan.getSampleSize()
                    )
            );

            fields.add(
                    Double.toString(
                            phaseOneAlgorithmSeconds
                    )
            );

            fields.add(
                    csv(
                            phaseOneAliasAlgorithmSeconds
                    )
            );

            fields.add(
                    Double.toString(
                            phaseTwoAlgorithmSeconds
                    )
            );

            fields.add(
                    Double.toString(
                            phaseThreeAlgorithmSeconds
                    )
            );

            fields.add(
                    csv(
                            phaseThreeAliasAlgorithmSeconds
                    )
            );

            fields.add(
                    Double.toString(
                            fullAlgorithmSeconds
                    )
            );

            // -------------------------------------------------------------
            // Optional detailed fields - always present in the schema.
            // -------------------------------------------------------------

            if (WRITE_DETAILED_BENCHMARK_DATA) {

                fields.add(
                        Long.toString(
                                System.currentTimeMillis()
                        )
                );

                fields.add(
                        csv(
                                implementation
                        )
                );

                fields.add(
                        csv(
                                plan.getDatasetSeed()
                        )
                );

                fields.add(
                        csv(
                                plan.getRootAlias()
                        )
                );

                fields.add(
                        csv(
                                String.valueOf(
                                        plan.getLeafToRootOrder()
                                )
                        )
                );

                fields.add(
                        csv(
                                String.valueOf(
                                        phaseThreeAliasOrder(
                                                plan
                                        )
                                )
                        )
                );

                fields.add(
                        Long.toString(
                                phaseOneRows
                        )
                );

                fields.add(
                        Double.toString(
                                phaseOnePreloadSeconds
                        )
                );

                fields.add(
                        Double.toString(
                                phaseOneRowsPerSecond
                        )
                );

                fields.add(
                        csv(
                                phaseOneAliasRows
                        )
                );

                fields.add(
                        Integer.toString(
                                plan.getChildEdges(
                                        plan.getRootAlias()
                                ).size()
                        )
                );

                fields.add(
                        Long.toString(
                                phaseTwoRootRows
                        )
                );

                fields.add(
                        Long.toString(
                                rootTuplesSeen
                        )
                );

                fields.add(
                        Long.toString(
                                positiveRootCandidatesSeen
                        )
                );

                fields.add(
                        Double.toString(
                                totalRootGroupWeight
                        )
                );

                fields.add(
                        Integer.toString(
                                sampleInstanceCount
                        )
                );

                fields.add(
                        Integer.toString(
                                phaseTwoInstalledWorkerCount
                        )
                );

                fields.add(
                        Double.toString(
                                phaseTwoPreloadSeconds
                        )
                );

                fields.add(
                        Double.toString(
                                phaseTwoRowsPerSecond
                        )
                );

                fields.add(
                        csv(
                                phaseTwoStateRef
                        )
                );

                if (phaseThreeCompleted) {

                    fields.add(
                            Long.toString(
                                    phaseThreeRows
                            )
                    );

                    fields.add(
                            Double.toString(
                                    phaseThreePreloadSeconds
                            )
                    );

                    fields.add(
                            Double.toString(
                                    phaseThreeRowsPerSecond
                            )
                    );

                    fields.add(
                            csv(
                                    phaseThreeAliasRows
                            )
                    );

                    fields.add(
                            csv(
                                    finalPhaseThreeAlias
                            )
                    );

                    fields.add(
                            csv(
                                    finalPhaseThreeStateRef
                            )
                    );

                    fields.add(
                            Integer.toString(
                                    finalPhaseThreeSelectionCount
                            )
                    );

                    fields.add(
                            Integer.toString(
                                    finalPhaseThreeInstalledWorkerCount
                            )
                    );

                } else {

                    for (int i = 0; i < 8; i++) {
                        fields.add("");
                    }
                }

            } else {

                /*
                 * 28 optional detailed columns.
                 */
                for (int i = 0; i < 28; i++) {
                    fields.add("");
                }
            }

            writer.write(
                    String.join(
                            ",",
                            fields
                    )
                            + System.lineSeparator()
            );

        } finally {

            writer.close();
        }

        System.out.println(
                "Combined OnePass benchmark CSV appended to: "
                        + csvFile.getAbsolutePath()
        );

        System.out.println(
                "  phase1_algorithm_total_s = "
                        + phaseOneAlgorithmSeconds
        );

        System.out.println(
                "  phase2_algorithm_total_s = "
                        + phaseTwoAlgorithmSeconds
        );

        System.out.println(
                "  phase3_algorithm_total_s = "
                        + phaseThreeAlgorithmSeconds
        );

        System.out.println(
                "  full_algorithm_time_s    = "
                        + fullAlgorithmSeconds
        );
    }


    private static String csv(String value) {

        if (value == null) {
            return "";
        }

        String escaped = value.replace("\"", "\"\"");

        return "\"" + escaped + "\"";
    }

    private static String formatRowLimit(long rowLimit) {

        return rowLimit < 0L ? "FULL" : Long.toString(rowLimit);
    }

    // =====================================================================
    // JSON HELPERS
    // =====================================================================

    private static String textField(JsonNode node, String fieldName, String defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        String value = field.asText();

        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int intField(JsonNode node, String fieldName, int defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asInt(defaultValue);
    }

    private static long longField(JsonNode node, String fieldName, long defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String fieldName, double defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asDouble(defaultValue);
    }

    private static boolean booleanField(JsonNode node, String fieldName, boolean defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asBoolean(defaultValue);
    }

    private static void configureRuntimeArguments(String[] args) {

        /*
         * Priority:
         * 1. Command-line broker list
         * 2. -Donepass.kafka JVM property
         * 3. The LOCAL / SOFTNET default selected above
         */

        if (args != null && args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            BOOTSTRAP_SERVERS = args[0].trim();

        } else {
            String propertyValue = System.getProperty("onepass.kafka", "");
            if (propertyValue != null && !propertyValue.trim().isEmpty()) {
                BOOTSTRAP_SERVERS = propertyValue.trim();
            }
        }

        System.out.println("[OnePass TEST CONFIG]" + " kafka=" + BOOTSTRAP_SERVERS);
    }

    private static ObjectNode buildOnePassFinalResultRequest(String baseKey, String streamId, int uid) {

        /*
         * Every worker owns the same globally installed completed sample once
         * request 91 / phaseThreeComplete=true has been observed.
         *
         * Query worker 0 only. noOfP=1 is intentional: this makes the result a
         * single-worker Estimation and therefore sends it directly to OUTPUT_TOPIC
         * instead of trying to run another distributed OnePass reduction.
         */
        String workerKey = baseKey + "_" + EXPECTED_WORKERS + "_KEYED_0";
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", workerKey);
        request.put("key", workerKey);
        request.put("requestID", REQUEST_ESTIMATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);

        /*
         * Important:
         * We only want one worker's status/result because the completed global
         * sample has already been installed identically everywhere.
         */
        request.put("noOfP", 1);
        ArrayNode param = MAPPER.createArrayNode();
        param.add("FINAL_RESULT");
        request.set("param", param);

        return request;
    }

    private static JsonNode requestFinalQueryResult(KafkaProducer<String, String> controlProducer,
                                                    KafkaConsumer<String, String> outputConsumer, int uid,
                                                    String baseKey, String streamId, long timeoutMs) throws Exception {

        String workerKey = baseKey + "_" + EXPECTED_WORKERS + "_KEYED_0";
        ObjectNode request = buildOnePassFinalResultRequest(baseKey, streamId, uid);

        System.out.println();
        System.out.println("Requesting final OnePass query result from worker 0...");

        /*
         * Do NOT reinitialize the output consumer here.
         * It has just consumed the final request-91 barrier, so its current
         * position is exactly where we want it. The request-3 result will be a
         * new record written after this point.
         */
        sendJson(controlProducer, REQUEST_TOPIC, workerKey, request);
        controlProducer.flush();

        return waitForFinalQueryResult(outputConsumer, uid, timeoutMs);
    }

    private static JsonNode waitForFinalQueryResult(KafkaConsumer<String, String> consumer, int uid, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int recordsSeen = 0;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(1000L);
            for (ConsumerRecord<String, String> record : records) {
                recordsSeen++;
                JsonNode envelope;
                try {
                    envelope = MAPPER.readTree(record.value());
                } catch (Exception ignored) {
                    continue;
                }

              //This is a normal Estimation response to requestID=3.
                 int envelopeUid = intField(envelope, "UID", intField(envelope, "uid", -1));

                if (envelopeUid != uid) {
                    continue;
                }

                if (intField(envelope, "requestID", -1) != REQUEST_ESTIMATE) {
                    continue;
                }

                if (intField(envelope, "synopsisID", -1) != SYNOPSIS_ID) {
                    continue;
                }

                JsonNode payload = unwrapEstimationPayload(envelope);
                if (payload == null || payload.isNull() || !payload.isObject()) {
                    continue;
                }

                if (!booleanField(payload, "phaseThreeComplete", false)) {
                    throw new IllegalStateException("Final result request returned before Phase 3 was complete. " +
                            "Payload=" + payload);
                }

                int completedSampleCount = intField(payload, "completedSampleCount", -1);
                if (completedSampleCount <= 0) {
                    throw new IllegalStateException("Final OnePass result contains no completed samples. " +
                            "Payload=" + payload);
                }

                System.out.println("Observed final OnePass result:" + " completedSampleCount=" + completedSampleCount +
                        ", projectedSamplesIncluded=" + booleanField(payload, "projectedSamplesIncluded", false) +
                        ", projectedSamplesTruncated=" + booleanField(payload, "projectedSamplesTruncated", false));

                return payload.deepCopy();
            }
        }
        throw new IllegalStateException("Timed out waiting for final OnePass query result." + " uid=" + uid + ", recordsSeen=" + recordsSeen);
    }

    private static void printFinalQueryResultPreview(JsonNode result, int maxResults) throws Exception {

        if (result == null || result.isNull()) {
            throw new IllegalArgumentException("Final query result must not be null");
        }

        JsonNode samples = result.get("projectedCompletedSamples");
        /*
         * For benchmark-size samples the production synopsis intentionally does
         * not put all K tuples into the Kafka response. Instead it provides the
         * small projectedCompletedSamplesPreview array.
         *
         * For the current LIMIT 10000 test this is the path we expect.
         */
        if (samples == null || !samples.isArray() || samples.size() == 0) {
            samples = result.get("projectedCompletedSamplesPreview");
        }

        if (samples == null || !samples.isArray() || samples.size() == 0) {
            throw new IllegalStateException("Final OnePass result has no projected sample output. " + "Payload=" + result);
        }

        int count = Math.min(maxResults, samples.size());
        System.out.println();
        System.out.println("=======================================================");
        System.out.println(" FINAL ONEPASS* QUERY RESULT PREVIEW");
        System.out.println("=======================================================");
        System.out.println("completedSampleCount = " + intField(result, "completedSampleCount", -1));
        System.out.println("showing              = " + count);
        System.out.println();

        for (int i = 0; i < count; i++) {
            System.out.println("Result " + (i + 1) + ":");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(samples.get(i)));
            System.out.println();
        }

        System.out.println("=======================================================");
    }
}
