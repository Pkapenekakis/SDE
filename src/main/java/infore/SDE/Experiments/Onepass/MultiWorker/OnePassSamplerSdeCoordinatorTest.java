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
 * Local integration / benchmark test for the SHARDED OnePass* Phase 1 + Phase 2 design.
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
 * This keeps expensive local file parsing / JSON creation / producer.send()
 * outside both algorithm timers while preserving the actual Kafka visibility
 * boundary (read_committed + transaction commit) inside each measured phase.
 * <p>
 * Phase 3 is intentionally not started by this test.
 */
public final class OnePassSamplerSdeCoordinatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---------------------------------------------------------------------
    // LOCAL TEST SETTINGS
    // ---------------------------------------------------------------------

    private static final String BOOTSTRAP_SERVERS = System.getProperty("onepass.kafka",
            "localhost:9092");
    private static final String DATA_TOPIC = System.getProperty("onepass.dataTopic",
            "dataTopic");
    private static final String REQUEST_TOPIC = System.getProperty("onepass.requestTopic",
            "requestTopic");
    private static final String OUTPUT_TOPIC = System.getProperty("onepass.outputTopic",
            "estimationTopic");

    /*
     * The running SDE job still needs its State Topic configured, but this
     * driver does not consume it directly.
     */
    private static final String STATE_TOPIC = System.getProperty("onepass.stateTopic",
            "onepassStateTopic");

    private static final String TEST_TPCH_DIR = System.getProperty("onepass.tpchDir",
            "/home/vboxuser/Desktop/Thesis/tpch-data/sf1");

    private static final String PHASE1_BENCHMARK_CSV_PATH = System.getProperty("onepass.phase1Csv",
            "/home/vboxuser/Desktop/Thesis/onepass_multiworker_phase1_sharded_local.csv");

    private static final String PHASE2_BENCHMARK_CSV_PATH = System.getProperty("onepass.phase2Csv",
            "/home/vboxuser/Desktop/Thesis/onepass_multiworker_phase2_sharded_local.csv");

    // ---------------------------------------------------------------------
    // TEST CONFIGURATION
    // ---------------------------------------------------------------------

    private static final String PHASE1_INDEX_EXPORT_DIR = System.getProperty("onepass.phase1IndexExportDir",
            "/tmp/onepass-phase1-validator");
    private static final String PHASE1_VALIDATOR_JSON_PATH = System.getProperty("onepass.phase1ValidatorJson",
            "/tmp/onepass_wq3_alias_phase1_full_indexes.json");
    private static final long PHASE1_INDEX_EXPORT_TIMEOUT_MS = Long.
            parseLong(System.getProperty("onepass.phase1IndexExportTimeoutMs", "120000"));

    private static final String TEST_ONEPASS_SQL = "SELECT * FROM wq3_alias WEIGHTED BY " +
            "(" + "o.o_totalprice * (l.l_extendedprice * (2 - l.l_discount))) " +
            "LIMIT 10000 /* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

//    private static final String TEST_ONEPASS_SQL = "SELECT * FROM w_branch_supplier WEIGHTED BY " +
//            "(" + "l1.l_extendedprice * l2.l_extendedprice" + ") " +
//            "LIMIT 1000 " + "/* catalog='tpch-onepass-catalog.json', " + "seed='branch-test-123', scalefactor=1 */";

    //Use -1 for the full TPC-H relation.
    private static final long TEST_ROW_LIMIT = Long.parseLong(System.getProperty("onepass.testRowLimit",
            "100000"));

    private static final int EXPECTED_WORKERS = Integer.parseInt(System.getProperty("onepass.workers",
            "4"));

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
    private static final boolean WRITE_PHASE1_BENCHMARK_CSV = true;
    private static final boolean WRITE_PHASE2_BENCHMARK_CSV = true;

    private static final boolean RUN_PHASE_2 = Boolean.parseBoolean(System.getProperty("onepass.runPhase2", "true"));

    private static final int SYNOPSIS_ID = 30;
    private static final int REQUEST_ADD = 1;
    private static final int REQUEST_UPDATE = 7;


    /*
     * Timing maps deliberately contain only algorithm timings.
     * Kafka/TPC-H preload is tracked separately and never added to
     * phase1_algorithm_total.
     */
    private static final Map<String, Long> benchmarkNanos = new LinkedHashMap<String, Long>();
    private static final Map<String, Long> benchmarkCounts = new LinkedHashMap<String, Long>();

    /*
     * DEBUG / CORRECTNESS VALIDATION ONLY.
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
    private static final boolean EXPORT_PHASE1_INDEXES = true;

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
     *
     * No Run.java flag or JVM -D property is required.
     */
    private static final int REQUEST_DEBUG_VALIDATE_PHASE2 = 89;
    private static final boolean VALIDATE_PHASE2 = true;

    private static final String PHASE2_VALIDATION_DIR =
            System.getProperty("onepass.phase2ValidationDir", "/tmp/onepass-phase2-validator");

    private static final String PHASE2_VALIDATION_JSON_PATH =
            System.getProperty("onepass.phase2ValidatorJson", "/tmp/onepass_phase2_validation.json");

    private static final long PHASE2_VALIDATION_TIMEOUT_MS =
            Long.parseLong(System.getProperty("onepass.phase2ValidationTimeoutMs", "120000"));

    private static final String PHASE2_CHECKSUM_VERSION =
            "ONEPASS_PHASE2_ROOT_SAMPLE_SHA256_V1";

    private OnePassSamplerSdeCoordinatorTest() {}

    public static void main(String[] args) throws Exception {

        int uid = UUID.randomUUID().toString().hashCode() & 0x7fffffff;

        benchmarkNanos.clear();
        benchmarkCounts.clear();

        String streamId = "onepass-sharded-phase12-local-test";

        String baseKey = "onepass-phase12-" + uid;

        System.out.println("=======================================================");
        System.out.println(" OnePass* SHARDED PHASE 1 + PHASE 2 - LOCAL TEST");
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
        System.out.println("EXPORT_PHASE1_INDEXES = " + EXPORT_PHASE1_INDEXES);
        System.out.println("VALIDATE_PHASE2  = " + VALIDATE_PHASE2);

        if (EXPORT_PHASE1_INDEXES) {
            System.out.println("phase1IndexExportDir = " + PHASE1_INDEX_EXPORT_DIR);
            System.out.println("phase1ValidatorJson  = " + PHASE1_VALIDATOR_JSON_PATH);
        }

        if (VALIDATE_PHASE2) {
            System.out.println("phase2ValidationDir  = " + PHASE2_VALIDATION_DIR);
            System.out.println("phase2ValidatorJson  = " + PHASE2_VALIDATION_JSON_PATH);
            System.out.println("phase2ChecksumVersion= " + PHASE2_CHECKSUM_VERSION);
        }

        System.out.println("SQL:");
        System.out.println(TEST_ONEPASS_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_ONEPASS_SQL);

        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassCatalog catalog = OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        validatePlanForShardedPhaseOneV1(plan);

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Root child edges: " + plan.getChildEdges(plan.getRootAlias()));
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Required fields by alias: " + plan.getRequiredFieldsByAlias());
        System.out.println();

        KafkaProducer<String, String> controlProducer = createProducer();

        KafkaConsumer<String, String> phaseOneFeedbackConsumer = createObserverConsumer();

        KafkaConsumer<String, String> phaseTwoOutputConsumer = null;

        List<PreparedAliasTransaction> preparedPhaseOne = new ArrayList<PreparedAliasTransaction>();

        PreparedAliasTransaction preparedPhaseTwoRoot = null;

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

            writePhaseOneBenchmarkCsv(plan, phaseOnePreloadNanos, "SDE_KAFKA_MULTIWORKER_SHARDED_PHASE1_LOCAL");

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

                printPhaseTwoBenchmarkSummary(plan, preparedPhaseTwoRoot.rows, phaseTwoPreloadNanos, ready, installed);

                writePhaseTwoBenchmarkCsv(plan, preparedPhaseTwoRoot.rows, phaseTwoPreloadNanos, ready, installed, "SDE_KAFKA_MULTIWORKER_SHARDED_PHASE2_LOCAL");

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
                System.out.println("SUCCESS: Phase 1 + sharded Phase 2 completed locally.");
                System.out.println("Global root sample is installed on all " + EXPECTED_WORKERS + " workers. Phase 3 is intentionally not started.");

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

    private static void validatePlanForShardedPhaseOneV1(CompiledOnePassPlan plan) {

        if (plan == null) {
            throw new IllegalArgumentException("Compiled plan must not be null");
        }

        if (plan.getLeafToRootOrder() == null || plan.getLeafToRootOrder().isEmpty()) {

            throw new IllegalStateException("Compiled plan has empty leafToRootOrder: " + plan);
        }

        /*
         * Every alias processed during Phase 1 is non-root and must therefore
         * have exactly one parent edge in the rooted join tree.
         *
         * Multiple CHILD edges are supported by the sharded enrichment path.
         */
        for (String alias : plan.getLeafToRootOrder()) {

            if (plan.getParentEdge(alias) == null) {

                throw new IllegalStateException("Phase-1 alias has no parent edge: " + alias);
            }
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

        if (!WRITE_PHASE1_BENCHMARK_CSV) {
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


    private static void writePhaseTwoBenchmarkCsv(CompiledOnePassPlan plan, long rootRows, long preloadNanos, JsonNode ready, JsonNode installed, String implementation) throws Exception {

        if (!WRITE_PHASE2_BENCHMARK_CSV) {

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
}
