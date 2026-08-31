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
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.io.FileWriter;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;

/**
 * Multi-worker OnePass* coordinator test using real query compilation and TPC-H input.

 * For WQ3 the Phase 1 flow under the new protocol is:
 *
 *   alias l:
 *     stream l
 *     END_ALIAS(l)
 *     merge GLOBAL_PHASE1_RESULT
 *     RequestTopic: BEGIN / CHUNK... / COMMIT
 *     START_NEXT_ALIAS(o, requiredStateRef)
 *
 *   alias o:
 *     stream o
 *     END_ALIAS(o)
 *     merge GLOBAL_PHASE1_RESULT
 *     RequestTopic: BEGIN / CHUNK... / COMMIT
 *     START_PHASE_2(c, requiredStateRef)
 *
 * Phase 1 no longer waits for DATA_BARRIER_ACK / INSTALL_GLOBAL_INDEX_ACK.
 *
 * Phase 2 and Phase 3 remain on the legacy barrier/install-ACK protocol in
 * this mixed-mode integration test.
 */
public final class OnePassSamplerSdeCoordinatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    //Local Test variables
    /*
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DATA_TOPIC = "dataTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";
    private static final String REQUEST_TOPIC = "requestTopic";
    private static final String GLOBAL_STATE_TOPIC = "globalStateTopic"; //unused
    private static final String TEST_TPCH_DIR = "/home/vboxuser/Desktop/Thesis/tpch-data/sf1";
    private static final String PHASE1_BENCHMARK_CSV_PATH =
            "/home/vboxuser/Desktop/Thesis/onepass_multiworker_phase1_benchmark.csv";

    */

    //softnet cluster Test variables

    private static final String BOOTSTRAP_SERVERS = "clu02.softnet.tuc.gr:6667," + "clu03.softnet.tuc.gr:6667,"
            + "clu04.softnet.tuc.gr:6667," + "clu06.softnet.tuc.gr:6667";
    private static final String DATA_TOPIC = "pkapenekakis-dataTopic";
    private static final String ESTIMATION_TOPIC = "pkapenekakis-estimationTopic";
    private static final String REQUEST_TOPIC = "pkapenekakis-requestTopic";
    private static final String GLOBAL_STATE_TOPIC = "pkapenekakis-globalStateTopic";
    private static final String TEST_TPCH_DIR = "/home/pkapenekakis/onepass/tpch-data/sf1";
    private static final String PHASE1_BENCHMARK_CSV_PATH = "/home/pkapenekakis/onepass/results/" +
            "onepass_multiworker_phase1_benchmark.csv";



    private static final int REQUEST_TOPIC_PARTITIONS = 4;

    private static final String TEST_ONEPASS_SQL =
            "SELECT * FROM wq3_alias WEIGHTED BY (" +
                    "o.o_totalprice * (l.l_extendedprice * (1 - l.l_discount))) " +
                    "LIMIT 10000 /* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    //Use -1 for the whole file.
    private static final long TEST_ROW_LIMIT = 1000000L;

    private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";

    private static final int SYNOPSIS_ID = 30;
    private static final int REQUEST_ADD = 1;
    private static final int REQUEST_UPDATE = 7;

    private static final int EXPECTED_WORKERS = 4;
    private static final long TIMEOUT_MS = 30L * 60L * 1000L;

    private static final boolean ENABLE_REQUIRED_FIELD_PRUNING = true;
    private static final boolean PRINT_MATCHED_PAYLOADS = false;
    private static final boolean WAIT_FOR_FULL_GLOBAL_RESULTS = false;

    private static final boolean EXPORT_FINAL_PHASE1_INDEX = false;
    private static final String PHASE1_INDEX_EXPORT_DIR = "/tmp/onepass_wq3_alias_phase1_full_indexes.json";

    private static final boolean STOP_AFTER_PHASE1_BENCHMARK = true;
    private static final boolean WRITE_PHASE1_BENCHMARK_CSV = true;

    /*
     * Phase 1 benchmark output.
     *
     * The common metric with the existing single-worker benchmark is
     * phase1_stream_send. The new asynchronous implementation additionally
     * records the END_ALIAS -> global merge -> feedback transition time.
     */
    private static final Map<String, Long> benchmarkNanos = new LinkedHashMap<String, Long>();
    private static final Map<String, Long> benchmarkCounts = new LinkedHashMap<String, Long>();


    private OnePassSamplerSdeCoordinatorTest() {
    }

    public static void main(String[] args) throws Exception {
        int uid = Math.abs(UUID.randomUUID().toString().hashCode());

        benchmarkNanos.clear();
        benchmarkCounts.clear();

        String streamId = "onepass-coordinator-tpch-test";
        String baseKey = "onepass-phase1-" + uid;

        System.out.println("=== OnePassSamplerSdeCoordinatorTest: SQL/TPC-H multi-alias Phase 1 ===");
        System.out.println("uid = " + uid);
        System.out.println("baseKey = " + baseKey);
        System.out.println("expectedWorkers = " + EXPECTED_WORKERS);
        System.out.println("TEST_TPCH_DIR = " + TEST_TPCH_DIR);
        System.out.println("TEST_ROW_LIMIT = " + TEST_ROW_LIMIT);
        System.out.println("SQL:");
        System.out.println(TEST_ONEPASS_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_ONEPASS_SQL);
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);
        OnePassCatalog catalog = OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        if (plan.getLeafToRootOrder() == null || plan.getLeafToRootOrder().isEmpty()) {
            throw new IllegalStateException("Compiled plan has empty leafToRootOrder: " + plan);
        }

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Root-to-leaf order: " + plan.getRootToLeafOrder());
        System.out.println("Required fields by alias: " + plan.getRequiredFieldsByAlias());
        System.out.println();

        KafkaProducer<String, String> producer = createProducer();
        //KafkaConsumer<String, String> consumer = createConsumer("onepass-coordinator-tpch-test-" + uid);

        /*
         * Phase 1 asynchronous benchmark observes feedback directly on RequestTopic.
         *
         * estimationTopic is only required by the still-legacy Phase 2/3 validation.
         */
        KafkaConsumer<String, String> consumer = null;

        if (!STOP_AFTER_PHASE1_BENCHMARK) {
            consumer = createConsumer("onepass-coordinator-tpch-test-" + uid);

            consumer.subscribe(Collections.singletonList(ESTIMATION_TOPIC));
        }

        KafkaConsumer<String, String> feedbackConsumer = createObserverConsumer();

        try {
            if (consumer != null) {
                drainConsumer(consumer);
            }

            initializePhaseOneFeedbackObserver(feedbackConsumer, REQUEST_TOPIC);


            System.out.println("1. Sending ADD OnePass request with SQL and noOfP=" + EXPECTED_WORKERS + "...");
            ObjectNode addRequest = buildOnePassAddRequest(baseKey, streamId, uid, EXPECTED_WORKERS);
            sendJson(producer, REQUEST_TOPIC, baseKey, addRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(addRequest));
            System.out.println();

            /*
             * Temporary synchronization. Later this should become ADD_ACK.
             */
            Thread.sleep(3000L);

            int aliasPosition = 0;

            String finalPhaseOneAlias = "";
            String finalPhaseOneEdgeId = "";
            PhaseOneFeedbackTrace finalPhaseOneFeedback = null;

            /*
             * Start Phase 1 timing after the temporary ADD synchronization.
             * This matches the single-worker phase1_stream_send boundary: SQL
             * compilation and ADD setup are not included.
             */
            long phaseOneTotalStartNanos = tic();

            for (String phaseOneAlias : plan.getLeafToRootOrder()) {
                long phaseOneAliasTotalStartNanos = tic();
                aliasPosition++;

                CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                        plan.getParentEdge(phaseOneAlias);

                if (parentEdge == null) {
                    throw new IllegalStateException(
                            "Phase 1 alias has no parent edge: " + phaseOneAlias
                    );
                }

                String expectedEdgeId = parentEdge.getEdgeId();
                String resultId = "PHASE1_" + phaseOneAlias + "_" + uid;
                String expectedStateRef = uid + "_PHASE1_" + resultId + "_GLOBAL_STATE";

                boolean lastPhaseOneAlias =
                        aliasPosition == plan.getLeafToRootOrder().size();

                String nextCommand;
                String nextAlias;

                if (lastPhaseOneAlias) {
                    nextCommand = "START_PHASE_2";
                    nextAlias = plan.getRootAlias();
                } else {
                    nextCommand = "START_NEXT_ALIAS";

                    /*
                     * aliasPosition is 1-based here.
                     * For WQ3, position 1 is l, so index 1 is o.
                     */
                    nextAlias = plan.getLeafToRootOrder().get(aliasPosition);
                }

                System.out.println();
                System.out.println("=======================================================");
                System.out.println("PHASE_1 alias "
                        + aliasPosition
                        + "/"
                        + plan.getLeafToRootOrder().size()
                        + ": "
                        + phaseOneAlias);
                System.out.println("expectedEdgeId = " + expectedEdgeId);
                System.out.println("resultId = " + resultId);
                System.out.println("expectedStateRef = " + expectedStateRef);
                System.out.println("nextCommand = " + nextCommand);
                System.out.println("nextAlias = " + nextAlias);
                System.out.println("=======================================================");
                System.out.println();

                System.out.println(
                        "Streaming PHASE_1 alias from TPC-H: "
                                + phaseOneAlias
                                + "..."
                );

                long phaseOneAliasStreamStartNanos = tic();

                long rowsSent = streamAlias(
                        producer,
                        DATA_TOPIC,
                        baseKey,
                        streamId,
                        catalog,
                        plan,
                        phaseOneAlias,
                        TEST_ROW_LIMIT,
                        plan.getRequiredFieldsByAlias()
                );

                producer.flush();

                recordDuration(
                        "phase1_stream_send",
                        phaseOneAliasStreamStartNanos
                );

                recordDuration(
                        "phase1_alias_" + phaseOneAlias + "_stream_send",
                        phaseOneAliasStreamStartNanos
                );

                recordCount(
                        "phase1_rows_sent",
                        rowsSent
                );

                recordCount(
                        "phase1_alias_" + phaseOneAlias + "_rows_sent",
                        rowsSent
                );

                if (rowsSent <= 0L) {
                    throw new IllegalStateException(
                            "No rows were streamed for alias "
                                    + phaseOneAlias
                    );
                }

                System.out.println(
                        "Rows sent for alias "
                                + phaseOneAlias
                                + ": "
                                + rowsSent
                );

                /*
                 * New Phase 1 completion protocol:
                 *
                 * END_ALIAS travels on DataTopic and is broadcast by the
                 * OnePass-aware router to every logical OnePass worker.
                 *
                 * Each worker exports LOCAL_PHASE1_RESULT directly.
                 * No DATA_BARRIER_ACK and no FINISH_PHASE_1 request are used.
                 */
                System.out.println(
                        "Sending END_ALIAS for PHASE_1 alias "
                                + phaseOneAlias
                                + "..."
                );

                /*
                 * This timer is the new-protocol counterpart of the old Phase 1
                 * completion/control cost. It includes:
                 *
                 *   END_ALIAS send
                 *   -> P local exports
                 *   -> global reduce/merge
                 *   -> BEGIN/CHUNK/COMMIT
                 *   -> START_NEXT_ALIAS / START_PHASE_2 observed on RequestTopic
                 *
                 * It does not wait for installation ACKs because the new design
                 * intentionally has no centralized installation-ACK barrier.
                 */
                long phaseOneAliasFeedbackStartNanos = tic();

                ObjectNode endAliasDatapoint =
                        buildEndAliasDatapoint(
                                baseKey,
                                streamId,
                                uid,
                                phaseOneAlias,
                                aliasPosition,
                                resultId,
                                EXPECTED_WORKERS,
                                nextCommand,
                                nextAlias
                        );

                sendJson(
                        producer,
                        DATA_TOPIC,
                        baseKey,
                        endAliasDatapoint
                );

                producer.flush();

                /*
                 * Observe the actual Kafka feedback path:
                 *
                 *   GLOBAL_PHASE1_RESULT
                 *   -> splitter
                 *   -> BEGIN / CHUNK... / COMMIT
                 *   -> coordinator
                 *   -> START_NEXT_ALIAS / START_PHASE_2
                 *   -> RequestTopic
                 */
                System.out.println(
                        "Waiting for Phase 1 RequestTopic feedback: "
                                + "BEGIN / CHUNK... / COMMIT / "
                                + nextCommand
                                + "..."
                );

                PhaseOneFeedbackTrace feedback =
                        waitForPhaseOneFeedbackSequence(
                                feedbackConsumer,
                                uid,
                                resultId,
                                expectedStateRef,
                                nextCommand,
                                nextAlias,
                                EXPORT_FINAL_PHASE1_INDEX && lastPhaseOneAlias,
                                TIMEOUT_MS
                        );

                if (feedback.totalEntryCount <= 0) {
                    throw new IllegalStateException(
                            "Expected a non-empty global Phase 1 index for alias "
                                    + phaseOneAlias
                                    + ", but totalEntryCount="
                                    + feedback.totalEntryCount
                    );
                }

                recordDuration(
                        "phase1_feedback_total",
                        phaseOneAliasFeedbackStartNanos
                );

                recordDuration(
                        "phase1_alias_" + phaseOneAlias + "_feedback",
                        phaseOneAliasFeedbackStartNanos
                );

                recordDuration(
                        "phase1_alias_" + phaseOneAlias + "_total",
                        phaseOneAliasTotalStartNanos
                );

                System.out.println(
                        "Phase 1 feedback complete:"
                                + " alias="
                                + phaseOneAlias
                                + ", stateRef="
                                + feedback.stateRef
                                + ", chunkCount="
                                + feedback.chunkCount
                                + ", totalEntryCount="
                                + feedback.totalEntryCount
                                + ", nextCommand="
                                + feedback.nextCommand
                                + ", nextAlias="
                                + feedback.nextAlias
                );

                finalPhaseOneAlias = phaseOneAlias;
                finalPhaseOneEdgeId = expectedEdgeId;

                if (lastPhaseOneAlias) {
                    finalPhaseOneFeedback = feedback;
                }
            }

            recordDuration(
                    "phase1_total_observed",
                    phaseOneTotalStartNanos
            );

            /*
             * Print/write immediately after Phase 1. This way a later legacy
             * Phase 2/3 failure does not lose the Phase 1 benchmark result.
             * Full-index debug export is deliberately outside the timer.
             */
            printPhaseOneBenchmarkSummary(plan);
            writePhaseOneBenchmarkCsv(
                    plan,
                    "SDE_KAFKA_MULTIWORKER_ASYNC_PHASE1"
            );

            if (EXPORT_FINAL_PHASE1_INDEX) {

                if (finalPhaseOneFeedback == null) {
                    throw new IllegalStateException(
                            "Cannot export final Phase 1 index because the final "
                                    + "Phase 1 feedback trace is null."
                    );
                }

                writeFinalPhaseOneIndexForPythonValidator(
                        finalPhaseOneFeedback,
                        uid,
                        plan.getQueryName(),
                        plan.getRootAlias(),
                        finalPhaseOneAlias,
                        finalPhaseOneEdgeId
                );
            }

            if (STOP_AFTER_PHASE1_BENCHMARK) {

                printPhaseOneBenchmarkSummary(plan);
                //writePhaseOneBenchmarkCsv(plan, "SDE_KAFKA_MULTIWORKER_ASYNC_PHASE1");

                System.out.println();
                System.out.println("SUCCESS: Phase 1 benchmark completed. " + "Stopping before legacy Phase 2.");

                return;
            }

            /*
             * PHASE 2:
             *
             * Now that the final global Phase 1 index is installed on every worker,
             * all workers should be in PHASE_2.
             *
             * Stream the root alias, then finish Phase 2 and wait for the merged
             * GLOBAL_PHASE2_ROOT_SAMPLE.
             */
            String rootAlias = plan.getRootAlias();
            String phaseTwoResultId = "PHASE2_ROOT_" + uid;
            String phaseTwoBarrierId = "TPCH_PHASE2_" + rootAlias + "_" + uid + "_" + System.nanoTime();

            System.out.println();
            System.out.println("=======================================================");
            System.out.println("PHASE_2 root alias: " + rootAlias);
            System.out.println("phaseTwoResultId = " + phaseTwoResultId);
            System.out.println("phaseTwoBarrierId = " + phaseTwoBarrierId);
            System.out.println("=======================================================");
            System.out.println();

            /*
             * Phase 2 is still on the legacy barrier protocol.
             *
             * The new Phase 1 transition itself is asynchronous and workers are
             * protected by requiredStateRef + tuple buffering. The legacy Phase 2
             * barrier, however, has not yet been converted to the new gate.
             *
             * This short grace period is therefore a temporary mixed-mode test
             * synchronization only. Remove it when Phase 2 is migrated.
             */
            Thread.sleep(2000L);

            System.out.println("Streaming PHASE_2 root alias from TPC-H: " + rootAlias + "...");

            long rootRowsSent = streamAlias(
                    producer,
                    DATA_TOPIC,
                    baseKey,
                    streamId,
                    catalog,
                    plan,
                    rootAlias,
                    TEST_ROW_LIMIT,
                    plan.getRequiredFieldsByAlias()
            );

            producer.flush();

            if (rootRowsSent <= 0L) {
                throw new IllegalStateException("No rows were streamed for root alias " + rootAlias);
            }

            System.out.println("Rows sent for PHASE_2 root alias " + rootAlias + ": " + rootRowsSent);
            System.out.println();

            System.out.println("Sending PHASE_2 data barrier for root alias " + rootAlias + "...");

            ObjectNode phaseTwoBarrierDatapoint = buildDataBarrierDatapoint(
                    baseKey,
                    streamId,
                    uid,
                    "PHASE2",
                    rootAlias,
                    phaseTwoBarrierId,
                    EXPECTED_WORKERS
            );

            sendJson(producer, DATA_TOPIC, baseKey, phaseTwoBarrierDatapoint);
            producer.flush();

            System.out.println("Waiting for GLOBAL_BARRIER_READY for PHASE_2...");

            JsonNode phaseTwoBarrierReady = waitForCoordinatorMessage(
                    consumer,
                    uid,
                    "GLOBAL_BARRIER_READY",
                    "barrierId",
                    phaseTwoBarrierId,
                    EXPECTED_WORKERS,
                    TIMEOUT_MS
            );

            validateNoMissingSynopsisWorkers(phaseTwoBarrierReady);
            printMatchedPayload("GLOBAL_BARRIER_READY PHASE2 " + rootAlias, phaseTwoBarrierReady);

            System.out.println("Sending FINISH_PHASE_2 request...");

            ObjectNode finishPhaseTwoRequest = buildFinishPhaseTwoRequest(
                    baseKey,
                    streamId,
                    uid,
                    phaseTwoResultId,
                    EXPECTED_WORKERS
            );

            sendJson(producer, REQUEST_TOPIC, baseKey, finishPhaseTwoRequest);
            producer.flush();

            if (WAIT_FOR_FULL_GLOBAL_RESULTS) {
                System.out.println("Waiting for GLOBAL_PHASE2_ROOT_SAMPLE...");

                JsonNode globalPhaseTwoRootSample = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "GLOBAL_PHASE2_ROOT_SAMPLE",
                        "resultId",
                        phaseTwoResultId,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validateGlobalPhaseTwoRootSample(
                        globalPhaseTwoRootSample,
                        rootAlias,
                        plan.getSampleSize(),
                        EXPECTED_WORKERS
                );

                printMatchedPayload("GLOBAL_PHASE2_ROOT_SAMPLE", globalPhaseTwoRootSample);
            } else {
                System.out.println("Skipping full GLOBAL_PHASE2_ROOT_SAMPLE wait for large-scale mode.");
            }

            System.out.println("Waiting for GLOBAL_PHASE2_ROOT_SAMPLE_READY...");
            JsonNode phaseTwoReady = waitForCoordinatorMessage(
                    consumer,
                    uid,
                    "GLOBAL_PHASE2_ROOT_SAMPLE_READY",
                    "resultId",
                    phaseTwoResultId,
                    EXPECTED_WORKERS,
                    TIMEOUT_MS
            );

            validateGlobalPhaseTwoRootSampleReady(
                    phaseTwoReady,
                    rootAlias,
                    plan.getSampleSize(),
                    EXPECTED_WORKERS
            );

            String phaseTwoStateRef = textField(phaseTwoReady, "stateRef", "");

            System.out.println("Waiting for GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED...");
            JsonNode phaseTwoInstalled = waitForCoordinatorMessage(
                    consumer,
                    uid,
                    "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED",
                    "stateRef",
                    phaseTwoStateRef,
                    EXPECTED_WORKERS,
                    TIMEOUT_MS
            );

            validateGenericInstalled(phaseTwoInstalled, EXPECTED_WORKERS, "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED");

            validateGlobalPhaseTwoInstalled(phaseTwoInstalled, rootAlias, EXPECTED_WORKERS);
            printMatchedPayload("GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED", phaseTwoInstalled);

            System.out.println();
            System.out.println("SUCCESS: PHASE_2 global root sample produced and installed.");
            System.out.println("rootAlias=" + rootAlias
                    + ", rootRowsSent=" + rootRowsSent
                    + ", sampleSize=" + plan.getSampleSize()
                    + ", expectedWorkers=" + EXPECTED_WORKERS);

            /*
             * PHASE 3:
             *
             * Replay side aliases in root-to-leaf order, excluding the root alias.
             * For WQ3 rooted at c this should be: o -> l.
             *
             * Each alias is extended in a distributed way:
             *   START_PHASE_3_ALIAS(alias)
             *   stream alias tuples
             *   data barrier
             *   FINISH_PHASE_3_ALIAS(alias)
             *   wait GLOBAL_PHASE3_ALIAS_RESULT
             *   wait GLOBAL_PHASE3_ALIAS_RESULT_READY
             *   wait PHASE3_ALIAS_SELECTIONS_INSTALLED
             */
            int phaseThreePosition = 0;

            for (String phaseThreeAlias : plan.getRootToLeafOrder()) {
                if (rootAlias.equals(phaseThreeAlias)) {
                    continue;
                }

                phaseThreePosition++;

                String phaseThreeResultId = "PHASE3_ALIAS_" + phaseThreeAlias + "_" + uid;
                String phaseThreeBarrierId = "TPCH_PHASE3_" + phaseThreeAlias + "_" + uid + "_" + System.nanoTime();
                String expectedPhaseThreeStateRef = uid
                        + "_PHASE3_ALIAS_"
                        + phaseThreeResultId
                        + "_GLOBAL_SELECTIONS";

                System.out.println();
                System.out.println("=======================================================");
                System.out.println("PHASE_3 alias " + phaseThreePosition + ": " + phaseThreeAlias);
                System.out.println("phaseThreeResultId = " + phaseThreeResultId);
                System.out.println("phaseThreeBarrierId = " + phaseThreeBarrierId);
                System.out.println("expectedPhaseThreeStateRef = " + expectedPhaseThreeStateRef);
                System.out.println("=======================================================");
                System.out.println();

                System.out.println("Sending START_PHASE_3_ALIAS request for alias " + phaseThreeAlias + "...");
                ObjectNode startPhaseThreeAliasRequest = buildStartPhaseThreeAliasRequest(
                        baseKey,
                        streamId,
                        uid,
                        phaseThreeAlias,
                        EXPECTED_WORKERS
                );

                sendJson(producer, REQUEST_TOPIC, baseKey, startPhaseThreeAliasRequest);
                producer.flush();

                /*
                 * START_PHASE_3_ALIAS is a control command. Give it a short moment to
                 * reach all workers before replaying the alias stream.
                 */
                Thread.sleep(1000L);

                System.out.println("Streaming PHASE_3 alias from TPC-H: " + phaseThreeAlias + "...");
                long phaseThreeRowsSent = streamAlias(
                        producer,
                        DATA_TOPIC,
                        baseKey,
                        streamId,
                        catalog,
                        plan,
                        phaseThreeAlias,
                        TEST_ROW_LIMIT,
                        plan.getRequiredFieldsByAlias()
                );
                producer.flush();

                if (phaseThreeRowsSent <= 0L) {
                    throw new IllegalStateException("No rows were streamed for Phase 3 alias " + phaseThreeAlias);
                }

                System.out.println("Rows sent for PHASE_3 alias " + phaseThreeAlias + ": " + phaseThreeRowsSent);
                System.out.println();

                System.out.println("Sending PHASE_3 data barrier for alias " + phaseThreeAlias + "...");
                ObjectNode phaseThreeBarrierDatapoint = buildDataBarrierDatapoint(
                        baseKey,
                        streamId,
                        uid,
                        "PHASE3",
                        phaseThreeAlias,
                        phaseThreeBarrierId,
                        EXPECTED_WORKERS
                );

                sendJson(producer, DATA_TOPIC, baseKey, phaseThreeBarrierDatapoint);
                producer.flush();

                System.out.println("Waiting for GLOBAL_BARRIER_READY for PHASE_3 alias " + phaseThreeAlias + "...");
                JsonNode phaseThreeBarrierReady = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "GLOBAL_BARRIER_READY",
                        "barrierId",
                        phaseThreeBarrierId,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validateNoMissingSynopsisWorkers(phaseThreeBarrierReady);
                printMatchedPayload("GLOBAL_BARRIER_READY PHASE3 " + phaseThreeAlias, phaseThreeBarrierReady);

                System.out.println("Sending FINISH_PHASE_3_ALIAS request for alias " + phaseThreeAlias + "...");
                ObjectNode finishPhaseThreeAliasRequest = buildFinishPhaseThreeAliasRequest(
                        baseKey,
                        streamId,
                        uid,
                        phaseThreeResultId,
                        phaseThreeAlias,
                        EXPECTED_WORKERS
                );

                sendJson(producer, REQUEST_TOPIC, baseKey, finishPhaseThreeAliasRequest);
                producer.flush();

                if (WAIT_FOR_FULL_GLOBAL_RESULTS) {
                    System.out.println("Waiting for GLOBAL_PHASE3_ALIAS_RESULT for alias " + phaseThreeAlias + "...");

                    JsonNode globalPhaseThreeAliasResult = waitForCoordinatorMessage(
                            consumer,
                            uid,
                            "GLOBAL_PHASE3_ALIAS_RESULT",
                            "resultId",
                            phaseThreeResultId,
                            EXPECTED_WORKERS,
                            TIMEOUT_MS
                    );

                    validateGlobalPhaseThreeAliasResult(
                            globalPhaseThreeAliasResult,
                            phaseThreeAlias,
                            plan.getSampleSize(),
                            EXPECTED_WORKERS
                    );

                    printMatchedPayload("GLOBAL_PHASE3_ALIAS_RESULT " + phaseThreeAlias, globalPhaseThreeAliasResult);
                } else {
                    System.out.println("Skipping full GLOBAL_PHASE3_ALIAS_RESULT wait for large-scale mode, alias="
                            + phaseThreeAlias);
                }

                System.out.println("Waiting for GLOBAL_PHASE3_ALIAS_RESULT_READY for alias " + phaseThreeAlias + "...");
                JsonNode globalPhaseThreeAliasReady = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "GLOBAL_PHASE3_ALIAS_RESULT_READY",
                        "resultId",
                        phaseThreeResultId,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validateGlobalPhaseThreeAliasReady(
                        globalPhaseThreeAliasReady,
                        expectedPhaseThreeStateRef,
                        phaseThreeAlias,
                        EXPECTED_WORKERS
                );
                printMatchedPayload("GLOBAL_PHASE3_ALIAS_RESULT_READY " + phaseThreeAlias, globalPhaseThreeAliasReady);

                System.out.println("Waiting for PHASE3_ALIAS_SELECTIONS_INSTALLED for alias " + phaseThreeAlias + "...");
                JsonNode phaseThreeSelectionsInstalled = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "PHASE3_ALIAS_SELECTIONS_INSTALLED",
                        "stateRef",
                        expectedPhaseThreeStateRef,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validatePhaseThreeAliasSelectionsInstalled(
                        phaseThreeSelectionsInstalled,
                        phaseThreeAlias,
                        EXPECTED_WORKERS
                );
                printMatchedPayload("PHASE3_ALIAS_SELECTIONS_INSTALLED " + phaseThreeAlias, phaseThreeSelectionsInstalled);

                System.out.println("Phase 3 alias " + phaseThreeAlias + " completed and installed on all workers.");
            }

            System.out.println("Sending FINISH_PHASE_3 request...");
            ObjectNode finishPhaseThreeRequest = buildFinishPhaseThreeRequest(
                    baseKey,
                    streamId,
                    uid,
                    EXPECTED_WORKERS
            );

            sendJson(producer, REQUEST_TOPIC, baseKey, finishPhaseThreeRequest);
            producer.flush();

            /*
             * FINISH_PHASE_3 currently finalizes the lifecycle inside SDEcoFlatMap.
             * Some SDE versions drop lightweight control ACKs from estimationTopic,
             * so the distributed validation milestone is the successful installation
             * of every Phase 3 alias selection above.
             */
            Thread.sleep(1000L);

            System.out.println();
            System.out.println("SUCCESS: new Phase 1 feedback + legacy Phase 2/3 Kafka/Flink integration test passed.");
            System.out.println("Validated Phase 1 aliases=" + plan.getLeafToRootOrder());
            System.out.println("Validated Phase 3 aliases=" + nonRootRootToLeafAliases(plan));
            System.out.println("expectedWorkers=" + EXPECTED_WORKERS);

        } finally {
            try {
                System.out.println("Removing OnePass benchmark synopsis uid=" + uid);

                ObjectNode removeRequest = buildOnePassRemoveRequest(baseKey, streamId, uid, EXPECTED_WORKERS);
                sendJson(producer, REQUEST_TOPIC, baseKey, removeRequest);

                producer.flush();
                Thread.sleep(500L);

            } catch (Exception cleanupError) {

                System.err.println("WARNING: OnePass cleanup failed for uid=" + uid);
                cleanupError.printStackTrace();
            }
            try {
                if(consumer != null){
                    consumer.close();
                }
            } catch (Exception ignored) {
            }

            try {
                feedbackConsumer.close();
            } catch (Exception ignored) {
            }
            try {
                producer.close();
            } catch (Exception ignored) {
            }
        }
    }

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

    private static ObjectNode buildFinishPhaseOneRequest(
            String datasetKey,
            String streamId,
            int uid,
            String resultId,
            String activeAlias,
            int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("FINISH_PHASE_1");
        param.add(resultId);
        param.add(activeAlias);
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", "FINISH_PHASE_1");
        parameters.put("onePassResultId", resultId);
        parameters.put("onePassAlias", activeAlias);
        parameters.put("phaseOneAlias", activeAlias);
        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildFinishPhaseTwoRequest(
            String datasetKey,
            String streamId,
            int uid,
            String resultId,
            int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("FINISH_PHASE_2");
        param.add(resultId);
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", "FINISH_PHASE_2");
        parameters.put("onePassResultId", resultId);
        request.set("parameters", parameters);

        return request;
    }


    private static ObjectNode buildStartPhaseThreeAliasRequest(
            String datasetKey,
            String streamId,
            int uid,
            String alias,
            int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("START_PHASE_3_ALIAS");
        param.add(alias);
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", "START_PHASE_3_ALIAS");
        parameters.put("onePassAlias", alias);
        parameters.put("phaseThreeAlias", alias);
        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildFinishPhaseThreeAliasRequest(
            String datasetKey,
            String streamId,
            int uid,
            String resultId,
            String alias,
            int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("FINISH_PHASE_3_ALIAS");
        param.add(alias);
        param.add(resultId);
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", "FINISH_PHASE_3_ALIAS");
        parameters.put("onePassResultId", resultId);
        parameters.put("onePassAlias", alias);
        parameters.put("phaseThreeAlias", alias);
        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildFinishPhaseThreeRequest(
            String datasetKey,
            String streamId,
            int uid,
            int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("FINISH_PHASE_3");
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", "FINISH_PHASE_3");
        request.set("parameters", parameters);

        return request;
    }

    private static long streamAlias(
            KafkaProducer<String, String> producer,
            String topic,
            String datasetKey,
            String streamId,
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias,
            long maxRows,
            Map<String, Set<String>> requiredFieldsByAlias) throws Exception {

        File file = tableFileForAlias(catalog, plan, alias);
        List<String> columns = columnsForAlias(catalog, plan, alias);
        String separator = separatorForAlias(catalog, plan, alias);
        Set<String> requiredFields = requiredFieldsByAlias == null ? null : requiredFieldsByAlias.get(alias);

        if (ENABLE_REQUIRED_FIELD_PRUNING && requiredFields == null) {
            throw new IllegalStateException(
                    "Required-field pruning is enabled, but plan has no required fields for alias: " + alias);
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

                if (count % 5000L == 0L) {
                    //producer.flush();
                    System.out.println("    sent " + count + " rows for alias " + alias);
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
            throw new IllegalStateException("Catalog does not define table '" + relation.getTable()
                    + "' for alias '" + alias + "'");
        }

        File file = new File(TEST_TPCH_DIR, table.getFile());

        if (!file.exists()) {
            throw new IllegalStateException("Missing TPC-H file for alias '" + alias + "': "
                    + file.getAbsolutePath());
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

    private static ObjectNode tupleJsonFromLine(
            String alias,
            List<String> columns,
            String separator,
            String line,
            Set<String> requiredFields) {

        String[] parts = line.split("\\Q" + separator + "\\E", -1);
        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", alias);

        int limit = Math.min(columns.size(), parts.length);

        for (int i = 0; i < limit; i++) {
            String fieldName = columns.get(i);

            if (ENABLE_REQUIRED_FIELD_PRUNING
                    && requiredFields != null
                    && !requiredFields.contains("*")
                    && !requiredFields.contains(fieldName)) {
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

        /*
         * Datapoint JSON must not contain "key".
         * Datapoint only has dataSetkey, streamID, and values.
         */
        datapoint.put("dataSetkey", datasetKey);
        datapoint.put("streamID", streamId);
        datapoint.set("values", tuple.deepCopy());

        return datapoint;
    }

    private static ObjectNode buildDataBarrierDatapoint(
            String datasetKey,
            String streamId,
            int uid,
            String phase,
            String alias,
            String barrierId,
            int expectedWorkers) {

        ObjectNode barrier = MAPPER.createObjectNode();

        barrier.put(ONEPASS_DATA_BARRIER_FIELD, true);
        barrier.put("uid", uid);
        barrier.put("phase", phase);
        barrier.put("alias", alias);
        barrier.put("barrierId", barrierId);
        barrier.put("expectedWorkers", expectedWorkers);

        ObjectNode datapoint = MAPPER.createObjectNode();

        /*
         * Datapoint JSON must not contain "key".
         */
        datapoint.put("dataSetkey", datasetKey);
        datapoint.put("streamID", streamId);
        datapoint.set("values", barrier);

        return datapoint;
    }

    private static void validateNoMissingSynopsisWorkers(JsonNode payload) {
        JsonNode missing = payload.get("missingSynopsisWorkers");

        if (missing != null && missing.isArray() && missing.size() > 0) {
            throw new IllegalStateException("Expected missingSynopsisWorkers to be empty, got: " + missing);
        }
    }

    private static void validateActiveAlias(JsonNode payload, String expectedAlias) {
        String actual = textField(payload, "activeAlias", "");

        if (actual == null || actual.trim().isEmpty()) {
            /*
             * Older intermediate messages may not have it yet.
             * Keep this as a warning instead of failure for compatibility.
             */
            System.out.println("WARNING: payload has no activeAlias. Expected " + expectedAlias
                    + ". Payload type=" + textField(payload, "type", ""));
            return;
        }

        if (!expectedAlias.equals(actual)) {
            throw new IllegalStateException("Expected activeAlias=" + expectedAlias
                    + ", got " + actual + ". Payload: " + payload);
        }
    }

    private static void validateGlobalPhaseOneResult(
            JsonNode payload,
            String expectedEdgeId,
            int expectedWorkers) {

        int localResultCount = intField(payload, "localResultCount", -1);

        if (localResultCount != expectedWorkers) {
            throw new IllegalStateException("Expected localResultCount=" + expectedWorkers
                    + ", got " + localResultCount + ". Payload: " + payload);
        }

        JsonNode receivedWorkers = payload.get("receivedWorkers");

        if (receivedWorkers == null || !receivedWorkers.isArray() || receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException("Expected receivedWorkers size=" + expectedWorkers
                    + ", got " + receivedWorkers + ". Payload: " + payload);
        }

        JsonNode globalPhaseOneResult = payload.get("globalPhaseOneResult");

        if (globalPhaseOneResult == null || globalPhaseOneResult.isNull()) {
            throw new IllegalStateException("GLOBAL_PHASE1_RESULT missing globalPhaseOneResult: " + payload);
        }

        JsonNode edgeSummaries = globalPhaseOneResult.get("edgeSummaries");

        if (edgeSummaries == null || !edgeSummaries.isObject()) {
            throw new IllegalStateException("GLOBAL_PHASE1_RESULT missing edgeSummaries: " + payload);
        }

        JsonNode summary = edgeSummaries.get(expectedEdgeId);

        if (summary == null || summary.isNull()) {
            throw new IllegalStateException("Missing expected edge summary " + expectedEdgeId
                    + " in " + edgeSummaries);
        }

        int keyCount = intField(summary, "numberOfKeys", 0);
        double totalWeight = doubleField(summary, "totalWeight", 0.0d);

        if (keyCount <= 0) {
            throw new IllegalStateException("Expected " + expectedEdgeId + " numberOfKeys > 0, got " + keyCount
                    + ". Summary: " + summary);
        }

        if (totalWeight <= 0.0d) {
            throw new IllegalStateException("Expected " + expectedEdgeId + " totalWeight > 0, got " + totalWeight
                    + ". Summary: " + summary);
        }

        System.out.println("Validated GLOBAL_PHASE1_RESULT edge summary: edgeId=" + expectedEdgeId
                + ", numberOfKeys=" + keyCount
                + ", totalWeight=" + totalWeight);
    }

    private static void validateGlobalPhaseOneReady(
            JsonNode payload,
            String expectedStateRef,
            int expectedWorkers) {

        String stateRef = textField(payload, "stateRef", "");

        if (!expectedStateRef.equals(stateRef)) {
            throw new IllegalStateException("Expected stateRef=" + expectedStateRef
                    + ", got " + stateRef + ". Payload: " + payload);
        }

        int localResultCount = intField(payload, "localResultCount", -1);

        if (localResultCount != expectedWorkers) {
            throw new IllegalStateException("Expected localResultCount=" + expectedWorkers
                    + ", got " + localResultCount + ". Payload: " + payload);
        }
    }

    private static void validateGlobalPhaseOneIndexInstalled(JsonNode payload, int expectedWorkers) {
        JsonNode receivedWorkers = payload.get("receivedWorkers");

        if (receivedWorkers == null || !receivedWorkers.isArray() || receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException("Expected receivedWorkers size=" + expectedWorkers
                    + ", got " + receivedWorkers + ". Payload: " + payload);
        }

        JsonNode failedWorkers = payload.get("failedWorkers");

        if (failedWorkers != null && failedWorkers.isArray() && failedWorkers.size() > 0) {
            throw new IllegalStateException("Expected failedWorkers to be empty, got: " + failedWorkers);
        }

        int installedWorkerCount = intField(payload, "installedWorkerCount", -1);

        if (installedWorkerCount != expectedWorkers) {
            throw new IllegalStateException("Expected installedWorkerCount=" + expectedWorkers
                    + ", got " + installedWorkerCount + ". Payload: " + payload);
        }
    }

    private static void validateGlobalPhaseTwoRootSample(
            JsonNode payload,
            String expectedRootAlias,
            int expectedSampleSize,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"GLOBAL_PHASE2_ROOT_SAMPLE".equals(type)) {
            throw new IllegalStateException(
                    "Expected GLOBAL_PHASE2_ROOT_SAMPLE, got " + type + ". Payload: " + payload
            );
        }

        String rootAlias = textField(payload, "rootAlias", "");

        if (!expectedRootAlias.equals(rootAlias)) {
            throw new IllegalStateException(
                    "Expected rootAlias=" + expectedRootAlias
                            + ", got " + rootAlias
                            + ". Payload: " + payload
            );
        }

        int localResultCount = intField(payload, "localResultCount", -1);

        if (localResultCount != expectedWorkers) {
            throw new IllegalStateException(
                    "Expected localResultCount=" + expectedWorkers
                            + ", got " + localResultCount
                            + ". Payload: " + payload
            );
        }

        JsonNode receivedWorkers = payload.get("receivedWorkers");

        if (receivedWorkers == null
                || !receivedWorkers.isArray()
                || receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException(
                    "Expected receivedWorkers size=" + expectedWorkers
                            + ", got " + receivedWorkers
                            + ". Payload: " + payload
            );
        }

        long rootTuplesSeen = longField(payload, "rootTuplesSeen", -1L);
        long positiveRootCandidatesSeen = longField(payload, "positiveRootCandidatesSeen", -1L);
        double totalRootGroupWeight = doubleField(payload, "totalRootGroupWeight", 0.0d);

        if (rootTuplesSeen <= 0L) {
            throw new IllegalStateException(
                    "Expected rootTuplesSeen > 0, got " + rootTuplesSeen
                            + ". Payload: " + payload
            );
        }

        if (positiveRootCandidatesSeen <= 0L) {
            throw new IllegalStateException(
                    "Expected positiveRootCandidatesSeen > 0, got "
                            + positiveRootCandidatesSeen
                            + ". Payload: " + payload
            );
        }

        if (totalRootGroupWeight <= 0.0d) {
            throw new IllegalStateException(
                    "Expected totalRootGroupWeight > 0, got "
                            + totalRootGroupWeight
                            + ". Payload: " + payload
            );
        }

        int sampleSize = intField(payload, "sampleSize", -1);

        if (sampleSize != expectedSampleSize) {
            throw new IllegalStateException(
                    "Expected sampleSize=" + expectedSampleSize
                            + ", got " + sampleSize
                            + ". Payload: " + payload
            );
        }

        int sampleInstanceCount = intField(payload, "sampleInstanceCount", -1);

        if (sampleInstanceCount != expectedSampleSize) {
            throw new IllegalStateException(
                    "Expected sampleInstanceCount=" + expectedSampleSize
                            + ", got " + sampleInstanceCount
                            + ". Payload: " + payload
            );
        }

        JsonNode sampleInstances = payload.get("sampleInstances");

        if (sampleInstances == null
                || !sampleInstances.isArray()
                || sampleInstances.size() != expectedSampleSize) {
            throw new IllegalStateException(
                    "Expected sampleInstances size=" + expectedSampleSize
                            + ", got " + sampleInstances
                            + ". Payload: " + payload
            );
        }

        JsonNode globalReservoir = payload.get("globalReservoir");

        if (globalReservoir == null || !globalReservoir.isArray() || globalReservoir.size() <= 0) {
            throw new IllegalStateException(
                    "Expected non-empty globalReservoir. Payload: " + payload
            );
        }

        System.out.println("Validated GLOBAL_PHASE2_ROOT_SAMPLE:"
                + " rootAlias=" + rootAlias
                + ", rootTuplesSeen=" + rootTuplesSeen
                + ", positiveRootCandidatesSeen=" + positiveRootCandidatesSeen
                + ", totalRootGroupWeight=" + totalRootGroupWeight
                + ", globalReservoirSize=" + globalReservoir.size()
                + ", sampleInstanceCount=" + sampleInstanceCount);
    }



    private static void validateGlobalPhaseTwoReady(
            JsonNode payload,
            String expectedStateRef,
            String expectedRootAlias,
            int expectedSampleSize,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"GLOBAL_PHASE2_ROOT_SAMPLE_READY".equals(type)) {
            throw new IllegalStateException("Expected GLOBAL_PHASE2_ROOT_SAMPLE_READY, got "
                    + type + ". Payload: " + payload);
        }

        String stateRef = textField(payload, "stateRef", "");

        if (!expectedStateRef.equals(stateRef)) {
            throw new IllegalStateException("Expected Phase 2 stateRef=" + expectedStateRef
                    + ", got " + stateRef + ". Payload: " + payload);
        }

        String rootAlias = textField(payload, "rootAlias", "");

        if (!expectedRootAlias.equals(rootAlias)) {
            throw new IllegalStateException("Expected rootAlias=" + expectedRootAlias
                    + ", got " + rootAlias + ". Payload: " + payload);
        }

        int sampleSize = intField(payload, "sampleSize", -1);

        if (sampleSize != expectedSampleSize) {
            throw new IllegalStateException("Expected Phase 2 ready sampleSize=" + expectedSampleSize
                    + ", got " + sampleSize + ". Payload: " + payload);
        }

        int expected = intField(payload, "expectedWorkers", -1);

        if (expected != expectedWorkers) {
            throw new IllegalStateException("Expected expectedWorkers=" + expectedWorkers
                    + ", got " + expected + ". Payload: " + payload);
        }
    }

    private static void validateGlobalPhaseTwoInstalled(
            JsonNode payload,
            String expectedRootAlias,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED".equals(type)) {
            throw new IllegalStateException("Expected GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED, got "
                    + type + ". Payload: " + payload);
        }

        String rootAlias = textField(payload, "rootAlias", "");

        if (!expectedRootAlias.equals(rootAlias)) {
            throw new IllegalStateException("Expected rootAlias=" + expectedRootAlias
                    + ", got " + rootAlias + ". Payload: " + payload);
        }

        validateInstallWorkers(payload, expectedWorkers);
    }

    private static void validateGlobalPhaseThreeAliasResult(
            JsonNode payload,
            String expectedAlias,
            int expectedSampleSize,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"GLOBAL_PHASE3_ALIAS_RESULT".equals(type)) {
            throw new IllegalStateException("Expected GLOBAL_PHASE3_ALIAS_RESULT, got "
                    + type + ". Payload: " + payload);
        }

        String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));

        if (!expectedAlias.equals(alias)) {
            throw new IllegalStateException("Expected phaseThreeAlias=" + expectedAlias
                    + ", got " + alias + ". Payload: " + payload);
        }

        int localResultCount = intField(payload, "localResultCount", -1);

        if (localResultCount != expectedWorkers) {
            throw new IllegalStateException("Expected localResultCount=" + expectedWorkers
                    + ", got " + localResultCount + ". Payload: " + payload);
        }

        JsonNode receivedWorkers = payload.get("receivedWorkers");

        if (receivedWorkers == null || !receivedWorkers.isArray() || receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException("Expected receivedWorkers size=" + expectedWorkers
                    + ", got " + receivedWorkers + ". Payload: " + payload);
        }

        int selectionCount = intField(payload, "selectionCount", -1);

        if (selectionCount != expectedSampleSize) {
            throw new IllegalStateException("Expected selectionCount=" + expectedSampleSize
                    + ", got " + selectionCount + ". Payload: " + payload);
        }

        JsonNode selections = payload.get("selections");

        if (selections == null || !selections.isArray() || selections.size() != expectedSampleSize) {
            throw new IllegalStateException("Expected selections size=" + expectedSampleSize
                    + ", got " + selections + ". Payload: " + payload);
        }

        int selectedRows = 0;

        for (JsonNode selection : selections) {
            if (selection != null
                    && selection.has("hasSelection")
                    && selection.get("hasSelection").asBoolean(false)) {
                selectedRows++;
            }
        }

        if (selectedRows != expectedSampleSize) {
            throw new IllegalStateException("Expected every Phase 3 selection to have hasSelection=true. selectedRows="
                    + selectedRows + ", expected=" + expectedSampleSize + ". Payload: " + payload);
        }

        long totalCandidatesSeen = longField(payload, "totalCandidatesSeen", -1L);
        double totalCandidateWeight = doubleField(payload, "totalCandidateWeight", 0.0d);

        if (totalCandidatesSeen <= 0L) {
            throw new IllegalStateException("Expected totalCandidatesSeen > 0, got "
                    + totalCandidatesSeen + ". Payload: " + payload);
        }

        if (totalCandidateWeight <= 0.0d) {
            throw new IllegalStateException("Expected totalCandidateWeight > 0, got "
                    + totalCandidateWeight + ". Payload: " + payload);
        }

        System.out.println("Validated GLOBAL_PHASE3_ALIAS_RESULT: alias=" + alias
                + ", selectionCount=" + selectionCount
                + ", totalCandidatesSeen=" + totalCandidatesSeen
                + ", totalCandidateWeight=" + totalCandidateWeight);
    }

    private static void validateGlobalPhaseThreeAliasReady(
            JsonNode payload,
            String expectedStateRef,
            String expectedAlias,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"GLOBAL_PHASE3_ALIAS_RESULT_READY".equals(type)) {
            throw new IllegalStateException("Expected GLOBAL_PHASE3_ALIAS_RESULT_READY, got "
                    + type + ". Payload: " + payload);
        }

        String stateRef = textField(payload, "stateRef", "");

        if (!expectedStateRef.equals(stateRef)) {
            throw new IllegalStateException("Expected Phase 3 stateRef=" + expectedStateRef
                    + ", got " + stateRef + ". Payload: " + payload);
        }

        String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));

        if (!expectedAlias.equals(alias)) {
            throw new IllegalStateException("Expected phaseThreeAlias=" + expectedAlias
                    + ", got " + alias + ". Payload: " + payload);
        }

        int expected = intField(payload, "expectedWorkers", -1);

        if (expected != expectedWorkers) {
            throw new IllegalStateException("Expected expectedWorkers=" + expectedWorkers
                    + ", got " + expected + ". Payload: " + payload);
        }
    }

    private static void validatePhaseThreeAliasSelectionsInstalled(
            JsonNode payload,
            String expectedAlias,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"PHASE3_ALIAS_SELECTIONS_INSTALLED".equals(type)) {
            throw new IllegalStateException("Expected PHASE3_ALIAS_SELECTIONS_INSTALLED, got "
                    + type + ". Payload: " + payload);
        }

        String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));

        if (!expectedAlias.equals(alias)) {
            throw new IllegalStateException("Expected phaseThreeAlias=" + expectedAlias
                    + ", got " + alias + ". Payload: " + payload);
        }

        validateInstallWorkers(payload, expectedWorkers);
    }

    private static void validateInstallWorkers(JsonNode payload, int expectedWorkers) {
        JsonNode receivedWorkers = payload.get("receivedWorkers");

        if (receivedWorkers == null || !receivedWorkers.isArray() || receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException("Expected receivedWorkers size=" + expectedWorkers
                    + ", got " + receivedWorkers + ". Payload: " + payload);
        }

        JsonNode failedWorkers = payload.get("failedWorkers");

        if (failedWorkers != null && failedWorkers.isArray() && failedWorkers.size() > 0) {
            throw new IllegalStateException("Expected failedWorkers to be empty, got: " + failedWorkers
                    + ". Payload: " + payload);
        }

        int installedWorkerCount = intField(payload, "installedWorkerCount", -1);

        if (installedWorkerCount >= 0 && installedWorkerCount != expectedWorkers) {
            throw new IllegalStateException("Expected installedWorkerCount=" + expectedWorkers
                    + ", got " + installedWorkerCount + ". Payload: " + payload);
        }
    }

    private static List<String> nonRootRootToLeafAliases(CompiledOnePassPlan plan) {
        List<String> aliases = new java.util.ArrayList<String>();

        if (plan == null) {
            return aliases;
        }

        String rootAlias = plan.getRootAlias();

        for (String alias : plan.getRootToLeafOrder()) {
            if (rootAlias != null && rootAlias.equals(alias)) {
                continue;
            }

            aliases.add(alias);
        }

        return aliases;
    }

    private static long tic() {
        return System.nanoTime();
    }

    private static void recordDuration(
            String label,
            long startNanos) {

        long elapsed =
                System.nanoTime()
                        - startNanos;

        benchmarkNanos.merge(
                label,
                elapsed,
                Long::sum
        );
    }

    private static long nanosFor(
            String label) {

        Long value =
                benchmarkNanos.get(
                        label
                );

        return value == null
                ? 0L
                : value.longValue();
    }

    private static double secondsFor(
            String label) {

        return nanosFor(label)
                / 1_000_000_000.0d;
    }

    private static void recordCount(
            String label,
            long value) {

        benchmarkCounts.merge(
                label,
                value,
                Long::sum
        );
    }

    private static long countFor(
            String label) {

        Long value =
                benchmarkCounts.get(
                        label
                );

        return value == null
                ? 0L
                : value.longValue();
    }

    private static double rowsPerSecond(
            long rows,
            double seconds) {

        if (seconds <= 0.0d) {
            return 0.0d;
        }

        return rows / seconds;
    }

    private static void printTimingLine(
            String label,
            double seconds) {

        System.out.printf(
                "%-45s %12.3f s%n",
                label,
                seconds
        );
    }

    private static void printRateLine(
            String label,
            double rowsPerSecond) {

        System.out.printf(
                "%-45s %12.3f rows/s%n",
                label,
                rowsPerSecond
        );
    }

    /**
     * Phase 1-only summary for direct comparison with the existing
     * single-worker benchmark.
     *
     * Comparable metric:
     *   phase1_stream_send
     *
     * New-protocol completion metric:
     *   phase1_feedback_total
     *
     * For the legacy single-worker test, the closest equivalent to
     * phase1_feedback_total is:
     *
     *   phase1_data_barrier_ack
     *   + phase1_finish_ack
     *   + phase1_status
     *
     * because all of those were required before Phase 2 could start.
     */
    private static void printPhaseOneBenchmarkSummary(
            CompiledOnePassPlan plan) {

        double streamSeconds =
                secondsFor(
                        "phase1_stream_send"
                );

        double feedbackSeconds =
                secondsFor(
                        "phase1_feedback_total"
                );

        double totalSeconds =
                secondsFor(
                        "phase1_total_observed"
                );

        long rows =
                countFor(
                        "phase1_rows_sent"
                );

        System.out.println();
        System.out.println(
                "=== Multi-worker OnePass Phase 1 benchmark ==="
        );

        System.out.println(
                "TEST_ROW_LIMIT:  "
                        + formatRowLimit(
                        TEST_ROW_LIMIT
                )
        );

        System.out.println(
                "workers:         "
                        + EXPECTED_WORKERS
        );

        System.out.println(
                "leaf-to-root:    "
                        + plan.getLeafToRootOrder()
        );

        printTimingLine(
                "phase1_stream_send",
                streamSeconds
        );

        printTimingLine(
                "phase1_feedback_total",
                feedbackSeconds
        );

        printTimingLine(
                "phase1_total_observed",
                totalSeconds
        );

        System.out.println(
                "phase1_rows_sent: "
                        + rows
        );

        printRateLine(
                "phase1_send_rows_per_sec",
                rowsPerSecond(
                        rows,
                        streamSeconds
                )
        );

        printRateLine(
                "phase1_end_to_end_rows_per_sec",
                rowsPerSecond(
                        rows,
                        totalSeconds
                )
        );

        System.out.println();
        System.out.println(
                "Per-alias timings:"
        );

        for (String alias
                : plan.getLeafToRootOrder()) {

            long aliasRows =
                    countFor(
                            "phase1_alias_"
                                    + alias
                                    + "_rows_sent"
                    );

            double aliasStream =
                    secondsFor(
                            "phase1_alias_"
                                    + alias
                                    + "_stream_send"
                    );

            double aliasFeedback =
                    secondsFor(
                            "phase1_alias_"
                                    + alias
                                    + "_feedback"
                    );

            double aliasTotal =
                    secondsFor(
                            "phase1_alias_"
                                    + alias
                                    + "_total"
                    );

            System.out.println(
                    "  alias="
                            + alias
                            + ", rows="
                            + aliasRows
                            + ", stream_s="
                            + aliasStream
                            + ", feedback_s="
                            + aliasFeedback
                            + ", total_s="
                            + aliasTotal
            );
        }

        System.out.println(
                "============================================"
        );
        System.out.println();
    }

    private static void writePhaseOneBenchmarkCsv(
            CompiledOnePassPlan plan,
            String implementation) throws Exception {

        if (!WRITE_PHASE1_BENCHMARK_CSV) {
            return;
        }

        double streamSeconds =
                secondsFor(
                        "phase1_stream_send"
                );

        double feedbackSeconds =
                secondsFor(
                        "phase1_feedback_total"
                );

        double totalSeconds =
                secondsFor(
                        "phase1_total_observed"
                );

        long rows =
                countFor(
                        "phase1_rows_sent"
                );

        File csvFile =
                new File(
                        PHASE1_BENCHMARK_CSV_PATH
                );

        boolean writeHeader =
                !csvFile.exists()
                        || csvFile.length()
                        == 0L;

        FileWriter writer =
                new FileWriter(
                        csvFile,
                        true
                );

        try {
            if (writeHeader) {
                writer.write(
                        "timestamp_ms,"
                                + "implementation,"
                                + "workers,"
                                + "query_name,"
                                + "seed,"
                                + "test_row_limit,"
                                + "sample_size_limit,"
                                + "root_alias,"
                                + "leaf_to_root_order,"
                                + "phase1_rows_sent,"
                                + "phase1_stream_send_s,"
                                + "phase1_feedback_total_s,"
                                + "phase1_total_observed_s,"
                                + "phase1_send_rows_per_sec,"
                                + "phase1_end_to_end_rows_per_sec,"
                                + "phase1_alias_rows_sent,"
                                + "phase1_alias_stream_send_s,"
                                + "phase1_alias_feedback_s,"
                                + "phase1_alias_total_s"
                                + System.lineSeparator()
                );
            }

            writer.write(
                    Long.toString(
                            System.currentTimeMillis()
                    )
                            + ","
                            + csv(
                            implementation
                    )
                            + ","
                            + EXPECTED_WORKERS
                            + ","
                            + csv(
                            plan.getQueryName()
                    )
                            + ","
                            + csv(
                            plan.getDatasetSeed()
                    )
                            + ","
                            + csv(
                            formatRowLimit(
                                    TEST_ROW_LIMIT
                            )
                    )
                            + ","
                            + plan.getSampleSize()
                            + ","
                            + csv(
                            plan.getRootAlias()
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    plan.getLeafToRootOrder()
                            )
                    )
                            + ","
                            + rows
                            + ","
                            + Double.toString(
                            streamSeconds
                    )
                            + ","
                            + Double.toString(
                            feedbackSeconds
                    )
                            + ","
                            + Double.toString(
                            totalSeconds
                    )
                            + ","
                            + Double.toString(
                            rowsPerSecond(
                                    rows,
                                    streamSeconds
                            )
                    )
                            + ","
                            + Double.toString(
                            rowsPerSecond(
                                    rows,
                                    totalSeconds
                            )
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    phaseOneAliasRowsMap(
                                            plan
                                    )
                            )
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    phaseOneAliasSecondsMap(
                                            plan,
                                            "stream_send"
                                    )
                            )
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    phaseOneAliasSecondsMap(
                                            plan,
                                            "feedback"
                                    )
                            )
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    phaseOneAliasSecondsMap(
                                            plan,
                                            "total"
                                    )
                            )
                    )
                            + System.lineSeparator()
            );

        } finally {
            writer.close();
        }

        System.out.println(
                "Phase 1 benchmark CSV appended to: "
                        + csvFile.getAbsolutePath()
        );
    }

    private static Map<String, Long> phaseOneAliasRowsMap(
            CompiledOnePassPlan plan) {

        Map<String, Long> out =
                new LinkedHashMap<String, Long>();

        for (String alias
                : plan.getLeafToRootOrder()) {

            out.put(
                    alias,
                    countFor(
                            "phase1_alias_"
                                    + alias
                                    + "_rows_sent"
                    )
            );
        }

        return out;
    }

    private static Map<String, Double> phaseOneAliasSecondsMap(
            CompiledOnePassPlan plan,
            String suffix) {

        Map<String, Double> out =
                new LinkedHashMap<String, Double>();

        for (String alias
                : plan.getLeafToRootOrder()) {

            out.put(
                    alias,
                    secondsFor(
                            "phase1_alias_"
                                    + alias
                                    + "_"
                                    + suffix
                    )
            );
        }

        return out;
    }

    private static String csv(
            String value) {

        if (value == null) {
            return "";
        }

        String escaped =
                value.replace(
                        "\"",
                        "\"\""
                );

        return "\""
                + escaped
                + "\"";
    }

    private static String formatRowLimit(
            long rowLimit) {

        if (rowLimit < 0L) {
            return "FULL";
        }

        return Long.toString(
                rowLimit
        );
    }

    /**
     * RequestTopic trace for one merged Phase 1 state.
     */
    private static final class PhaseOneFeedbackTrace {

        private String resultId = "";
        private String stateRef = "";
        private int chunkCount = -1;
        private int totalEntryCount = -1;
        private String checksum = "";
        private String nextCommand = "";
        private String nextAlias = "";

        /*
         * Populated only when EXPORT_FINAL_PHASE1_INDEX is enabled for the
         * final Phase 1 alias. This avoids keeping a second full copy of each
         * intermediate index during normal Kafka/Flink tests.
         */
        private JsonNode seenTuplesByAlias;
        private JsonNode edgeSummaries;

        private final Map<Integer, ArrayNode> capturedChunkEntries =
                new java.util.HashMap<Integer, ArrayNode>();

        private ObjectNode reconstructedEdgeIndexes;
    }

    /**
     * Observes and validates the real Phase 1 Kafka feedback protocol.
     *
     * RequestTopic receives one logical copy of:
     *
     *   GLOBAL_STATE_BEGIN
     *   GLOBAL_STATE_CHUNK x N
     *   GLOBAL_STATE_COMMIT
     *   START_NEXT_ALIAS / START_PHASE_2
     *
     * The test consumer has its own Kafka consumer group, so observing the
     * records does not interfere with the running SDE RequestTopic consumer.
     */
    /**
     * Observes the Phase 1 feedback protocol on RequestTopic.
     *
     * IMPORTANT:
     * The Kafka/Flink transport is allowed to deliver BEGIN / CHUNK / COMMIT /
     * transition records to this observer in a different arrival order.
     * Correctness is based on stateRef + chunkId + chunkCount, not on the
     * observer seeing BEGIN first.
     *
     * This mirrors OnePassPhaseOneWorkerProtocol, which can receive chunks or
     * the transition before the state is complete and activates the transition
     * only after requiredStateRef has been installed.
     */
    private static PhaseOneFeedbackTrace waitForPhaseOneFeedbackSequence(
            KafkaConsumer<String, String> consumer,
            int uid,
            String expectedResultId,
            String expectedStateRef,
            String expectedNextCommand,
            String expectedNextAlias,
            boolean captureFullIndex,
            long timeoutMs) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + timeoutMs;

        PhaseOneFeedbackTrace trace =
                new PhaseOneFeedbackTrace();

        trace.resultId =
                expectedResultId;

        boolean beginSeen = false;
        boolean commitSeen = false;
        boolean transitionSeen = false;

        Set<Integer> chunksSeen =
                new java.util.HashSet<Integer>();

        int reconstructedEntryCount = 0;
        int observedChunkCount = -1;
        int recordsSeen = 0;

        String commitChecksum = "";

        while (System.currentTimeMillis() < deadline) {

            ConsumerRecords<String, String> records =
                    consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {

                recordsSeen++;

                String value = record.value();

                if (value == null
                        || value.trim().isEmpty()) {
                    continue;
                }

                JsonNode request;

                try {
                    request = MAPPER.readTree(value);
                } catch (Exception ignored) {
                    continue;
                }

                if (!matchesIntField(request, "uid", uid)) {
                    continue;
                }

                if (intField(request, "synopsisID", -1) != SYNOPSIS_ID) {
                    continue;
                }

                if (intField(request, "requestID", -1) != REQUEST_UPDATE) {
                    continue;
                }

                JsonNode payload = request.get("parameters");

                if (payload == null
                        || payload.isNull()
                        || !payload.isObject()) {
                    continue;
                }

                String resultId =
                        textField(payload, "resultId", "");

                if (!expectedResultId.equals(resultId)) {
                    continue;
                }

                String stateRef =
                        textField(payload, "stateRef",
                                textField(payload, "requiredStateRef", ""));

                if (!expectedStateRef.equals(stateRef)) {
                    continue;
                }

                String type =
                        textField(payload, "type", "");

                if ("GLOBAL_STATE_BEGIN".equals(type)) {

                    if (beginSeen) {
                        // Identical duplicate delivery is harmless for this E2E observer.
                        continue;
                    }

                    trace.stateRef =
                            textField(payload, "stateRef", "");

                    trace.chunkCount =
                            intField(payload, "chunkCount", -1);

                    trace.totalEntryCount =
                            intField(payload, "totalEntryCount", -1);

                    trace.checksum =
                            textField(payload, "checksum", "");

                    if (trace.chunkCount <= 0) {
                        throw new IllegalStateException(
                                "BEGIN has invalid chunkCount: " + payload
                        );
                    }

                    if (trace.totalEntryCount < 0) {
                        throw new IllegalStateException(
                                "BEGIN has invalid totalEntryCount: " + payload
                        );
                    }

                    if (trace.checksum.isEmpty()) {
                        throw new IllegalStateException(
                                "BEGIN has no checksum: " + payload
                        );
                    }

                    if (observedChunkCount > 0
                            && observedChunkCount != trace.chunkCount) {
                        throw new IllegalStateException(
                                "BEGIN chunkCount="
                                        + trace.chunkCount
                                        + " conflicts with previously observed chunkCount="
                                        + observedChunkCount
                        );
                    }

                    observedChunkCount = trace.chunkCount;
                    beginSeen = true;

                    if (captureFullIndex) {

                        JsonNode seenTuplesByAlias =
                                payload.get(
                                        "seenTuplesByAlias"
                                );

                        JsonNode edgeSummaries =
                                payload.get(
                                        "edgeSummaries"
                                );

                        if (seenTuplesByAlias != null
                                && !seenTuplesByAlias.isNull()) {

                            trace.seenTuplesByAlias =
                                    seenTuplesByAlias.deepCopy();
                        }

                        if (edgeSummaries != null
                                && !edgeSummaries.isNull()) {

                            trace.edgeSummaries =
                                    edgeSummaries.deepCopy();
                        }
                    }

                    System.out.println(
                            "  Phase1 feedback BEGIN observed: stateRef="
                                    + trace.stateRef
                                    + ", chunkCount="
                                    + trace.chunkCount
                                    + ", totalEntryCount="
                                    + trace.totalEntryCount
                                    + ", chunksAlreadySeen="
                                    + chunksSeen.size()
                    );
                }

                else if ("GLOBAL_STATE_CHUNK".equals(type)) {

                    int chunkId =
                            intField(payload, "chunkId", -1);

                    int chunkCount =
                            intField(payload, "chunkCount", -1);

                    if (chunkId < 0
                            || chunkCount <= 0
                            || chunkId >= chunkCount) {
                        throw new IllegalStateException(
                                "Invalid chunk metadata: " + payload
                        );
                    }

                    if (observedChunkCount > 0
                            && observedChunkCount != chunkCount) {
                        throw new IllegalStateException(
                                "Conflicting chunkCount values. previous="
                                        + observedChunkCount
                                        + ", current="
                                        + chunkCount
                                        + ". Payload="
                                        + payload
                        );
                    }

                    observedChunkCount = chunkCount;

                    if (beginSeen
                            && trace.chunkCount != chunkCount) {
                        throw new IllegalStateException(
                                "Chunk count mismatch. BEGIN="
                                        + trace.chunkCount
                                        + ", chunk="
                                        + chunkCount
                                        + ". Payload="
                                        + payload
                        );
                    }

                    JsonNode entries = payload.get("entries");

                    if (entries == null
                            || !entries.isArray()) {
                        throw new IllegalStateException(
                                "Chunk has no entries array: " + payload
                        );
                    }

                    int entryCount =
                            intField(payload, "entryCount", -1);

                    if (entryCount != entries.size()) {
                        throw new IllegalStateException(
                                "Chunk entryCount mismatch: " + payload
                        );
                    }

                    if (chunksSeen.add(chunkId)) {
                        reconstructedEntryCount += entryCount;

                        if (captureFullIndex) {
                            trace.capturedChunkEntries.put(
                                    chunkId,
                                    (ArrayNode) entries.deepCopy()
                            );
                        }
                    }

                    System.out.println(
                            "  Phase1 feedback CHUNK observed: "
                                    + chunkId
                                    + "/"
                                    + chunkCount
                                    + ", beginSeen="
                                    + beginSeen
                    );
                }

                else if ("GLOBAL_STATE_COMMIT".equals(type)) {

                    int commitChunkCount =
                            intField(payload, "chunkCount", -1);

                    int commitTotalEntryCount =
                            intField(payload, "totalEntryCount", -1);

                    commitChecksum =
                            textField(payload, "checksum", "");

                    if (commitChunkCount <= 0
                            || commitTotalEntryCount < 0
                            || commitChecksum.isEmpty()) {
                        throw new IllegalStateException(
                                "Invalid COMMIT payload: " + payload
                        );
                    }

                    if (observedChunkCount > 0
                            && observedChunkCount != commitChunkCount) {
                        throw new IllegalStateException(
                                "COMMIT chunkCount="
                                        + commitChunkCount
                                        + " conflicts with observed chunkCount="
                                        + observedChunkCount
                        );
                    }

                    observedChunkCount = commitChunkCount;

                    if (beginSeen) {
                        if (trace.chunkCount != commitChunkCount) {
                            throw new IllegalStateException(
                                    "BEGIN/COMMIT chunkCount mismatch"
                            );
                        }

                        if (trace.totalEntryCount != commitTotalEntryCount) {
                            throw new IllegalStateException(
                                    "BEGIN/COMMIT totalEntryCount mismatch"
                            );
                        }

                        if (!trace.checksum.equals(commitChecksum)) {
                            throw new IllegalStateException(
                                    "BEGIN/COMMIT checksum mismatch"
                            );
                        }
                    }

                    String commitNextCommand =
                            textField(payload, "nextCommand", "");

                    String commitNextAlias =
                            textField(payload, "nextAlias", "");

                    if (!expectedNextCommand.equals(commitNextCommand)) {
                        throw new IllegalStateException(
                                "Expected COMMIT nextCommand="
                                        + expectedNextCommand
                                        + ", got "
                                        + commitNextCommand
                        );
                    }

                    if (!expectedNextAlias.equals(commitNextAlias)) {
                        throw new IllegalStateException(
                                "Expected COMMIT nextAlias="
                                        + expectedNextAlias
                                        + ", got "
                                        + commitNextAlias
                        );
                    }

                    commitSeen = true;

                    System.out.println(
                            "  Phase1 feedback COMMIT observed: beginSeen="
                                    + beginSeen
                                    + ", chunksSeen="
                                    + chunksSeen.size()
                                    + "/"
                                    + commitChunkCount
                    );
                }

                else if (expectedNextCommand.equals(type)) {

                    String requiredStateRef =
                            textField(payload, "requiredStateRef", "");

                    String nextAlias =
                            textField(payload, "nextAlias", "");

                    if (!expectedStateRef.equals(requiredStateRef)) {
                        throw new IllegalStateException(
                                "Expected requiredStateRef="
                                        + expectedStateRef
                                        + ", got "
                                        + requiredStateRef
                                        + ". Payload="
                                        + payload
                        );
                    }

                    if (!expectedNextAlias.equals(nextAlias)) {
                        throw new IllegalStateException(
                                "Expected transition nextAlias="
                                        + expectedNextAlias
                                        + ", got "
                                        + nextAlias
                                        + ". Payload="
                                        + payload
                        );
                    }

                    trace.nextCommand = type;
                    trace.nextAlias = nextAlias;
                    transitionSeen = true;

                    System.out.println(
                            "  Phase1 feedback transition observed: "
                                    + type
                                    + " -> "
                                    + nextAlias
                                    + ", commitSeen="
                                    + commitSeen
                    );
                }

                /*
                 * Do not require arrival order.
                 * Return only when the complete protocol has been observed.
                 */
                if (beginSeen
                        && commitSeen
                        && transitionSeen
                        && trace.chunkCount > 0
                        && chunksSeen.size() == trace.chunkCount) {

                    if (reconstructedEntryCount
                            != trace.totalEntryCount) {
                        throw new IllegalStateException(
                                "Reconstructed entry count mismatch. expected="
                                        + trace.totalEntryCount
                                        + ", actual="
                                        + reconstructedEntryCount
                        );
                    }

                    if (!trace.checksum.equals(commitChecksum)) {
                        throw new IllegalStateException(
                                "BEGIN/COMMIT checksum mismatch. begin="
                                        + trace.checksum
                                        + ", commit="
                                        + commitChecksum
                        );
                    }

                    if (captureFullIndex) {
                        trace.reconstructedEdgeIndexes =
                                reconstructPhaseOneEdgeIndexes(
                                        trace
                                );
                    }

                    return trace;
                }
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for complete Phase 1 feedback protocol. "
                        + "uid="
                        + uid
                        + ", resultId="
                        + expectedResultId
                        + ", stateRef="
                        + expectedStateRef
                        + ", beginSeen="
                        + beginSeen
                        + ", commitSeen="
                        + commitSeen
                        + ", transitionSeen="
                        + transitionSeen
                        + ", chunksSeen="
                        + chunksSeen.size()
                        + "/"
                        + (trace.chunkCount > 0 ? trace.chunkCount : observedChunkCount)
                        + ", recordsSeen="
                        + recordsSeen
        );
    }

    private static JsonNode waitForCoordinatorMessage(
            KafkaConsumer<String, String> consumer,
            int uid,
            String expectedType,
            String idField,
            String expectedId,
            int expectedWorkers,
            long timeoutMs) throws Exception {

        long deadline = System.currentTimeMillis() + timeoutMs;
        int recordsSeen = 0;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                recordsSeen++;

                String value = record.value();

                if (value == null || value.trim().isEmpty()) {
                    continue;
                }

                JsonNode envelope;

                try {
                    envelope = MAPPER.readTree(value);
                } catch (Exception ignored) {
                    continue;
                }

                if (!matchesIntField(envelope, "uid", uid)) {
                    continue;
                }

                JsonNode payload = extractEstimationPayload(envelope);

                if (payload == null || payload.isNull()) {
                    continue;
                }

                String type = textField(payload, "type", "");

                if (!expectedType.equals(type)) {
                    continue;
                }

                if (idField != null && expectedId != null) {
                    String actualId = textField(payload, idField, "");

                    if (!expectedId.equals(actualId)) {
                        continue;
                    }
                }

                int received = payload.has("receivedWorkers") && payload.get("receivedWorkers").isArray()
                        ? payload.get("receivedWorkers").size()
                        : -1;

                System.out.println("Candidate " + expectedType
                        + ": " + idField + "=" + expectedId
                        + ", receivedWorkers=" + received
                        + "/" + expectedWorkers);

                return payload;
            }
        }

        throw new IllegalStateException("Timed out waiting for " + expectedType
                + ", uid=" + uid
                + ", " + idField + "=" + expectedId
                + ", expectedWorkers=" + expectedWorkers
                + ", recordsSeen=" + recordsSeen);
    }

    private static JsonNode extractEstimationPayload(JsonNode envelope) throws Exception {
        if (envelope == null || envelope.isNull()) {
            return null;
        }

        JsonNode estimationNode = envelope.get("estimation");

        if (estimationNode == null || estimationNode.isNull()) {
            return envelope;
        }

        if (estimationNode.isTextual()) {
            String nested = estimationNode.asText();

            if (nested == null || nested.trim().isEmpty()) {
                return null;
            }

            return MAPPER.readTree(nested);
        }

        return estimationNode;
    }

    private static boolean matchesIntField(JsonNode node, String fieldName, int expectedValue) {
        if (node == null || node.isNull()) {
            return false;
        }

        JsonNode field = node.get(fieldName);

        return field != null && field.asInt(Integer.MIN_VALUE) == expectedValue;
    }

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

    private static void printMatchedPayload(String label, JsonNode payload) throws Exception {
        if (!PRINT_MATCHED_PAYLOADS) {
            return;
        }

        System.out.println();
        System.out.println(label + " received:");
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(payload));
        System.out.println();
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("batch.size", "16384");
        props.put("linger.ms", "1");
        props.put("buffer.memory", "268435456");
        props.put("max.request.size", "104857600");
        props.put("delivery.timeout.ms", "900000");
        props.put("request.timeout.ms", "300000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    private static KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("request.timeout.ms", "300000");
        props.put("fetch.max.bytes", "104857600");
        props.put("max.partition.fetch.bytes", "104857600");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

    private static KafkaConsumer<String, String> createObserverConsumer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        //This consumer is a passive test observer. It uses direct partition assignment, not Kafka group coordination.
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("request.timeout.ms", "300000");
        props.put("fetch.max.bytes", "104857600");
        props.put("max.partition.fetch.bytes", "104857600");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

    private static void initializePhaseOneFeedbackObserver(KafkaConsumer<String, String> consumer, String topic) {

        List<TopicPartition> partitions = new ArrayList<TopicPartition>();

        for (int partition = 0; partition < REQUEST_TOPIC_PARTITIONS; partition++) {
            partitions.add(new TopicPartition(topic, partition));
        }

        System.out.println("Assigning Phase 1 feedback observer directly to " + partitions.size() + " RequestTopic partitions...");
        consumer.assign(partitions);

        /*
         * No group coordination.
         *
         * poll() resolves the initial position according to
         * auto.offset.reset=latest BEFORE the ADD request is sent.
         */
        System.out.println("Initializing feedback observer positions...");

        consumer.poll(1000L);

        System.out.println("Phase 1 feedback observer READY on " + topic);
    }

    private static void sendJsonAsync(KafkaProducer<String, String> producer, String topic, String key, JsonNode json) {
        producer.send(new ProducerRecord<String, String>(topic, key, json.toString()));
    }

    private static void sendJson(
            KafkaProducer<String, String> producer,
            String topic,
            String key,
            JsonNode json) throws Exception {

        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topic, key, json.toString()));
        future.get();
    }

    private static void drainConsumer(KafkaConsumer<String, String> consumer) {
        consumer.poll(500);
        consumer.poll(500);
    }

    /**
     * Reconstructs the full Phase 1 edgeIndexes object from the transport
     * chunks emitted by OnePassPhaseOneRequestSplitter.
     *
     * The splitter flattens:
     *
     *   edgeId -> joinKey -> globalWeight
     *
     * into transport entries:
     *
     *   { edgeId, joinKey, globalWeight }
     *
     * This method reverses that transport representation for the external
     * Phase 1 validator.
     */
    private static ObjectNode reconstructPhaseOneEdgeIndexes(
            PhaseOneFeedbackTrace trace) throws Exception {

        if (trace == null) {
            throw new IllegalArgumentException(
                    "trace must not be null"
            );
        }

        if (trace.chunkCount <= 0) {
            throw new IllegalStateException(
                    "Cannot reconstruct Phase 1 index: invalid chunkCount="
                            + trace.chunkCount
            );
        }

        if (trace.capturedChunkEntries.size()
                != trace.chunkCount) {

            throw new IllegalStateException(
                    "Cannot reconstruct Phase 1 index. capturedChunks="
                            + trace.capturedChunkEntries.size()
                            + "/"
                            + trace.chunkCount
            );
        }

        ObjectNode edgeIndexes =
                MAPPER.createObjectNode();

        ArrayNode flattenedEntries =
                MAPPER.createArrayNode();

        int reconstructedEntryCount =
                0;

        for (int chunkId = 0;
             chunkId < trace.chunkCount;
             chunkId++) {

            ArrayNode entries =
                    trace.capturedChunkEntries
                            .get(
                                    chunkId
                            );

            if (entries == null) {
                throw new IllegalStateException(
                        "Missing captured Phase 1 chunk "
                                + chunkId
                                + "/"
                                + trace.chunkCount
                );
            }

            for (JsonNode entry : entries) {

                String edgeId =
                        textField(
                                entry,
                                "edgeId",
                                ""
                        );

                String joinKey =
                        textField(
                                entry,
                                "joinKey",
                                ""
                        );

                JsonNode globalWeightNode =
                        entry.get(
                                "globalWeight"
                        );

                if (edgeId.isEmpty()
                        || joinKey.isEmpty()
                        || globalWeightNode == null
                        || !globalWeightNode.isNumber()) {

                    throw new IllegalStateException(
                            "Invalid flattened Phase 1 entry: "
                                    + entry
                    );
                }

                ObjectNode edgeIndex;

                JsonNode existingEdge =
                        edgeIndexes.get(
                                edgeId
                        );

                if (existingEdge == null) {

                    edgeIndex =
                            edgeIndexes.putObject(
                                    edgeId
                            );

                } else if (existingEdge.isObject()) {

                    edgeIndex =
                            (ObjectNode) existingEdge;

                } else {

                    throw new IllegalStateException(
                            "Invalid reconstructed edge object for edgeId="
                                    + edgeId
                    );
                }

                if (edgeIndex.has(joinKey)) {
                    throw new IllegalStateException(
                            "Duplicate Phase 1 index entry for edgeId="
                                    + edgeId
                                    + ", joinKey="
                                    + joinKey
                    );
                }

                edgeIndex.set(
                        joinKey,
                        globalWeightNode.deepCopy()
                );

                flattenedEntries.add(
                        entry.deepCopy()
                );

                reconstructedEntryCount++;
            }
        }

        if (reconstructedEntryCount
                != trace.totalEntryCount) {

            throw new IllegalStateException(
                    "Full Phase 1 reconstruction entry-count mismatch. expected="
                            + trace.totalEntryCount
                            + ", actual="
                            + reconstructedEntryCount
            );
        }

        /*
         * Verify that the exact chunk-order reconstruction corresponds to the
         * state whose checksum was announced in BEGIN/COMMIT.
         */
        String reconstructedChecksum =
                sha256Hex(
                        flattenedEntries.toString()
                );

        if (!trace.checksum.equals(
                reconstructedChecksum
        )) {

            throw new IllegalStateException(
                    "Full Phase 1 reconstruction checksum mismatch. expected="
                            + trace.checksum
                            + ", actual="
                            + reconstructedChecksum
            );
        }

        return edgeIndexes;
    }

    private static String sha256Hex(
            String value) throws Exception {

        java.security.MessageDigest digest =
                java.security.MessageDigest
                        .getInstance(
                                "SHA-256"
                        );

        byte[] hash =
                digest.digest(
                        value.getBytes(
                                java.nio.charset.StandardCharsets.UTF_8
                        )
                );

        StringBuilder out =
                new StringBuilder(
                        hash.length * 2
                );

        for (byte b : hash) {
            out.append(
                    String.format(
                            "%02x",
                            b & 0xff
                    )
            );
        }

        return out.toString();
    }

    /**
     * Writes the final globally merged Phase 1 index in the same top-level
     * format expected by validate_onepass_catalog_phase1.py.
     *
     * Under the new architecture the full GLOBAL_PHASE1_RESULT is no longer
     * written to estimationTopic. The test therefore reconstructs edgeIndexes
     * from BEGIN/CHUNK/COMMIT on RequestTopic.
     */
    private static void writeFinalPhaseOneIndexForPythonValidator(
            PhaseOneFeedbackTrace feedback,
            int uid,
            String queryName,
            String rootAlias,
            String finalAlias,
            String finalEdgeId) throws Exception {

        if (feedback == null) {
            throw new IllegalStateException(
                    "Cannot export final Phase 1 index because feedback is null."
            );
        }

        if (feedback.reconstructedEdgeIndexes == null) {

            throw new IllegalStateException(
                    "Cannot export final Phase 1 index because edgeIndexes "
                            + "were not captured/reconstructed. Make sure "
                            + "EXPORT_FINAL_PHASE1_INDEX=true."
            );
        }

        if (feedback.edgeSummaries == null
                || !feedback.edgeSummaries.isObject()) {

            throw new IllegalStateException(
                    "Cannot export final Phase 1 index because BEGIN did not "
                            + "contain edgeSummaries."
            );
        }

        ObjectNode export =
                MAPPER.createObjectNode();

        /*
         * IMPORTANT:
         * The Python validator expects edgeIndexes / edgeSummaries at the
         * top level.
         */
        export.put(
                "type",
                "ONEPASS_PHASE1_FULL_INDEX_EXPORT"
        );

        export.put(
                "implementation",
                "parallel-sde"
        );

        export.put(
                "uid",
                uid
        );

        export.put(
                "queryName",
                queryName == null
                        ? ""
                        : queryName
        );

        export.put(
                "rootAlias",
                rootAlias == null
                        ? ""
                        : rootAlias
        );

        export.put(
                "finalAlias",
                finalAlias == null
                        ? ""
                        : finalAlias
        );

        export.put(
                "finalEdgeId",
                finalEdgeId == null
                        ? ""
                        : finalEdgeId
        );

        export.put(
                "resultId",
                feedback.resultId
        );

        export.put(
                "stateRef",
                feedback.stateRef
        );

        export.put(
                "chunkCount",
                feedback.chunkCount
        );

        export.put(
                "totalEntryCount",
                feedback.totalEntryCount
        );

        export.put(
                "checksum",
                feedback.checksum
        );

        if (feedback.seenTuplesByAlias != null
                && !feedback.seenTuplesByAlias.isNull()) {

            export.set(
                    "seenTuplesByAlias",
                    feedback.seenTuplesByAlias
                            .deepCopy()
            );
        }

        export.set(
                "edgeIndexes",
                feedback.reconstructedEdgeIndexes
                        .deepCopy()
        );

        export.set(
                "edgeSummaries",
                feedback.edgeSummaries
                        .deepCopy()
        );

        File outputFile =
                new File(
                        PHASE1_INDEX_EXPORT_DIR
                );

        FileWriter writer =
                new FileWriter(
                        outputFile
                );

        try {
            writer.write(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(
                                    export
                            )
            );

            writer.write(
                    System.lineSeparator()
            );

        } finally {
            writer.close();
        }

        System.out.println();
        System.out.println(
                "Exported final Phase 1 index reconstructed from "
                        + "RequestTopic feedback:"
        );

        System.out.println(
                outputFile.getAbsolutePath()
        );

        System.out.println(
                "queryName="
                        + queryName
        );

        System.out.println(
                "rootAlias="
                        + rootAlias
        );

        System.out.println(
                "finalAlias="
                        + finalAlias
        );

        System.out.println(
                "finalEdgeId="
                        + finalEdgeId
        );

        System.out.println(
                "stateRef="
                        + feedback.stateRef
        );

        System.out.println(
                "chunkCount="
                        + feedback.chunkCount
                        + ", totalEntryCount="
                        + feedback.totalEntryCount
        );

        System.out.println();
    }

    private static void validateGlobalPhaseTwoRootSampleReady(
            JsonNode payload,
            String expectedRootAlias,
            int expectedSampleSize,
            int expectedWorkers) {

        String type = textField(payload, "type", "");

        if (!"GLOBAL_PHASE2_ROOT_SAMPLE_READY".equals(type)) {
            throw new IllegalStateException("Expected GLOBAL_PHASE2_ROOT_SAMPLE_READY, got " + type
                    + ". Payload: " + payload);
        }

        String rootAlias = textField(payload, "rootAlias", "");

        if (!expectedRootAlias.equals(rootAlias)) {
            throw new IllegalStateException("Expected rootAlias=" + expectedRootAlias
                    + ", got " + rootAlias + ". Payload: " + payload);
        }

        int expected = intField(payload, "expectedWorkers", -1);

        if (expected != expectedWorkers) {
            throw new IllegalStateException("Expected expectedWorkers=" + expectedWorkers
                    + ", got " + expected + ". Payload: " + payload);
        }

        int sampleSize = intField(payload, "sampleSize", -1);
        int sampleInstanceCount = intField(payload, "sampleInstanceCount", -1);

        if (sampleSize != expectedSampleSize) {
            throw new IllegalStateException("Expected sampleSize=" + expectedSampleSize
                    + ", got " + sampleSize + ". Payload: " + payload);
        }

        if (sampleInstanceCount != expectedSampleSize) {
            throw new IllegalStateException("Expected sampleInstanceCount=" + expectedSampleSize
                    + ", got " + sampleInstanceCount + ". Payload: " + payload);
        }

        if (longField(payload, "rootTuplesSeen", 0L) <= 0L) {
            throw new IllegalStateException("Expected rootTuplesSeen > 0. Payload: " + payload);
        }

        if (longField(payload, "positiveRootCandidatesSeen", 0L) <= 0L) {
            throw new IllegalStateException("Expected positiveRootCandidatesSeen > 0. Payload: " + payload);
        }

        if (doubleField(payload, "totalRootGroupWeight", 0.0d) <= 0.0d) {
            throw new IllegalStateException("Expected totalRootGroupWeight > 0. Payload: " + payload);
        }
    }

    private static void validateGenericInstalled(JsonNode payload, int expectedWorkers, String expectedType) {
        String type = textField(payload, "type", "");

        if (!expectedType.equals(type)) {
            throw new IllegalStateException("Expected type=" + expectedType
                    + ", got " + type + ". Payload: " + payload);
        }

        JsonNode receivedWorkers = payload.get("receivedWorkers");

        if (receivedWorkers == null || !receivedWorkers.isArray() || receivedWorkers.size() != expectedWorkers) {
            throw new IllegalStateException("Expected receivedWorkers size=" + expectedWorkers
                    + ", got " + receivedWorkers + ". Payload: " + payload);
        }

        JsonNode failedWorkers = payload.get("failedWorkers");

        if (failedWorkers != null && failedWorkers.isArray() && failedWorkers.size() > 0) {
            throw new IllegalStateException("Expected failedWorkers to be empty, got: " + failedWorkers);
        }

        int installedWorkerCount = intField(payload, "installedWorkerCount", -1);

        if (installedWorkerCount != expectedWorkers) {
            throw new IllegalStateException("Expected installedWorkerCount=" + expectedWorkers
                    + ", got " + installedWorkerCount + ". Payload: " + payload);
        }
    }

    private static ObjectNode buildEndAliasDatapoint(String datasetKey, String streamId, int uid, String alias,
                                                     int epoch, String resultId, int expectedWorkers,
                                                     String nextCommand, String nextAlias) {

        ObjectNode marker = MAPPER.createObjectNode();

        marker.put("type", "END_ALIAS");
        marker.put("synopsisID", 30);
        marker.put("uid", uid);
        marker.put("phase", "PHASE1");
        marker.put("alias", alias);
        marker.put("epoch",epoch);
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
}
