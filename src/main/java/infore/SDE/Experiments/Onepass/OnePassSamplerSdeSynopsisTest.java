package infore.SDE.Experiments.Onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnlineMultinomialSample;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnlineMultinomialSampler;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
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
import java.io.FileWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * End-to-end SDE/Kafka test for the combined OnePassSamplerSdeSynopsis.
 *
 * This test assumes the SDE Run class is already running.
 *
 * Flow:
 *
 *   1. Send ADD request for synopsisID = 30.
 *   2. Stream Phase 1 side aliases in leaf-to-root order.
 *   3. Send FINISH_PHASE_1 request.
 *   4. Validate transition to PHASE_2.
 *   5. Stream Phase 2 root alias.
 *   6. Send FINISH_PHASE_2 request.
 *   7. Validate transition to PHASE_3 and Phase 2 root-sampling result.
 *   8. For every non-root alias in root-to-leaf order:
 *          START_PHASE_3_ALIAS(alias)
 *          stream alias tuples again
 *          FINISH_PHASE_3_ALIAS
 *   9. Send FINISH_PHASE_3 request.
 *  10. Validate transition to DONE and completed Phase 3 joined samples.
 *
 * This tests the SDE-facing lifecycle:
 *
 *   SDEcoFlatMap
 *      -> OnePassSamplerSdeSynopsis
 *          -> OnePassSamplerSynopsis
 *              -> OnePassPhaseOneState
 *              -> OnePassPhaseTwoState
 *                  -> OnePassRootSampler
 *                      -> OnlineMultinomialSampler
 *              -> OnePassPhaseThreeState
 */
public final class OnePassSamplerSdeSynopsisTest {

    /*
     * Adjust these only if your existing OnePassPhaseOneTest uses different topics.
     */
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String REQUEST_TOPIC = "requestTopic";
    private static final String DATA_TOPIC = "dataTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";

    private static final String TEST_TPCH_DIR =
            "/home/vboxuser/Desktop/Thesis/tpch-data/sf1";

    //private static final String TEST_ONEPASS_SQL =
    //        "SELECT * FROM wq3_alias WEIGHTED BY (o.o_totalprice * " +
    //                "(l.l_extendedprice * (1 - l.l_discount))) LIMIT 100 " +
    //               "/* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    private static final String TEST_ONEPASS_SQL =
            "SELECT o.o_orderkey, l1.l_linenumber FROM w_two_lineitems WEIGHTED BY ("+
                    "o.o_totalprice * l1.l_extendedprice * (4 - l2.l_discount))" +
                    "LIMIT 100 /* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    /*
     * Use 5000 first while testing wiring.
     * Use -1 for full TPC-H files.
     *
     * Important:
     * Phase 3 must replay the same side-stream subset that Phase 1 indexed.
     */
    private static final long TEST_ROW_LIMIT = 14000;

    private static final int SYNOPSIS_ID = 30;
    private static final int PHASE1_DEBUG_SYNOPSIS_ID = 31;
    private static final int REQUEST_ADD = 1;
    private static final int REQUEST_ESTIMATE = 3;
    private static final int REQUEST_UPDATE = 7;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, Map<String, Double>> expectedIndexesByEdge =
            new LinkedHashMap<String, Map<String, Double>>();

    private static final Map<String, Long> sentRowsByAlias = new LinkedHashMap<String, Long>();

    //Commands
    private static final String COMMAND_FINISH_PHASE_1 = "FINISH_PHASE_1";
    private static final String COMMAND_FINISH_PHASE_2 = "FINISH_PHASE_2";
    private static final String COMMAND_START_PHASE_3_ALIAS = "START_PHASE_3_ALIAS";
    private static final String COMMAND_FINISH_PHASE_3_ALIAS = "FINISH_PHASE_3_ALIAS";
    private static final String COMMAND_FINISH_PHASE_3 = "FINISH_PHASE_3";

    //Barrier flags
    private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";
    private static final long DATA_BARRIER_TIMEOUT_MS = 120000L;
    private static final int EXPECTED_DATA_BARRIER_ACKS = 1;

    //Validation flags
    private static long expectedRootTuplesSeen = 0L;
    private static long expectedPositiveRootCandidatesSeen = 0L;
    private static double expectedTotalRootGroupWeight = 0.0d;

    //Debug Flags
    private static final boolean PRINT_FULL_KAFKA_RECORDS = false;
    private static final boolean PRINT_FULL_STATUS_PAYLOADS = false;
    private static final int FINAL_SAMPLE_PREVIEW_LIMIT = 5;

    private static final long FULL_VALIDATION_MAX_ROW_LIMIT = 15000L;
    private static final boolean RUN_EXACT_EXPECTED_VALIDATION =
            TEST_ROW_LIMIT >= 0L && TEST_ROW_LIMIT <= FULL_VALIDATION_MAX_ROW_LIMIT;
    private static final boolean EXPORT_PHASE1_FULL_INDEXES = RUN_EXACT_EXPECTED_VALIDATION;

    //Timing/Benchmark flags
    private static final Map<String, Long> benchmarkNanos = new LinkedHashMap<String, Long>();
    private static final boolean PRINT_DETAILED_BENCHMARK_TIMINGS = false;
    private static final boolean WRITE_BENCHMARK_CSV = true;
    private static final String BENCHMARK_CSV_PATH = "/home/vboxuser/Desktop/Thesis/onepass_sde_benchmark.csv";

    private OnePassSamplerSdeSynopsisTest() {
    }

    public static void main(String[] args) throws Exception {
        int uid = Math.abs(UUID.randomUUID().toString().hashCode());
        int phaseOneDebugUid = uid == Integer.MAX_VALUE ? uid - 1 : uid + 1;

        expectedIndexesByEdge.clear();
        sentRowsByAlias.clear();
        benchmarkNanos.clear();
        expectedRootTuplesSeen = 0L;
        expectedPositiveRootCandidatesSeen = 0L;
        expectedTotalRootGroupWeight = 0.0d;

        runOnlineMultinomialSamplerDuplicateSelfTest();

        String datasetKey = Integer.toString(uid);
        String streamId = "onepass-sampler";

        System.out.println("=== OnePassSamplerSdeSynopsisTest ===");
        System.out.println("UID: " + uid);
        System.out.println("Phase 1 debug UID: " + phaseOneDebugUid);
        System.out.println("datasetKey: " + datasetKey);
        System.out.println("streamId: " + streamId);
        System.out.println("SQL:");
        System.out.println(TEST_ONEPASS_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_ONEPASS_SQL);
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);
        OnePassCatalog catalog =
                OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        initializeExpectedIndexes(plan);

        OnePassWeightEvaluator expectedWeightEvaluator = new OnePassWeightEvaluator(plan.getWeightSpec());

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Root-to-leaf order: " + plan.getRootToLeafOrder());
        System.out.println("Weights by alias: " + plan.getWeightsByAlias());
        System.out.println();

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer("onepass-sampler-test-" + uid);

        consumer.subscribe(Collections.singletonList(ESTIMATION_TOPIC));

        try {
            drainConsumer(consumer);

            /*
             * Start benchmark timing after local test setup/SQL compilation.
             * This keeps the measurement focused on the SDE/Kafka One-pass*
             * lifecycle that will be compared against the original code.
             */
            long totalStartNanos = tic();

            System.out.println("1. Sending ADD request for OnePassSamplerSdeSynopsis...");
            ObjectNode addRequest = buildAddRequest(uid, datasetKey, streamId);

            sendJson(producer, REQUEST_TOPIC, datasetKey, addRequest);
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(addRequest));
            System.out.println();

            if (EXPORT_PHASE1_FULL_INDEXES) {
                System.out.println("1b. Sending ADD request for temporary Phase 1 debug synopsis...");
                ObjectNode phaseOneDebugAddRequest =
                        buildPhaseOneDebugAddRequest(phaseOneDebugUid, datasetKey, streamId);

                sendJson(producer, REQUEST_TOPIC, datasetKey, phaseOneDebugAddRequest);
                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseOneDebugAddRequest));
                System.out.println();
            }

            /*
             * Give SDE a short moment to create the synopsis before sending tuples.
             * This is an artificial synchronization wait, so it is excluded from
             * adjusted benchmark time.
             */
            long initialAddWaitStartNanos = tic();
            Thread.sleep(1500L);
            recordDuration("total_artificial_wait", initialAddWaitStartNanos);
            recordDetailedDuration("initial_add_wait", initialAddWaitStartNanos);

            long phaseOneStreamStartNanos = tic(); //TIMING PHASE1
            System.out.println("2. Streaming PHASE_1 side aliases...");
            long phaseOneRows = 0L;

            for (String alias : plan.getLeafToRootOrder()) {
                long count = streamAlias(
                        producer,
                        DATA_TOPIC,
                        datasetKey,
                        streamId,
                        catalog,
                        plan,
                        alias,
                        TEST_ROW_LIMIT,
                        "PHASE1",
                        expectedWeightEvaluator
                );

                phaseOneRows += count;

                System.out.println("  PHASE_1 alias " + alias + " rows: " + count);
            }

            producer.flush();
            recordDuration("phase1_stream_send", phaseOneStreamStartNanos); //TIMING PHASE1 END

            System.out.println("Total PHASE_1 rows sent: " + phaseOneRows);
            System.out.println();

            /*
             * Temporary testing barrier.
             * Kafka/Flink does not guarantee ordering across dataTopic and requestTopic.
             * This gives SDE time to consume PHASE_1 tuples before FINISH_PHASE_1.
             */
            System.out.println("Waiting for SDE data barrier after PHASE_1 side tuples...");
            long phaseOneBarrierStartNanos = tic();

            sendDataBarrierAndWait(producer, consumer, datasetKey, streamId, uid, "PHASE1", null,
                    EXPECTED_DATA_BARRIER_ACKS, DATA_BARRIER_TIMEOUT_MS);

            recordDuration("phase1_data_barrier_ack", phaseOneBarrierStartNanos);
            recordDetailedDuration("phase1_data_barrier_ack_detail", phaseOneBarrierStartNanos);

            System.out.println("3. Sending FINISH_PHASE_1 request...");
            long phaseOneFinishAckStartNanos = tic();

            ObjectNode finishPhaseOneRequest =
                    buildControlRequest(
                            uid,
                            datasetKey,
                            streamId,
                            COMMAND_FINISH_PHASE_1
                    );

            sendJson(producer, REQUEST_TOPIC, datasetKey, finishPhaseOneRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(finishPhaseOneRequest));
            System.out.println();

            System.out.println("4. Waiting for FINISH_PHASE_1 ACK...");
            JsonNode phaseOneAck = waitForResponseContaining(consumer, uid, COMMAND_FINISH_PHASE_1, 120000L);

            System.out.println("FINISH_PHASE_1 ACK envelope:");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseOneAck));
            System.out.println();

            validateControlAck(phaseOneAck, COMMAND_FINISH_PHASE_1);
            recordDuration("phase1_finish_ack", phaseOneFinishAckStartNanos);
            recordDetailedDuration("phase1_finish_ack_detail", phaseOneFinishAckStartNanos);

            System.out.println("4a. Requesting OnePassSamplerSdeSynopsis STATUS after FINISH_PHASE_1...");
            long phaseOneStatusStartNanos = tic();
            ObjectNode phaseOneStatusRequest = buildStatusRequest(uid, datasetKey, streamId);

            sendJson(producer, REQUEST_TOPIC, datasetKey, phaseOneStatusRequest);
            producer.flush();

            System.out.println(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(phaseOneStatusRequest)
            );
            System.out.println();

            JsonNode phaseOneStatusEnvelope = waitForResponseContaining(consumer, uid, "PHASE_2",
                            120000L);

            System.out.println("PHASE_1 STATUS envelope:");
            printPayloadSummary(extractEstimationPayload(phaseOneStatusEnvelope));
            System.out.println();

            JsonNode phaseOneStatusPayload = extractEstimationPayload(phaseOneStatusEnvelope);

            recordDuration("phase1_status", phaseOneStatusStartNanos);
            recordDetailedDuration("phase1_status_detail", phaseOneStatusStartNanos);

            validatePhaseOneTransition(phaseOneStatusPayload, plan);

            if (EXPORT_PHASE1_FULL_INDEXES) {
                System.out.println("4b. Requesting full Phase 1 indexes from temporary debug synopsis...");
                long phaseOneDebugExportStartNanos = tic();

                ObjectNode phaseOneDebugEstimateRequest = buildPhaseOneDebugEstimateRequest(
                                phaseOneDebugUid, datasetKey, streamId);

                sendJson(producer, REQUEST_TOPIC, datasetKey, phaseOneDebugEstimateRequest);
                producer.flush();

                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().
                        writeValueAsString(phaseOneDebugEstimateRequest));
                System.out.println();

                JsonNode phaseOneFullIndexes = waitForPhaseOneDebugResult(consumer, phaseOneDebugUid,
                        plan.getQueryName(), 120000L);

                validateFullIndexPayload(phaseOneFullIndexes);
                writePhaseOneFullIndexFile(phaseOneFullIndexes, plan.getQueryName());
                recordDetailedDuration("phase1_debug_full_index_export", phaseOneDebugExportStartNanos);
            }

            long phaseTwoStreamStartNanos = tic(); //TIMING PHASE2
            System.out.println("5. Streaming PHASE_2 root alias...");
            long rootRows = streamAlias(
                    producer,
                    DATA_TOPIC,
                    datasetKey,
                    streamId,
                    catalog,
                    plan,
                    plan.getRootAlias(),
                    TEST_ROW_LIMIT,
                    "PHASE2",
                    expectedWeightEvaluator);

            producer.flush();
            recordDuration("phase2_root_stream_send", phaseTwoStreamStartNanos);//TIMING PHASE2 END

            System.out.println("PHASE_2 root alias " + plan.getRootAlias() + " rows: " + rootRows);
            System.out.println();

            System.out.println("Waiting for SDE data barrier after PHASE_2 root tuples...");
            long phaseTwoBarrierStartNanos = tic();

            sendDataBarrierAndWait(producer, consumer, datasetKey, streamId, uid, "PHASE2", plan.getRootAlias(),
                    EXPECTED_DATA_BARRIER_ACKS, DATA_BARRIER_TIMEOUT_MS);

            recordDuration("phase2_data_barrier_ack", phaseTwoBarrierStartNanos);
            recordDetailedDuration("phase2_data_barrier_ack_detail", phaseTwoBarrierStartNanos);

            System.out.println("6. Sending FINISH_PHASE_2 request...");
            long phaseTwoFinishAckStartNanos = tic();

            ObjectNode finishPhaseTwoRequest = buildControlRequest(uid, datasetKey, streamId,
                    COMMAND_FINISH_PHASE_2);

            sendJson(producer, REQUEST_TOPIC, datasetKey, finishPhaseTwoRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(finishPhaseTwoRequest));
            System.out.println();

            System.out.println("7. Waiting for FINISH_PHASE_2 ACK...");
            JsonNode phaseTwoAck =
                    waitForResponseContaining(consumer, uid, COMMAND_FINISH_PHASE_2, 120000L);

            System.out.println("FINISH_PHASE_2 ACK envelope:");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseTwoAck));
            System.out.println();

            validateControlAck(phaseTwoAck, COMMAND_FINISH_PHASE_2);
            recordDuration("phase2_finish_ack", phaseTwoFinishAckStartNanos);
            recordDetailedDuration("phase2_finish_ack_detail", phaseTwoFinishAckStartNanos);

            System.out.println("7a. Requesting OnePassSamplerSdeSynopsis STATUS after FINISH_PHASE_2...");
            long phaseTwoStatusStartNanos = tic();
            ObjectNode phaseTwoStatusRequest = buildStatusRequest(uid, datasetKey, streamId);

            sendJson(producer, REQUEST_TOPIC, datasetKey, phaseTwoStatusRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseTwoStatusRequest));
            System.out.println();

            JsonNode phaseTwoStatusEnvelope =
                    waitForResponseContaining(consumer, uid, "PHASE_3", 120000L);

            System.out.println("PHASE_2 STATUS envelope:");
            printPayloadSummary(extractEstimationPayload(phaseTwoStatusEnvelope));
            System.out.println();

            JsonNode phaseTwoStatusPayload =
                    extractEstimationPayload(phaseTwoStatusEnvelope);

            recordDuration("phase2_status", phaseTwoStatusStartNanos);
            recordDetailedDuration("phase2_status_detail", phaseTwoStatusStartNanos);

            validatePhaseTwoTransitionToPhaseThree(
                    phaseTwoStatusPayload,
                    plan.getSampleSize(),
                    expectedRootTuplesSeen,
                    expectedPositiveRootCandidatesSeen,
                    expectedTotalRootGroupWeight
            );

            /*
             * Phase 3.
             * Replay side aliases in root-to-leaf order, skipping the root.
             *
             * Timing is split into:
             *   - start control + ACK
             *   - stream/send only
             *   - artificial barrier sleep
             *   - finish control + ACK
             *   - total alias wall-clock time
             */
            long phaseThreeReplayStartNanos = tic();
            System.out.println("8. Streaming PHASE_3 side aliases in root-to-leaf order...");

            for (String alias : plan.getRootToLeafOrder()) {
                if (plan.isRoot(alias)) {
                    continue;
                }

                long phaseThreeAliasTotalStartNanos = tic();

                System.out.println("8a. Starting PHASE_3 alias: " + alias);

                long phaseThreeAliasStartControlStartNanos = tic();

                ObjectNode startAliasRequest = buildStartPhaseThreeAliasRequest(uid, datasetKey, streamId, alias);

                sendJson(producer, REQUEST_TOPIC, datasetKey, startAliasRequest);
                producer.flush();

                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(startAliasRequest));
                System.out.println();

                JsonNode startAliasAck = waitForResponseContaining(consumer, uid,
                        COMMAND_START_PHASE_3_ALIAS, 120000L);

                recordDuration("phase3_control_ack_total", phaseThreeAliasStartControlStartNanos);
                recordDetailedDuration("phase3_alias_" + alias + "_start_ack", phaseThreeAliasStartControlStartNanos);

                System.out.println("START_PHASE_3_ALIAS ACK envelope:");
                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(startAliasAck));
                System.out.println();

                validateControlAck(startAliasAck, COMMAND_START_PHASE_3_ALIAS);

                System.out.println("8b. Streaming PHASE_3 alias " + alias + "...");

                long phaseThreeAliasStreamStartNanos = tic();

                long phaseThreeAliasRows = streamAlias(
                        producer,
                        DATA_TOPIC,
                        datasetKey,
                        streamId,
                        catalog,
                        plan,
                        alias,
                        TEST_ROW_LIMIT,
                        null,
                        expectedWeightEvaluator);

                producer.flush();

                recordDuration("phase3_side_stream_send_total", phaseThreeAliasStreamStartNanos);
                recordDetailedDuration("phase3_alias_" + alias + "_stream_send", phaseThreeAliasStreamStartNanos);

                System.out.println("PHASE_3 alias " + alias + " rows: " + phaseThreeAliasRows);
                System.out.println();

                System.out.println("Waiting for SDE data barrier after PHASE_3 alias " + alias + " tuples...");
                long phaseThreeAliasBarrierStartNanos = tic();
                sendDataBarrierAndWait(producer, consumer, datasetKey, streamId, uid, "PHASE3", alias,
                        EXPECTED_DATA_BARRIER_ACKS, DATA_BARRIER_TIMEOUT_MS);

                recordDuration("phase3_data_barrier_ack_total", phaseThreeAliasBarrierStartNanos);
                recordDetailedDuration("phase3_alias_" + alias + "_data_barrier_ack", phaseThreeAliasBarrierStartNanos);

                System.out.println("8c. Finishing PHASE_3 alias: " + alias);

                long phaseThreeAliasFinishControlStartNanos = tic();

                ObjectNode finishAliasRequest = buildControlRequest(uid, datasetKey, streamId,
                        COMMAND_FINISH_PHASE_3_ALIAS);

                sendJson(producer, REQUEST_TOPIC, datasetKey, finishAliasRequest);
                producer.flush();

                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(finishAliasRequest));
                System.out.println();

                JsonNode finishAliasAck = waitForResponseContaining(consumer, uid,
                        COMMAND_FINISH_PHASE_3_ALIAS, 120000L);

                recordDuration("phase3_control_ack_total", phaseThreeAliasFinishControlStartNanos);
                recordDetailedDuration("phase3_alias_" + alias + "_finish_ack", phaseThreeAliasFinishControlStartNanos);

                System.out.println("FINISH_PHASE_3_ALIAS ACK envelope:");
                System.out.println(
                        MAPPER.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(finishAliasAck)
                );
                System.out.println();

                validateControlAck(finishAliasAck, COMMAND_FINISH_PHASE_3_ALIAS);

                recordDetailedDuration("phase3_alias_" + alias + "_total", phaseThreeAliasTotalStartNanos);
            }

            recordDetailedDuration("phase3_side_replay_total", phaseThreeReplayStartNanos);//TIMING Phase3 END

            System.out.println("9. Sending FINISH_PHASE_3 request...");
            long phaseThreeFinishAckStartNanos = tic();

            ObjectNode finishPhaseThreeRequest = buildControlRequest(uid, datasetKey, streamId, COMMAND_FINISH_PHASE_3);

            sendJson(producer, REQUEST_TOPIC, datasetKey, finishPhaseThreeRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(finishPhaseThreeRequest));
            System.out.println();

            System.out.println("10. Waiting for FINISH_PHASE_3 ACK...");
            JsonNode phaseThreeAck =
                    waitForResponseContaining(
                            consumer,
                            uid,
                            COMMAND_FINISH_PHASE_3,
                            120000L
                    );

            System.out.println("FINISH_PHASE_3 ACK envelope:");
            System.out.println(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(phaseThreeAck)
            );
            System.out.println();

            validateControlAck(phaseThreeAck, COMMAND_FINISH_PHASE_3);
            recordDuration("phase3_finish_ack", phaseThreeFinishAckStartNanos);
            recordDetailedDuration("phase3_finish_ack_detail", phaseThreeFinishAckStartNanos);

            System.out.println("10a. Requesting OnePassSamplerSdeSynopsis STATUS after FINISH_PHASE_3...");
            long phaseThreeStatusStartNanos = tic();
            ObjectNode phaseThreeStatusRequest =
                    buildStatusRequest(
                            uid,
                            datasetKey,
                            streamId
                    );

            sendJson(producer, REQUEST_TOPIC, datasetKey, phaseThreeStatusRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseThreeStatusRequest));
            System.out.println();

            JsonNode phaseThreeStatusEnvelope = waitForResponseContaining(consumer, uid,
                    "phaseThreeComplete", 120000L);

            System.out.println("PHASE_3 STATUS envelope:");
            printPayloadSummary(extractEstimationPayload(phaseThreeStatusEnvelope));
            System.out.println();

            JsonNode phaseThreeStatusPayload = extractEstimationPayload(phaseThreeStatusEnvelope);

            recordDuration("phase3_status", phaseThreeStatusStartNanos);
            recordDetailedDuration("phase3_status_detail", phaseThreeStatusStartNanos);

            /*
             * Print a few final SELECT-projected samples so we can manually inspect
             * that projection is applied correctly.
             */
            printProjectedCompletedSamplesPreview(phaseThreeStatusPayload, FINAL_SAMPLE_PREVIEW_LIMIT);

            long phaseThreeValidationStartNanos = tic();

            validatePhaseThreeResult(phaseThreeStatusPayload, phaseTwoStatusPayload, plan);

            recordDetailedDuration("phase3_validation", phaseThreeValidationStartNanos);

            recordDuration("total_end_to_end_observed", totalStartNanos);
            printBenchmarkSummary(plan);
            writeBenchmarkCsv(plan, "SDE_KAFKA");

            System.out.println();
            System.out.println("SUCCESS: OnePassSamplerSdeSynopsis SDE Phase 1/2/3 test passed.");
        } finally {
            try {
                producer.close();
            } catch (Exception ignored) {
            }

            try {
                consumer.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static ObjectNode buildAddRequest(int uid,
                                              String datasetKey,
                                              String streamId) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_ADD);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", 1);

        /*
         * Keep both forms for compatibility with OnePassRequestParser.
         *
         * If your parser only uses param[0], this works.
         * If your parser uses parameters.onePassSql, this also works.
         */
        ArrayNode param = MAPPER.createArrayNode();
        param.add("ONEPASS_SQL");
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassSql", TEST_ONEPASS_SQL);
        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildPhaseOneDebugAddRequest(int uid,
                                                           String datasetKey,
                                                           String streamId) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_ADD);
        request.put("synopsisID", PHASE1_DEBUG_SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", 1);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("catalog-replay");
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassSql", TEST_ONEPASS_SQL);
        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildPhaseOneDebugEstimateRequest(int uid,
                                                                String datasetKey,
                                                                String streamId) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_ESTIMATE);
        request.put("synopsisID", PHASE1_DEBUG_SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", 1);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("debug-full-indexes");
        request.set("param", param);

        return request;
    }

    private static ObjectNode buildControlRequest(int uid,
                                                  String datasetKey,
                                                  String streamId,
                                                  String command) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", 1);

        /*
         * Again keep both forms:
         *   param[0]
         *   parameters.onePassCommand
         */
        ArrayNode param = MAPPER.createArrayNode();
        param.add(command);
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", command);
        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildStartPhaseThreeAliasRequest(int uid,
                                                               String datasetKey,
                                                               String streamId,
                                                               String alias) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_UPDATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", 1);

        /*
         * Support all accepted forms:
         *
         *   param[0] = START_PHASE_3_ALIAS
         *   param[1] = alias
         *
         *   parameters.onePassCommand = START_PHASE_3_ALIAS
         *   parameters.onePassAlias = alias
         *   parameters.phaseThreeAlias = alias
         */
        ArrayNode param = MAPPER.createArrayNode();
        param.add(COMMAND_START_PHASE_3_ALIAS);
        param.add(alias);
        request.set("param", param);

        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("onePassCommand", COMMAND_START_PHASE_3_ALIAS);
        parameters.put("onePassAlias", alias);
        parameters.put("phaseThreeAlias", alias);
        request.set("parameters", parameters);

        return request;
    }

    @SuppressWarnings("unused")
    private static ObjectNode buildStatusRequest(int uid,
                                                 String datasetKey,
                                                 String streamId) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("key", datasetKey);
        request.put("requestID", REQUEST_ESTIMATE);
        request.put("synopsisID", SYNOPSIS_ID);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", 1);

        ArrayNode param = MAPPER.createArrayNode();
        param.add("STATUS");
        request.set("param", param);

        return request;
    }

    private static long streamAlias(KafkaProducer<String, String> producer,
                                    String topic,
                                    String datasetKey,
                                    String streamId,
                                    OnePassCatalog catalog,
                                    CompiledOnePassPlan plan,
                                    String alias,
                                    long maxRows,
                                    String expectedUpdateMode,
                                    OnePassWeightEvaluator expectedWeightEvaluator) throws Exception {
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

                ObjectNode tuple =
                        tupleJsonFromLine(alias, columns, separator, line);

                ObjectNode datapoint =
                        wrapTupleAsDatapoint(datasetKey, streamId, tuple);

                sendJsonAsync(producer, topic, datasetKey, datapoint);

                updateExpectedState(
                        plan,
                        expectedWeightEvaluator,
                        tuple,
                        expectedUpdateMode
                );

                count++;

                if (count % 1000 == 0) {
                    producer.flush();
                    System.out.println("    sent " + count + " rows for alias " + alias);
                }
            }
        } finally {
            br.close();
        }

        sentRowsByAlias.put(alias, count);

        return count;
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

    private static ObjectNode wrapTupleAsDatapoint(String datasetKey,
                                                   String streamId,
                                                   ObjectNode tuple) {
        ObjectNode datapoint =
                MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);
        datapoint.put("streamID", streamId);

        /*
         * OnePassTupleExtractor expects Datapoint.values to be the tuple
         * JSON object itself, not an array and not a JSON string.
         */
        datapoint.set("values", tuple.deepCopy());

        return datapoint;
    }

    private static String separatorForAlias(OnePassCatalog catalog,
                                            CompiledOnePassPlan plan,
                                            String alias) {
        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(alias);

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

    private static void validatePhaseOneTransition(JsonNode payload,
                                                   CompiledOnePassPlan plan) {
        JsonNode phaseNode = findFirst(payload, "phase");

        if (phaseNode == null) {
            throw new IllegalStateException(
                    "FINISH_PHASE_1 response does not contain field 'phase'"
            );
        }

        String phase = phaseNode.asText();

        if (!"PHASE_2".equals(phase)) {
            throw new IllegalStateException(
                    "Expected phase PHASE_2 after FINISH_PHASE_1, got: "
                            + phase
            );
        }

        JsonNode completeNode = findFirst(payload, "phaseOneComplete");

        if (completeNode == null || !completeNode.asBoolean()) {
            throw new IllegalStateException(
                    "Expected phaseOneComplete = true after FINISH_PHASE_1"
            );
        }

        JsonNode edgeIndexCountNode = findFirst(payload, "edgeIndexCount");

        if (edgeIndexCountNode == null || edgeIndexCountNode.asInt() <= 0) {
            throw new IllegalStateException(
                    "Expected edgeIndexCount > 0 after FINISH_PHASE_1, got: "
                            + edgeIndexCountNode
            );
        }

        int expectedEdgeIndexCount = plan.getLeafToRootOrder().size();
        int actualEdgeIndexCount = edgeIndexCountNode.asInt();

        if (actualEdgeIndexCount != expectedEdgeIndexCount) {
            throw new IllegalStateException(
                    "Expected edgeIndexCount="
                            + expectedEdgeIndexCount
                            + " but got "
                            + actualEdgeIndexCount
            );
        }

        JsonNode edgeIndexIds = findFirst(payload, "edgeIndexIds");

        if (edgeIndexIds == null || !edgeIndexIds.isArray()) {
            throw new IllegalStateException(
                    "Expected edgeIndexIds array after FINISH_PHASE_1, got: "
                            + edgeIndexIds
            );
        }

        System.out.println("Validated FINISH_PHASE_1 transition payload.");
    }

    private static void validatePhaseTwoTransitionToPhaseThree(JsonNode payload, int expectedSampleSize,
                                                               long expectedRootRows, long expectedPositiveCandidates,
                                                               double expectedTotalWeight) {
        JsonNode phaseNode = findFirst(payload, "phase");

        if (phaseNode == null || !"PHASE_3".equals(phaseNode.asText())) {
            throw new IllegalStateException("Expected phase PHASE_3 after FINISH_PHASE_2, got: " + phaseNode);
        }

        JsonNode phaseTwoComplete = findFirst(payload, "phaseTwoComplete");

        if (phaseTwoComplete == null || !phaseTwoComplete.asBoolean()) {
            throw new IllegalStateException("Expected phaseTwoComplete = true after FINISH_PHASE_2");
        }

        JsonNode phaseThreeComplete = findFirst(payload, "phaseThreeComplete");

        if (phaseThreeComplete != null && phaseThreeComplete.asBoolean()) {
            throw new IllegalStateException("Expected phaseThreeComplete = false immediately after FINISH_PHASE_2");
        }

        JsonNode rootTuplesSeen = findFirst(payload, "rootTuplesSeen");

        if (rootTuplesSeen == null) {
            throw new IllegalStateException("Missing rootTuplesSeen");
        }

        JsonNode positiveRootCandidatesSeen =
                findFirst(payload, "positiveRootCandidatesSeen");

        if (positiveRootCandidatesSeen == null) {
            throw new IllegalStateException("Missing positiveRootCandidatesSeen");
        }

        JsonNode totalRootGroupWeight =
                findFirst(payload, "totalRootGroupWeight");

        if (totalRootGroupWeight == null) {
            throw new IllegalStateException("Missing totalRootGroupWeight");
        }

        /*
         * Small correctness mode:
         *   compare exact expected values built by the test.
         *
         * Full-scale/runtime mode:
         *   expected values are intentionally not built, so only check
         *   that the real SDE payload is positive and structurally valid.
         */
        if (RUN_EXACT_EXPECTED_VALIDATION) {
            if (rootTuplesSeen.asLong() != expectedRootRows) {
                throw new IllegalStateException("rootTuplesSeen mismatch. Expected " +
                        expectedRootRows + " but got " + rootTuplesSeen.asLong());
            }

            if (positiveRootCandidatesSeen.asLong() != expectedPositiveCandidates) {
                throw new IllegalStateException("positiveRootCandidatesSeen mismatch. Expected " +
                        expectedPositiveCandidates + " but got " + positiveRootCandidatesSeen.asLong());
            }

            assertClose("totalRootGroupWeight", expectedTotalWeight, totalRootGroupWeight.asDouble());
        } else {
            if (rootTuplesSeen.asLong() <= 0L) {
                throw new IllegalStateException("Expected rootTuplesSeen > 0 in scale mode, got " +
                        rootTuplesSeen.asLong());
            }

            if (positiveRootCandidatesSeen.asLong() <= 0L) {
                throw new IllegalStateException("Expected positiveRootCandidatesSeen > 0 in scale mode, got " +
                        positiveRootCandidatesSeen.asLong());
            }

            if (totalRootGroupWeight.asDouble() <= 0.0d) {
                throw new IllegalStateException("Expected totalRootGroupWeight > 0 in scale mode, got " +
                        totalRootGroupWeight.asDouble());
            }
        }

        JsonNode sampleInstancesIncludedNode = findFirst(payload, "sampleInstancesIncluded");

        boolean sampleInstancesIncluded =
                sampleInstancesIncludedNode == null || sampleInstancesIncludedNode.asBoolean();

        JsonNode sampleInstanceCountNode =
                findFirst(payload, "sampleInstanceCount");

        if (sampleInstanceCountNode != null) {
            if (sampleInstanceCountNode.asInt() != expectedSampleSize) {
                throw new IllegalStateException("Expected sampleInstanceCount = " + expectedSampleSize + ", got " +
                                sampleInstanceCountNode.asInt());
            }
        }

        JsonNode sampleInstances = findFirst(payload, "sampleInstances");

        if (sampleInstances == null || !sampleInstances.isArray()) {
            throw new IllegalStateException("Expected sampleInstances array, got: " + sampleInstances);
        }

        if (sampleInstancesIncluded) {
            if (sampleInstances.size() != expectedSampleSize) {
                throw new IllegalStateException(
                        "Expected sampleInstances.size() = " + expectedSampleSize + ", got " + sampleInstances.size());
            }
        } else {
            System.out.println("Validated compact FINISH_PHASE_2 payload: sampleInstanceCount=" + expectedSampleSize
                            + ". Full sampleInstances omitted from status because sample size is large.");
        }

        /*
         * This special duplicate-root check only makes sense in exact
         * small-test mode, because it depends on expectedPositiveCandidates.
         */
        if (sampleInstancesIncluded && RUN_EXACT_EXPECTED_VALIDATION && expectedPositiveCandidates == 1L &&
                sampleInstances.size() > 1) {

            JsonNode firstSource = sampleInstances.get(0).get("sourceCandidateId");

            for (JsonNode sampleInstance : sampleInstances) {
                JsonNode source = sampleInstance.get("sourceCandidateId");

                if (source == null || !source.equals(firstSource)) {
                    throw new IllegalStateException(
                            "Single-positive-candidate test failed. "
                                    + "All duplicate sample instances should have the same sourceCandidateId."
                    );
                }
            }
        }

        System.out.println("Validated FINISH_PHASE_2 transition payload.");
    }

    private static void validatePhaseThreeResult(JsonNode phaseThreePayload, JsonNode phaseTwoPayload,
                                                 CompiledOnePassPlan plan) {
        JsonNode phaseNode = findFirst(phaseThreePayload, "phase");

        if (phaseNode == null || !"DONE".equals(phaseNode.asText())) {
            throw new IllegalStateException("Expected phase DONE after FINISH_PHASE_3, got: " + phaseNode);
        }

        JsonNode phaseTwoComplete = findFirst(phaseThreePayload, "phaseTwoComplete");

        if (phaseTwoComplete == null || !phaseTwoComplete.asBoolean()) {
            throw new IllegalStateException("Expected phaseTwoComplete = true after FINISH_PHASE_3");
        }

        JsonNode phaseThreeComplete = findFirst(phaseThreePayload, "phaseThreeComplete");

        if (phaseThreeComplete == null || !phaseThreeComplete.asBoolean()) {
            throw new IllegalStateException("Expected phaseThreeComplete = true after FINISH_PHASE_3");
        }

        JsonNode completedSampleCount = findFirst(phaseThreePayload, "completedSampleCount");

        if (completedSampleCount == null) {
            throw new IllegalStateException("Missing completedSampleCount");
        }

        validateProjectedCompletedSamplesIfPresent(phaseThreePayload, completedSampleCount.asInt(), plan);

        JsonNode completedSamples = findFirst(phaseThreePayload, "completedSamples");

        if (completedSamples == null || !completedSamples.isArray()) {
            throw new IllegalStateException("Expected completedSamples array, got: " + completedSamples);
        }

        JsonNode completedSamplesIncludedNode = findFirst(phaseThreePayload, "completedSamplesIncluded");

        boolean completedSamplesIncluded = completedSamplesIncludedNode == null ||
                completedSamplesIncludedNode.asBoolean();

        JsonNode phaseTwoSampleInstances = findFirst(phaseTwoPayload, "sampleInstances");

        JsonNode phaseTwoSampleInstanceCountNode = findFirst(phaseTwoPayload, "sampleInstanceCount");

        int expectedPhaseTwoSampleCount;

        if (phaseTwoSampleInstanceCountNode != null) {
            expectedPhaseTwoSampleCount = phaseTwoSampleInstanceCountNode.asInt();
        } else if (phaseTwoSampleInstances != null && phaseTwoSampleInstances.isArray()) {
            expectedPhaseTwoSampleCount = phaseTwoSampleInstances.size();
        } else {
            throw new IllegalStateException(
                    "Could not find Phase 2 sampleInstances or sampleInstanceCount for Phase 3 validation");
        }

        if (completedSampleCount.asInt() != expectedPhaseTwoSampleCount) {
            throw new IllegalStateException("Phase 3 completed sample count does not match Phase 2 sample count. " +
                    "completedSampleCount=" + completedSampleCount.asInt() + ", Phase 2 sample count=" +
                    expectedPhaseTwoSampleCount);
        }

        boolean phaseTwoSamplesAvailable = phaseTwoSampleInstances != null && phaseTwoSampleInstances.isArray()
                        && phaseTwoSampleInstances.size() == expectedPhaseTwoSampleCount;

        if (!completedSamplesIncluded) {
            System.out.println(
                    "Validated compact FINISH_PHASE_3 payload: completedSampleCount="
                            + completedSampleCount.asInt()
                            + ". Full completedSamples omitted from status because sample size is large.");
            return;
        }

        if (completedSampleCount.asInt() != completedSamples.size()) {
            throw new IllegalStateException(
                    "completedSampleCount mismatch. Field says "
                            + completedSampleCount.asInt()
                            + " but completedSamples.size() is "
                            + completedSamples.size()
            );
        }

        if (completedSamples.size() != expectedPhaseTwoSampleCount) {
            throw new IllegalStateException(
                    "Phase 3 completed sample count does not match Phase 2 sample count. "
                            + "Phase 3 completedSamples.size()="
                            + completedSamples.size()
                            + ", Phase 2 sample count="
                            + expectedPhaseTwoSampleCount
            );
        }

        if (phaseTwoSamplesAvailable) {
            Set<Long> phaseTwoSampleIds = collectSampleIds(phaseTwoSampleInstances);

            Set<Long> phaseThreeSampleIds = collectSampleIds(completedSamples);

            if (!phaseTwoSampleIds.equals(phaseThreeSampleIds)) {
                throw new IllegalStateException(
                        "Phase 3 sample ids do not match Phase 2 sample ids. "
                                + "Phase 2 ids="
                                + phaseTwoSampleIds
                                + ", Phase 3 ids="
                                + phaseThreeSampleIds
                );
            }
        } else {
            System.out.println(
                    "Skipping exact Phase 2/Phase 3 sample-id equality check because Phase 2 sampleInstances were compacted."
            );
        }

        for (JsonNode completedSample : completedSamples) {
            validateCompletedSample(completedSample, plan);
        }

        System.out.println("Validated FINISH_PHASE_3 payload.");
    }

    private static Set<Long> collectSampleIds(JsonNode sampleArray) {
        Set<Long> ids =
                new LinkedHashSet<Long>();

        for (JsonNode sample : sampleArray) {
            JsonNode idNode =
                    sample.get("sampleInstanceId");

            if (idNode == null || idNode.isNull()) {
                throw new IllegalStateException(
                        "Sample is missing sampleInstanceId: "
                                + sample
                );
            }

            ids.add(idNode.asLong());
        }

        return ids;
    }

    private static void validateCompletedSample(JsonNode completedSample,
                                                CompiledOnePassPlan plan) {
        JsonNode sampleId =
                completedSample.get("sampleInstanceId");

        if (sampleId == null || sampleId.isNull()) {
            throw new IllegalStateException(
                    "Completed sample is missing sampleInstanceId: "
                            + completedSample
            );
        }

        for (String alias : plan.getAliases()) {
            JsonNode tupleNode =
                    getTupleNodeForAlias(completedSample, alias);

            if (tupleNode == null || tupleNode.isNull()) {
                throw new IllegalStateException(
                        "Completed sample "
                                + sampleId.asLong()
                                + " is missing alias "
                                + alias
                                + ". Sample: "
                                + completedSample
                );
            }

            JsonNode tableNode =
                    tupleNode.get("table");

            if (tableNode != null
                    && !tableNode.isNull()
                    && !alias.equals(tableNode.asText())) {
                throw new IllegalStateException(
                        "Completed sample "
                                + sampleId.asLong()
                                + " has tuple alias mismatch. Expected "
                                + alias
                                + " but got "
                                + tableNode.asText()
                );
            }
        }

        validateCompletedSampleJoinConditions(completedSample, plan);
    }

    private static void validateCompletedSampleJoinConditions(JsonNode completedSample,
                                                              CompiledOnePassPlan plan) {
        JsonNode sampleId =
                completedSample.get("sampleInstanceId");

        for (String alias : plan.getRootToLeafOrder()) {
            if (plan.isRoot(alias)) {
                continue;
            }

            CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                    plan.getParentEdge(alias);

            if (parentEdge == null) {
                throw new IllegalStateException(
                        "Non-root alias has no parent edge: " + alias
                );
            }

            JsonNode parentTuple =
                    getTupleNodeForAlias(
                            completedSample,
                            parentEdge.getParentAlias()
                    );

            JsonNode childTuple =
                    getTupleNodeForAlias(
                            completedSample,
                            parentEdge.getChildAlias()
                    );

            if (parentTuple == null || childTuple == null) {
                throw new IllegalStateException(
                        "Completed sample "
                                + sampleId
                                + " is missing parent/child tuple for edge "
                                + parentEdge
                );
            }

            String parentKey =
                    joinKey(parentTuple, parentEdge.getParentFields());

            String childKey =
                    joinKey(childTuple, parentEdge.getChildFields());

            if (!parentKey.equals(childKey)) {
                throw new IllegalStateException(
                        "Join mismatch in completed sample "
                                + sampleId
                                + " on edge "
                                + parentEdge
                                + ". parentKey="
                                + parentKey
                                + ", childKey="
                                + childKey
                );
            }
        }
    }

    private static JsonNode getTupleNodeForAlias(JsonNode completedSample,
                                                 String alias) {
        JsonNode tuplesByAlias =
                completedSample.get("tuplesByAlias");

        if (tuplesByAlias != null
                && tuplesByAlias.isObject()
                && tuplesByAlias.has(alias)) {
            return tuplesByAlias.get(alias);
        }

        /*
         * Fallback name in case the model changes later.
         */
        JsonNode selectedTuplesByAlias =
                completedSample.get("selectedTuplesByAlias");

        if (selectedTuplesByAlias != null
                && selectedTuplesByAlias.isObject()
                && selectedTuplesByAlias.has(alias)) {
            return selectedTuplesByAlias.get(alias);
        }

        return null;
    }

    private static String joinKey(JsonNode tupleNode,
                                  List<String> fields) {
        StringBuilder sb =
                new StringBuilder();

        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append("|");
            }

            String field =
                    fields.get(i);

            String value =
                    getTupleFieldAsText(tupleNode, field);

            if (value == null) {
                throw new IllegalStateException(
                        "Missing join field '"
                                + field
                                + "' in serialized tuple: "
                                + tupleNode
                );
            }

            sb.append(value);
        }

        return sb.toString();
    }

    private static String getTupleFieldAsText(JsonNode tupleNode,
                                              String fieldName) {
        if (tupleNode == null || tupleNode.isNull()) {
            return null;
        }

        JsonNode fields =
                tupleNode.get("fields");

        if (fields != null && fields.has(fieldName)) {
            JsonNode value =
                    fields.get(fieldName);

            return value == null || value.isNull() ? null : value.asText();
        }

        JsonNode rawJson =
                tupleNode.get("rawJson");

        if (rawJson != null && rawJson.has(fieldName)) {
            JsonNode value =
                    rawJson.get(fieldName);

            return value == null || value.isNull() ? null : value.asText();
        }

        if (tupleNode.has(fieldName)) {
            JsonNode value =
                    tupleNode.get(fieldName);

            return value == null || value.isNull() ? null : value.asText();
        }

        return null;
    }

    private static void updateExpectedState(CompiledOnePassPlan plan,
                                            OnePassWeightEvaluator expectedWeightEvaluator,
                                            ObjectNode tuple,
                                            String expectedUpdateMode) {

        if (!RUN_EXACT_EXPECTED_VALIDATION) {
            return;
        }

        if (expectedUpdateMode == null) {
            return;
        }

        if ("PHASE1".equalsIgnoreCase(expectedUpdateMode)) {
            updateExpectedIndexes(plan, expectedWeightEvaluator, tuple);
            return;
        }

        if ("PHASE2".equalsIgnoreCase(expectedUpdateMode)) {
            updateExpectedRootStats(plan, expectedWeightEvaluator, tuple);
        }
    }

    private static void initializeExpectedIndexes(CompiledOnePassPlan plan) {
        for (String alias : plan.getLeafToRootOrder()) {
            CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                    plan.getParentEdge(alias);

            if (parentEdge != null
                    && !expectedIndexesByEdge.containsKey(parentEdge.getEdgeId())) {
                expectedIndexesByEdge.put(
                        parentEdge.getEdgeId(),
                        new LinkedHashMap<String, Double>()
                );
            }
        }
    }

    private static void updateExpectedIndexes(CompiledOnePassPlan plan,
                                              OnePassWeightEvaluator expectedWeightEvaluator,
                                              ObjectNode tupleJson) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(tupleJson);
        String alias = tuple.getTable();

        if (plan.isRoot(alias)) {
            return;
        }

        CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                plan.getParentEdge(alias);

        if (parentEdge == null) {
            return;
        }

        double ownWeight = expectedWeightEvaluator.evaluate(tuple);
        double continuationWeight = 1.0d;

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge
                : plan.getChildEdges(alias)) {
            String parentSideKey =
                    joinKey(tuple, childEdge.getParentFields());

            Map<String, Double> childIndex =
                    expectedIndexesByEdge.get(childEdge.getEdgeId());

            if (childIndex == null) {
                throw new IllegalStateException(
                        "Missing expected child index for edge "
                                + childEdge.getEdgeId()
                );
            }

            Double childWeight = childIndex.get(parentSideKey);

            if (childWeight == null) {
                childWeight = 0.0d;
            }

            continuationWeight *= childWeight.doubleValue();
        }

        double subtreeWeight = ownWeight * continuationWeight;

        if (subtreeWeight == 0.0d) {
            return;
        }

        String childSideKey = joinKey(tuple, parentEdge.getChildFields());

        Map<String, Double> parentIndex =
                expectedIndexesByEdge.get(parentEdge.getEdgeId());

        if (parentIndex == null) {
            throw new IllegalStateException(
                    "Missing expected parent index for edge "
                            + parentEdge.getEdgeId()
            );
        }

        addToMap(parentIndex, childSideKey, subtreeWeight);
    }

    private static void updateExpectedRootStats(CompiledOnePassPlan plan,
                                                OnePassWeightEvaluator expectedWeightEvaluator,
                                                ObjectNode tupleJson) {
        OnePassTuple tuple = OnePassTupleExtractor.extract(tupleJson);

        if (!plan.isRoot(tuple.getTable())) {
            return;
        }

        expectedRootTuplesSeen++;

        double rootGroupWeight =
                computeExpectedRootGroupWeight(plan, expectedWeightEvaluator, tuple);

        if (rootGroupWeight > 0.0d) {
            expectedPositiveRootCandidatesSeen++;
            expectedTotalRootGroupWeight += rootGroupWeight;
        }
    }

    private static double computeExpectedRootGroupWeight(
            CompiledOnePassPlan plan,
            OnePassWeightEvaluator expectedWeightEvaluator,
            OnePassTuple rootTuple) {

        double rootOwnWeight = expectedWeightEvaluator.evaluate(rootTuple);

        if (rootOwnWeight == 0.0d) {
            return 0.0d;
        }

        double continuationWeight = 1.0d;

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge
                : plan.getChildEdges(plan.getRootAlias())) {
            String parentSideKey =
                    joinKey(rootTuple, childEdge.getParentFields());

            Map<String, Double> childIndex =
                    expectedIndexesByEdge.get(childEdge.getEdgeId());

            if (childIndex == null) {
                throw new IllegalStateException(
                        "Missing expected root child index for edge "
                                + childEdge.getEdgeId()
                );
            }

            Double childWeight = childIndex.get(parentSideKey);

            if (childWeight == null || childWeight.doubleValue() == 0.0d) {
                return 0.0d;
            }

            continuationWeight *= childWeight.doubleValue();
        }

        return rootOwnWeight * continuationWeight;
    }

    private static String joinKey(OnePassTuple tuple, List<String> fields) {
        return JoinValue.fromTuple(tuple, fields).toString();
    }

    private static void addToMap(Map<String, Double> map,
                                 String key,
                                 double delta) {
        Double current = map.get(key);

        if (current == null) {
            current = 0.0d;
        }

        map.put(key, current + delta);
    }

    private static JsonNode waitForPhaseOneDebugResult(
            KafkaConsumer<String, String> consumer,
            int phaseOneDebugUid,
            String queryName,
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
                    envelope = parseJsonLenient(value);
                } catch (Exception ignored) {
                    continue;
                }

                JsonNode uidNode = findFirst(envelope, "uid");
                JsonNode synopsisIdNode = findFirst(envelope, "synopsisID");

                if (uidNode == null || uidNode.asInt() != phaseOneDebugUid) {
                    continue;
                }

                if (synopsisIdNode == null
                        || synopsisIdNode.asInt() != PHASE1_DEBUG_SYNOPSIS_ID) {
                    continue;
                }

                JsonNode payload = extractEstimationPayload(envelope);
                JsonNode edgeIndexes = findFirst(payload, "edgeIndexes");

                if (edgeIndexes == null || edgeIndexes.isNull()) {
                    continue;
                }

                JsonNode queryNameNode = findFirst(payload, "queryName");

                if (queryNameNode != null
                        && !queryName.equals(queryNameNode.asText())) {
                    throw new IllegalStateException(
                            "Received Phase 1 debug result for query "
                                    + queryNameNode.asText()
                                    + " but expected "
                                    + queryName
                    );
                }

                System.out.println("Received Phase 1 debug full-index result.");
                return payload;
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for Phase 1 debug full-index result. "
                        + "Records seen from estimationTopic: "
                        + recordsSeen
        );
    }

    private static void validateFullIndexPayload(JsonNode payload) {
        JsonNode edgeIndexes = findFirst(payload, "edgeIndexes");

        if (edgeIndexes == null || edgeIndexes.isNull()) {
            throw new IllegalStateException("Missing edgeIndexes field");
        }

        for (Map.Entry<String, Map<String, Double>> expectedEdge
                : expectedIndexesByEdge.entrySet()) {
            String edgeId = expectedEdge.getKey();
            Map<String, Double> expectedIndex = expectedEdge.getValue();

            JsonNode actualIndex = edgeIndexes.get(edgeId);

            if (actualIndex == null || actualIndex.isNull()) {
                throw new IllegalStateException("Missing full index for edge " + edgeId);
            }

            if (actualIndex.size() != expectedIndex.size()) {
                throw new IllegalStateException(
                        edgeId + " key count expected "
                                + expectedIndex.size()
                                + " but got "
                                + actualIndex.size()
                );
            }

            for (Map.Entry<String, Double> expectedEntry
                    : expectedIndex.entrySet()) {
                String key = expectedEntry.getKey();
                double expectedValue = expectedEntry.getValue();

                JsonNode actualValueNode = actualIndex.get(key);

                if (actualValueNode == null || actualValueNode.isNull()) {
                    throw new IllegalStateException(
                            edgeId + " missing key " + key
                    );
                }

                double actualValue = actualValueNode.asDouble();
                assertClose(edgeId + "[" + key + "]", expectedValue, actualValue);
            }

            System.out.println(
                    "Full-index validation passed for "
                            + edgeId
                            + ". Keys checked: "
                            + expectedIndex.size()
            );
        }
    }

    private static void writePhaseOneFullIndexFile(JsonNode payload,
                                                   String queryName) throws Exception {
        String path =
                "/tmp/onepass_"
                        + queryName
                        + "_phase1_full_indexes.json";

        writeJsonToFile(payload, path);
    }

    private static void writeJsonToFile(JsonNode json,
                                        String path) throws Exception {
        File outputFile = new File(path);
        FileWriter writer = new FileWriter(outputFile);

        try {
            writer.write(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(json)
            );
            writer.write(System.lineSeparator());
        } finally {
            writer.close();
        }

        System.out.println("Wrote SDE result to: " + outputFile.getAbsolutePath());
    }

    private static double sum(Map<String, Double> map) {
        double total = 0.0d;

        for (Double value : map.values()) {
            if (value != null) {
                total += value.doubleValue();
            }
        }

        return total;
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
                    label + " mismatch. Expected "
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

    private static void runOnlineMultinomialSamplerDuplicateSelfTest() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(5, "single-positive-candidate-test");

        sampler.add("only", 10.0d);

        OnlineMultinomialSample<String> sample = sampler.finish();

        if (sample.getSamples().size() != 5) {
            throw new IllegalStateException(
                    "OnlineMultinomialSampler duplicate self-test failed: expected 5 samples, got "
                            + sample.getSamples().size()
            );
        }

        for (String item : sample.getSamples()) {
            if (!"only".equals(item)) {
                throw new IllegalStateException(
                        "OnlineMultinomialSampler duplicate self-test failed: unexpected item "
                                + item
                );
            }
        }

        System.out.println(
                "OnlineMultinomialSampler duplicate self-test passed: "
                        + sample.getSamples()
        );
    }

    private static JsonNode waitForResponseContaining(KafkaConsumer<String, String> consumer,
                                                      int uid,
                                                      String requiredText,
                                                      long timeoutMs) throws Exception {
        long deadline =
                System.currentTimeMillis() + timeoutMs;

        String uidText =
                Integer.toString(uid);

        int recordsSeen =
                0;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records =
                    consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                recordsSeen++;

                String value =
                        record.value();

                System.out.println("Received estimationTopic record");
                System.out.println("  topic=" + record.topic()
                        + ", partition=" + record.partition()
                        + ", offset=" + record.offset()
                        + ", key=" + record.key());

                if (PRINT_FULL_KAFKA_RECORDS) {
                    System.out.println("  raw value:");
                    System.out.println(value);
                    System.out.println();
                }

                if (value == null || value.trim().isEmpty()) {
                    continue;
                }

                JsonNode envelope;

                try {
                    envelope =
                            parseJsonLenient(value);
                } catch (Exception parseError) {
                    System.out.println(
                            "Skipping estimationTopic record because it is not valid JSON."
                    );
                    parseError.printStackTrace(System.out);
                    continue;
                }

                JsonNode payload =
                        null;

                try {
                    payload = extractEstimationPayload(envelope);

                    if (PRINT_FULL_STATUS_PAYLOADS) {
                        System.out.println("  extracted estimation payload:");
                        System.out.println(
                                MAPPER.writerWithDefaultPrettyPrinter()
                                        .writeValueAsString(payload)
                        );
                        System.out.println();
                    } else {
                        printPayloadSummary(payload);
                    }
                } catch (Exception extractionError) {
                    System.out.println(
                            "Could not extract estimation payload from envelope."
                    );
                    extractionError.printStackTrace(System.out);
                }

                boolean rawHasRequiredText =
                        value.contains(requiredText);

                boolean payloadHasRequiredText =
                        payload != null
                                && payload.toString().contains(requiredText);

                boolean rawHasUid =
                        value.contains(uidText);

                boolean payloadHasUid =
                        payload != null
                                && payload.toString().contains(uidText);

                if (!rawHasUid && !payloadHasUid) {
                    System.out.println(
                            "Skipping estimationTopic record because it does not contain uid="
                                    + uid
                    );
                    System.out.println();
                    continue;
                }

                if (rawHasRequiredText || payloadHasRequiredText) {
                    System.out.println(
                            "Matched estimationTopic record for uid="
                                    + uid
                                    + " and expected text: "
                                    + requiredText
                    );
                    System.out.println();

                    return envelope;
                }

                System.out.println(
                        "Skipping estimationTopic record because it does not contain expected text: "
                                + requiredText
                );
                System.out.println();
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for response containing text='"
                        + requiredText
                        + "'. Records seen from estimationTopic: "
                        + recordsSeen
                        + ". Expected uid="
                        + uid
                        + ". If recordsSeen is 0, SDE did not publish any estimation. "
                        + "If records were printed above, inspect the raw value and extracted payload."
        );
    }

    private static JsonNode extractEstimationPayload(JsonNode envelope) throws Exception {
        if (envelope == null || envelope.isNull()) {
            throw new IllegalStateException("Missing response envelope");
        }

        JsonNode estimation =
                envelope.get("estimation");

        if (estimation == null || estimation.isNull()) {
            return envelope;
        }

        /*
         * OnePassSamplerSdeSynopsis returns Estimation.estimation as a JSON
         * string to avoid Flink/Kryo copying nested OnePass index objects.
         */
        if (estimation.isTextual()) {
            return MAPPER.readTree(estimation.asText());
        }

        return estimation;
    }

    private static JsonNode parseJsonLenient(String value) throws Exception {
        try {
            return MAPPER.readTree(value);
        } catch (Exception first) {
            /*
             * Some SDE output paths may stringify objects differently.
             * If this throws, print the raw value to make debugging direct.
             */
            System.out.println("Could not parse response as JSON:");
            System.out.println(value);
            throw first;
        }
    }

    private static JsonNode findFirst(JsonNode root,
                                      String fieldName) {
        if (root == null || fieldName == null) {
            return null;
        }

        if (root.has(fieldName)) {
            return root.get(fieldName);
        }

        if (root.isObject()) {
            java.util.Iterator<String> names =
                    root.fieldNames();

            while (names.hasNext()) {
                String name =
                        names.next();

                JsonNode found =
                        findFirst(root.get(name), fieldName);

                if (found != null) {
                    return found;
                }
            }
        }

        if (root.isArray()) {
            for (JsonNode child : root) {
                JsonNode found =
                        findFirst(child, fieldName);

                if (found != null) {
                    return found;
                }
            }
        }

        return null;
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props =
                new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("batch.size", "16384");
        props.put("linger.ms", "1");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    private static KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props =
                new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

    private static void sendDataBarrierAndWait(KafkaProducer<String, String> producer,
                                               KafkaConsumer<String, String> consumer,
                                               String datasetKey,
                                               String streamId,
                                               int uid,
                                               String phase,
                                               String alias,
                                               int expectedAcks,
                                               long timeoutMs) throws Exception {
        String barrierId =
                buildBarrierId(uid, phase, alias);

        ObjectNode barrierDatapoint =
                buildDataBarrierDatapoint(
                        datasetKey,
                        streamId,
                        uid,
                        phase,
                        alias,
                        barrierId,
                        expectedAcks
                );

        System.out.println("Sending data barrier:"
                + " phase=" + phase
                + ", alias=" + alias
                + ", barrierId=" + barrierId
                + ", expectedAcks=" + expectedAcks);

        sendJson(producer, DATA_TOPIC, datasetKey, barrierDatapoint);
        producer.flush();

        waitForDataBarrierAcks(
                consumer,
                uid,
                barrierId,
                expectedAcks,
                timeoutMs
        );
    }

    private static ObjectNode buildDataBarrierDatapoint(String datasetKey,
                                                        String streamId,
                                                        int uid,
                                                        String phase,
                                                        String alias,
                                                        String barrierId,
                                                        int expectedWorkers) {
        ObjectNode barrier =
                MAPPER.createObjectNode();

        barrier.put(ONEPASS_DATA_BARRIER_FIELD, true);
        barrier.put("uid", uid);
        barrier.put("phase", phase);
        barrier.put("barrierId", barrierId);
        barrier.put("expectedWorkers", expectedWorkers);

        if (alias != null && !alias.trim().isEmpty()) {
            barrier.put("alias", alias.trim());
        }

        ObjectNode datapoint =
                MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);
        datapoint.put("streamID", streamId);
        datapoint.set("values", barrier);

        return datapoint;
    }

    private static void waitForDataBarrierAcks(KafkaConsumer<String, String> consumer, int uid, String barrierId,
                                               int expectedAcks, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;

        Set<Integer> workerIds = new LinkedHashSet<Integer>();

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
                    envelope = parseJsonLenient(value);
                } catch (Exception ignored) {
                    continue;
                }

                JsonNode uidNode = findFirst(envelope, "uid");

                if (uidNode == null || uidNode.asInt() != uid) {
                    continue;
                }

                JsonNode ackPayload = extractDataBarrierAckPayload(envelope);

                boolean rawMatches = value.contains("DATA_BARRIER_ACK") && value.contains(barrierId);

                boolean payloadMatches = ackPayload != null
                        && "DATA_BARRIER_ACK".equals(textOrNull(ackPayload.get("type")))
                        && barrierId.equals(textOrNull(ackPayload.get("barrierId")));

                if (!rawMatches && !payloadMatches) {
                    continue;
                }

                int workerId = -1;

                if (ackPayload != null && ackPayload.has("workerId")) {
                    workerId = ackPayload.get("workerId").asInt();
                }

                workerIds.add(workerId);

                System.out.println("Received DATA_BARRIER_ACK:" + " barrierId=" + barrierId + ", workerId=" + workerId
                        + ", receivedAcks=" + workerIds.size() + "/" + expectedAcks);

                if (workerIds.size() >= expectedAcks) {
                    System.out.println("Data barrier completed: barrierId=" + barrierId + ", workers=" + workerIds);
                    System.out.println();
                    return;
                }
            }
        }

        throw new IllegalStateException("Timed out waiting for DATA_BARRIER_ACK barrierId=" + barrierId +
                ". Received workers=" + workerIds + ", expectedAcks=" + expectedAcks + ", recordsSeen=" + recordsSeen);
    }

    private static JsonNode extractDataBarrierAckPayload(JsonNode envelope) {
        if (envelope == null || envelope.isNull()) {
            return null;
        }

        JsonNode estimation = envelope.get("estimation");

        if (estimation == null || estimation.isNull()) {
            return envelope;
        }

        if (estimation.isObject()) {
            return estimation;
        }

        if (estimation.isTextual()) {
            String text = estimation.asText();

            if (text == null || text.trim().isEmpty()) {
                return null;
            }

            try {
                return MAPPER.readTree(text);
            } catch (Exception ignored) {
                return null;
            }
        }

        return null;
    }

    private static String buildBarrierId(int uid, String phase, String alias) {
        String cleanPhase = phase == null ? "UNKNOWN" : phase.replaceAll("[^A-Za-z0-9_]", "_");

        String cleanAlias = alias == null || alias.trim().isEmpty() ? "none" :
                alias.replaceAll("[^A-Za-z0-9_]", "_");

        return cleanPhase + "_" + cleanAlias + "_" + uid + "_" + System.nanoTime();
    }

    private static void sendJsonAsync(KafkaProducer<String, String> producer, String topic, String key, JsonNode json) {
        producer.send(new ProducerRecord<String, String>(topic, key, json.toString()));
    }

    private static void sendJson(KafkaProducer<String, String> producer,
                                 String topic,
                                 String key,
                                 JsonNode json) throws Exception {
        Future<RecordMetadata> future =
                producer.send(
                        new ProducerRecord<String, String>(
                                topic,
                                key,
                                json.toString()
                        )
                );

        future.get();
    }

    private static void drainConsumer(KafkaConsumer<String, String> consumer) {
        consumer.poll(500);
        consumer.poll(500);
    }

    private static void validateControlAck(JsonNode response,
                                           String expectedCommand) {
        if (response == null || response.isNull()) {
            throw new IllegalStateException(
                    "Missing ACK response for command " + expectedCommand
            );
        }

        JsonNode requestId =
                findFirst(response, "requestID");

        if (requestId == null || requestId.asInt() != REQUEST_UPDATE) {
            throw new IllegalStateException(
                    "Expected requestID="
                            + REQUEST_UPDATE
                            + " for command "
                            + expectedCommand
                            + ", got: "
                            + requestId
            );
        }

        JsonNode synopsisId =
                findFirst(response, "synopsisID");

        if (synopsisId == null || synopsisId.asInt() != SYNOPSIS_ID) {
            throw new IllegalStateException(
                    "Expected synopsisID="
                            + SYNOPSIS_ID
                            + " for command "
                            + expectedCommand
                            + ", got: "
                            + synopsisId
            );
        }

        if (!response.toString().contains(expectedCommand)) {
            throw new IllegalStateException(
                    "ACK response does not contain expected command "
                            + expectedCommand
                            + ". Response: "
                            + response
            );
        }

        System.out.println("Validated ACK for " + expectedCommand);
    }

    private static void printPayloadSummary(JsonNode payload) {
        if (payload == null || payload.isNull()) {
            System.out.println("  payload: null");
            return;
        }

        JsonNode phase =
                findFirst(payload, "phase");

        JsonNode phaseOneComplete =
                findFirst(payload, "phaseOneComplete");

        JsonNode phaseTwoComplete =
                findFirst(payload, "phaseTwoComplete");

        JsonNode phaseThreeComplete =
                findFirst(payload, "phaseThreeComplete");

        JsonNode rootTuplesSeen =
                findFirst(payload, "rootTuplesSeen");

        JsonNode positiveRootCandidatesSeen =
                findFirst(payload, "positiveRootCandidatesSeen");

        JsonNode sampleInstances =
                findFirst(payload, "sampleInstances");

        JsonNode completedSamples =
                findFirst(payload, "completedSamples");

        System.out.println("  payload summary:"
                + " phase=" + textOrNull(phase)
                + ", phaseOneComplete=" + boolOrNull(phaseOneComplete)
                + ", phaseTwoComplete=" + boolOrNull(phaseTwoComplete)
                + ", phaseThreeComplete=" + boolOrNull(phaseThreeComplete)
                + ", rootTuplesSeen=" + longOrNull(rootTuplesSeen)
                + ", positiveRootCandidatesSeen=" + longOrNull(positiveRootCandidatesSeen)
                + ", sampleInstances=" + arraySizeOrNull(sampleInstances)
                + ", completedSamples=" + arraySizeOrNull(completedSamples));
    }

    private static String textOrNull(JsonNode node) {
        return node == null || node.isNull() ? "null" : node.asText();
    }

    private static String boolOrNull(JsonNode node) {
        return node == null || node.isNull() ? "null" : Boolean.toString(node.asBoolean());
    }

    private static String longOrNull(JsonNode node) {
        return node == null || node.isNull() ? "null" : Long.toString(node.asLong());
    }

    private static String arraySizeOrNull(JsonNode node) {
        return node == null || !node.isArray() ? "null" : Integer.toString(node.size());
    }

    private static void validateProjectedCompletedSamplesIfPresent(JsonNode phaseThreePayload,
                                                                   int expectedCompletedSampleCount,
                                                                   CompiledOnePassPlan plan) {

        JsonNode projectedSamplesIncludedNode = findFirst(phaseThreePayload, "projectedSamplesIncluded");

        boolean projectedSamplesIncluded = projectedSamplesIncludedNode == null ||
                projectedSamplesIncludedNode.asBoolean();

        JsonNode projectedCompletedSamples = findFirst(phaseThreePayload, "projectedCompletedSamples");

        if (projectedCompletedSamples == null || !projectedCompletedSamples.isArray()) {
            throw new IllegalStateException("Expected projectedCompletedSamples array, got: " +
                    projectedCompletedSamples);
        }

        if (!projectedSamplesIncluded) {
            System.out.println("Projected completed samples are compacted in this run. " +
                    "Full projected output omitted because sample size is large.");
            return;
        }

        if (projectedCompletedSamples.size() != expectedCompletedSampleCount) {
            throw new IllegalStateException("projectedCompletedSamples size mismatch. Expected " +
                    expectedCompletedSampleCount + ", got " + projectedCompletedSamples.size());
        }

        for (JsonNode sample : projectedCompletedSamples) {
            JsonNode projected = sample.get("projected");

            if (projected == null || !projected.isObject()) {
                throw new IllegalStateException("Projected sample is missing projected object: " + sample);
            }

            for (String projectionItem : plan.getProjection()) {
                if (projectionItem == null) {
                    continue;
                }

                String trimmed = projectionItem.trim();

                if (trimmed.isEmpty() || "*".equals(trimmed)) {
                    continue;
                }

                if (!projected.has(trimmed)) {
                    throw new IllegalStateException("Projected sample is missing field " + trimmed +
                            ". Sample: " + sample);
                }
            }
        }

        System.out.println("Validated projected completed sample output.");
    }

    private static void printProjectedCompletedSamplesPreview(JsonNode phaseThreePayload, int limit)
            throws Exception {
        if (phaseThreePayload == null || phaseThreePayload.isNull()) {
            System.out.println("No Phase 3 payload available for projected sample preview.");
            return;
        }

        JsonNode projectedSamples = findFirst(phaseThreePayload, "projectedCompletedSamples");
        String sourceField = "projectedCompletedSamples";

        /*
         * In large benchmark runs, the full projectedCompletedSamples array may be
         * intentionally empty and only projectedCompletedSamplesPreview is sent.
         */
        if (projectedSamples == null || !projectedSamples.isArray() | projectedSamples.size() == 0) {

            projectedSamples = findFirst(phaseThreePayload, "projectedCompletedSamplesPreview");
            sourceField = "projectedCompletedSamplesPreview";
        }

        if (projectedSamples == null || !projectedSamples.isArray()) {
            System.out.println("No projected completed samples found in Phase 3 payload. "
                            + "Expected projectedCompletedSamples or projectedCompletedSamplesPreview.");
            return;
        }

        int count = Math.min(limit, projectedSamples.size());

        System.out.println();
        System.out.println("=== Projected final sample preview ===");
        System.out.println("sourceField: " + sourceField);
        System.out.println("showing: " + count + " of " + projectedSamples.size());
        System.out.println();

        for (int i = 0; i < count; i++) {
            System.out.println("Projected sample #" + (i + 1) + ":");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(projectedSamples.get(i)));
            System.out.println();
        }

        System.out.println("======================================");
        System.out.println();
    }

    private static long tic() {
        return System.nanoTime();
    }

    private static void recordDuration(String label, long startNanos) {
        long elapsed = System.nanoTime() - startNanos;

        benchmarkNanos.merge(label, elapsed, Long::sum);
    }

    private static void recordDetailedDuration(String label, long startNanos) {
        if (PRINT_DETAILED_BENCHMARK_TIMINGS) {
            recordDuration(label, startNanos);
        }
    }

    private static long nanosFor(String label) {
        Long value = benchmarkNanos.get(label);
        return value == null ? 0L : value;
    }

    private static double secondsFor(String label) {
        return nanosFor(label) / 1_000_000_000.0d;
    }

    private static void printTimingLine(String label, double seconds) {
        System.out.printf("%-45s %12.3f s%n", label, seconds);
    }

    private static void writeBenchmarkCsv(CompiledOnePassPlan plan, String implementation) throws Exception {
        if (!WRITE_BENCHMARK_CSV) {
            return;
        }

        double phase1Stream = secondsFor("phase1_stream_send");
        double phase2Stream = secondsFor("phase2_root_stream_send");
        double phase3Stream = secondsFor("phase3_side_stream_send_total");

        double phase1DataBarrier = secondsFor("phase1_data_barrier_ack");
        double phase2DataBarrier = secondsFor("phase2_data_barrier_ack");
        double phase3DataBarrier = secondsFor("phase3_data_barrier_ack_total");

        double dataBarrierTotal = phase1DataBarrier + phase2DataBarrier + phase3DataBarrier;
        double dataStreamTotal = phase1Stream + phase2Stream + phase3Stream;

        double phase1ControlStatus = secondsFor("phase1_finish_ack") + secondsFor("phase1_status");
        double phase2ControlStatus = secondsFor("phase2_finish_ack") + secondsFor("phase2_status");
        double phase3ControlStatus = secondsFor("phase3_control_ack_total") +
                secondsFor("phase3_finish_ack") + secondsFor("phase3_status");

        double controlStatusTotal = phase1ControlStatus + phase2ControlStatus + phase3ControlStatus;
        double artificialWait = secondsFor("total_artificial_wait");
        double observedEndToEnd = secondsFor("total_end_to_end_observed");
        double adjustedEndToEnd = observedEndToEnd - artificialWait;

        if (adjustedEndToEnd < 0.0d) {
            adjustedEndToEnd = 0.0d;
        }

        File csvFile = new File(BENCHMARK_CSV_PATH);
        boolean writeHeader = !csvFile.exists() || csvFile.length() == 0L;

        FileWriter writer = new FileWriter(csvFile, true);

        try {
            if (writeHeader) {
                writer.write(
                        "timestamp_ms,"
                                + "implementation,"
                                + "query_name,"
                                + "seed,"
                                + "test_row_limit,"
                                + "sample_size_limit,"
                                + "root_alias,"
                                + "leaf_to_root_order,"
                                + "root_to_leaf_order,"
                                + "projection,"
                                + "phase1_stream_send_s,"
                                + "phase2_root_stream_send_s,"
                                + "phase3_side_stream_send_total_s,"
                                + "data_stream_send_total_s,"
                                + "phase1_data_barrier_ack_s,"
                                + "phase2_data_barrier_ack_s,"
                                + "phase3_data_barrier_ack_total_s,"
                                + "data_barrier_ack_total_s,"
                                + "phase1_control_status_s,"
                                + "phase2_control_status_s,"
                                + "phase3_control_status_s,"
                                + "control_status_total_s,"
                                + "artificial_wait_s,"
                                + "adjusted_end_to_end_no_artificial_wait_s,"
                                + "observed_end_to_end_with_test_waits_s"
                                + System.lineSeparator()
                );
            }

            writer.write(
                    Long.toString(System.currentTimeMillis())
                            + "," + csv(implementation)
                            + "," + csv(plan.getQueryName())
                            + "," + csv(plan.getDatasetSeed())
                            + "," + csv(formatRowLimit(TEST_ROW_LIMIT))
                            + "," + plan.getSampleSize()
                            + "," + csv(plan.getRootAlias())
                            + "," + csv(String.valueOf(plan.getLeafToRootOrder()))
                            + "," + csv(String.valueOf(plan.getRootToLeafOrder()))
                            + "," + csv(String.valueOf(plan.getProjection()))
                            + "," + doubleCsv(phase1Stream)
                            + "," + doubleCsv(phase2Stream)
                            + "," + doubleCsv(phase3Stream)
                            + "," + doubleCsv(dataStreamTotal)
                            + "," + doubleCsv(phase1DataBarrier)
                            + "," + doubleCsv(phase2DataBarrier)
                            + "," + doubleCsv(phase3DataBarrier)
                            + "," + doubleCsv(dataBarrierTotal)
                            + "," + doubleCsv(phase1ControlStatus)
                            + "," + doubleCsv(phase2ControlStatus)
                            + "," + doubleCsv(phase3ControlStatus)
                            + "," + doubleCsv(controlStatusTotal)
                            + "," + doubleCsv(artificialWait)
                            + "," + doubleCsv(adjustedEndToEnd)
                            + "," + doubleCsv(observedEndToEnd)
                            + System.lineSeparator()
            );
        } finally {
            writer.close();
        }

        System.out.println("Benchmark CSV appended to: " + csvFile.getAbsolutePath());
    }

    private static String doubleCsv(double value) {
        return Double.toString(value);
    }

    private static String csv(String value) {
        if (value == null) {
            return "";
        }
        String escaped = value.replace("\"", "\"\"");

        return "\"" + escaped + "\"";
    }

    private static String formatRowLimit(long rowLimit) {
        if (rowLimit < 0L) {
            return "FULL";
        }

        return Long.toString(rowLimit);
    }

    private static void printBenchmarkSummary(CompiledOnePassPlan plan) {
        double phase1Stream = secondsFor("phase1_stream_send");
        double phase2Stream = secondsFor("phase2_root_stream_send");
        double phase3Stream = secondsFor("phase3_side_stream_send_total");
        double dataStreamTotal = phase1Stream + phase2Stream + phase3Stream;

        double phase1DataBarrier = secondsFor("phase1_data_barrier_ack");
        double phase2DataBarrier = secondsFor("phase2_data_barrier_ack");
        double phase3DataBarrier = secondsFor("phase3_data_barrier_ack_total");

        double dataBarrierTotal = secondsFor("phase1_data_barrier_ack") +
                secondsFor("phase2_data_barrier_ack") +
                secondsFor("phase3_data_barrier_ack_total");

        double phase1ControlStatus = secondsFor("phase1_finish_ack") + secondsFor("phase1_status");
        double phase2ControlStatus = secondsFor("phase2_finish_ack") + secondsFor("phase2_status");
        double phase3ControlStatus = secondsFor("phase3_control_ack_total") +
                secondsFor("phase3_finish_ack") + secondsFor("phase3_status");

        double controlStatusTotal = phase1ControlStatus + phase2ControlStatus + phase3ControlStatus;
        double observedEndToEnd = secondsFor("total_end_to_end_observed");
        double adjustedEndToEnd = observedEndToEnd - secondsFor("total_artificial_wait");

        if (adjustedEndToEnd < 0.0d) {
            adjustedEndToEnd = 0.0d;
        }

        System.out.println();
        System.out.println("=== OnePass comparison benchmark ===");
        System.out.println("TEST_ROW_LIMIT:            " + formatRowLimit(TEST_ROW_LIMIT));
        System.out.println("phase2_sample_count_LIMIT: " + plan.getSampleSize());
        printTimingLine("phase1_stream_send", phase1Stream);
        printTimingLine("phase2_root_stream_send", phase2Stream);
        printTimingLine("phase3_side_stream_send_total", phase3Stream);
        printTimingLine("data_stream_send_total", dataStreamTotal);
        printTimingLine("data_barrier_ack_total", dataBarrierTotal);
        printTimingLine("data_barrier_ack_total", dataBarrierTotal);
        printTimingLine("control_status_total", controlStatusTotal);
        printTimingLine("adjusted_end_to_end_no_artificial_wait", adjustedEndToEnd);
        printTimingLine("observed_end_to_end_with_test_waits", observedEndToEnd);
        System.out.println("====================================");
        System.out.println();

        if (PRINT_DETAILED_BENCHMARK_TIMINGS) {
            System.out.println("=== Detailed OnePass benchmark timings ===");

            for (Map.Entry<String, Long> entry : benchmarkNanos.entrySet()) {
                printTimingLine(entry.getKey(), entry.getValue() / 1_000_000_000.0d);
            }

            System.out.println("===========================================");
            System.out.println();
        }
    }
}