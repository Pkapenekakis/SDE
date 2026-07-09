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
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.io.FileWriter;

/**
 * Multi-worker OnePass* coordinator test using real query compilation and TPC-H input.

 * For WQ3 the expected flow is:
 *
 *   alias l:
 *     stream l
 *     barrier
 *     FINISH_PHASE_1(resultId, alias=l)
 *     merge/install global l<->o
 *     workers stay in PHASE_1
 *
 *   alias o:
 *     stream o
 *     workers use installed global l<->o
 *     barrier
 *     FINISH_PHASE_1(resultId, alias=o)
 *     merge/install global c<->o
 *     workers move to PHASE_2
 *
 * This test stops after the complete Phase 1 index is installed.
 */
public final class OnePassSamplerSdeCoordinatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DATA_TOPIC = "dataTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";
    private static final String REQUEST_TOPIC = "requestTopic";

    /*
     * Keep your Run.java default or argument value aligned with this topic name.
     */
    @SuppressWarnings("unused")
    private static final String GLOBAL_STATE_TOPIC = "globalStateTopic";

    private static final String TEST_TPCH_DIR = "/home/vboxuser/Desktop/Thesis/tpch-data/sf1";

    private static final String TEST_ONEPASS_SQL =
            "SELECT * FROM wq3_alias WEIGHTED BY (" +
                    "o.o_totalprice * (l.l_extendedprice * (1 - l.l_discount))) " +
                    "LIMIT 10000 /* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    /*
     * Use -1 for the whole file.
     * Keep this modest while debugging.
     */
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

    private OnePassSamplerSdeCoordinatorTest() {
    }

    public static void main(String[] args) throws Exception {
        int uid = Math.abs(UUID.randomUUID().toString().hashCode());

        String streamId = "onepass-coordinator-tpch-test";
        String phase = "PHASE1";
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
        KafkaConsumer<String, String> consumer = createConsumer("onepass-coordinator-tpch-test-" + uid);

        consumer.subscribe(Collections.singletonList(ESTIMATION_TOPIC));

        try {
            drainConsumer(consumer);

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

            JsonNode finalGlobalPhaseOneResult = null;
            String finalPhaseOneAlias = "";
            String finalPhaseOneEdgeId = "";

            for (String phaseOneAlias : plan.getLeafToRootOrder()) {
                aliasPosition++;

                CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                        plan.getParentEdge(phaseOneAlias);

                if (parentEdge == null) {
                    throw new IllegalStateException("Phase 1 alias has no parent edge: " + phaseOneAlias);
                }

                String expectedEdgeId = parentEdge.getEdgeId();
                String resultId = "PHASE1_" + phaseOneAlias + "_" + uid;
                String barrierId = "TPCH_PHASE1_" + phaseOneAlias + "_" + uid + "_" + System.nanoTime();
                String expectedStateRef = uid + "_PHASE1_" + resultId + "_GLOBAL_STATE";

                System.out.println();
                System.out.println("=======================================================");
                System.out.println("PHASE_1 alias " + aliasPosition + "/" + plan.getLeafToRootOrder().size()
                        + ": " + phaseOneAlias);
                System.out.println("expectedEdgeId = " + expectedEdgeId);
                System.out.println("resultId = " + resultId);
                System.out.println("expectedStateRef = " + expectedStateRef);
                System.out.println("=======================================================");
                System.out.println();

                System.out.println("Streaming PHASE_1 alias from TPC-H: " + phaseOneAlias + "...");
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

                if (rowsSent <= 0L) {
                    throw new IllegalStateException("No rows were streamed for alias " + phaseOneAlias);
                }

                System.out.println("Rows sent for alias " + phaseOneAlias + ": " + rowsSent);
                System.out.println();

                System.out.println("Sending Phase 1 data barrier for alias " + phaseOneAlias + "...");
                ObjectNode barrierDatapoint = buildDataBarrierDatapoint(
                        baseKey,
                        streamId,
                        uid,
                        phase,
                        phaseOneAlias,
                        barrierId,
                        EXPECTED_WORKERS
                );

                sendJson(producer, DATA_TOPIC, baseKey, barrierDatapoint);
                producer.flush();

                System.out.println("Waiting for GLOBAL_BARRIER_READY for alias " + phaseOneAlias + "...");
                JsonNode barrierReady = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "GLOBAL_BARRIER_READY",
                        "barrierId",
                        barrierId,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validateNoMissingSynopsisWorkers(barrierReady);
                printMatchedPayload("GLOBAL_BARRIER_READY " + phaseOneAlias, barrierReady);

                System.out.println("Sending FINISH_PHASE_1 request for alias " + phaseOneAlias + "...");
                ObjectNode finishPhaseOneRequest = buildFinishPhaseOneRequest(
                        baseKey,
                        streamId,
                        uid,
                        resultId,
                        phaseOneAlias,
                        EXPECTED_WORKERS
                );

                sendJson(producer, REQUEST_TOPIC, baseKey, finishPhaseOneRequest);
                producer.flush();

                JsonNode globalPhaseOneResult = null;

                if (WAIT_FOR_FULL_GLOBAL_RESULTS) {
                    System.out.println("Waiting for GLOBAL_PHASE1_RESULT for alias " + phaseOneAlias + "...");
                    globalPhaseOneResult = waitForCoordinatorMessage(
                            consumer,
                            uid,
                            "GLOBAL_PHASE1_RESULT",
                            "resultId",
                            resultId,
                            EXPECTED_WORKERS,
                            TIMEOUT_MS
                    );

                    finalGlobalPhaseOneResult = globalPhaseOneResult;
                    finalPhaseOneAlias = phaseOneAlias;
                    finalPhaseOneEdgeId = expectedEdgeId;

                    validateGlobalPhaseOneResult(globalPhaseOneResult, expectedEdgeId, EXPECTED_WORKERS);
                    validateActiveAlias(globalPhaseOneResult, phaseOneAlias);
                    printMatchedPayload("GLOBAL_PHASE1_RESULT " + phaseOneAlias, globalPhaseOneResult);
                } else {
                    System.out.println("Skipping full GLOBAL_PHASE1_RESULT wait for large-scale mode, alias=" + phaseOneAlias);
                    finalPhaseOneAlias = phaseOneAlias;
                    finalPhaseOneEdgeId = expectedEdgeId;
                }
                System.out.println("Waiting for GLOBAL_PHASE1_RESULT_READY for alias " + phaseOneAlias + "...");
                JsonNode globalPhaseOneReady = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "GLOBAL_PHASE1_RESULT_READY",
                        "resultId",
                        resultId,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validateGlobalPhaseOneReady(globalPhaseOneReady, expectedStateRef, EXPECTED_WORKERS);
                validateActiveAlias(globalPhaseOneReady, phaseOneAlias);
                printMatchedPayload("GLOBAL_PHASE1_RESULT_READY " + phaseOneAlias, globalPhaseOneReady);

                System.out.println("Waiting for GLOBAL_PHASE1_INDEX_INSTALLED for alias " + phaseOneAlias + "...");
                JsonNode installed = waitForCoordinatorMessage(
                        consumer,
                        uid,
                        "GLOBAL_PHASE1_INDEX_INSTALLED",
                        "stateRef",
                        expectedStateRef,
                        EXPECTED_WORKERS,
                        TIMEOUT_MS
                );

                validateGlobalPhaseOneIndexInstalled(installed, EXPECTED_WORKERS);
                printMatchedPayload("GLOBAL_PHASE1_INDEX_INSTALLED " + phaseOneAlias, installed);

                System.out.println("Alias " + phaseOneAlias + " completed and installed on all workers.");
            }

            if (EXPORT_FINAL_PHASE1_INDEX) {
                if (!WAIT_FOR_FULL_GLOBAL_RESULTS) {
                    throw new IllegalStateException(
                            "EXPORT_FINAL_PHASE1_INDEX requires WAIT_FOR_FULL_GLOBAL_RESULTS=true."
                    );
                }

                writeFinalPhaseOneIndexForPythonValidator(
                        finalGlobalPhaseOneResult,
                        uid,
                        plan.getQueryName(),
                        plan.getRootAlias(),
                        finalPhaseOneAlias,
                        finalPhaseOneEdgeId
                );
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
            System.out.println("SUCCESS: SQL/TPC-H multi-worker OnePass* Phase 1 + Phase 2 + Phase 3 test passed.");
            System.out.println("Validated Phase 1 aliases=" + plan.getLeafToRootOrder());
            System.out.println("Validated Phase 3 aliases=" + nonRootRootToLeafAliases(plan));
            System.out.println("expectedWorkers=" + EXPECTED_WORKERS);

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
                    producer.flush();
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

    private static void writeFinalPhaseOneIndexForPythonValidator(
            JsonNode globalPhaseOneEnvelope,
            int uid,
            String queryName,
            String rootAlias,
            String finalAlias,
            String finalEdgeId) throws Exception {

        if (globalPhaseOneEnvelope == null || globalPhaseOneEnvelope.isNull()) {
            throw new IllegalStateException(
                    "Cannot export final Phase 1 index because GLOBAL_PHASE1_RESULT is null."
            );
        }

        JsonNode globalPhaseOneResult =
                globalPhaseOneEnvelope.get("globalPhaseOneResult");

        if (globalPhaseOneResult == null || globalPhaseOneResult.isNull()) {
            throw new IllegalStateException(
                    "Cannot export final Phase 1 index. Missing globalPhaseOneResult: "
                            + globalPhaseOneEnvelope
            );
        }

        JsonNode edgeIndexes = globalPhaseOneResult.get("edgeIndexes");

        if (edgeIndexes == null || !edgeIndexes.isObject()) {
            throw new IllegalStateException(
                    "Cannot export final Phase 1 index. Missing edgeIndexes: "
                            + globalPhaseOneResult
            );
        }

        JsonNode edgeSummaries = globalPhaseOneResult.get("edgeSummaries");

        if (edgeSummaries == null || !edgeSummaries.isObject()) {
            throw new IllegalStateException(
                    "Cannot export final Phase 1 index. Missing edgeSummaries: "
                            + globalPhaseOneResult
            );
        }

        ObjectNode export = MAPPER.createObjectNode();

        /*
         * IMPORTANT:
         * The Python validator expects edgeIndexes / edgeSummaries at the top level.
         * Do not nest them under globalPhaseOneResult.
         */
        export.put("type", "ONEPASS_PHASE1_FULL_INDEX_EXPORT");
        export.put("implementation", "parallel-sde");
        export.put("uid", uid);
        export.put("queryName", queryName == null ? "" : queryName);
        export.put("rootAlias", rootAlias == null ? "" : rootAlias);
        export.put("finalAlias", finalAlias == null ? "" : finalAlias);
        export.put("finalEdgeId", finalEdgeId == null ? "" : finalEdgeId);

        JsonNode resultId = globalPhaseOneEnvelope.get("resultId");
        JsonNode stateRef = globalPhaseOneEnvelope.get("stateRef");
        JsonNode seenTuplesByAlias = globalPhaseOneResult.get("seenTuplesByAlias");

        if (resultId != null && !resultId.isNull()) {
            export.set("resultId", resultId);
        }

        if (stateRef != null && !stateRef.isNull()) {
            export.set("stateRef", stateRef);
        }

        if (seenTuplesByAlias != null && !seenTuplesByAlias.isNull()) {
            export.set("seenTuplesByAlias", seenTuplesByAlias);
        }

        export.set("edgeIndexes", edgeIndexes);
        export.set("edgeSummaries", edgeSummaries);

        File outputFile = new File(PHASE1_INDEX_EXPORT_DIR);
        FileWriter writer = new FileWriter(outputFile);

        try {
            writer.write(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(export)
            );
            writer.write(System.lineSeparator());
        } finally {
            writer.close();
        }

        System.out.println();
        System.out.println("Exported final Phase 1 index for Python validator:");
        System.out.println(outputFile.getAbsolutePath());
        System.out.println("queryName=" + queryName);
        System.out.println("rootAlias=" + rootAlias);
        System.out.println("finalAlias=" + finalAlias);
        System.out.println("finalEdgeId=" + finalEdgeId);
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
}
