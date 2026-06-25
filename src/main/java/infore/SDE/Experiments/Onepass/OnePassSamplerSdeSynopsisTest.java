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
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
 *   4. Wait for returned status/result.
 *   5. Stream Phase 2 root alias.
 *   6. Send FINISH_PHASE_2 request.
 *   7. Wait for Phase 2 result.
 *
 * This tests the new SDE-facing lifecycle:
 *
 *   SDEcoFlatMap
 *      -> OnePassSamplerSdeSynopsis
 *          -> OnePassSamplerSynopsis
 *              -> OnePassPhaseOneState
 *              -> OnePassPhaseTwoState
 *                  -> OnePassRootSampler
 *                      -> OnlineMultinomialSampler
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

    private static final String TEST_ONEPASS_SQL =
            "SELECT * FROM wq3_alias WEIGHTED BY (o.o_totalprice * " +
                    "(l.l_extendedprice * (1 - l.l_discount))) LIMIT 100 "+
                    "/* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    /*
     * Use 5000 first while testing wiring.
     * Use -1 for full TPC-H files.
     */
    private static final long TEST_ROW_LIMIT = 5000L;

    private static final int SYNOPSIS_ID = 30;
    private static final int PHASE1_DEBUG_SYNOPSIS_ID = 31;
    private static final int REQUEST_ADD = 1;
    private static final int REQUEST_ESTIMATE = 3;
    private static final int REQUEST_UPDATE = 7;

    /*
     * Keep this enabled while validating Phase 1 inside the combined
     * OnePassSamplerSdeSynopsis lifecycle.
     *
     * The combined synopsis intentionally does not send full Phase 1 indexes
     * in its status payload because those internal objects are heavy and may
     * trigger Flink/Kryo serialization issues.
     *
     * Therefore, this test also creates a temporary synopsisID=31 Phase 1
     * debug synopsis under the same dataset key. The same Phase 1 side tuples
     * are delivered to both synopses. We then request debug-full-indexes from
     * the Phase 1 debug synopsis and write the JSON file expected by the
     * external Python validator.
     */
    private static final boolean EXPORT_PHASE1_FULL_INDEXES = true;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, Map<String, Double>> expectedIndexesByEdge =
            new LinkedHashMap<String, Map<String, Double>>();

    private static final Map<String, Long> sentRowsByAlias =
            new LinkedHashMap<String, Long>();

    private static long expectedRootTuplesSeen = 0L;
    private static long expectedPositiveRootCandidatesSeen = 0L;
    private static double expectedTotalRootGroupWeight = 0.0d;

    private OnePassSamplerSdeSynopsisTest() {
    }

    public static void main(String[] args) throws Exception {
        int uid = Math.abs(UUID.randomUUID().toString().hashCode());
        int phaseOneDebugUid = uid == Integer.MAX_VALUE ? uid - 1 : uid + 1;

        expectedIndexesByEdge.clear();
        sentRowsByAlias.clear();
        expectedRootTuplesSeen = 0L;
        expectedPositiveRootCandidatesSeen = 0L;
        expectedTotalRootGroupWeight = 0.0d;

        runOnlineMultinomialSamplerDuplicateSelfTest();

        String datasetKey = Integer.toString(uid);//"onepass-sampler-wq3_alias-" + uid;
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
        OnePassWeightEvaluator expectedWeightEvaluator =
                new OnePassWeightEvaluator(params.getWeight());

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Root-to-leaf order: " + plan.getRootToLeafOrder());
        System.out.println("Weights by alias: " + plan.getWeightsByAlias());
        System.out.println();

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer =
                createConsumer("onepass-sampler-test-" + uid);

        consumer.subscribe(Collections.singletonList(ESTIMATION_TOPIC));

        try {
            drainConsumer(consumer);

            System.out.println("1. Sending ADD request for OnePassSamplerSdeSynopsis...");
            ObjectNode addRequest = buildAddRequest(uid, datasetKey, streamId);

            sendJson(producer, REQUEST_TOPIC, datasetKey, addRequest);
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(addRequest));
            System.out.println();

            if (EXPORT_PHASE1_FULL_INDEXES) {
                System.out.println("1b. Sending ADD request for temporary Phase 1 debug synopsis...");
                ObjectNode phaseOneDebugAddRequest =
                        buildPhaseOneDebugAddRequest(
                                phaseOneDebugUid,
                                datasetKey,
                                streamId
                        );

                sendJson(producer, REQUEST_TOPIC, datasetKey, phaseOneDebugAddRequest);
                System.out.println(
                        MAPPER.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(phaseOneDebugAddRequest)
                );
                System.out.println();
            }

            /*
             * Give SDE a short moment to create the synopsis before sending tuples.
             */
            Thread.sleep(1500L);

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
                        expectedWeightEvaluator);

                phaseOneRows += count;

                System.out.println("  PHASE_1 alias " + alias + " rows: " + count);
            }

            producer.flush();

            System.out.println("Total PHASE_1 rows sent: " + phaseOneRows);
            System.out.println();

            /*
             * Temporary testing barrier.
             * Kafka/Flink does not guarantee ordering across dataTopic and requestTopic.
             * This gives SDE time to consume PHASE_1 tuples before FINISH_PHASE_1.
             */
            System.out.println("Waiting for SDE to consume PHASE_1 side tuples...");
            Thread.sleep(10000L);

            System.out.println("3. Sending FINISH_PHASE_1 request...");
            ObjectNode finishPhaseOneRequest =
                    buildControlRequest(
                            uid,
                            datasetKey,
                            streamId,
                            "FINISH_PHASE_1"
                    );

            sendJson(producer, REQUEST_TOPIC, datasetKey, finishPhaseOneRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(finishPhaseOneRequest));

            System.out.println();

            System.out.println("4. Waiting for FINISH_PHASE_1 ACK...");
            JsonNode phaseOneAck =
                    waitForResponseContaining(
                            consumer,
                            uid,
                            "FINISH_PHASE_1",
                            120000L
                    );

            System.out.println("FINISH_PHASE_1 ACK envelope:");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseOneAck));
            System.out.println();

            validateControlAck(phaseOneAck, "FINISH_PHASE_1");

            System.out.println("4a. Requesting OnePassSamplerSdeSynopsis STATUS after FINISH_PHASE_1...");
            ObjectNode phaseOneStatusRequest =
                    buildStatusRequest(
                            uid,
                            datasetKey,
                            streamId
                    );

            sendJson(producer, REQUEST_TOPIC, datasetKey, phaseOneStatusRequest);
            producer.flush();

            System.out.println(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(phaseOneStatusRequest)
            );
            System.out.println();

            JsonNode phaseOneStatusEnvelope =
                    waitForResponseContaining(
                            consumer,
                            uid,
                            "PHASE_2",
                            120000L
                    );

            System.out.println("PHASE_1 STATUS envelope:");
            System.out.println(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(phaseOneStatusEnvelope)
            );
            System.out.println();

            JsonNode phaseOneStatusPayload =
                    extractEstimationPayload(phaseOneStatusEnvelope);

            validatePhaseOneTransition(phaseOneStatusPayload, plan);

            if (EXPORT_PHASE1_FULL_INDEXES) {
                System.out.println("4b. Requesting full Phase 1 indexes from temporary debug synopsis...");

                ObjectNode phaseOneDebugEstimateRequest =
                        buildPhaseOneDebugEstimateRequest(
                                phaseOneDebugUid,
                                datasetKey,
                                streamId
                        );

                sendJson(producer, REQUEST_TOPIC, datasetKey, phaseOneDebugEstimateRequest);
                producer.flush();

                System.out.println(
                        MAPPER.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(phaseOneDebugEstimateRequest)
                );
                System.out.println();

                JsonNode phaseOneFullIndexes =
                        waitForPhaseOneDebugResult(
                                consumer,
                                phaseOneDebugUid,
                                plan.getQueryName(),
                                120000L
                        );

                validateFullIndexPayload(phaseOneFullIndexes);
                writePhaseOneFullIndexFile(phaseOneFullIndexes, plan.getQueryName());
            }

            System.out.println("5. Streaming PHASE_2 root alias...");
            long rootRows =
                    streamAlias(
                            producer,
                            DATA_TOPIC,
                            datasetKey,
                            streamId,
                            catalog,
                            plan,
                            plan.getRootAlias(),
                            TEST_ROW_LIMIT,
                            "PHASE2",
                            expectedWeightEvaluator
                    );

            producer.flush();

            System.out.println("PHASE_2 root alias " + plan.getRootAlias()
                    + " rows: " + rootRows);
            System.out.println();

            //Temporary barrier
            System.out.println("Waiting for SDE to consume PHASE_2 tuples...");
            Thread.sleep(10000L);

            System.out.println("6. Sending FINISH_PHASE_2 request...");
            ObjectNode finishPhaseTwoRequest =
                    buildControlRequest(
                            uid,
                            datasetKey,
                            streamId,
                            "FINISH_PHASE_2"
                    );

            sendJson(producer, REQUEST_TOPIC, datasetKey, finishPhaseTwoRequest);
            producer.flush();

            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(finishPhaseTwoRequest));
            System.out.println();

            System.out.println("7. Waiting for FINISH_PHASE_2 ACK...");
            JsonNode phaseTwoAck =
                    waitForResponseContaining(
                            consumer,
                            uid,
                            "FINISH_PHASE_2",
                            120000L
                    );

            System.out.println("FINISH_PHASE_2 ACK envelope:");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseTwoAck));
            System.out.println();

            validateControlAck(phaseTwoAck, "FINISH_PHASE_2");

            System.out.println("7a. Requesting OnePassSamplerSdeSynopsis STATUS after FINISH_PHASE_2...");
            ObjectNode phaseTwoStatusRequest =
                    buildStatusRequest(
                            uid,
                            datasetKey,
                            streamId
                    );

            sendJson(producer, REQUEST_TOPIC, datasetKey, phaseTwoStatusRequest);
            producer.flush();

            System.out.println(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(phaseTwoStatusRequest)
            );
            System.out.println();

            JsonNode phaseTwoStatusEnvelope =
                    waitForResponseContaining(
                            consumer,
                            uid,
                            "DONE",
                            120000L
                    );

            System.out.println("PHASE_2 STATUS envelope:");
            System.out.println(
                    MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(phaseTwoStatusEnvelope)
            );
            System.out.println();

            JsonNode phaseTwoStatusPayload =
                    extractEstimationPayload(phaseTwoStatusEnvelope);

            validatePhaseTwoResult(
                    phaseTwoStatusPayload,
                    plan.getSampleSize(),
                    expectedRootTuplesSeen,
                    expectedPositiveRootCandidatesSeen,
                    expectedTotalRootGroupWeight
            );

            System.out.println();
            System.out.println("SUCCESS: OnePassSamplerSdeSynopsis SDE test passed.");
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

    private static ObjectNode buildAddRequest(int uid, String datasetKey, String streamId) {
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

    private static void validatePhaseTwoResult(JsonNode payload,
                                               int expectedSampleSize,
                                               long expectedRootRows,
                                               long expectedPositiveCandidates,
                                               double expectedTotalWeight) {
        JsonNode phaseNode = findFirst(payload, "phase");

        if (phaseNode == null || !"DONE".equals(phaseNode.asText())) {
            throw new IllegalStateException(
                    "Expected phase DONE after FINISH_PHASE_2, got: "
                            + phaseNode
            );
        }

        JsonNode phaseTwoComplete = findFirst(payload, "phaseTwoComplete");

        if (phaseTwoComplete == null || !phaseTwoComplete.asBoolean()) {
            throw new IllegalStateException(
                    "Expected phaseTwoComplete = true after FINISH_PHASE_2"
            );
        }

        JsonNode rootTuplesSeen = findFirst(payload, "rootTuplesSeen");

        if (rootTuplesSeen == null) {
            throw new IllegalStateException("Missing rootTuplesSeen");
        }

        if (rootTuplesSeen.asLong() != expectedRootRows) {
            throw new IllegalStateException(
                    "rootTuplesSeen mismatch. Expected "
                            + expectedRootRows
                            + " but got "
                            + rootTuplesSeen.asLong()
            );
        }

        JsonNode positiveRootCandidatesSeen =
                findFirst(payload, "positiveRootCandidatesSeen");

        if (positiveRootCandidatesSeen == null) {
            throw new IllegalStateException("Missing positiveRootCandidatesSeen");
        }

        if (positiveRootCandidatesSeen.asLong() != expectedPositiveCandidates) {
            throw new IllegalStateException(
                    "positiveRootCandidatesSeen mismatch. Expected "
                            + expectedPositiveCandidates
                            + " but got "
                            + positiveRootCandidatesSeen.asLong()
            );
        }

        JsonNode totalRootGroupWeight =
                findFirst(payload, "totalRootGroupWeight");

        if (totalRootGroupWeight == null) {
            throw new IllegalStateException("Missing totalRootGroupWeight");
        }

        assertClose(
                "totalRootGroupWeight",
                expectedTotalWeight,
                totalRootGroupWeight.asDouble()
        );

        JsonNode sampleInstances = findFirst(payload, "sampleInstances");

        if (sampleInstances == null || !sampleInstances.isArray()) {
            throw new IllegalStateException(
                    "Expected sampleInstances array, got: "
                            + sampleInstances
            );
        }

        if (expectedPositiveCandidates > 0L
                && sampleInstances.size() != expectedSampleSize) {
            throw new IllegalStateException(
                    "Expected sampleInstances.size() = "
                            + expectedSampleSize
                            + ", got "
                            + sampleInstances.size()
            );
        }

        if (expectedPositiveCandidates == 1L && sampleInstances.size() > 1) {
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

        System.out.println("Validated FINISH_PHASE_2 payload.");
    }

    private static void updateExpectedState(CompiledOnePassPlan plan,
                                            OnePassWeightEvaluator expectedWeightEvaluator,
                                            ObjectNode tuple,
                                            String expectedUpdateMode) {
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
                System.out.println("  raw value:");
                System.out.println(value);
                System.out.println();

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
                    payload =
                            extractEstimationPayload(envelope);

                    System.out.println("  extracted estimation payload:");
                    System.out.println(
                            MAPPER.writerWithDefaultPrettyPrinter()
                                    .writeValueAsString(payload)
                    );
                    System.out.println();
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

    private static void sendJsonAsync(KafkaProducer<String, String> producer,
                                      String topic,
                                      String key,
                                      JsonNode json) {
        producer.send(
                new ProducerRecord<String, String>(
                        topic,
                        key,
                        json.toString()
                )
        );
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
}