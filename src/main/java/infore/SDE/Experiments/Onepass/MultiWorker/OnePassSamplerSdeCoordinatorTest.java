package infore.SDE.Experiments.Onepass.MultiWorker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Kafka/SDE integration test for the One-pass* coordinator with one logical base key.
 *
 * Required setup:
 *
 *   RunOnepass must use OnePassRoundRobinDataRouterCoFlatMap.
 *
 * This test verifies:
 *
 *   1. One ADD request with noOfP = 2 creates worker synopses.
 *   2. Small Phase 1 tuples are routed round-robin.
 *   3. One barrier is broadcast to both worker keys.
 *   4. Coordinator emits GLOBAL_BARRIER_READY.
 *   5. One FINISH_PHASE_1 request is routed to both workers.
 *   6. Coordinator receives two LOCAL_PHASE1_RESULT messages.
 *   7. Coordinator emits GLOBAL_PHASE1_RESULT_READY with localResultCount = 2.
 */
public final class OnePassSamplerSdeCoordinatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DATA_TOPIC = "dataTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";
    private static final String REQUEST_TOPIC = "requestTopic";

    private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";

    private static final int EXPECTED_WORKERS = 4;
    private static final long TIMEOUT_MS = 120000L;

    private OnePassSamplerSdeCoordinatorTest() {
    }

    public static void main(String[] args) throws Exception {
        int uid = Math.abs(UUID.randomUUID().toString().hashCode());

        String streamId = "onepass-coordinator-test";
        String phase = "PHASE1";
        String alias = "l";

        String barrierId = "COORD_PIPELINE_" + uid;

        /*
         * This is the logical query key.
         *
         * The OnePassRoundRobinDataRouterCoFlatMap should route data from:
         *
         *   baseKey
         *
         * into:
         *
         *   baseKey_2_KEYED_0
         *   baseKey_2_KEYED_1
         */
        String baseKey = "onepass-phase1-" + uid;

        String phaseOneResultId = "PHASE1_RESULT_" + uid;

        System.out.println("=== OnePassSamplerSdeCoordinatorTest ===");
        System.out.println("uid = " + uid);
        System.out.println("baseKey = " + baseKey);
        System.out.println("expectedWorkers = " + EXPECTED_WORKERS);
        System.out.println("barrierId = " + barrierId);
        System.out.println("phaseOneResultId = " + phaseOneResultId);
        System.out.println();

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer("onepass-coordinator-test-" + uid);

        consumer.subscribe(Collections.singletonList(ESTIMATION_TOPIC));

        try {
            drainConsumer(consumer);

            /*
             * Step 1:
             * Send one logical ADD request with noOfP = 2.
             *
             * RqRouterFlatMap should duplicate it to:
             *   baseKey_2_KEYED_0
             *   baseKey_2_KEYED_1
             *
             * OnePassRoundRobinDataRouterCoFlatMap should also register
             * baseKey -> parallelism 2 for future data routing.
             */
            System.out.println("Sending ADD OnePass request...");

            ObjectNode addRequest = buildOnePassAddRequest(baseKey, streamId, uid, EXPECTED_WORKERS);
            sendJson(producer, REQUEST_TOPIC, baseKey, addRequest);
            producer.flush();

            /*
             * Give request routing time to create the worker synopses.
             * Later we should replace this sleep with an explicit ADD ACK.
             */
            Thread.sleep(3000L);

            /*
             * Step 2:
             * Send small Phase 1 lineitem tuples.
             *
             * With round-robin routing and expectedWorkers = 2, these should be split
             * across the two worker synopses.
             */
            System.out.println("Sending small Phase 1 Test tuples...");

            for(int i=0; i<60; i++){
                int j=i+1;
                sendJson(producer, DATA_TOPIC, baseKey,
                        buildDataTuple(baseKey,streamId,tupleLineitem(i,j)));
            }

            sendJson(producer, DATA_TOPIC, baseKey,
                    buildDataTuple(baseKey, streamId, tupleLineitem(1, 1)));

            sendJson(producer, DATA_TOPIC, baseKey,
                    buildDataTuple(baseKey, streamId, tupleLineitem(1, 2)));

            sendJson(producer, DATA_TOPIC, baseKey,
                    buildDataTuple(baseKey, streamId, tupleLineitem(2, 1)));

            sendJson(producer, DATA_TOPIC, baseKey,
                    buildDataTuple(baseKey, streamId, tupleLineitem(3, 1)));

            producer.flush();

            /*
             * Step 3:
             * Send one logical Phase 1 barrier.
             *
             * The OnePassRoundRobinDataRouterCoFlatMap should broadcast this barrier to:
             *   baseKey_2_KEYED_0
             *   baseKey_2_KEYED_1
             */
            System.out.println("Sending Phase 1 barrier...");

            ObjectNode barrierDatapoint = buildDataBarrierDatapoint(baseKey, streamId, uid,
                            phase, alias, barrierId, EXPECTED_WORKERS);

            sendJson(producer, DATA_TOPIC, baseKey, barrierDatapoint);
            producer.flush();

            System.out.println("Waiting for GLOBAL_BARRIER_READY...");

            JsonNode ready = waitForGlobalBarrierReady(consumer, uid, barrierId, EXPECTED_WORKERS, TIMEOUT_MS);
            JsonNode missing = ready.get("missingSynopsisWorkers");

            if (missing != null && missing.size() > 0) {
                throw new IllegalStateException("Expected missingSynopsisWorkers to be empty, but got: "
                                + missing.toString());
            }

            System.out.println();
            System.out.println("GLOBAL_BARRIER_READY received:");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(ready));
            System.out.println();

            /*
             * Step 4:
             * Send one logical FINISH_PHASE_1 request with noOfP = 2.
             *
             * RqRouterFlatMap should duplicate it to both worker keys.
             * Each worker emits LOCAL_PHASE1_RESULT.
             * Coordinator emits GLOBAL_PHASE1_RESULT_READY.
             */
            System.out.println("Sending FINISH_PHASE_1 request...");

            ObjectNode finishPhaseOneRequest = buildFinishPhaseOneRequest(baseKey, streamId, uid,
                            phaseOneResultId, EXPECTED_WORKERS);

            sendJson(producer, REQUEST_TOPIC, baseKey, finishPhaseOneRequest);
            producer.flush();

            System.out.println("Waiting for GLOBAL_PHASE1_RESULT_READY...");

            JsonNode phaseOneReady = waitForCoordinatorMessage(consumer, uid, "GLOBAL_PHASE1_RESULT_READY",
                    "resultId", phaseOneResultId, EXPECTED_WORKERS, TIMEOUT_MS);

            int localResultCount = phaseOneReady.has("localResultCount") ?
                    phaseOneReady.get("localResultCount").asInt() : -1;

            if (localResultCount != EXPECTED_WORKERS) {
                throw new IllegalStateException(
                        "Expected localResultCount="
                                + EXPECTED_WORKERS
                                + ", but got "
                                + localResultCount
                                + ". Payload: "
                                + phaseOneReady.toString()
                );
            }

            System.out.println();
            System.out.println("GLOBAL_PHASE1_RESULT_READY received:");
            System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(phaseOneReady));
            System.out.println();
            System.out.println("Coordinator integration test PASSED.");

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

    private static ObjectNode buildDataTuple(String datasetKey, String streamId, ObjectNode tuple) {

        ObjectNode datapoint = MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);
        datapoint.put("streamID", streamId);
        datapoint.set("values", tuple);

        return datapoint;
    }

    private static ObjectNode tupleLineitem(int orderKey, int lineNumber) {

        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "l");
        tuple.put("l_orderkey", orderKey);
        tuple.put("l_linenumber", lineNumber);

        return tuple;
    }

    private static ObjectNode buildDataBarrierDatapoint(String datasetKey, String streamId, int uid, String phase,
            String alias, String barrierId, int expectedWorkers) {

        ObjectNode barrier = MAPPER.createObjectNode();

        barrier.put(ONEPASS_DATA_BARRIER_FIELD, true);
        barrier.put("uid", uid);
        barrier.put("phase", phase);
        barrier.put("alias", alias);
        barrier.put("barrierId", barrierId);
        barrier.put("expectedWorkers", expectedWorkers);

        ObjectNode datapoint = MAPPER.createObjectNode();

        datapoint.put("dataSetkey", datasetKey);
        datapoint.put("streamID", streamId);
        datapoint.set("values", barrier);

        return datapoint;
    }

    private static ObjectNode buildOnePassAddRequest(String datasetKey, String streamId, int uid, int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("requestID", 1);
        request.put("synopsisID", 30);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        request.putArray("param").add("onepass-coordinator-add-test");

        ObjectNode parameters = MAPPER.createObjectNode();
        ObjectNode onePassParams = MAPPER.createObjectNode();

        onePassParams.put("queryName", "WQ3_COORDINATOR_TEST");
        onePassParams.put("mainTable", "c");

        ObjectNode dataset = MAPPER.createObjectNode();

        dataset.put("name", "tpch");
        dataset.put("dbConfig", "tpch.json");
        dataset.put("scaleFactor", 1);
        dataset.put("seed", "test123");

        onePassParams.set("dataset", dataset);

        onePassParams.putArray("relations")
                .add(relation("customer", "c"))
                .add(relation("orders", "o"))
                .add(relation("lineitem", "l"));

        onePassParams.putArray("joins")
                .add(join("c", "c_custkey", "o", "o_custkey"))
                .add(join("o", "o_orderkey", "l", "l_orderkey"));

        ObjectNode weight = MAPPER.createObjectNode();
        weight.put("expression", "1");

        ObjectNode weightsByAlias = MAPPER.createObjectNode();

        weightsByAlias.put("c", "1");
        weightsByAlias.put("o", "1");
        weightsByAlias.put("l", "1");

        weight.set("weightsByAlias", weightsByAlias);
        onePassParams.set("weight", weight);

        ObjectNode output = MAPPER.createObjectNode();

        output.put("sampleSize", 10);
        output.putArray("projection").add("c.c_custkey").add("o.o_orderkey").add("l.l_linenumber");
        onePassParams.set("output", output);
        parameters.set("onePassParams", onePassParams);

        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode buildFinishPhaseOneRequest(String datasetKey, String streamId, int uid,
            String resultId, int noOfP) {

        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("requestID", 7);
        request.put("synopsisID", 30);
        request.put("uid", uid);
        request.put("streamID", streamId);
        request.put("noOfP", noOfP);

        request.putArray("param").add("FINISH_PHASE_1").add(resultId);

        ObjectNode parameters = MAPPER.createObjectNode();

        parameters.put("onePassCommand", "FINISH_PHASE_1");
        parameters.put("onePassResultId", resultId);

        request.set("parameters", parameters);

        return request;
    }

    private static ObjectNode relation(String table, String alias) {
        ObjectNode node = MAPPER.createObjectNode();

        node.put("table", table);
        node.put("alias", alias);

        return node;
    }

    private static ObjectNode join(String leftAlias, String leftField, String rightAlias, String rightField) {

        ObjectNode node = MAPPER.createObjectNode();

        node.put("leftAlias", leftAlias);
        node.put("leftField", leftField);
        node.put("rightAlias", rightAlias);
        node.put("rightField", rightField);

        return node;
    }

    private static JsonNode waitForGlobalBarrierReady(KafkaConsumer<String, String> consumer, int uid,
            String barrierId, int expectedWorkers, long timeoutMs) throws Exception {

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
                    envelope =MAPPER.readTree(value);
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

                if (!"GLOBAL_BARRIER_READY".equals(type)) {
                    continue;
                }

                if (!barrierId.equals(textField(payload, "barrierId", ""))) {
                    continue;
                }

                int received = payload.has("receivedWorkers") ? payload.get("receivedWorkers").size() : 0;

                System.out.println("Candidate GLOBAL_BARRIER_READY:" + " barrierId=" + barrierId +
                        ", receivedWorkers=" + received + "/" + expectedWorkers);

                if (received >= expectedWorkers) {
                    return payload;
                }
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for GLOBAL_BARRIER_READY" + ", uid=" + uid + ", barrierId=" + barrierId
                        + ", expectedWorkers=" + expectedWorkers + ", recordsSeen=" + recordsSeen);
    }

    private static JsonNode waitForCoordinatorMessage(KafkaConsumer<String, String> consumer, int uid,
            String expectedType, String idField, String expectedId, int expectedWorkers, long timeoutMs) throws Exception {

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

                int received = payload.has("receivedWorkers") ? payload.get("receivedWorkers").size() : 0;

                System.out.println("Candidate " + expectedType
                        + ": " + idField + "=" + expectedId
                        + ", receivedWorkers=" + received
                        + "/" + expectedWorkers);

                if (received >= expectedWorkers) {
                    return payload;
                }
            }
        }

        throw new IllegalStateException("Timed out waiting for " + expectedType + ", uid=" + uid + ", " +
                idField + "=" + expectedId + ", expectedWorkers=" + expectedWorkers + ", recordsSeen=" + recordsSeen);
    }

    private static JsonNode extractEstimationPayload(JsonNode envelope) throws Exception {
        if (envelope == null || envelope.isNull()) {
            return null;
        }

        JsonNode estimationNode = envelope.get("estimation");
        if (estimationNode == null || estimationNode.isNull()) {
            return null;
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

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("retries", "0");
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
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

    private static void sendJson(KafkaProducer<String, String> producer, String topic, String key, JsonNode json) throws Exception {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topic, key, json.toString()));
        future.get();
    }

    private static void drainConsumer(KafkaConsumer<String, String> consumer) {
        consumer.poll(500);
        consumer.poll(500);
    }
}