package infore.SDE.Experiments.Onepass.SingleWorker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class PhaseOneSdeIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKERS = "localhost:9092";

    private static final String DATA_TOPIC = "dataTopic";
    private static final String REQUEST_TOPIC = "requestTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";

    private static final int UID = (int) (System.currentTimeMillis() % 1000000000);
    private static final String DATASET_KEY = "onepass-sde-test-" + UID;

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();

        prepareConsumerAtEnd(consumer);

        System.out.println("Using UID: " + UID);
        System.out.println("Using dataSetkey: " + DATASET_KEY);

        System.out.println();
        System.out.println("1. Sending ADD request to SDE...");
        send(producer, REQUEST_TOPIC, buildAddPhaseOneRequest());

        Thread.sleep(3000);

        System.out.println();
        System.out.println("2. Sending side-stream tuples to SDE...");
        sendSideStreamTuples(producer);

        Thread.sleep(3000);

        System.out.println();
        System.out.println("3. Sending ESTIMATE request to SDE...");
        send(producer, REQUEST_TOPIC, buildEstimateRequest());

        System.out.println();
        System.out.println("4. Waiting for Phase 1 estimation result...");
        waitForAndValidateResult(consumer);

        producer.close();
        consumer.close();

        System.out.println();
        System.out.println("SUCCESS: Phase 1 works through the real SDE Run class.");
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<String, String>(props);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "onepass-phase1-sde-integration-" + UID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /*
         * We manually seek to the end before sending the test messages.
         * This avoids reading old estimationTopic messages from previous tests.
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<String, String>(props);
    }

    private static void prepareConsumerAtEnd(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList(ESTIMATION_TOPIC));

        long deadline = System.currentTimeMillis() + 10000L;

        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(100L);
        }

        if (consumer.assignment().isEmpty()) {
            throw new IllegalStateException("Could not get Kafka partition assignment for estimationTopic");
        }

        consumer.seekToEnd(consumer.assignment());

        System.out.println("Consumer is ready and positioned at the end of estimationTopic.");
    }

    private static void send(KafkaProducer<String, String> producer,
                             String topic,
                             ObjectNode json) throws Exception {

        String value = MAPPER.writeValueAsString(json);

        producer.send(new ProducerRecord<String, String>(topic, DATASET_KEY, value)).get();
        producer.flush();

        System.out.println("Sent to " + topic + ":");
        System.out.println(value);
    }

    private static ObjectNode buildAddPhaseOneRequest() {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", DATASET_KEY);
        request.put("requestID", 1);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "onepass");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");
        param.add("unused");

        ObjectNode parameters = request.putObject("parameters");
        ObjectNode onePassParams = parameters.putObject("onePassParams");

        onePassParams.put("queryName", "sde-test-a-b-c-e");
        onePassParams.put("mainTable", "A");

        ObjectNode dataset = onePassParams.putObject("dataset");
        dataset.put("name", "synthetic");
        dataset.put("scaleFactor", 1);
        dataset.put("seed", "1");

        ArrayNode relations = onePassParams.putArray("relations");
        addRelation(relations, "A", "A");
        addRelation(relations, "B", "B");
        addRelation(relations, "C", "C");
        addRelation(relations, "E", "E");

        ArrayNode joins = onePassParams.putArray("joins");
        addJoin(joins, "A", "ab_key", "B", "ab_key");
        addJoin(joins, "B", "bc_key", "C", "bc_key");
        addJoin(joins, "B", "be_key", "E", "be_key");

        ObjectNode weight = onePassParams.putObject("weight");
        weight.put("expression", "weight");

        ArrayNode variables = weight.putArray("variables");
        variables.add("weight");

        ObjectNode output = onePassParams.putObject("output");
        output.put("sampleSize", 10);

        ArrayNode projection = output.putArray("projection");
        projection.add("A");
        projection.add("B");
        projection.add("C");
        projection.add("E");

        return request;
    }

    private static ObjectNode buildEstimateRequest() {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", DATASET_KEY);
        request.put("requestID", 3);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "onepass");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");
        param.add("unused");

        return request;
    }

    private static void addRelation(ArrayNode relations,
                                    String alias,
                                    String table) {

        ObjectNode relation = relations.addObject();
        relation.put("alias", alias);
        relation.put("table", table);
    }

    private static void addJoin(ArrayNode joins,
                                String leftAlias,
                                String leftField,
                                String rightAlias,
                                String rightField) {

        ObjectNode join = joins.addObject();

        join.put("leftAlias", leftAlias);
        join.put("leftField", leftField);
        join.put("rightAlias", rightAlias);
        join.put("rightField", rightField);
    }

    private static void sendSideStreamTuples(KafkaProducer<String, String> producer) throws Exception {
        /*
         * Join tree:
         *
         *          A
         *          |
         *          B
         *        /   \
         *       C     E
         *
         * Phase 1 receives only side-stream tuples.
         * Leaf-to-root order:
         *
         *      C first
         *      E second
         *      B last
         */

        send(producer, DATA_TOPIC, datapoint("C", tupleC("k1", 5.0)));
        send(producer, DATA_TOPIC, datapoint("C", tupleC("k1", 8.0)));
        send(producer, DATA_TOPIC, datapoint("C", tupleC("k2", 1.0)));

        send(producer, DATA_TOPIC, datapoint("E", tupleE("z1", 4.0)));
        send(producer, DATA_TOPIC, datapoint("E", tupleE("z1", 6.0)));
        send(producer, DATA_TOPIC, datapoint("E", tupleE("z2", 3.0)));

        send(producer, DATA_TOPIC, datapoint("B", tupleB("x1", "k1", "z1", 2.0)));
    }

    private static ObjectNode datapoint(String streamId, ObjectNode tupleValues) {
        ObjectNode dp = MAPPER.createObjectNode();

        dp.put("dataSetkey", DATASET_KEY);
        dp.put("streamID", streamId);
        dp.set("values", tupleValues);

        return dp;
    }

    private static ObjectNode tupleC(String bcKey, double weight) {
        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "C");
        tuple.put("bc_key", bcKey);
        tuple.put("weight", weight);

        return tuple;
    }

    private static ObjectNode tupleE(String beKey, double weight) {
        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "E");
        tuple.put("be_key", beKey);
        tuple.put("weight", weight);

        return tuple;
    }

    private static ObjectNode tupleB(String abKey,
                                     String bcKey,
                                     String beKey,
                                     double weight) {

        ObjectNode tuple = MAPPER.createObjectNode();

        tuple.put("alias", "B");
        tuple.put("ab_key", abKey);
        tuple.put("bc_key", bcKey);
        tuple.put("be_key", beKey);
        tuple.put("weight", weight);

        return tuple;
    }

    private static void waitForAndValidateResult(KafkaConsumer<String, String> consumer) throws Exception {
        long deadline = System.currentTimeMillis() + 45000L;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(500L);

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();

                System.out.println();
                System.out.println("Received from estimationTopic:");
                System.out.println(value);

                JsonNode root;

                try {
                    root = MAPPER.readTree(value);
                } catch (Exception e) {
                    System.out.println("Skipping non-JSON message.");
                    continue;
                }

                if (!root.has("uid") || root.get("uid").asInt() != UID) {
                    System.out.println("Skipping message with different UID.");
                    continue;
                }

                if (!root.has("synopsisID") || root.get("synopsisID").asInt() != 31) {
                    System.out.println("Skipping message with different synopsisID.");
                    continue;
                }

                JsonNode estimation = root.get("estimation");

                if (estimation == null || estimation.isNull()) {
                    throw new IllegalStateException("estimation field is missing or null");
                }

                JsonNode edgeIndexes = estimation.get("edgeIndexes");

                if (edgeIndexes == null || edgeIndexes.isNull()) {
                    throw new IllegalStateException("edgeIndexes missing from Phase 1 result");
                }

                validateEdgeIndexes(edgeIndexes);
                validateSeenTuples(estimation);

                return;
            }
        }

        throw new IllegalStateException("Timed out waiting for Phase 1 SDE estimation result");
    }

    private static void validateEdgeIndexes(JsonNode edgeIndexes) {
        assertWeight(edgeIndexes, "B<->C", "k1", 13.0);
        assertWeight(edgeIndexes, "B<->C", "k2", 1.0);

        assertWeight(edgeIndexes, "B<->E", "z1", 10.0);
        assertWeight(edgeIndexes, "B<->E", "z2", 3.0);

        assertWeight(edgeIndexes, "A<->B", "x1", 260.0);
    }

    private static void validateSeenTuples(JsonNode estimation) {
        JsonNode seen = estimation.get("seenTuplesByAlias");

        if (seen == null || seen.isNull()) {
            throw new IllegalStateException("seenTuplesByAlias missing from Phase 1 result");
        }

        assertSeen(seen, "A", 0);
        assertSeen(seen, "B", 1);
        assertSeen(seen, "C", 3);
        assertSeen(seen, "E", 3);
    }

    private static void assertWeight(JsonNode edgeIndexes,
                                     String edgeId,
                                     String key,
                                     double expected) {

        JsonNode edge = edgeIndexes.get(edgeId);

        if (edge == null || edge.isNull()) {
            throw new IllegalStateException("Missing edge index: " + edgeId);
        }

        JsonNode value = edge.get(key);

        if (value == null || value.isNull()) {
            throw new IllegalStateException(
                    "Missing key '" + key + "' in edge index " + edgeId
            );
        }

        double actual = value.asDouble();
        double tolerance = 0.000001d;

        if (Math.abs(expected - actual) > tolerance) {
            throw new IllegalStateException(
                    edgeId + "[" + key + "] expected " + expected + " but got " + actual
            );
        }
    }

    private static void assertSeen(JsonNode seen,
                                   String alias,
                                   int expected) {

        JsonNode value = seen.get(alias);

        if (value == null || value.isNull()) {
            throw new IllegalStateException("Missing seen counter for alias " + alias);
        }

        int actual = value.asInt();

        if (actual != expected) {
            throw new IllegalStateException(
                    "Seen counter for " + alias + " expected " + expected + " but got " + actual
            );
        }
    }
}