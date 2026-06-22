package infore.SDE.Experiments.Onepass.Deprecated;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.io.FileWriter;

public class PhaseOneTpchQ3Test {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKERS = "localhost:9092";
    private static final String DATA_TOPIC = "dataTopic";
    private static final String REQUEST_TOPIC = "requestTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";

    private static final int UID = (int) (System.currentTimeMillis() % 1000000000);
    private static final String DATASET_KEY = "tpch-q3-weighted-phase1-" + UID;

    /*
     * Expected Phase 1 indexes, calculated independently from the .tbl files.
     *
     * For the weighted test:
     *
     * lineitem<->orders[l_orderkey]
     *      = sum(l_extendedprice) for each order
     *
     * customer<->orders[o_custkey]
     *      = sum(o_totalprice * lineitemSubtreeWeightForThatOrder)
     */
    private static final Map<String, Double> expectedLineitemsByOrder =
            new LinkedHashMap<String, Double>();

    private static final Map<String, Double> expectedOrdersByCustomer =
            new LinkedHashMap<String, Double>();

    private static long sentLineitems = 0L;
    private static long sentOrders = 0L;

    public static void main(String[] args) throws Exception {
        String defaultTpchDir =
                System.getProperty("user.home") + "/Desktop/Thesis/tpch-data/sf1";

        String tpchDir = args.length >= 1 ? args[0] : defaultTpchDir;

        /*
         * Example IntelliJ args:
         *
         * /home/fileboxuser/Desktop/Thesis/tpch-data/sf1 50 50
         * /home/fileboxuser/Desktop/Thesis/tpch-data/sf1 500 500
         * /home/fileboxuser/Desktop/Thesis/tpch-data/sf1 5000 5000
         */
        long maxLineitemRows = args.length >= 2 ? Long.parseLong(args[1]) : 50000L;
        long maxOrdersRows = args.length >= 3 ? Long.parseLong(args[2]) : 50000L;

        File lineitemFile = new File(tpchDir, "lineitem.tbl");
        File ordersFile = new File(tpchDir, "orders.tbl");

        if (!lineitemFile.exists()) {
            throw new IllegalStateException("Missing file: " + lineitemFile.getAbsolutePath());
        }

        if (!ordersFile.exists()) {
            throw new IllegalStateException("Missing file: " + ordersFile.getAbsolutePath());
        }

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();

        prepareConsumerAtEnd(consumer);

        System.out.println("Using UID: " + UID);
        System.out.println("Using dataSetkey: " + DATASET_KEY);
        System.out.println("TPC-H directory: " + tpchDir);
        System.out.println("Max lineitem rows: " + maxLineitemRows);
        System.out.println("Max orders rows: " + maxOrdersRows);

        System.out.println();
        System.out.println("1. Sending ADD request for SQL/catalog weighted TPC-H Q3 Phase 1...");

        ObjectNode addRequest = buildAddPhaseOneSqlRequest();

        System.out.println();
        System.out.println("ADD request:");
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(addRequest));

        send(producer, REQUEST_TOPIC, addRequest);

        Thread.sleep(3000L);

        System.out.println();
        System.out.println("2. Sending weighted lineitem rows first...");
        sendLineitemRows(producer, lineitemFile, maxLineitemRows);

        System.out.println();
        System.out.println("3. Sending weighted orders rows second...");
        sendOrdersRows(producer, ordersFile, maxOrdersRows);

        producer.flush();

        System.out.println();
        System.out.println("Sent lineitem rows: " + sentLineitems);
        System.out.println("Sent orders rows: " + sentOrders);
        System.out.println("Expected lineitem index size: " + expectedLineitemsByOrder.size());
        System.out.println("Expected customer index size: " + expectedOrdersByCustomer.size());
        System.out.println("Expected lineitem<->orders total weight: " + sumExpected(expectedLineitemsByOrder));
        System.out.println("Expected customer<->orders total weight: " + sumExpected(expectedOrdersByCustomer));

        Thread.sleep(5000L);

        System.out.println();
        System.out.println("4. Sending ESTIMATE request...");

        ObjectNode estimateRequest = buildEstimateRequest();

        System.out.println();
        System.out.println("ESTIMATE request:");
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(estimateRequest));

        send(producer, REQUEST_TOPIC, estimateRequest);

        System.out.println();
        System.out.println("5. Waiting for weighted TPC-H Q3 Phase 1 result...");
        waitForAndValidateResult(consumer);

        producer.close();
        consumer.close();

        System.out.println();
        System.out.println("SUCCESS: Weighted TPC-H Q3 Phase 1 works through SDE Run class.");
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tpch-q3-weighted-phase1-test-" + UID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
    }

    private static ObjectNode buildAddPhaseOneRequest() {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", DATASET_KEY);
        request.put("requestID", 1);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "tpch-q3-weighted");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");
        param.add("unused");

        ObjectNode parameters = request.putObject("parameters");
        ObjectNode onePassParams = parameters.putObject("onePassParams");

        onePassParams.put("queryName", "tpch-q3-weighted");
        onePassParams.put("mainTable", "customer");

        ObjectNode dataset = onePassParams.putObject("dataset");
        dataset.put("name", "tpch");
        dataset.put("scaleFactor", 1);
        dataset.put("seed", "1");

        ArrayNode relations = onePassParams.putArray("relations");
        addRelation(relations, "customer", "customer");
        addRelation(relations, "orders", "orders");
        addRelation(relations, "lineitem", "lineitem");

        ArrayNode joins = onePassParams.putArray("joins");

        /*
         * Q3 join tree:
         *
         * customer.c_custkey = orders.o_custkey
         * orders.o_orderkey = lineitem.l_orderkey
         */
        addJoin(joins, "customer", "c_custkey", "orders", "o_custkey");
        addJoin(joins, "orders", "o_orderkey", "lineitem", "l_orderkey");

        /*
         * -------------------------------
         * Unweighted Phase 1 test version:
         * -------------------------------
         *
         * ObjectNode weight = onePassParams.putObject("weight");
         * weight.put("expression", "1");
         * ArrayNode variables = weight.putArray("variables");
         *
         * In the unweighted version:
         *
         * lineitem<->orders[l_orderkey]
         *      = number of lineitem tuples for each order
         *
         * customer<->orders[o_custkey]
         *      = number of joined orders-lineitems for each customer
         */

        /*
         * -------------------------------
         * Weighted Phase 1 test version:
         * -------------------------------
         *
         * The OnePassWeightEvaluator reads the numeric field called "weight"
         * from each tuple.
         *
         * For this test:
         *
         * lineitem.weight = l_extendedprice
         * orders.weight   = o_totalprice
         */
        ObjectNode weight = onePassParams.putObject("weight");
        weight.put("expression", "weight");

        ArrayNode variables = weight.putArray("variables");
        variables.add("weight");

        ObjectNode output = onePassParams.putObject("output");
        output.put("sampleSize", 10);

        ArrayNode projection = output.putArray("projection");
        projection.add("customer");
        projection.add("orders");
        projection.add("lineitem");

        return request;
    }

    private static ObjectNode buildEstimateRequest() {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", DATASET_KEY);
        request.put("requestID", 3);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "tpch-q3-weighted");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");
        //param.add("summary");
        //param.add("sample=5");
        /*
         * Full-index validation mode.
         *
         * OnePassPhaseOne.estimate(...) returns edgeIndexes whenever the request
         * is not in summary mode.
         */
        param.add("debug-full-indexes");

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

    private static void sendLineitemRows(KafkaProducer<String, String> producer,
                                         File lineitemFile,
                                         long maxRows) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(lineitemFile));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && sentLineitems >= maxRows) {
                    break;
                }

                String[] parts = line.split("\\|", -1);

                if (parts.length < 6) {
                    throw new IllegalStateException("Invalid lineitem row: " + line);
                }

                String lOrderKey = parts[0];
                String lPartKey = parts[1];
                String lSuppKey = parts[2];
                String lLineNumber = parts[3];
                String lQuantity = parts[4];
                String lExtendedPrice = parts[5];

                double lineitemWeight = parseDoubleSafe(lExtendedPrice);

                ObjectNode tuple = MAPPER.createObjectNode();

                tuple.put("alias", "lineitem");
                tuple.put("l_orderkey", lOrderKey);
                tuple.put("l_partkey", lPartKey);
                tuple.put("l_suppkey", lSuppKey);
                tuple.put("l_linenumber", lLineNumber);
                tuple.put("l_quantity", parseDoubleSafe(lQuantity));
                tuple.put("l_extendedprice", lineitemWeight);

                /*
                 * Weighted test:
                 *
                 * ownWeight(lineitem) = l_extendedprice
                 */
                tuple.put("weight", lineitemWeight);

                send(producer, DATA_TOPIC, datapoint("lineitem", tuple));

                /*
                 * Weighted expected value:
                 *
                 * lineitem<->orders[l_orderkey]
                 *      += l_extendedprice
                 */
                addToMap(expectedLineitemsByOrder, lOrderKey, lineitemWeight);

                /*
                 * Unweighted version:
                 *
                 * addToMap(expectedLineitemsByOrder, lOrderKey, 1.0d);
                 */

                sentLineitems++;

                if (sentLineitems % 10000L == 0L) {
                    System.out.println("Sent lineitem rows: " + sentLineitems);
                }
            }
        } finally {
            br.close();
        }
    }

    private static void sendOrdersRows(KafkaProducer<String, String> producer,
                                       File ordersFile,
                                       long maxRows) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(ordersFile));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && sentOrders >= maxRows) {
                    break;
                }

                String[] parts = line.split("\\|", -1);

                if (parts.length < 4) {
                    throw new IllegalStateException("Invalid orders row: " + line);
                }

                String oOrderKey = parts[0];
                String oCustKey = parts[1];
                String oStatus = parts[2];
                String oTotalPrice = parts[3];

                double orderWeight = parseDoubleSafe(oTotalPrice);

                ObjectNode tuple = MAPPER.createObjectNode();

                tuple.put("alias", "orders");
                tuple.put("o_orderkey", oOrderKey);
                tuple.put("o_custkey", oCustKey);
                tuple.put("o_status", oStatus);
                tuple.put("o_totalprice", orderWeight);

                /*
                 * Weighted test:
                 *
                 * ownWeight(orders) = o_totalprice
                 */
                tuple.put("weight", orderWeight);

                send(producer, DATA_TOPIC, datapoint("orders", tuple));

                Double lineitemSubtreeWeight = expectedLineitemsByOrder.get(oOrderKey);

                if (lineitemSubtreeWeight != null && lineitemSubtreeWeight > 0.0d) {
                    /*
                     * Weighted expected value:
                     *
                     * customer<->orders[o_custkey]
                     *      += o_totalprice * lineitem<->orders[o_orderkey]
                     */
                    addToMap(
                            expectedOrdersByCustomer,
                            oCustKey,
                            orderWeight * lineitemSubtreeWeight
                    );

                    /*
                     * Unweighted version:
                     *
                     * addToMap(expectedOrdersByCustomer, oCustKey, lineitemSubtreeWeight);
                     */
                }

                sentOrders++;

                if (sentOrders % 10000L == 0L) {
                    System.out.println("Sent orders rows: " + sentOrders);
                }
            }
        } finally {
            br.close();
        }
    }

    private static ObjectNode datapoint(String streamId, ObjectNode tupleValues) {
        ObjectNode dp = MAPPER.createObjectNode();

        dp.put("dataSetkey", DATASET_KEY);
        dp.put("streamID", streamId);
        dp.set("values", tupleValues);

        return dp;
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

    private static double parseDoubleSafe(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0d;
        }

        return Double.parseDouble(value.trim());
    }

    private static void waitForAndValidateResult(KafkaConsumer<String, String> consumer) throws Exception {
        long deadline = System.currentTimeMillis() + 120000L;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(500L);

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();

                JsonNode root;

                try {
                    root = MAPPER.readTree(value);
                } catch (Exception e) {
                    continue;
                }

                if (!root.has("uid") || root.get("uid").asInt() != UID) {
                    continue;
                }

                if (!root.has("synopsisID") || root.get("synopsisID").asInt() != 31) {
                    continue;
                }

                JsonNode estimation = root.get("estimation");

                if (estimation == null || estimation.isNull()) {
                    throw new IllegalStateException("Missing estimation field");
                }

                /*
                JsonNode edgeSummaries = estimation.get("edgeSummaries");

                if (edgeSummaries == null || edgeSummaries.isNull()) {
                    throw new IllegalStateException("Missing edgeSummaries field");
                }

                validateTpchQ3Summary(edgeSummaries, estimation);

                System.out.println();
                System.out.println("Received valid weighted Phase 1 summary:");
                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(estimation));

                writeSdeEstimationToFile(estimation);

                return;
                */
                JsonNode edgeIndexes = estimation.get("edgeIndexes");

                if (edgeIndexes == null || edgeIndexes.isNull()) {
                    throw new IllegalStateException(
                            "Missing edgeIndexes field. " +
                                    "For full-index validation, the estimate request must NOT use summary mode."
                    );
                }

                validateTpchQ3FullIndexes(edgeIndexes, estimation);

                writeSdeFullIndexesToFile(estimation);

                System.out.println();
                System.out.println("Received valid weighted Phase 1 full indexes.");
                System.out.println("Full indexes were written to /tmp/sde_q3_weighted_full_indexes.json");

                return;
            }
        }

        throw new IllegalStateException("Timed out waiting for weighted TPC-H Q3 Phase 1 summary result");
    }

    private static void validateTpchQ3Summary(JsonNode edgeSummaries,
                                              JsonNode estimation) {

        JsonNode lineitemOrdersSummary = edgeSummaries.get("lineitem<->orders");
        JsonNode customerOrdersSummary = edgeSummaries.get("customer<->orders");

        if (lineitemOrdersSummary == null || lineitemOrdersSummary.isNull()) {
            throw new IllegalStateException("Missing lineitem<->orders summary");
        }

        if (customerOrdersSummary == null || customerOrdersSummary.isNull()) {
            throw new IllegalStateException("Missing customer<->orders summary");
        }

        int expectedLineitemKeys = expectedLineitemsByOrder.size();
        int actualLineitemKeys = lineitemOrdersSummary.get("numberOfKeys").asInt();

        if (actualLineitemKeys != expectedLineitemKeys) {
            throw new IllegalStateException(
                    "lineitem<->orders numberOfKeys expected " +
                            expectedLineitemKeys + " but got " + actualLineitemKeys
            );
        }

        double expectedLineitemTotal = sumExpected(expectedLineitemsByOrder);
        double actualLineitemTotal = lineitemOrdersSummary.get("totalWeight").asDouble();

        assertClose(
                "lineitem<->orders totalWeight",
                expectedLineitemTotal,
                actualLineitemTotal
        );

        int expectedCustomerKeys = expectedOrdersByCustomer.size();
        int actualCustomerKeys = customerOrdersSummary.get("numberOfKeys").asInt();

        if (actualCustomerKeys != expectedCustomerKeys) {
            throw new IllegalStateException(
                    "customer<->orders numberOfKeys expected " +
                            expectedCustomerKeys + " but got " + actualCustomerKeys
            );
        }

        double expectedCustomerTotal = sumExpected(expectedOrdersByCustomer);
        double actualCustomerTotal = customerOrdersSummary.get("totalWeight").asDouble();

        assertClose(
                "customer<->orders totalWeight",
                expectedCustomerTotal,
                actualCustomerTotal
        );

        validateSeenTuples(estimation);

        System.out.println();
        System.out.println("Validation summary:");
        System.out.println("lineitem<->orders numberOfKeys: " + actualLineitemKeys);
        System.out.println("lineitem<->orders totalWeight: " + actualLineitemTotal);
        System.out.println("customer<->orders numberOfKeys: " + actualCustomerKeys);
        System.out.println("customer<->orders totalWeight: " + actualCustomerTotal);
    }

    private static void validateSeenTuples(JsonNode estimation) {
        JsonNode seen = estimation.get("seenTuplesByAlias");

        if (seen == null || seen.isNull()) {
            throw new IllegalStateException("Missing seenTuplesByAlias");
        }

        assertSeen(seen, "customer", 0L);
        assertSeen(seen, "lineitem", sentLineitems);
        assertSeen(seen, "orders", sentOrders);
    }

    private static void assertSeen(JsonNode seen,
                                   String alias,
                                   long expected) {

        JsonNode value = seen.get(alias);

        if (value == null || value.isNull()) {
            throw new IllegalStateException("Missing seen counter for alias " + alias);
        }

        long actual = value.asLong();

        if (actual != expected) {
            throw new IllegalStateException(
                    "Seen counter for " + alias + " expected " + expected + " but got " + actual
            );
        }
    }

    private static double sumExpected(Map<String, Double> map) {
        double total = 0.0d;

        for (Double value : map.values()) {
            if (value != null) {
                total += value;
            }
        }

        return total;
    }

    private static void writeSdeEstimationToFile(JsonNode estimation) throws Exception {
        File outputFile = new File("/tmp/sde_q3_weighted_summary.json");

        String prettyJson =
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(estimation);

        FileWriter writer = new FileWriter(outputFile);

        try {
            writer.write(prettyJson);
            writer.write(System.lineSeparator());
        } finally {
            writer.close();
        }

        System.out.println();
        System.out.println("SDE estimation JSON written to:");
        System.out.println(outputFile.getAbsolutePath());
    }

    private static void writeSdeFullIndexesToFile(JsonNode estimation) throws Exception {
        File outputFile = new File("/tmp/sde_q3_weighted_full_indexes.json");

        String prettyJson =
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(estimation);

        FileWriter writer = new FileWriter(outputFile);

        try {
            writer.write(prettyJson);
            writer.write(System.lineSeparator());
        } finally {
            writer.close();
        }

        System.out.println();
        System.out.println("Full SDE Phase 1 indexes written to:");
        System.out.println(outputFile.getAbsolutePath());
    }

    private static void validateTpchQ3FullIndexes(JsonNode edgeIndexes,
                                                  JsonNode estimation) {

        JsonNode lineitemOrdersIndex = edgeIndexes.get("lineitem<->orders");
        JsonNode customerOrdersIndex = edgeIndexes.get("customer<->orders");

        if (lineitemOrdersIndex == null || lineitemOrdersIndex.isNull()) {
            throw new IllegalStateException("Missing lineitem<->orders index");
        }

        if (customerOrdersIndex == null || customerOrdersIndex.isNull()) {
            throw new IllegalStateException("Missing customer<->orders index");
        }

        compareEveryKey(
                "lineitem<->orders",
                expectedLineitemsByOrder,
                lineitemOrdersIndex
        );

        compareEveryKey(
                "customer<->orders",
                expectedOrdersByCustomer,
                customerOrdersIndex
        );

        validateSeenTuples(estimation);

        System.out.println();
        System.out.println("Full-index Java validation summary:");
        System.out.println("lineitem<->orders keys: " + lineitemOrdersIndex.size());
        System.out.println("customer<->orders keys: " + customerOrdersIndex.size());
        System.out.println("lineitem<->orders total weight: " + sumExpected(expectedLineitemsByOrder));
        System.out.println("customer<->orders total weight: " + sumExpected(expectedOrdersByCustomer));
    }

    private static void compareEveryKey(String edgeName,
                                        Map<String, Double> expected,
                                        JsonNode actualIndex) {

        if (actualIndex.size() != expected.size()) {
            throw new IllegalStateException(
                    edgeName + " key count expected " +
                            expected.size() + " but got " + actualIndex.size()
            );
        }

        for (Map.Entry<String, Double> entry : expected.entrySet()) {
            String key = entry.getKey();
            double expectedValue = entry.getValue();

            JsonNode actualNode = actualIndex.get(key);

            if (actualNode == null || actualNode.isNull()) {
                throw new IllegalStateException(
                        edgeName + " is missing key: " + key
                );
            }

            double actualValue = actualNode.asDouble();

            assertClose(
                    edgeName + "[" + key + "]",
                    expectedValue,
                    actualValue
            );
        }
    }

    private static ObjectNode buildAddPhaseOneSqlRequest() {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", DATASET_KEY);
        request.put("requestID", 1);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "tpch-q3-sql");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");
        param.add("unused");

        ObjectNode parameters = request.putObject("parameters");

        parameters.put(
                "onePassSql",
                "SELECT * FROM wq3 ROOT customer LIMIT 1000000 " +
                        "/* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */"
        );

        return request;
    }

    private static void assertClose(String label,
                                    double expected,
                                    double actual) {

        /*
         * Weighted TPC-H values are large, so use a relative tolerance.
         */
        double absoluteTolerance = 0.000001d;
        double relativeTolerance = 0.000000001d;

        double diff = Math.abs(expected - actual);
        double allowed = Math.max(absoluteTolerance, Math.abs(expected) * relativeTolerance);

        if (diff > allowed) {
            throw new IllegalStateException(
                    label + " expected " + expected + " but got " + actual +
                            " . Difference = " + diff + ", allowed = " + allowed
            );
        }
    }
}