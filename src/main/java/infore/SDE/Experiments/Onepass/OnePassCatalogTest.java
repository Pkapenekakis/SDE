package infore.SDE.Experiments.Onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
import infore.SDE.transformations.onepass.sql.OnePassCatalog;
import infore.SDE.transformations.onepass.sql.OnePassQueryCatalogLoader;
import infore.SDE.transformations.onepass.sql.OnePassSqlCompiler;
import infore.SDE.transformations.onepass.sql.OnePassSqlParser;
import infore.SDE.transformations.onepass.sql.OnePassSqlRequest;
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
import java.io.FileWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OnePassCatalogTest {

    /*
     * ============================================================
     * Generic OnePass Phase 1 catalog test configuration
     * ============================================================
     *
     * Change only these values when you want to test another valid
     * tree-shaped catalog query.
     */
    private static final String TEST_TPCH_DIR =
            "/home/vboxuser/Desktop/Thesis/tpch-data/sf1";

    /*
     * Query name must exist inside tpch-onepass-catalog.json.
     *
     * Examples:
     *   "wq3_alias"
     *   "w_two_lineitems"
     *   "wqx_alias"
     */
    private static final String TEST_QUERY_NAME = "wq3_alias";

    /*
     * Use "default" to use the catalog's defaultRoot.
     * Or write the root alias manually, e.g. "c", "o", "nat".
     */
    private static final String TEST_ROOT_ALIAS = "default";

    /*
     * Use:
     *   "full"    -> compare every key and every value in edgeIndexes
     *   "summary" -> compare only numberOfKeys and totalWeight
     */
    private static final String TEST_MODE = "full";

    /*
     * Row limits.
     *
     * Examples:
     *   "50"              -> every alias gets 50 rows
     *   "all=50"          -> every alias gets 50 rows
     *   "l=50,o=100"      -> alias l gets 50 rows, alias o gets 100 rows
     *   "all=500,l=50"    -> default 500 rows, but alias l gets only 50
     *   "l1=50,l2=50"     -> useful for repeated table alias tests
     *
     * Use -1 for unlimited rows, for example:
     *   "all=-1"
     */
    private static final String TEST_ROW_LIMITS = "all=50";

    private static final String TEST_CATALOG_REF = "tpch-onepass-catalog.json";

    private static final int TEST_SAMPLE_LIMIT = 1000000;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKERS = "localhost:9092";
    private static final String DATA_TOPIC = "dataTopic";
    private static final String REQUEST_TOPIC = "requestTopic";
    private static final String ESTIMATION_TOPIC = "estimationTopic";

    private static final int UID = (int) (System.currentTimeMillis() % 1000000000);

    private static String datasetKey;

    private static final Map<String, Map<String, Double>> expectedIndexesByEdge =
            new LinkedHashMap<String, Map<String, Double>>();

    private static final Map<String, Long> sentRowsByAlias =
            new LinkedHashMap<String, Long>();

    public static void main(String[] args) throws Exception {
        expectedIndexesByEdge.clear();
        sentRowsByAlias.clear();

        datasetKey = "onepass-catalog-" + TEST_QUERY_NAME + "-" + UID;

        String sql = buildSql(
                TEST_QUERY_NAME,
                TEST_ROOT_ALIAS,
                TEST_SAMPLE_LIMIT,
                TEST_CATALOG_REF
        );

        OnePassParams params = OnePassSqlCompiler.compile(sql);
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        OnePassSqlRequest sqlRequest = OnePassSqlParser.parse(sql);
        OnePassCatalog catalog = OnePassQueryCatalogLoader.load(sqlRequest.getCatalogRef());

        initializeExpectedIndexes(plan);

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();

        prepareConsumerAtEnd(consumer);

        System.out.println("Using UID: " + UID);
        System.out.println("Using dataSetkey: " + datasetKey);
        System.out.println("TPC-H directory: " + TEST_TPCH_DIR);
        System.out.println("Query name: " + TEST_QUERY_NAME);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Mode: " + TEST_MODE);
        System.out.println("Row limits: " + TEST_ROW_LIMITS);
        System.out.println("Catalog ref: " + TEST_CATALOG_REF);
        System.out.println("Sample limit: " + TEST_SAMPLE_LIMIT);
        System.out.println("Compiled plan: " + plan);
        System.out.println("Leaf-to-root replay order: " + plan.getLeafToRootOrder());
        System.out.println("SQL:");
        System.out.println(sql);

        System.out.println();
        System.out.println("1. Sending ADD request...");
        ObjectNode addRequest = buildAddRequest(sql);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(addRequest));
        send(producer, REQUEST_TOPIC, addRequest);

        Thread.sleep(3000L);

        System.out.println();
        System.out.println("2. Replaying Phase 1 aliases in leaf-to-root order...");

        OnePassWeightEvaluator expectedWeightEvaluator =
                new OnePassWeightEvaluator(params.getWeight());

        for (String alias : plan.getLeafToRootOrder()) {
            long maxRowsForAlias = rowsForAlias(alias);

            replayAlias(
                    producer,
                    new File(TEST_TPCH_DIR),
                    catalog,
                    plan,
                    expectedWeightEvaluator,
                    alias,
                    maxRowsForAlias
            );
        }

        producer.flush();

        System.out.println();
        System.out.println("Rows sent by alias:");
        for (Map.Entry<String, Long> entry : sentRowsByAlias.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }

        System.out.println();
        System.out.println("Expected indexes:");
        for (Map.Entry<String, Map<String, Double>> entry : expectedIndexesByEdge.entrySet()) {
            System.out.println(
                    entry.getKey()
                            + " keys=" + entry.getValue().size()
                            + ", totalWeight=" + sum(entry.getValue())
            );
        }

        Thread.sleep(5000L);

        System.out.println();
        System.out.println("3. Sending ESTIMATE request...");
        ObjectNode estimateRequest = buildEstimateRequest(TEST_MODE);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(estimateRequest));
        send(producer, REQUEST_TOPIC, estimateRequest);

        System.out.println();
        System.out.println("4. Waiting for Phase 1 result...");
        waitForAndValidateResult(consumer, TEST_MODE, TEST_QUERY_NAME);

        producer.close();
        consumer.close();

        System.out.println();
        System.out.println("SUCCESS: Generic catalog replay Phase 1 test passed.");
    }

    private static String buildSql(String queryName,
                                   String rootAlias,
                                   int limit,
                                   String catalogRef) {

        StringBuilder sql = new StringBuilder();

        sql.append("SELECT * FROM ");
        sql.append(queryName);
        sql.append(" ");

        if (!isBlank(rootAlias) && !"default".equalsIgnoreCase(rootAlias)) {
            sql.append("ROOT ");
            sql.append(rootAlias);
            sql.append(" ");
        }

        sql.append("LIMIT ");
        sql.append(limit);
        sql.append(" ");
        sql.append("/* catalog='");
        sql.append(catalogRef);
        sql.append("', seed='test123', scalefactor=1 */");

        return sql.toString();
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "onepass-catalog-replay-test-" + UID);
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
            throw new IllegalStateException(
                    "Could not get Kafka partition assignment for estimationTopic"
            );
        }

        consumer.seekToEnd(consumer.assignment());

        System.out.println("Consumer is ready and positioned at the end of estimationTopic.");
    }

    private static void send(KafkaProducer<String, String> producer,
                             String topic,
                             ObjectNode json) throws Exception {

        String value = MAPPER.writeValueAsString(json);

        producer.send(new ProducerRecord<String, String>(topic, datasetKey, value)).get();
    }

    private static ObjectNode buildAddRequest(String sql) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("requestID", 1);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "onepass-catalog");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");
        param.add("catalog-replay");

        ObjectNode parameters = request.putObject("parameters");
        parameters.put("onePassSql", sql);

        return request;
    }

    private static ObjectNode buildEstimateRequest(String mode) {
        ObjectNode request = MAPPER.createObjectNode();

        request.put("dataSetkey", datasetKey);
        request.put("requestID", 3);
        request.put("synopsisID", 31);
        request.put("uid", UID);
        request.put("streamID", "onepass-catalog");
        request.put("noOfP", 1);

        ArrayNode param = request.putArray("param");

        if ("summary".equalsIgnoreCase(mode)) {
            param.add("summary");
            param.add("sample=5");
        } else {
            param.add("debug-full-indexes");
        }

        return request;
    }

    private static void initializeExpectedIndexes(CompiledOnePassPlan plan) {
        for (String alias : plan.getLeafToRootOrder()) {
            CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);

            if (parentEdge != null && !expectedIndexesByEdge.containsKey(parentEdge.getEdgeId())) {
                expectedIndexesByEdge.put(
                        parentEdge.getEdgeId(),
                        new LinkedHashMap<String, Double>()
                );
            }
        }
    }

    private static void replayAlias(KafkaProducer<String, String> producer,
                                    File tpchDir,
                                    OnePassCatalog catalog,
                                    CompiledOnePassPlan plan,
                                    OnePassWeightEvaluator expectedWeightEvaluator,
                                    String alias,
                                    long maxRows) throws Exception {

        CompiledOnePassPlan.RelationNode relation = plan.getRelation(alias);

        if (relation == null) {
            throw new IllegalStateException("Unknown alias in plan: " + alias);
        }

        String tableName = relation.getTable();

        OnePassCatalog.CatalogTable table = catalog.getDataset().getTables().get(tableName);

        if (table == null) {
            throw new IllegalStateException(
                    "Catalog does not define table '" + tableName + "' for alias '" + alias + "'"
            );
        }

        File tableFile = new File(tpchDir, table.getFile());

        if (!tableFile.exists()) {
            throw new IllegalStateException(
                    "Missing table file for alias '" + alias + "': " + tableFile.getAbsolutePath()
            );
        }

        List<String> columns = table.getColumns();

        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException("Catalog table '" + tableName + "' has no columns");
        }

        String separator = table.getSeparator();

        if (separator == null || separator.length() == 0) {
            separator = "|";
        }

        String splitRegex = "\\Q" + separator + "\\E";

        System.out.println();
        System.out.println(
                "Replaying alias '" + alias + "' from table '" + tableName + "' file "
                        + tableFile.getAbsolutePath()
                        + " with maxRows="
                        + maxRows
        );

        long sent = 0L;

        BufferedReader br = new BufferedReader(new FileReader(tableFile));

        try {
            String line;

            while ((line = br.readLine()) != null) {
                if (maxRows >= 0 && sent >= maxRows) {
                    break;
                }

                String[] parts = line.split(splitRegex, -1);

                ObjectNode tuple = MAPPER.createObjectNode();
                tuple.put("alias", alias);

                int limit = Math.min(columns.size(), parts.length);

                for (int i = 0; i < limit; i++) {
                    putTypedValue(tuple, columns.get(i), parts[i]);
                }

                send(producer, DATA_TOPIC, datapoint(alias, tuple));

                updateExpectedIndexes(plan, expectedWeightEvaluator, tuple);

                sent++;

                if (sent % 10000L == 0L) {
                    System.out.println("Sent " + sent + " rows for alias " + alias);
                }
            }
        } finally {
            br.close();
        }

        sentRowsByAlias.put(alias, sent);

        System.out.println("Finished alias '" + alias + "'. Sent rows: " + sent);
    }

    private static ObjectNode datapoint(String streamId, ObjectNode tupleValues) {
        ObjectNode dp = MAPPER.createObjectNode();

        dp.put("dataSetkey", datasetKey);
        dp.put("streamID", streamId);
        dp.set("values", tupleValues);

        return dp;
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

            return Long.parseLong(value);
        } catch (Exception e) {
            return null;
        }
    }

    private static Double tryParseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (Exception e) {
            return null;
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

        CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(alias);

        if (parentEdge == null) {
            return;
        }

        double ownWeight = expectedWeightEvaluator.evaluate(tuple);
        double continuationWeight = 1.0d;

        for (CompiledOnePassPlan.DirectedJoinEdge childEdge : plan.getChildEdges(alias)) {
            String parentSideKey = joinKey(tuple, childEdge.getParentFields());

            Map<String, Double> childIndex = expectedIndexesByEdge.get(childEdge.getEdgeId());

            if (childIndex == null) {
                throw new IllegalStateException(
                        "Missing expected child index for edge " + childEdge.getEdgeId()
                );
            }

            Double childWeight = childIndex.get(parentSideKey);

            if (childWeight == null) {
                childWeight = 0.0d;
            }

            continuationWeight *= childWeight.doubleValue();
        }

        double subtreeWeight = ownWeight * continuationWeight;

        /*
         * Phase1LinkWeightIndex.add(...) ignores zero deltas.
         *
         * Therefore, if a tuple has no valid continuation from one of its child
         * branches, its subtree weight is zero and it must not create a key in the
         * expected index.
         */
        if (subtreeWeight == 0.0d) {
            return;
        }

        String childSideKey = joinKey(tuple, parentEdge.getChildFields());

        Map<String, Double> parentIndex = expectedIndexesByEdge.get(parentEdge.getEdgeId());

        if (parentIndex == null) {
            throw new IllegalStateException(
                    "Missing expected parent index for edge " + parentEdge.getEdgeId()
            );
        }

        addToMap(parentIndex, childSideKey, subtreeWeight);
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

    private static long rowsForAlias(String alias) {
        if (TEST_ROW_LIMITS == null || TEST_ROW_LIMITS.trim().isEmpty()) {
            return 50L;
        }

        String spec = TEST_ROW_LIMITS.trim();

        /*
         * Simple form:
         *
         * "50"
         *
         * means every alias gets 50 rows.
         */
        if (spec.matches("-?\\d+")) {
            return Long.parseLong(spec);
        }

        long defaultRows = 50L;
        Long specificRows = null;

        String[] parts = spec.split(",");

        for (String part : parts) {
            if (part == null || part.trim().isEmpty()) {
                continue;
            }

            String[] kv = part.split("=", 2);

            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid TEST_ROW_LIMITS entry: " + part +
                                ". Use examples like all=50 or l=50,o=100"
                );
            }

            String key = kv[0].trim();
            long value = Long.parseLong(kv[1].trim());

            if ("all".equalsIgnoreCase(key)
                    || "default".equalsIgnoreCase(key)
                    || "*".equals(key)) {
                defaultRows = value;
            } else if (key.equals(alias)) {
                specificRows = value;
            }
        }

        if (specificRows != null) {
            return specificRows.longValue();
        }

        return defaultRows;
    }

    private static void waitForAndValidateResult(KafkaConsumer<String, String> consumer,
                                                 String mode,
                                                 String queryName) throws Exception {

        long deadline = System.currentTimeMillis() + 300000L;

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

                if ("summary".equalsIgnoreCase(mode)) {
                    JsonNode edgeSummaries = estimation.get("edgeSummaries");

                    if (edgeSummaries == null || edgeSummaries.isNull()) {
                        throw new IllegalStateException("Missing edgeSummaries field");
                    }

                    validateSummaries(edgeSummaries);
                    writeJsonToFile(estimation, "/tmp/onepass_" + queryName + "_phase1_summary.json");

                    System.out.println();
                    System.out.println("Received valid Phase 1 summary result.");
                    return;
                }

                JsonNode edgeIndexes = estimation.get("edgeIndexes");

                if (edgeIndexes == null || edgeIndexes.isNull()) {
                    throw new IllegalStateException("Missing edgeIndexes field");
                }

                validateFullIndexes(edgeIndexes);
                writeJsonToFile(estimation, "/tmp/onepass_" + queryName + "_phase1_full_indexes.json");

                System.out.println();
                System.out.println("Received valid Phase 1 full-index result.");
                return;
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for generic catalog Phase 1 result"
        );
    }

    private static void validateSummaries(JsonNode edgeSummaries) {
        for (Map.Entry<String, Map<String, Double>> expectedEdge : expectedIndexesByEdge.entrySet()) {
            String edgeId = expectedEdge.getKey();
            Map<String, Double> expectedIndex = expectedEdge.getValue();

            JsonNode actualSummary = edgeSummaries.get(edgeId);

            if (actualSummary == null || actualSummary.isNull()) {
                throw new IllegalStateException("Missing summary for edge " + edgeId);
            }

            int expectedKeyCount = expectedIndex.size();
            int actualKeyCount = actualSummary.get("numberOfKeys").asInt();

            if (expectedKeyCount != actualKeyCount) {
                throw new IllegalStateException(
                        edgeId + " numberOfKeys expected "
                                + expectedKeyCount + " but got " + actualKeyCount
                );
            }

            double expectedTotal = sum(expectedIndex);
            double actualTotal = actualSummary.get("totalWeight").asDouble();

            assertClose(edgeId + " totalWeight", expectedTotal, actualTotal);
        }

        System.out.println("Summary validation passed for all edges.");
    }

    private static void validateFullIndexes(JsonNode edgeIndexes) {
        for (Map.Entry<String, Map<String, Double>> expectedEdge : expectedIndexesByEdge.entrySet()) {
            String edgeId = expectedEdge.getKey();
            Map<String, Double> expectedIndex = expectedEdge.getValue();

            JsonNode actualIndex = edgeIndexes.get(edgeId);

            if (actualIndex == null || actualIndex.isNull()) {
                throw new IllegalStateException("Missing full index for edge " + edgeId);
            }

            if (actualIndex.size() != expectedIndex.size()) {
                throw new IllegalStateException(
                        edgeId + " key count expected "
                                + expectedIndex.size() + " but got " + actualIndex.size()
                );
            }

            for (Map.Entry<String, Double> expectedEntry : expectedIndex.entrySet()) {
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

    private static void writeJsonToFile(JsonNode json,
                                        String path) throws Exception {

        File outputFile = new File(path);

        FileWriter writer = new FileWriter(outputFile);

        try {
            writer.write(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(json));
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
        double allowed = Math.max(absoluteTolerance, Math.abs(expected) * relativeTolerance);

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

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}