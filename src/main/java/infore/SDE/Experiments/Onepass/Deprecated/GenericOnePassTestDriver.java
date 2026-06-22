package infore.SDE.Experiments.Onepass.Deprecated;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class GenericOnePassTestDriver {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String REQUEST_TOPIC = "requestTopic";
    private static final String DATA_TOPIC = "dataTopic";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();

        try {
            runPhase2Example(producer);

        } finally {
            producer.flush();
            producer.close();
        }

        System.out.println("Done sending test messages.");
    }

    // ------------------------------------------------------------
    // Phase 2 Test
    // ------------------------------------------------------------
    private static void runPhase2Example(KafkaProducer<String, String> producer) throws Exception {
        String addRequest = buildRequestJson(
                "INTEL",          // streamID
                30,               // synopsisID
                1,                // requestID = ADD
                "Forex",          // dataSetkey
                new String[]{"StockID", "price", "joinKey", "group_id", "weight", "5"},
                1,                // noOfP
                1110,             // uid
                buildOnePassParams()   // optional parameters block, ignored by Phase2
        );

        sendRequest(producer, addRequest);
        Thread.sleep(1000);

        for (int i = 0; i < 10; i++) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("time", "2025-12-02 16:00:0" + i);
            fields.put("StockID", "INTEL");
            fields.put("price", 100 + i);
            fields.put("joinKey", "O_" + i);
            fields.put("group_id", "G" + (i % 3));
            fields.put("weight", 1.0 + i);

            sendDatapoint(producer, "INTEL", "Forex", fields);
        }

        Thread.sleep(1000);

        String estimateRequest = buildRequestJson(
                "INTEL",
                30,
                3,            // requestID = ESTIMATE
                "Forex",
                new String[]{},
                1,
                1110,
                null
        );

        sendRequest(producer, estimateRequest);
    }

    // ------------------------------------------------------------
    // Example scenario 2: request-only OnePass parser test
    // ------------------------------------------------------------
    private static void runOnePassRequestOnlyExample(KafkaProducer<String, String> producer) throws Exception {
        String addRequest = buildRequestJson(
                "tpch",
                31,
                1,
                "tpch_sf1_qy",
                new String[]{"onepass"},
                1,
                2001,
                buildOnePassParams()
        );

        sendRequest(producer, addRequest);
    }

    // ------------------------------------------------------------
    // Generic send helpers
    // ------------------------------------------------------------
    private static void sendRequest(KafkaProducer<String, String> producer, String json) throws Exception {
        ProducerRecord<String, String> rec = new ProducerRecord<>(REQUEST_TOPIC, json);
        RecordMetadata meta = producer.send(rec).get();

        System.out.println("\nSENT request to " + meta.topic()
                + " partition " + meta.partition()
                + " offset " + meta.offset()
                + "\npayload=" + json);
    }

    private static void sendDatapoint(KafkaProducer<String, String> producer,
                                      String streamID,
                                      String dataSetkey,
                                      Map<String, Object> tupleFields) throws Exception {

        String valuesJson = MAPPER.writeValueAsString(tupleFields);

        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("values", valuesJson);
        wrapper.put("streamID", streamID);
        wrapper.put("dataSetkey", dataSetkey);

        String json = MAPPER.writeValueAsString(wrapper);

        ProducerRecord<String, String> rec = new ProducerRecord<>(DATA_TOPIC, json);
        RecordMetadata meta = producer.send(rec).get();

        System.out.println("SENT datapoint to " + meta.topic()
                + " partition " + meta.partition()
                + " offset " + meta.offset()
                + "\npayload=" + json);
    }

    // ------------------------------------------------------------
    // Generic JSON builders
    // ------------------------------------------------------------
    private static String buildRequestJson(String streamID,
                                           int synopsisID,
                                           int requestID,
                                           String dataSetkey,
                                           String[] param,
                                           int noOfP,
                                           int uid,
                                           Map<String, Object> parameters) throws Exception {

        Map<String, Object> rq = new LinkedHashMap<>();
        rq.put("streamID", streamID);
        rq.put("synopsisID", synopsisID);
        rq.put("requestID", requestID);
        rq.put("dataSetkey", dataSetkey);
        rq.put("param", param);
        rq.put("noOfP", noOfP);
        rq.put("uid", uid);

        if (parameters != null && !parameters.isEmpty()) {
            rq.put("parameters", parameters);
        }

        return MAPPER.writeValueAsString(rq);
    }

    private static Map<String, Object> buildOnePassParams() {
        Map<String, Object> root = new LinkedHashMap<>();
        Map<String, Object> onePassParams = new LinkedHashMap<>();

        onePassParams.put("queryName", "QY");
        onePassParams.put("mainTable", "l1");

        Map<String, Object> dataset = new LinkedHashMap<>();
        dataset.put("name", "tpch");
        dataset.put("dbConfig", "tpch.json");
        dataset.put("scaleFactor", 1);
        dataset.put("seed", "test123");
        onePassParams.put("dataset", dataset);

        Object[] relations = new Object[]{
                relation("lineitem", "l1"),
                relation("orders", "o1")
        };
        onePassParams.put("relations", relations);

        Object[] joins = new Object[]{
                join("l1", "l_orderkey", "o1", "o_orderkey")
        };
        onePassParams.put("joins", joins);

        Map<String, Object> weight = new LinkedHashMap<>();
        weight.put("expression", "w");
        weight.put("variables", new String[]{"w"});
        onePassParams.put("weight", weight);

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("sampleSize", 5);
        onePassParams.put("output", output);

        root.put("onePassParams", onePassParams);
        return root;
    }

    private static Map<String, Object> relation(String table, String alias) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("table", table);
        m.put("alias", alias);
        return m;
    }

    private static Map<String, Object> join(String leftAlias, String leftField,
                                            String rightAlias, String rightField) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("leftAlias", leftAlias);
        m.put("leftField", leftField);
        m.put("rightAlias", rightAlias);
        m.put("rightField", rightField);
        return m;
    }
}