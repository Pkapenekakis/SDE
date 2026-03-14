package infore.SDE.Experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;

public class Phase2TestDriver {

    private static KafkaProducer<String, String> createProducer() {
        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        return new KafkaProducer<>(props);
    }



    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();

        // 1. ADD request
        sendAddPhase2Request(producer);

        //Small pause to create the synopsis
        Thread.sleep(1000);

        // 2. Sends a few datapoints to the dataTopic
        sendTestData(producer);
        Thread.sleep(1000);

        // 3. Sends an ESTIMATE request
        sendEstimateRequest(producer);

        producer.flush();
        producer.close();

        System.out.println("Done sending Phase 2 test messages.");
    }

    private static void sendAddPhase2Request(KafkaProducer<String, String> producer) throws Exception {
       /* Request before the JSON schema
       String json = "{\"streamID\":\"INTEL\","
                + "\"synopsisID\":30,"
                + "\"requestID\":1,"
                + "\"dataSetkey\":\"Forex\","
                + "\"param\":[\"StockID\",\"price\",\"joinKey\",\"group_id\",\"weight\",\"5\"],"
                + "\"noOfP\":1,"
                + "\"uid\":1110}"; */

        String json = "{"
                + "\"streamID\":\"INTEL\","
                + "\"synopsisID\":30,"
                + "\"requestID\":1,"
                + "\"dataSetkey\":\"Forex\","
                + "\"param\":[\"StockID\",\"price\",\"joinKey\",\"group_id\",\"weight\",\"5\"],"
                + "\"noOfP\":1,"
                + "\"uid\":1110,"
                + "\"parameters\":{"
                + "  \"onePassStarParams\":{"
                + "    \"queryName\":\"QY\","
                + "    \"mainTable\":\"l1\","
                + "    \"dataset\":{"
                + "      \"name\":\"tpch\","
                + "      \"dbConfig\":\"tpch.json\","
                + "      \"scaleFactor\":1,"
                + "      \"seed\":\"test123\""
                + "    },"
                + "    \"relations\":["
                + "      {\"table\":\"lineitem\",\"alias\":\"l1\"},"
                + "      {\"table\":\"orders\",\"alias\":\"o1\"}"
                + "    ],"
                + "    \"joins\":["
                + "      {\"leftAlias\":\"l1\",\"leftField\":\"l_orderkey\",\"rightAlias\":\"o1\",\"rightField\":\"o_orderkey\"}"
                + "    ],"
                + "    \"weight\":{"
                + "      \"expression\":\"w\","
                + "      \"variables\":[\"w\"]"
                + "    },"
                + "    \"output\":{"
                + "      \"sampleSize\":5"
                + "    }"
                + "  }"
                + "}"
                + "}";


        ProducerRecord<String, String> rec =
                new ProducerRecord<>("requestTopic", json);

        // Synchronous send - wait for completion
        org.apache.kafka.clients.producer.RecordMetadata meta =
                producer.send(rec).get();

        System.out.println("SENT Add request to " + meta.topic() +
                " partition " + meta.partition() +
                " offset " + meta.offset());
    }

    private static void sendTestData(KafkaProducer<String, String> producer) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        for (int i = 0; i < 10; i++) {
            // Inner JSON with the actual fields of Phase 2
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put("time", "2025-12-02 16:00:0" + i);
            valueMap.put("StockID", "INTEL");
            valueMap.put("price", 100 + i);          // Could be String
            valueMap.put("joinKey", "O_" + i);
            valueMap.put("group_id", "G" + (i % 3));
            valueMap.put("weight", 1.0 + i);

            //Values need to be JSON-string
            String valuesJson = mapper.writeValueAsString(valueMap);

            Map<String, Object> wrapper = new HashMap<>();
            wrapper.put("values", valuesJson);
            wrapper.put("streamID", "INTEL");  //Same streamID as the Request
            wrapper.put("dataSetkey", "Forex"); //Same as the dataSetkey of the Request

            String json = mapper.writeValueAsString(wrapper);

            ProducerRecord<String, String> rec =
                    new ProducerRecord<>("dataTopic", json);

            RecordMetadata meta = producer.send(rec).get();
            System.out.println("Sent datapoint to " + meta.topic()
                    + " partition " + meta.partition()
                    + " offset " + meta.offset()
                    + " payload=" + json);
        }
    }

    private static void sendEstimateRequest(KafkaProducer<String, String> producer) throws Exception {
        // 3 = ESTIMATE, according to SDE
        String json = "{\"streamID\":\"INTEL\","
                + "\"synopsisID\":30,"
                + "\"requestID\":3,"     // ESTIMATE
                + "\"dataSetkey\":\"Forex\","
                + "\"param\":[],"
                + "\"noOfP\":1,"
                + "\"uid\":1110}";       //TODO different UID for the estimate ???

        ProducerRecord<String, String> rec =
                new ProducerRecord<>("requestTopic", json);

        org.apache.kafka.clients.producer.RecordMetadata meta =
                producer.send(rec).get();

        System.out.println("\nSENT ESTIMATE Request to " + meta.topic()
                + " partition " + meta.partition()
                + " offset " + meta.offset()
                + "\npayload=" + json);
    }
}
