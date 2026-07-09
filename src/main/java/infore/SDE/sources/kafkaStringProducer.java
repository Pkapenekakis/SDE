package infore.SDE.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.nio.charset.StandardCharsets;

/**
 * Kafka producer for plain JSON strings.
 *
 * Used for OnePass global-state chunks.
 */
public class kafkaStringProducer {

    private FlinkKafkaProducer<String> myProducer;

    public kafkaStringProducer(String brokerlist, String outputTopic) {
        myProducer = new FlinkKafkaProducer<String>(
                brokerlist,
                outputTopic,
                new JsonStringKeyedSerializer()
        );

        myProducer.setWriteTimestampToKafka(true);
    }

    public SinkFunction<String> getProducer() {
        return myProducer;
    }
}

class JsonStringKeyedSerializer implements KeyedSerializationSchema<String> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serializeKey(String element) {
        try {
            JsonNode node = MAPPER.readTree(element);

            JsonNode workerKey = node.get("workerKey");

            if (workerKey != null && !workerKey.isNull()) {
                return workerKey.asText().getBytes(StandardCharsets.UTF_8);
            }

            JsonNode stateRef = node.get("stateRef");

            if (stateRef != null && !stateRef.isNull()) {
                return stateRef.asText().getBytes(StandardCharsets.UTF_8);
            }

        } catch (Exception ignored) {
        }

        return "onepass-global-state".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serializeValue(String element) {
        if (element == null) {
            return new byte[0];
        }

        return element.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getTargetTopic(String element) {
        return null;
    }
}