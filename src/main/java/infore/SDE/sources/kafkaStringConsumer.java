package infore.SDE.sources;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
public class kafkaStringConsumer {

    private FlinkKafkaConsumer<String> fc;

    public kafkaStringConsumer(String server, String topic) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", "test");
        //.setStartFromEarliest()
        fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

    }

    public kafkaStringConsumer(String server, String topic, boolean readCommitted) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", "test");

        if (readCommitted) {
            properties.setProperty("isolation.level", "read_committed");
        }

        fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    /**
     * Explicit cluster/benchmark constructor.
     * Use a different group for data/request/state and a fresh run id for every detached thesis run.
     */
    public kafkaStringConsumer(String server, String topic, String groupId, boolean readCommitted, boolean startFromLatest) {

        if (groupId == null || groupId.trim().isEmpty()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", groupId.trim());

        if (readCommitted) {
            properties.setProperty("isolation.level", "read_committed");
        }

        fc = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);

        if (startFromLatest) {
            /*
             * Thesis benchmark jobs are started before the producer test.
             * A fresh job should not replay stale state/control records from an
             * earlier experiment.
             */
            fc.setStartFromLatest();
        }
    }

    public void cancel() {

        fc.cancel();

    }

    public FlinkKafkaConsumer<String> getFc() {
        return fc;
    }

    public void setFc(FlinkKafkaConsumer<String> fc) {
        this.fc = fc;
    }
}
