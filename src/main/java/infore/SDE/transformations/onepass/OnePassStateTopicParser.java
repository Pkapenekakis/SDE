package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Datapoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * OnePass State Topic ingress parser/router.
 *
 * Targeted state work:
 *     workerKey present -> emit once
 *
 * Replicated global sample:
 *     broadcastToWorkers=true
 *         -> fan out inside Flink to every OnePass worker
 *
 * The fan-out is intentionally AFTER Kafka so the State Topic stores one
 * physical copy per global-sample chunk rather than P copies.
 */
public final class OnePassStateTopicParser extends RichFlatMapFunction<String, Datapoint> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();


    @Override
    public void flatMap(String json, Collector<Datapoint> out) throws Exception {
        if (json == null || json.trim().isEmpty()) {
            return;
        }

        JsonNode payload = MAPPER.readTree(json);

        if (payload == null || payload.isNull() || !payload.isObject()) {
            throw new IllegalStateException("OnePass State Topic record must be a JSON object.");
        }


        if (booleanField(payload, "broadcastToWorkers", false)) {
            broadcast((ObjectNode) payload, out);
            return;
        }

        routeTargeted(payload, out);
    }


    private void broadcast(ObjectNode payload, Collector<Datapoint> out) {
        int expectedWorkers = intField(payload, "expectedWorkers", 0);
        String baseKey = textField(payload, "baseKey", "");

        if (expectedWorkers <= 0) {
            throw new IllegalStateException("Broadcast State Topic record has invalid expectedWorkers="
                    + expectedWorkers + ": " + payload);
        }


        if (baseKey.isEmpty()) {
            throw new IllegalStateException("Broadcast State Topic record is missing baseKey: " + payload);
        }

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {
            String workerKey = OnePassShardOwnership.workerKey(baseKey, expectedWorkers, workerId);
            ObjectNode workerPayload = shallowObjectCopy(payload);

            //Add worker-local routing metadata only after Kafka.
            workerPayload.put("workerId", workerId);
            workerPayload.put("workerKey", workerKey);

            out.collect(new Datapoint(workerKey, "onepass-state-topic", workerPayload));
        }
    }

    private static ObjectNode shallowObjectCopy(ObjectNode source) {
        ObjectNode copy = MAPPER.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = source.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            /*
             * JsonNode values are not mutated by the StateTopic parser.
             * Reuse child nodes, especially the potentially large entries array.
             */
            copy.set(field.getKey(), field.getValue());
        }
        return copy;
    }


    private void routeTargeted(JsonNode payload, Collector<Datapoint> out) {
        String workerKey = textField(payload, "workerKey", "");

        if (workerKey.isEmpty()) {
            throw new IllegalStateException("Targeted OnePass State Topic record has no workerKey: " + payload);
        }

        out.collect(new Datapoint(workerKey, "onepass-state-topic", payload));
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
        return field == null || field.isNull() ? defaultValue : field.asInt(defaultValue);
    }


    private static boolean booleanField(JsonNode node, String fieldName, boolean defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? defaultValue : field.asBoolean(defaultValue);
    }
}