package Tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class OnePassCoordinatorFlinkPipelineTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ONEPASS_DATA_BARRIER_REQUEST_ID = 70;
    private static final int ONEPASS_COORDINATOR_REQUEST_ID = 71;
    private static final int ONEPASS_SYNOPSIS_ID = 30;

    @Before
    public void clearSink() {
        CollectSink.clear();
    }

    @Test
    public void coordinatorWorksInsideFlinkPipeline() throws Exception {
        int uid = 123;
        int expectedWorkers = 4;
        String datasetKey = "tpch-wq3";
        String phase = "PHASE1";
        String alias = "l";
        String barrierId = "PHASE1_l_123_pipeline_test";

        List<Estimation> input = new ArrayList<Estimation>();

        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 0, expectedWorkers));
        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 1, expectedWorkers));
        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 2, expectedWorkers));
        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 3, expectedWorkers));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.fromCollection(input)
                .flatMap(new OnePassCoordinatorOperator())
                .name("OnePass Coordinator Test")
                .setParallelism(1)
                .addSink(new CollectSink())
                .name("Collect Coordinator Output")
                .setParallelism(1);

        env.execute("OnePassCoordinatorFlinkPipelineTest");

        List<Estimation> output = CollectSink.values();

        assertEquals(
                "Coordinator should emit exactly one GLOBAL_BARRIER_READY message",
                1,
                output.size()
        );

        Estimation ready = output.get(0);

        assertEquals(ONEPASS_COORDINATOR_REQUEST_ID, ready.getRequestID());
        assertEquals(ONEPASS_SYNOPSIS_ID, ready.getSynopsisID());
        assertEquals(uid, ready.getUID());
        assertEquals(datasetKey, ready.getKey());

        JsonNode payload =
                MAPPER.readTree(ready.getEstimation().toString());

        assertEquals("GLOBAL_BARRIER_READY", payload.get("type").asText());
        assertEquals(uid, payload.get("uid").asInt());
        assertEquals(phase, payload.get("phase").asText());
        assertEquals(alias, payload.get("alias").asText());
        assertEquals(barrierId, payload.get("barrierId").asText());
        assertEquals(expectedWorkers, payload.get("expectedWorkers").asInt());
        assertEquals(expectedWorkers, payload.get("receivedWorkers").size());
    }

    @Test
    public void coordinatorDeduplicatesInsideFlinkPipeline() throws Exception {
        int uid = 124;
        int expectedWorkers = 2;
        String datasetKey = "tpch-wq3";
        String phase = "PHASE1";
        String alias = "o";
        String barrierId = "PHASE1_o_124_duplicate_pipeline_test";

        List<Estimation> input = new ArrayList<Estimation>();

        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 0, expectedWorkers));
        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 0, expectedWorkers));
        input.add(barrierAck(uid, datasetKey, phase, alias, barrierId, 1, expectedWorkers));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.fromCollection(input)
                .flatMap(new OnePassCoordinatorOperator())
                .name("OnePass Coordinator Duplicate Test")
                .setParallelism(1)
                .addSink(new CollectSink())
                .name("Collect Coordinator Output")
                .setParallelism(1);

        env.execute("OnePassCoordinatorFlinkPipelineDuplicateTest");

        List<Estimation> output = CollectSink.values();

        assertEquals(
                "Duplicate ACK from same worker must not create extra output",
                1,
                output.size()
        );

        JsonNode payload =
                MAPPER.readTree(output.get(0).getEstimation().toString());

        assertEquals("GLOBAL_BARRIER_READY", payload.get("type").asText());
        assertEquals(2, payload.get("receivedWorkers").size());
    }

    private static Estimation barrierAck(
            int uid,
            String datasetKey,
            String phase,
            String alias,
            String barrierId,
            int workerId,
            int expectedWorkers) throws Exception {

        String json =
                "{"
                        + "\"type\":\"DATA_BARRIER_ACK\","
                        + "\"barrierId\":\"" + barrierId + "\","
                        + "\"phase\":\"" + phase + "\","
                        + "\"alias\":\"" + alias + "\","
                        + "\"uid\":" + uid + ","
                        + "\"workerId\":" + workerId + ","
                        + "\"expectedWorkers\":" + expectedWorkers + ","
                        + "\"actualParallelism\":" + expectedWorkers + ","
                        + "\"foundOnePassSynopsis\":true"
                        + "}";

        String[] param =
                new String[] {
                        "DATA_BARRIER_ACK",
                        barrierId,
                        phase,
                        alias,
                        Integer.toString(workerId),
                        Integer.toString(expectedWorkers)
                };

        return new Estimation(
                uid,
                uid + "_" + phase + "_" + alias + "_" + barrierId + "_worker_" + workerId,
                ONEPASS_DATA_BARRIER_REQUEST_ID,
                ONEPASS_SYNOPSIS_ID,
                datasetKey,
                json,
                param,
                expectedWorkers
        );
    }

    public static final class CollectSink implements SinkFunction<Estimation> {
        private static final long serialVersionUID = 1L;

        private static final List<Estimation> VALUES =
                Collections.synchronizedList(new ArrayList<Estimation>());

        @Override
        public void invoke(Estimation value, Context context) {
            VALUES.add(value);
        }

        public static void clear() {
            VALUES.clear();
        }

        public static List<Estimation> values() {
            synchronized (VALUES) {
                return new ArrayList<Estimation>(VALUES);
            }
        }
    }
}