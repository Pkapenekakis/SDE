package Tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorOperator;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class OnePassCoordinatorOperatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ONEPASS_DATA_BARRIER_REQUEST_ID = 70;
    private static final int ONEPASS_COORDINATOR_REQUEST_ID = 71;
    private static final int ONEPASS_SYNOPSIS_ID = 30;

    @Test
    public void waitsForAllWorkersBeforeEmittingGlobalReady() throws Exception {
        OnePassCoordinatorOperator coordinator = new OnePassCoordinatorOperator();

        List<Estimation> output = new ArrayList<Estimation>();
        Collector<Estimation> collector = new ListCollector(output);

        int uid = 123;
        int expectedWorkers = 4;
        String datasetKey = "tpch-wq3";
        String phase = "PHASE1";
        String alias = "l";
        String barrierId = "PHASE1_l_123_test";

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 0, expectedWorkers),
                collector
        );

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 1, expectedWorkers),
                collector
        );

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 2, expectedWorkers),
                collector
        );

        assertEquals(
                "Coordinator must not emit before all workers have ACKed",
                0,
                output.size()
        );

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 3, expectedWorkers),
                collector
        );

        assertEquals(
                "Coordinator must emit exactly one GLOBAL_BARRIER_READY after all workers ACK",
                1,
                output.size()
        );

        Estimation ready = output.get(0);

        assertEquals(ONEPASS_COORDINATOR_REQUEST_ID, ready.getRequestID());
        assertEquals(ONEPASS_SYNOPSIS_ID, ready.getSynopsisID());
        assertEquals(uid, ready.getUID());
        assertEquals(datasetKey, ready.getKey());

        JsonNode readyPayload =
                MAPPER.readTree(ready.getEstimation().toString());

        assertEquals("GLOBAL_BARRIER_READY", readyPayload.get("type").asText());
        assertEquals(uid, readyPayload.get("uid").asInt());
        assertEquals(phase, readyPayload.get("phase").asText());
        assertEquals(alias, readyPayload.get("alias").asText());
        assertEquals(barrierId, readyPayload.get("barrierId").asText());
        assertEquals(expectedWorkers, readyPayload.get("expectedWorkers").asInt());
        assertEquals(4, readyPayload.get("receivedWorkers").size());
    }

    @Test
    public void ignoresDuplicateWorkerAck() throws Exception {
        OnePassCoordinatorOperator coordinator =
                new OnePassCoordinatorOperator();

        List<Estimation> output = new ArrayList<Estimation>();
        Collector<Estimation> collector = new ListCollector(output);

        int uid = 123;
        int expectedWorkers = 2;
        String datasetKey = "tpch-wq3";
        String phase = "PHASE1";
        String alias = "l";
        String barrierId = "PHASE1_l_123_duplicate_test";

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 0, expectedWorkers),
                collector
        );

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 0, expectedWorkers),
                collector
        );

        assertEquals(
                "Duplicate ACK from same worker must not complete the barrier",
                0,
                output.size()
        );

        coordinator.flatMap(
                barrierAck(uid, datasetKey, phase, alias, barrierId, 1, expectedWorkers),
                collector
        );

        assertEquals(
                "Barrier should complete only after two distinct workerIds",
                1,
                output.size()
        );
    }

    @Test
    public void ignoresNonOnePassBarrierMessages() throws Exception {
        OnePassCoordinatorOperator coordinator =
                new OnePassCoordinatorOperator();

        List<Estimation> output = new ArrayList<Estimation>();
        Collector<Estimation> collector = new ListCollector(output);

        Estimation normalEstimation =
                new Estimation(
                        123,
                        "normal",
                        3,
                        30,
                        "tpch-wq3",
                        "some normal estimate",
                        new String[] {"NORMAL"},
                        1
                );

        coordinator.flatMap(normalEstimation, collector);

        assertEquals(0, output.size());
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

    private static final class ListCollector implements Collector<Estimation> {
        private final List<Estimation> output;

        private ListCollector(List<Estimation> output) {
            this.output = output;
        }

        @Override
        public void collect(Estimation record) {
            output.add(record);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}