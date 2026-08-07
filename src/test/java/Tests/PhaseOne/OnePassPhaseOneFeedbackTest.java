package Tests.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.transformations.onepass.OnePassFeedbackRequestFactory;
import infore.SDE.transformations.onepass.OnePassPhaseOneRequestSplitter;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorFilter;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneWorkerProtocol;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OnePassPhaseOneFeedbackTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldFlowFromMergedResultToInstalledWorkerGate()
            throws Exception {

        Estimation merged =
                buildMergedPhaseOneResult();

        OnePassPhaseOneRequestSplitter splitter =
                new OnePassPhaseOneRequestSplitter(2);

        EstimationCollector splitterOutput =
                new EstimationCollector();

        splitter.flatMap(
                merged,
                splitterOutput
        );

        assertEquals(4, splitterOutput.values.size());

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        EstimationCollector coordinatorOutput =
                new EstimationCollector();

        for (Estimation message
                : splitterOutput.values) {

            coordinator.flatMap(
                    message,
                    coordinatorOutput
            );
        }

        // BEGIN + 2 CHUNK + COMMIT + START_NEXT_ALIAS
        assertEquals(5, coordinatorOutput.values.size());

        OnePassPhaseOneWorkerProtocol worker =
                new OnePassPhaseOneWorkerProtocol();

        JsonNode assembled = null;

        for (Estimation feedback
                : coordinatorOutput.values) {

            Request request = OnePassFeedbackRequestFactory.fromEstimation(feedback);

            JsonNode payload =
                    request.getParameters();

            assertNotNull(payload);

            String type =
                    payload.get("type").asText();

            if ("GLOBAL_STATE_BEGIN".equals(type)
                    || "GLOBAL_STATE_CHUNK".equals(type)
                    || "GLOBAL_STATE_COMMIT".equals(type)) {

                JsonNode maybeComplete =
                        worker.acceptStateMessage(payload);

                if (maybeComplete != null) {
                    assembled = maybeComplete;
                }

            } else if ("START_NEXT_ALIAS".equals(type)
                    || "START_PHASE_2".equals(type)) {

                boolean activated =
                        worker.acceptTransition(payload);

                // State is assembled, but installation is not yet simulated.
                assertFalse(activated);
            }
        }

        assertNotNull(assembled);

        assertEquals(
                3,
                assembled.get("entries").size()
        );

        assertFalse(worker.hasActiveTransition(123));

        // Simulate successful local synopsis installation.
        worker.markInstalled(
                123,
                "123_PHASE1_l_GLOBAL_STATE"
        );

        assertTrue(worker.hasActiveTransition(123));

        assertEquals(
                "o",
                worker.getActiveTransition(123)
                        .getNextAlias()
        );
    }

    private static Estimation buildMergedPhaseOneResult()
            throws Exception {

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put("type", "GLOBAL_PHASE1_RESULT");
        payload.put("uid", 123);
        payload.put("phase", "PHASE1");
        payload.put("resultId", "PHASE1_l_123");
        payload.put("queryName", "wq3_alias");
        payload.put("rootAlias", "c");
        payload.put("baseKey", "onepass-phase1-123");
        payload.put("activeAlias", "l");
        payload.put("activeEdgeId", "l<->o");
        payload.put("nextCommand", "START_NEXT_ALIAS");
        payload.put("nextAlias", "o");
        payload.put("stateRef", "123_PHASE1_l_GLOBAL_STATE");
        payload.put("expectedWorkers", 4);

        ObjectNode global =
                payload.putObject("globalPhaseOneResult");

        global.put("queryName", "wq3_alias");
        global.put("rootAlias", "c");
        global.put("activeAlias", "l");
        global.put("activeEdgeId", "l<->o");

        global.putObject("seenTuplesByAlias")
                .put("l", 3);

        ObjectNode edge =
                global.putObject("edgeIndexes")
                        .putObject("l<->o");

        edge.put("100", 10.0d);
        edge.put("200", 20.0d);
        edge.put("300", 30.0d);

        global.putObject("edgeSummaries")
                .putObject("l<->o")
                .put("numberOfKeys", 3);

        return new Estimation(
                123,
                "123_PHASE1_l_123",
                73,
                30,
                "123_PHASE1_l_123",
                payload.toString(),
                new String[] {
                        "GLOBAL_PHASE1_RESULT",
                        "PHASE1_l_123",
                        "PHASE1",
                        "l"
                },
                4
        );
    }

    private static final class EstimationCollector
            implements Collector<Estimation> {

        private final List<Estimation> values =
                new ArrayList<Estimation>();

        @Override
        public void collect(
                Estimation record) {

            values.add(record);
        }

        @Override
        public void close() {
            // Nothing to close.
        }
    }
}