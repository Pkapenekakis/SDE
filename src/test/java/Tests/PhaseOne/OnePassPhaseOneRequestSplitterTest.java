package Tests.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.transformations.onepass.OnePassFeedbackRequestFactory;
import infore.SDE.transformations.onepass.OnePassPhaseOneRequestSplitter;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorFilter;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class OnePassPhaseOneRequestSplitterTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldCreateOneBeginChunksAndOneCommit()
            throws Exception {

        Estimation mergedResult =
                buildMergedPhaseOneResult();

        OnePassPhaseOneRequestSplitter splitter =
                new OnePassPhaseOneRequestSplitter(2);

        ListCollector collector =
                new ListCollector();

        splitter.flatMap(
                mergedResult,
                collector
        );

        /*
         * Three entries with chunk size 2:
         *
         * BEGIN
         * CHUNK 0: two entries
         * CHUNK 1: one entry
         * COMMIT
         */
        assertEquals(4, collector.values.size());

        assertMessage(
                collector.values.get(0),
                "GLOBAL_STATE_BEGIN"
        );

        assertMessage(
                collector.values.get(1),
                "GLOBAL_STATE_CHUNK"
        );

        assertMessage(
                collector.values.get(2),
                "GLOBAL_STATE_CHUNK"
        );

        assertMessage(
                collector.values.get(3),
                "GLOBAL_STATE_COMMIT"
        );

        JsonNode begin =
                payload(collector.values.get(0));

        JsonNode firstChunk =
                payload(collector.values.get(1));

        JsonNode secondChunk =
                payload(collector.values.get(2));

        JsonNode commit =
                payload(collector.values.get(3));

        assertEquals(
                2,
                begin.get("chunkCount").asInt()
        );

        assertEquals(
                3,
                begin.get("totalEntryCount").asInt()
        );

        assertNotNull(begin.get("checksum"));

        assertEquals(
                "START_NEXT_ALIAS",
                commit.get("nextCommand").asText()
        );

        assertEquals(
                "o",
                commit.get("nextAlias").asText()
        );

        assertEquals(
                commit.get("stateRef").asText(),
                commit.get("requiredStateRef").asText()
        );

        assertEquals(
                0,
                firstChunk.get("chunkId").asInt()
        );

        assertEquals(
                2,
                firstChunk.get("entryCount").asInt()
        );

        assertEquals(
                1,
                secondChunk.get("chunkId").asInt()
        );

        assertEquals(
                1,
                secondChunk.get("entryCount").asInt()
        );

        assertEquals(
                begin.get("checksum").asText(),
                commit.get("checksum").asText()
        );

        /*
         * The splitter emits only one logical copy.
         * Worker duplication will happen later in RqRouterFlatMap.
         */
        for (Estimation estimation : collector.values) {
            JsonNode message =
                    payload(estimation);

            assertFalse(message.has("workerId"));
            assertFalse(message.has("workerKey"));
        }
    }

    @Test
    public void shouldSurviveOnePassFeedbackRequestConversion()
            throws Exception {

        Estimation mergedResult =
                buildMergedPhaseOneResult();

        OnePassPhaseOneRequestSplitter splitter =
                new OnePassPhaseOneRequestSplitter(2);

        ListCollector collector =
                new ListCollector();

        splitter.flatMap(
                mergedResult,
                collector
        );

        for (Estimation feedback : collector.values) {
            Request request =
                    OnePassFeedbackRequestFactory
                            .fromEstimation(feedback);

            assertEquals(
                    "onepass-phase1-123",
                    request.getDataSetkey()
            );

            assertEquals(
                    7,
                    request.getRequestID()
            );

            assertEquals(
                    30,
                    request.getSynopsisID()
            );

            assertEquals(
                    4,
                    request.getNoOfP()
            );

            assertNotNull(
                    request.getParameters()
            );

            assertEquals(
                    feedback.getParam()[0],
                    request.getParameters()
                            .get("type")
                            .asText()
            );
        }
    }

    @Test
    public void shouldEmitStartNextAliasAfterCoordinatorReceivesCommit()
            throws Exception {

        Estimation mergedResult =
                buildMergedPhaseOneResult();

        OnePassPhaseOneRequestSplitter splitter =
                new OnePassPhaseOneRequestSplitter(2);

        ListCollector splitterCollector =
                new ListCollector();

        splitter.flatMap(
                mergedResult,
                splitterCollector
        );

        assertEquals(
                4,
                splitterCollector.values.size()
        );

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        ListCollector coordinatorCollector =
                new ListCollector();

        /*
         * Feed the splitter output to the coordinator in stream order:
         *
         * BEGIN
         * CHUNK 0
         * CHUNK 1
         * COMMIT
         */
        for (Estimation feedback
                : splitterCollector.values) {

            coordinator.flatMap(
                    feedback,
                    coordinatorCollector
            );
        }

        /*
         * The coordinator forwards all four splitter messages and emits
         * START_NEXT_ALIAS immediately after COMMIT.
         *
         * BEGIN
         * CHUNK 0
         * CHUNK 1
         * COMMIT
         * START_NEXT_ALIAS
         */
        assertEquals(
                5,
                coordinatorCollector.values.size()
        );

        assertMessage(
                coordinatorCollector.values.get(0),
                "GLOBAL_STATE_BEGIN"
        );

        assertMessage(
                coordinatorCollector.values.get(1),
                "GLOBAL_STATE_CHUNK"
        );

        assertMessage(
                coordinatorCollector.values.get(2),
                "GLOBAL_STATE_CHUNK"
        );

        assertMessage(
                coordinatorCollector.values.get(3),
                "GLOBAL_STATE_COMMIT"
        );

        assertMessage(
                coordinatorCollector.values.get(4),
                "START_NEXT_ALIAS"
        );

        JsonNode commit =
                payload(
                        coordinatorCollector.values.get(3)
                );

        JsonNode transition =
                payload(
                        coordinatorCollector.values.get(4)
                );

        assertEquals(
                "o",
                transition.get("nextAlias").asText()
        );

        assertEquals(
                commit.get("stateRef").asText(),
                transition.get("requiredStateRef").asText()
        );

        assertEquals(
                "123_PHASE1_PHASE1_l_123_GLOBAL_STATE",
                transition.get("requiredStateRef").asText()
        );

        assertEquals(
                "onepass-phase1-123",
                transition.get("baseKey").asText()
        );

        assertEquals(
                123,
                transition.get("uid").asInt()
        );

        /*
         * Confirm that the generated transition also survives the
         * Estimation -> Request conversion.
         */
        Request transitionRequest =
                OnePassFeedbackRequestFactory
                        .fromEstimation(
                                coordinatorCollector.values.get(4)
                        );

        assertEquals(
                "onepass-phase1-123",
                transitionRequest.getDataSetkey()
        );

        assertEquals(
                7,
                transitionRequest.getRequestID()
        );

        assertEquals(
                30,
                transitionRequest.getSynopsisID()
        );

        assertEquals(
                4,
                transitionRequest.getNoOfP()
        );

        assertNotNull(
                transitionRequest.getParameters()
        );

        assertEquals(
                "START_NEXT_ALIAS",
                transitionRequest.getParameters()
                        .get("type")
                        .asText()
        );

        assertEquals(
                "o",
                transitionRequest.getParameters()
                        .get("nextAlias")
                        .asText()
        );

        assertEquals(
                "123_PHASE1_PHASE1_l_123_GLOBAL_STATE",
                transitionRequest.getParameters()
                        .get("requiredStateRef")
                        .asText()
        );
    }

    private static void assertMessage(
            Estimation estimation,
            String expectedType) throws Exception {

        assertEquals(
                7,
                estimation.getRequestID()
        );

        assertEquals(
                30,
                estimation.getSynopsisID()
        );

        assertEquals(
                "onepass-phase1-123",
                estimation.getEstimationkey()
        );

        assertEquals(
                "onepass-phase1-123",
                estimation.getKey()
        );

        assertEquals(
                4,
                estimation.getNoOfP()
        );

        assertEquals(
                expectedType,
                payload(estimation)
                        .get("type")
                        .asText()
        );
    }

    private static JsonNode payload(
            Estimation estimation) throws Exception {

        Object value =
                estimation.getEstimation();

        if (value instanceof JsonNode) {
            return (JsonNode) value;
        }

        return MAPPER.readTree(
                String.valueOf(value)
        );
    }

    private static Estimation buildMergedPhaseOneResult()
            throws Exception {

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put(
                "type",
                "GLOBAL_PHASE1_RESULT"
        );

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

        payload.put(
                "stateRef",
                "123_PHASE1_PHASE1_l_123_GLOBAL_STATE"
        );

        payload.put("expectedWorkers", 4);

        ObjectNode global =
                payload.putObject(
                        "globalPhaseOneResult"
                );

        global.put("queryName", "wq3_alias");
        global.put("rootAlias", "c");
        global.put("activeAlias", "l");
        global.put("activeEdgeId", "l<->o");

        ObjectNode seen =
                global.putObject(
                        "seenTuplesByAlias"
                );

        seen.put("l", 3);

        ObjectNode indexes =
                global.putObject(
                        "edgeIndexes"
                );

        ObjectNode edge =
                indexes.putObject("l<->o");

        edge.put("100", 10.0d);
        edge.put("200", 20.0d);
        edge.put("300", 30.0d);

        ObjectNode summaries =
                global.putObject(
                        "edgeSummaries"
                );

        summaries.putObject("l<->o")
                .put("numberOfKeys", 3);

        return new Estimation(
                123,
                "123_PHASE1_PHASE1_l_123",
                73,
                30,
                "123_PHASE1_PHASE1_l_123",
                payload.toString(),
                new String[]{
                        "GLOBAL_PHASE1_RESULT",
                        "PHASE1_l_123",
                        "PHASE1",
                        "l"
                },
                4
        );
    }

    private static final class ListCollector
            implements Collector<Estimation> {

        private final List<Estimation> values =
                new ArrayList<Estimation>();

        @Override
        public void collect(Estimation record) {
            values.add(record);
        }

        @Override
        public void close() {
            // Nothing to close.
        }
    }
}