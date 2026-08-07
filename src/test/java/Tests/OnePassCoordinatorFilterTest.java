package Tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.transformations.onepass.OnePassFeedbackRequestFactory;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorFilter;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OnePassCoordinatorFilterTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldForwardBeginWithoutCreatingTransition()
            throws Exception {

        OnePassCoordinatorFilter coordinator = new OnePassCoordinatorFilter();

        ListCollector collector =
                new ListCollector();

        Estimation begin =
                feedbackMessage(
                        "GLOBAL_STATE_BEGIN",
                        null,
                        null
                );

        coordinator.flatMap(begin, collector);

        assertEquals(1, collector.values.size());
        assertEquals(begin, collector.values.get(0));
    }

    @Test
    public void shouldForwardChunkWithoutCreatingTransition()
            throws Exception {

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        ListCollector collector =
                new ListCollector();

        Estimation chunk =
                feedbackMessage(
                        "GLOBAL_STATE_CHUNK",
                        null,
                        null
                );

        coordinator.flatMap(chunk, collector);

        assertEquals(1, collector.values.size());
        assertEquals(chunk, collector.values.get(0));
    }

    @Test
    public void shouldEmitStartNextAliasAfterCommit()
            throws Exception {

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        ListCollector collector =
                new ListCollector();

        Estimation commit =
                feedbackMessage(
                        "GLOBAL_STATE_COMMIT",
                        "START_NEXT_ALIAS",
                        "o"
                );

        coordinator.flatMap(commit, collector);

        /*
         * Output order must be:
         *
         *   COMMIT
         *   START_NEXT_ALIAS
         */
        assertEquals(2, collector.values.size());
        assertEquals(commit, collector.values.get(0));

        Estimation transition =
                collector.values.get(1);

        assertEquals(7, transition.getRequestID());
        assertEquals(30, transition.getSynopsisID());
        assertEquals(4, transition.getNoOfP());

        assertEquals(
                "onepass-phase1-123",
                transition.getEstimationkey()
        );

        JsonNode payload =
                payload(transition);

        assertEquals(
                "START_NEXT_ALIAS",
                payload.get("type").asText()
        );

        assertEquals(
                "o",
                payload.get("nextAlias").asText()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                payload.get("requiredStateRef").asText()
        );

        /*
         * Verify the full OnePass feedback conversion too.
         */
        Request request =
                OnePassFeedbackRequestFactory
                        .fromEstimation(transition);

        assertNotNull(request.getParameters());

        assertEquals(
                "START_NEXT_ALIAS",
                request.getParameters()
                        .get("type")
                        .asText()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                request.getParameters()
                        .get("requiredStateRef")
                        .asText()
        );
    }

    @Test
    public void shouldEmitStartPhaseTwoAfterFinalPhaseOneCommit()
            throws Exception {

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        ListCollector collector =
                new ListCollector();

        Estimation commit =
                feedbackMessage(
                        "GLOBAL_STATE_COMMIT",
                        "START_PHASE_2",
                        "c"
                );

        coordinator.flatMap(commit, collector);

        assertEquals(2, collector.values.size());

        JsonNode transition =
                payload(collector.values.get(1));

        assertEquals(
                "START_PHASE_2",
                transition.get("type").asText()
        );

        assertEquals(
                "c",
                transition.get("nextAlias").asText()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                transition.get("requiredStateRef").asText()
        );
    }

    @Test
    public void shouldIgnoreNonOnePassMessages()
            throws Exception {

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        ListCollector collector =
                new ListCollector();

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put("type", "GLOBAL_STATE_COMMIT");

        Estimation otherSynopsis =
                new Estimation(
                        999,
                        "generic-key",
                        7,
                        10,
                        "generic-key",
                        payload,
                        new String[] {
                                "GLOBAL_STATE_COMMIT"
                        },
                        4
                );

        coordinator.flatMap(
                otherSynopsis,
                collector
        );

        assertEquals(0, collector.values.size());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRejectCommitWithoutTransitionMetadata()
            throws Exception {

        OnePassCoordinatorFilter coordinator =
                new OnePassCoordinatorFilter();

        ListCollector collector =
                new ListCollector();

        Estimation commit =
                feedbackMessage(
                        "GLOBAL_STATE_COMMIT",
                        null,
                        null
                );

        coordinator.flatMap(commit, collector);
    }

    private static Estimation feedbackMessage(
            String type,
            String nextCommand,
            String nextAlias) {

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put("type", type);
        payload.put(
                "stateType",
                "GLOBAL_PHASE1_INDEX"
        );

        payload.put("uid", 123);
        payload.put("phase", "PHASE1");
        payload.put("resultId", "PHASE1_l_123");
        payload.put(
                "stateRef",
                "123_PHASE1_l_GLOBAL_STATE"
        );

        payload.put(
                "baseKey",
                "onepass-phase1-123"
        );

        payload.put("queryName", "wq3_alias");
        payload.put("rootAlias", "c");
        payload.put("activeAlias", "l");
        payload.put("activeEdgeId", "l<->o");
        payload.put("expectedWorkers", 4);

        if (nextCommand != null) {
            payload.put(
                    "nextCommand",
                    nextCommand
            );
        }

        if (nextAlias != null) {
            payload.put(
                    "nextAlias",
                    nextAlias
            );
        }

        return new Estimation(
                123,
                "onepass-phase1-123",
                7,
                30,
                "onepass-phase1-123",
                payload,
                new String[] {
                        type,
                        "123_PHASE1_l_GLOBAL_STATE"
                },
                4
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