package Tests.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.onepass.OnePassIndexReduceFunction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OnePassIndexReduceTransitionMetadataTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldCarryEndAliasTransitionIntoGlobalResult()
            throws Exception {

        OnePassIndexReduceFunction reducer =
                new OnePassIndexReduceFunction(
                        4,
                        0,
                        new String[] {
                                "LOCAL_PHASE1_RESULT",
                                "PHASE1_l_123"
                        },
                        30,
                        72
                );

        for (int workerId = 0;
             workerId < 4;
             workerId++) {

            boolean complete =
                    reducer.add(
                            localResult(
                                    workerId
                            )
                    );

            if (workerId < 3) {
                assertFalse(
                        complete
                );
            } else {
                assertTrue(
                        complete
                );
            }
        }

        String reduced =
                String.valueOf(
                        reducer.reduce()
                );

        JsonNode payload =
                MAPPER.readTree(
                        reduced
                );

        assertEquals(
                "GLOBAL_PHASE1_RESULT",
                payload.get("type")
                        .asText()
        );

        assertEquals(
                "l",
                payload.get("activeAlias")
                        .asText()
        );

        assertEquals(
                "START_NEXT_ALIAS",
                payload.get("nextCommand")
                        .asText()
        );

        assertEquals(
                "o",
                payload.get("nextAlias")
                        .asText()
        );

        assertEquals(
                4,
                payload.get("localResultCount")
                        .asInt()
        );

        /*
         * The active edge is summed across the four local workers.
         */
        assertEquals(
                4.0d,
                payload.get("globalPhaseOneResult")
                        .get("edgeIndexes")
                        .get("l<->o")
                        .get("100")
                        .asDouble(),
                0.0d
        );
    }

    private static Estimation localResult(
            int workerId) throws Exception {

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put(
                "type",
                "LOCAL_PHASE1_RESULT"
        );

        payload.put(
                "uid",
                123
        );

        payload.put(
                "workerId",
                workerId
        );

        payload.put(
                "expectedWorkers",
                4
        );

        payload.put(
                "phase",
                "PHASE1"
        );

        payload.put(
                "resultId",
                "PHASE1_l_123"
        );

        payload.put(
                "queryName",
                "wq3_alias"
        );

        payload.put(
                "rootAlias",
                "c"
        );

        payload.put(
                "baseKey",
                "onepass-phase1-123"
        );

        payload.put(
                "activeAlias",
                "l"
        );

        payload.put(
                "activeEdgeId",
                "l<->o"
        );

        payload.put(
                "nextCommand",
                "START_NEXT_ALIAS"
        );

        payload.put(
                "nextAlias",
                "o"
        );

        ObjectNode phaseOneResult =
                payload.putObject(
                        "phaseOneResult"
                );

        phaseOneResult
                .putObject(
                        "seenTuplesByAlias"
                )
                .put(
                        "l",
                        1
                );

        phaseOneResult
                .putObject(
                        "edgeIndexes"
                )
                .putObject(
                        "l<->o"
                )
                .put(
                        "100",
                        1.0d
                );

        String reduceKey =
                "123_PHASE1_PHASE1_l_123";

        return new Estimation(
                123,
                reduceKey,
                72,
                30,
                reduceKey,
                payload.toString(),
                new String[] {
                        "LOCAL_PHASE1_RESULT",
                        "PHASE1_l_123",
                        "PHASE1",
                        "l",
                        Integer.toString(
                                workerId
                        ),
                        "4",
                        "START_NEXT_ALIAS",
                        "o"
                },
                4
        );
    }
}