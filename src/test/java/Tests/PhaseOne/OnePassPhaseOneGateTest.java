package Tests.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneWorkerProtocol;
import infore.SDE.transformations.onepass.worker.OnePassTupleBufferGate;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class OnePassPhaseOneGateTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldReleaseNextAliasOnlyAfterRequiredStateInstalled()
            throws Exception {

        OnePassPhaseOneWorkerProtocol protocol =
                new OnePassPhaseOneWorkerProtocol();

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(10);

        int uid = 123;

        gate.registerIfAbsent(
                uid,
                "l"
        );

        gate.buffer(
                uid,
                "o",
                tuple(
                        "o",
                        1
                )
        );

        boolean activated =
                protocol.acceptTransition(
                        transition(
                                uid,
                                "START_NEXT_ALIAS",
                                "o",
                                "STATE_l_v1"
                        )
                );

        assertFalse(activated);

        assertNotNull(
                protocol.getPendingTransition(
                        uid
                )
        );

        protocol.markInstalled(
                uid,
                "STATE_l_v1"
        );

        OnePassPhaseOneWorkerProtocol.Transition ready =
                protocol.consumeActiveTransition(
                        uid
                );

        assertNotNull(ready);

        assertEquals(
                "o",
                ready.getNextAlias()
        );

        List<JsonNode> released =
                gate.activateAliasAndDrain(
                        uid,
                        ready.getNextAlias()
                );

        assertEquals(
                1,
                released.size()
        );

        assertEquals(
                "o",
                released.get(0)
                        .get("alias")
                        .asText()
        );

        assertEquals(
                0,
                gate.getBufferedCount(uid)
        );
    }

    private static ObjectNode transition(
            int uid,
            String command,
            String nextAlias,
            String requiredStateRef) {

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put(
                "type",
                command
        );

        payload.put(
                "uid",
                uid
        );

        payload.put(
                "nextAlias",
                nextAlias
        );

        payload.put(
                "requiredStateRef",
                requiredStateRef
        );

        return payload;
    }

    private static ObjectNode tuple(
            String alias,
            int id) {

        ObjectNode tuple =
                MAPPER.createObjectNode();

        tuple.put(
                "alias",
                alias
        );

        tuple.put(
                "id",
                id
        );

        return tuple;
    }
}