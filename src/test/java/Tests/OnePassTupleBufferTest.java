package Tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.transformations.onepass.worker.OnePassTupleBufferGate;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OnePassTupleBufferTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldAllowFirstAliasAndBufferNextAlias()
            throws Exception {

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(10);

        gate.registerIfAbsent(
                123,
                "l"
        );

        assertEquals(
                "l",
                gate.getAllowedAlias(123)
        );

        assertTrue(
                gate.isAllowed(
                        123,
                        "l"
                )
        );

        assertFalse(
                gate.isAllowed(
                        123,
                        "o"
                )
        );

        gate.buffer(
                123,
                "o",
                tuple(
                        "o",
                        1
                )
        );

        gate.buffer(
                123,
                "o",
                tuple(
                        "o",
                        2
                )
        );

        assertEquals(
                2,
                gate.getBufferedCount(123)
        );

        List<JsonNode> released =
                gate.activateAliasAndDrain(
                        123,
                        "o"
                );

        assertEquals(
                2,
                released.size()
        );

        assertEquals(
                1,
                released.get(0)
                        .get("id")
                        .asInt()
        );

        assertEquals(
                2,
                released.get(1)
                        .get("id")
                        .asInt()
        );

        assertEquals(
                0,
                gate.getBufferedCount(123)
        );
    }

    @Test
    public void shouldKeepLaterAliasBuffered()
            throws Exception {

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(10);

        gate.registerIfAbsent(
                123,
                "l"
        );

        gate.buffer(
                123,
                "o",
                tuple(
                        "o",
                        10
                )
        );

        gate.buffer(
                123,
                "c",
                tuple(
                        "c",
                        20
                )
        );

        List<JsonNode> orders =
                gate.activateAliasAndDrain(
                        123,
                        "o"
                );

        assertEquals(
                1,
                orders.size()
        );

        assertEquals(
                1,
                gate.getBufferedCount(123)
        );

        List<JsonNode> customer =
                gate.activateAliasAndDrain(
                        123,
                        "c"
                );

        assertEquals(
                1,
                customer.size()
        );

        assertEquals(
                0,
                gate.getBufferedCount(123)
        );
    }

    @Test
    public void repeatedRegistrationMustNotResetAlias()
            throws Exception {

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(10);

        gate.registerIfAbsent(
                123,
                "l"
        );

        gate.activateAliasAndDrain(
                123,
                "o"
        );

        gate.registerIfAbsent(
                123,
                "l"
        );

        assertEquals(
                "o",
                gate.getAllowedAlias(123)
        );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailWhenBoundIsExceeded()
            throws Exception {

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(2);

        gate.registerIfAbsent(
                123,
                "l"
        );

        gate.buffer(
                123,
                "o",
                tuple(
                        "o",
                        1
                )
        );

        gate.buffer(
                123,
                "o",
                tuple(
                        "o",
                        2
                )
        );

        gate.buffer(
                123,
                "o",
                tuple(
                        "o",
                        3
                )
        );
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

    @Test
    public void shouldSealAliasAfterEndAlias() {

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(10);

        gate.registerIfAbsent(
                123,
                "l"
        );

        assertTrue(
                gate.isAllowed(
                        123,
                        "l"
                )
        );

        gate.sealAlias(
                123,
                "l"
        );

        assertTrue(
                gate.isSealed(
                        123,
                        "l"
                )
        );

        assertFalse(
                gate.isAllowed(
                        123,
                        "l"
                )
        );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRejectTupleForSealedAlias() {

        OnePassTupleBufferGate gate =
                new OnePassTupleBufferGate(10);

        gate.registerIfAbsent(
                123,
                "l"
        );

        gate.sealAlias(
                123,
                "l"
        );

        ObjectNode lateTuple =
                MAPPER.createObjectNode();

        lateTuple.put(
                "alias",
                "l"
        );

        lateTuple.put(
                "id",
                999
        );

        gate.buffer(
                123,
                "l",
                lateTuple
        );
    }
}