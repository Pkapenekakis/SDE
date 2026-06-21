package Tests.PhaseOne;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class JoinValueTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testSingleFieldJoinValue() throws Exception {
        OnePassTuple tuple = new OnePassTuple(
                "C",
                MAPPER.readTree("{\"alias\":\"C\",\"c_key\":\"k1\",\"id\":\"x1\"}")
        );

        JoinValue key = JoinValue.fromTuple(tuple, Arrays.asList("c_key"));

        assertEquals(JoinValue.ofSingle("k1"), key);
        assertEquals("k1", key.toString());
    }

    @Test
    public void testCompositeJoinValue() throws Exception {
        OnePassTuple tuple = new OnePassTuple(
                "C",
                MAPPER.readTree("{\"alias\":\"C\",\"c_key\":\"k1\",\"id\":\"x1\"}")
        );

        JoinValue key = JoinValue.fromTuple(tuple, Arrays.asList("c_key", "id"));

        assertEquals("k1|x1", key.toString());
        assertEquals(new JoinValue(Arrays.asList("k1", "x1")), key);
    }

    @Test
    public void testEqualsAndHashCode() {
        JoinValue k1 = new JoinValue(Arrays.asList("a", "b"));
        JoinValue k2 = new JoinValue(Arrays.asList("a", "b"));
        JoinValue k3 = new JoinValue(Arrays.asList("b", "a"));

        assertEquals(k1, k2);
        assertEquals(k1.hashCode(), k2.hashCode());
        assertNotEquals(k1, k3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingFieldFails() throws Exception {
        OnePassTuple tuple = new OnePassTuple(
                "C",
                MAPPER.readTree("{\"alias\":\"C\",\"c_key\":\"k1\"}")
        );

        JoinValue.fromTuple(tuple, Arrays.asList("missing"));
    }
}