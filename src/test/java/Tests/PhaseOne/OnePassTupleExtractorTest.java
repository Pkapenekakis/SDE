package Tests.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OnePassTupleExtractorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testExtractFromJsonString() {
        String payload = "{\"alias\":\"C\",\"c_key\":\"k1\"}";
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        assertEquals("C", tuple.getTable());
        assertEquals("k1", tuple.getField("c_key").asText());
    }

    @Test
    public void testExtractFromJsonNode() throws Exception {
        JsonNode node = MAPPER.readTree("{\"alias\":\"C\",\"c_key\":\"k1\"}");
        OnePassTuple tuple = OnePassTupleExtractor.extract(node);

        assertEquals("C", tuple.getTable());
    }

    @Test
    public void testExtractFromMap() {
        Map<String, Object> payload = new HashMap<String, Object>();
        payload.put("alias", "C");
        payload.put("c_key", "k1");

        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        assertEquals("C", tuple.getTable());
        assertEquals("k1", tuple.getField("c_key").asText());
    }

    @Test
    public void testAliasPreferredOverTable() {
        String payload = "{\"alias\":\"c1\",\"table\":\"customer\",\"c_key\":\"k1\"}";
        OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

        assertEquals("c1", tuple.getTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingRelationIdFails() {
        String payload = "{\"c_key\":\"k1\"}";
        OnePassTupleExtractor.extract(payload);
    }
}