package Tests.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.*;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnePassRootSamplerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void createsExplicitSampleInstancesAndAllowsDuplicates() throws Exception {
        OnePassRootSampler sampler =
                new OnePassRootSampler("c", 50, "root-sampler-test");

        JsonNode light =
                MAPPER.readTree("{\"c_custkey\":1,\"name\":\"light\"}");

        JsonNode heavy =
                MAPPER.readTree("{\"c_custkey\":2,\"name\":\"heavy\"}");

        sampler.addRootTuple(light, 1.0d);
        sampler.addRootTuple(heavy, 9.0d);

        OnePassRootSampleResult result = sampler.finish();

        assertEquals("c", result.getRootAlias());
        assertEquals(50, result.getSampleInstances().size());
        assertEquals(2L, result.getRootTuplesSeen());
        assertEquals(2L, result.getPositiveRootCandidatesSeen());
        assertEquals(10.0d, result.getTotalRootGroupWeight(), 0.0d);

        int lightCount = countByCustKey(result.getSampleInstances(), 1);
        int heavyCount = countByCustKey(result.getSampleInstances(), 2);

        assertEquals(50, lightCount + heavyCount);

        assertTrue(
                "Expected heavy root to appear more often",
                heavyCount > lightCount
        );

        assertTrue(
                "Expected duplicates because Phase 2 output is multinomial",
                lightCount > 1 || heavyCount > 1
        );
    }

    @Test
    public void skipsZeroRootGroupWeight() throws Exception {
        OnePassRootSampler sampler =
                new OnePassRootSampler("c", 10, "zero-root-weight-test");

        JsonNode zero =
                MAPPER.readTree("{\"c_custkey\":1}");

        JsonNode positive =
                MAPPER.readTree("{\"c_custkey\":2}");

        sampler.addRootTuple(zero, 0.0d);
        sampler.addRootTuple(positive, 1.0d);

        OnePassRootSampleResult result = sampler.finish();

        assertEquals(2L, result.getRootTuplesSeen());
        assertEquals(1L, result.getPositiveRootCandidatesSeen());
        assertEquals(10, result.getSampleInstances().size());

        for (OnePassRootSampleInstance instance : result.getSampleInstances()) {
            assertEquals(
                    2,
                    instance.getRootTuple().get("c_custkey").asInt()
            );
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeRootGroupWeight() throws Exception {
        OnePassRootSampler sampler =
                new OnePassRootSampler("c", 10, "negative-root-weight-test");

        JsonNode root =
                MAPPER.readTree("{\"c_custkey\":1}");

        sampler.addRootTuple(root, -1.0d);
    }

    private static int countByCustKey(
            List<OnePassRootSampleInstance> instances,
            int custKey) {

        int count = 0;

        for (OnePassRootSampleInstance instance : instances) {
            if (instance.getRootTuple().get("c_custkey").asInt() == custKey) {
                count++;
            }
        }

        return count;
    }
}