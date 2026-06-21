package Tests.PhaseOne;

import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOne;
import infore.SDE.synopses.OnePassSampler.PhaseOne.TestOnePassFixtures;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class OnePassPhaseOneTest {

    @Test
    public void testAddAndEstimate() {
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(TestOnePassFixtures.simpleABCDPlan());
        OnePassPhaseOne synopsis = new OnePassPhaseOne(1, plan, TestOnePassFixtures.simpleABCDPlan().getWeight());

        synopsis.add("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":5}");
        synopsis.add("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":8}");

        Object result = synopsis.estimate(null);
        assertTrue(result instanceof Map);

        Map snapshot = (Map) result;
        assertTrue(snapshot.containsKey("edgeIndexes"));
    }

    @Test
    public void testMerge() {
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(TestOnePassFixtures.simpleABCDPlan());
        OnePassPhaseOne left = new OnePassPhaseOne(1, plan, TestOnePassFixtures.simpleABCDPlan().getWeight());
        OnePassPhaseOne right = new OnePassPhaseOne(2, plan, TestOnePassFixtures.simpleABCDPlan().getWeight());

        left.add("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":5}");
        right.add("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":8}");

        left.merge(right);

        Map snapshot = (Map) left.estimate(null);
        Map edgeIndexes = (Map) snapshot.get("edgeIndexes");
        Map cd = (Map) edgeIndexes.get("C<->D");

        assertEquals(13.0, ((Number) cd.get("k1")).doubleValue(), 1e-9);
    }
}