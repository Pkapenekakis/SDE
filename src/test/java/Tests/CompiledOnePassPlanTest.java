package Tests;

import infore.SDE.synopses.OnePassSampler.PhaseOne.TestOnePassFixtures;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class CompiledOnePassPlanTest {

    @Test
    public void testCompileSimpleChain() {
        OnePassParams params = TestOnePassFixtures.simpleABCDPlan();
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

        assertEquals("A", plan.getRootAlias());
        assertEquals(Arrays.asList("D", "C", "B"), plan.getLeafToRootOrder());
        assertEquals(Arrays.asList("A", "B", "C", "D"), plan.getRootToLeafOrder());
        assertEquals("B", plan.getParentAlias("C"));
        assertEquals("C", plan.getParentAlias("D"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCycleRejected() {
        OnePassParams params = TestOnePassFixtures.cyclicABCPlan();
        CompiledOnePassPlan.from(params);
    }
}