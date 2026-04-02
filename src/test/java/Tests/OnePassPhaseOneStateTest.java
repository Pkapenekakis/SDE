package Tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.*;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class OnePassPhaseOneStateTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testLeafTuplePushesOwnWeight() throws Exception {
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(TestOnePassFixtures.simpleABCDPlan());
        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(TestOnePassFixtures.simpleABCDPlan().getWeight());
        OnePassPhaseOneState state = new OnePassPhaseOneState(plan, evaluator);

        OnePassTuple d1 = new OnePassTuple(
                "D",
                MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":5}")
        );

        state.addTuple(d1);

        Phase1LinkWeightIndex cd = state.getIndex("C<->D");
        assertEquals(5.0, cd.getOrZero(new JoinValue(Arrays.asList("k1"))), 1e-9);
    }

    @Test
    public void testParentTupleUsesChildIndex() throws Exception {
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(TestOnePassFixtures.simpleABCDPlan());
        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(TestOnePassFixtures.simpleABCDPlan().getWeight());
        OnePassPhaseOneState state = new OnePassPhaseOneState(plan, evaluator);

        state.addTuple(new OnePassTuple("D", MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":5}")));
        state.addTuple(new OnePassTuple("D", MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":8}")));
        state.addTuple(new OnePassTuple("D", MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k2\",\"weight\":1}")));

        state.addTuple(new OnePassTuple("C", MAPPER.readTree(
                "{\"alias\":\"C\",\"bc_key\":\"x1\",\"cd_key\":\"k1\",\"weight\":2}"
        )));
        state.addTuple(new OnePassTuple("C", MAPPER.readTree(
                "{\"alias\":\"C\",\"bc_key\":\"x1\",\"cd_key\":\"k2\",\"weight\":4}"
        )));

        Phase1LinkWeightIndex bc = state.getIndex("B<->C");

        assertEquals(30.0, bc.getOrZero(new JoinValue(Arrays.asList("x1"))), 1e-9);
    }

    @Test
    public void testRootTupleIgnored() throws Exception {
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(TestOnePassFixtures.simpleABCDPlan());
        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(TestOnePassFixtures.simpleABCDPlan().getWeight());
        OnePassPhaseOneState state = new OnePassPhaseOneState(plan, evaluator);

        state.addTuple(new OnePassTuple(
                "A",
                MAPPER.readTree("{\"alias\":\"A\",\"ab_key\":\"r1\",\"weight\":99}")
        ));

        Phase1LinkWeightIndex ab = state.getIndex("A<->B");
        assertEquals(0.0, ab.getOrZero(new JoinValue(Arrays.asList("r1"))), 1e-9);
    }

    @Test
    public void testBLevelAggregation() throws Exception {
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(TestOnePassFixtures.simpleABCDPlan());
        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(TestOnePassFixtures.simpleABCDPlan().getWeight());
        OnePassPhaseOneState state = new OnePassPhaseOneState(plan, evaluator);

        state.addTuple(new OnePassTuple("D", MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":5}")));
        state.addTuple(new OnePassTuple("D", MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k1\",\"weight\":8}")));
        state.addTuple(new OnePassTuple("D", MAPPER.readTree("{\"alias\":\"D\",\"cd_key\":\"k2\",\"weight\":1}")));

        state.addTuple(new OnePassTuple("C", MAPPER.readTree(
                "{\"alias\":\"C\",\"bc_key\":\"m1\",\"cd_key\":\"k1\",\"weight\":2}"
        )));
        state.addTuple(new OnePassTuple("C", MAPPER.readTree(
                "{\"alias\":\"C\",\"bc_key\":\"m2\",\"cd_key\":\"k2\",\"weight\":4}"
        )));

        state.addTuple(new OnePassTuple("B", MAPPER.readTree(
                "{\"alias\":\"B\",\"ab_key\":\"root1\",\"bc_key\":\"m1\",\"weight\":3}"
        )));
        state.addTuple(new OnePassTuple("B", MAPPER.readTree(
                "{\"alias\":\"B\",\"ab_key\":\"root1\",\"bc_key\":\"m2\",\"weight\":5}"
        )));

        Phase1LinkWeightIndex ab = state.getIndex("A<->B");

        assertEquals(98.0, ab.getOrZero(new JoinValue(Arrays.asList("root1"))), 1e-9);
    }
}