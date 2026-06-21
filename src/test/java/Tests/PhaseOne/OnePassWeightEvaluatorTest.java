package Tests.PhaseOne;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassWeightEvaluator;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class OnePassWeightEvaluatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testBlankExpressionDefaultsToOne() throws Exception {
        WeightSpec spec = new WeightSpec();
        spec.setExpression("");

        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(spec);
        OnePassTuple tuple = new OnePassTuple("B", MAPPER.readTree("{\"alias\":\"B\",\"x\":10}"));

        assertEquals(1.0, evaluator.evaluate(tuple), 1e-9);
    }

    @Test
    public void testNumericLiteral() throws Exception {
        WeightSpec spec = new WeightSpec();
        spec.setExpression("5");

        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(spec);
        OnePassTuple tuple = new OnePassTuple("B", MAPPER.readTree("{\"alias\":\"B\",\"x\":10}"));

        assertEquals(5.0, evaluator.evaluate(tuple), 1e-9);
    }

    @Test
    public void testDirectFieldReference() throws Exception {
        WeightSpec spec = new WeightSpec();
        spec.setExpression("price");

        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(spec);
        OnePassTuple tuple = new OnePassTuple("B", MAPPER.readTree("{\"alias\":\"B\",\"price\":12.5}"));

        assertEquals(12.5, evaluator.evaluate(tuple), 1e-9);
    }

    @Test
    public void testArithmeticExpression() throws Exception {
        WeightSpec spec = new WeightSpec();
        spec.setExpression("price * (1 - discount)");
        spec.setVariables(Arrays.asList("price", "discount"));

        OnePassWeightEvaluator evaluator = new OnePassWeightEvaluator(spec);
        OnePassTuple tuple = new OnePassTuple(
                "B",
                MAPPER.readTree("{\"alias\":\"B\",\"price\":100.0,\"discount\":0.2}")
        );

        assertEquals(80.0, evaluator.evaluate(tuple), 1e-9);
    }
}