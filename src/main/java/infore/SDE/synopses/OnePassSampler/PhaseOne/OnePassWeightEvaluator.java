package infore.SDE.synopses.OnePassSampler.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Takes a OnePassTuple and returns the tuple's base weight according to the
 * weight section of OnePassParams
 *
 * Supported:
 * - null / blank expression => 1.0
 * - numeric literal => that numeric value
 * - direct field reference => tuple[field]
 * - arithmetic JavaScript expression over numeric tuple fields
 */
public class OnePassWeightEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String expression;
    private final List<String> variables;

    public OnePassWeightEvaluator(WeightSpec weightSpec) {
        this.expression = weightSpec == null ? null : weightSpec.getExpression();
        this.variables = new ArrayList<String>();

        if (weightSpec != null && weightSpec.getVariables() != null) {
            for (Object v : weightSpec.getVariables()) {
                variables.add(String.valueOf(v));
            }
        }
    }

    public double evaluate(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (expression == null || expression.trim().isEmpty()) {
            return 1.0d;
        }

        String expr = expression.trim();

        Double literal = tryParseDouble(expr);
        if (literal != null) {
            return literal;
        }

        if (tuple.hasField(expr)) {
            JsonNode direct = tuple.getField(expr);
            if (direct == null || direct.isNull() || !direct.isNumber()) {
                throw new IllegalArgumentException(
                        "Weight expression refers to non-numeric field '" + expr + "'");
            }
            return direct.asDouble();
        }

        ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
        if (engine == null) {
            throw new IllegalStateException(
                    "No JavaScript engine available to evaluate weight expression: " + expression);
        }

        try {
            Bindings bindings = engine.createBindings();

            for (Object rawEntryObj : tuple.getFields().entrySet()) {
                Map.Entry rawEntry = (Map.Entry) rawEntryObj;
                String fieldName = String.valueOf(rawEntry.getKey());
                JsonNode valueNode = (JsonNode) rawEntry.getValue();
                if (valueNode != null && valueNode.isNumber()) {
                    bindings.put(fieldName, valueNode.asDouble());
                }
            }

            for (String variable : variables) {
                if (!bindings.containsKey(variable) && tuple.hasField(variable)) {
                    JsonNode valueNode = tuple.getField(variable);
                    if (valueNode != null && valueNode.isNumber()) {
                        bindings.put(variable, valueNode.asDouble());
                    }
                }
            }

            Object result = engine.eval(expr, bindings);
            if (!(result instanceof Number)) {
                throw new IllegalArgumentException(
                        "Weight expression did not evaluate to a number: " + expression);
            }
            return ((Number) result).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to evaluate weight expression '" + expression + "' on tuple " + tuple, e);
        }
    }

    private Double tryParseDouble(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception ignore) {
            return null;
        }
    }
}