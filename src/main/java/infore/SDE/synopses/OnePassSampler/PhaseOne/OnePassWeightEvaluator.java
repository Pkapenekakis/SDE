package infore.SDE.synopses.OnePassSampler.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Takes a OnePassTuple and returns the tuple's base weight according to the
 * weight section of OnePassParams.
 *
 * Supported:
 * - null / blank expression => 1.0
 * - numeric literal => that numeric value
 * - direct field reference => tuple[field]
 * - arithmetic JavaScript expression over numeric tuple fields
 * - alias-specific weights through WeightSpec.weightsByAlias
 */
public class OnePassWeightEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String expression;
    private final List<String> variables;
    private final Map<String, String> weightsByAlias;

    public OnePassWeightEvaluator(WeightSpec weightSpec) {
        this.expression = weightSpec == null ? null : weightSpec.getExpression();
        this.variables = new ArrayList<String>();
        this.weightsByAlias = new LinkedHashMap<String, String>();

        if (weightSpec != null && weightSpec.getVariables() != null) {
            for (Object v : weightSpec.getVariables()) {
                variables.add(String.valueOf(v));
            }
        }

        if (weightSpec != null && weightSpec.getWeightsByAlias() != null) {
            for (Map.Entry<String, String> entry : weightSpec.getWeightsByAlias().entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    weightsByAlias.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    public double evaluate(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        String expr = expressionForTuple(tuple);

        if (expr == null || expr.trim().isEmpty()) {
            return 1.0d;
        }

        expr = normalizeExpressionForTuple(expr.trim(), tuple.getTable());

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
                    "No JavaScript engine available to evaluate weight expression: " + expr);
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
                String normalizedVariable = normalizeExpressionForTuple(variable, tuple.getTable());

                if (!bindings.containsKey(normalizedVariable) && tuple.hasField(normalizedVariable)) {
                    JsonNode valueNode = tuple.getField(normalizedVariable);

                    if (valueNode != null && valueNode.isNumber()) {
                        bindings.put(normalizedVariable, valueNode.asDouble());
                    }
                }
            }

            Object result = engine.eval(expr, bindings);

            if (!(result instanceof Number)) {
                throw new IllegalArgumentException(
                        "Weight expression did not evaluate to a number: " + expr);
            }

            return ((Number) result).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to evaluate weight expression '" + expr + "' on tuple " + tuple, e);
        }
    }

    private String expressionForTuple(OnePassTuple tuple) {
        String alias = tuple.getTable();

        if (alias != null && weightsByAlias.containsKey(alias)) {
            return weightsByAlias.get(alias);
        }

        return expression;
    }

    private String normalizeExpressionForTuple(String expr, String alias) {
        if (expr == null || alias == null || alias.trim().isEmpty()) {
            return expr;
        }

        /*
         * Allows both:
         *   l_extendedprice
         * and:
         *   lineitem.l_extendedprice
         *
         * For the current tuple alias, remove "alias." so JavaScript can bind the
         * local field name.
         */
        return expr.replaceAll("\\b" + Pattern.quote(alias) + "\\.", "");
    }

    private Double tryParseDouble(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception ignore) {
            return null;
        }
    }
}