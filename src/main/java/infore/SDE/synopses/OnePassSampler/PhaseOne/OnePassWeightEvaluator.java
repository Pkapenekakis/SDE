package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.messages.Onepass.WeightSpec;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.TupleWeightExpressionEvaluator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates the base weight of one OnePassTuple.
 *
 * Important:
 * The One-pass* algorithm requires the final joined-row weight to be
 * factorizable into base tuple weights:
 *
 *     w(join result) =
 *         w(tuple from alias A)
 *       * w(tuple from alias B)
 *       * ...
 *
 * Therefore, this evaluator receives one tuple at a time and evaluates only
 * the weight expression that belongs to that tuple's alias.
 *
 * Example SQL:
 *
 *     WEIGHTED BY (
 *         (l1.l_extendedprice * (1 - l1.l_discount))
 *         * o1.o_totalprice
 *         * (l2.l_extendedprice * (1 - l2.l_discount))
 *         * o2.o_totalprice
 *     )
 *
 * should be compiled into WeightSpec.weightsByAlias:
 *
 *     l1 -> l_extendedprice * (1 - l_discount)
 *     o1 -> o_totalprice
 *     l2 -> l_extendedprice * (1 - l_discount)
 *     o2 -> o_totalprice
 *
 * Then this evaluator evaluates only the local expression for the incoming
 * tuple alias.
 */
public class OnePassWeightEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String globalExpression;
    private final List<String> variables;
    private final Map<String, String> weightsByAlias;

    public OnePassWeightEvaluator(WeightSpec weightSpec) {
        this.globalExpression =
                weightSpec == null ? null : weightSpec.getExpression();

        this.variables = new ArrayList<String>();
        this.weightsByAlias = new LinkedHashMap<String, String>();

        if (weightSpec != null && weightSpec.getVariables() != null) {
            for (Object variable : weightSpec.getVariables()) {
                if (variable != null) {
                    variables.add(String.valueOf(variable));
                }
            }
        }

        if (weightSpec != null && weightSpec.getWeightsByAlias() != null) {
            for (Map.Entry<String, String> entry
                    : weightSpec.getWeightsByAlias().entrySet()) {

                if (entry.getKey() != null && entry.getValue() != null) {
                    String alias = entry.getKey().trim();
                    String expression = entry.getValue().trim();

                    if (!alias.isEmpty() && !expression.isEmpty()) {
                        weightsByAlias.put(alias, expression);
                    }
                }
            }
        }
    }

    public double evaluate(OnePassTuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        String expression = expressionForTuple(tuple);

        if (expression == null || expression.trim().isEmpty()) {
            return 1.0d;
        }

        /*
         * The WEIGHTED BY parser should already convert:
         *
         *     l1.l_extendedprice * (1 - l1.l_discount)
         *
         * into:
         *
         *     l_extendedprice * (1 - l_discount)
         *
         * for alias l1.
         *
         * However, keeping this normalization makes the evaluator robust if
         * catalog defaults or tests still use alias-qualified expressions.
         */
        String localExpression =
                normalizeExpressionForTuple(
                        expression.trim(),
                        tuple.getTable()
                );

        double value =
                TupleWeightExpressionEvaluator.evaluate(
                        localExpression,
                        tuple.getRawJson()
                );

        validateWeight(value, tuple, localExpression);

        return value;
    }

    private String expressionForTuple(OnePassTuple tuple) {
        String alias = tuple.getTable();

        if (alias != null && weightsByAlias.containsKey(alias)) {
            return weightsByAlias.get(alias);
        }

        /*
         * Backward compatibility:
         * If no alias-specific expression exists, use the old global expression.
         *
         * In the new WEIGHTED BY design, this should usually be empty/null,
         * because weights should come from weightsByAlias.
         */
        if (globalExpression != null && !globalExpression.trim().isEmpty()) {
            return globalExpression;
        }

        return "1";
    }

    private static void validateWeight(
            double value,
            OnePassTuple tuple,
            String expression) {

        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException(
                    "Weight expression produced invalid value for alias '"
                            + tuple.getTable()
                            + "': expression="
                            + expression
                            + ", value="
                            + value
            );
        }

        if (value < 0.0d) {
            throw new IllegalArgumentException(
                    "Weight expression produced negative value for alias '"
                            + tuple.getTable()
                            + "': expression="
                            + expression
                            + ", value="
                            + value
            );
        }
    }

    private String normalizeExpressionForTuple(String expression, String alias) {
        if (expression == null || alias == null || alias.trim().isEmpty()) {
            return expression;
        }

        /*
         * Remove only the current tuple alias prefix.
         *
         * Example:
         *   alias = l1
         *   l1.l_extendedprice * (1 - l1.l_discount)
         *
         * becomes:
         *   l_extendedprice * (1 - l_discount)
         */
        return expression.replaceAll(
                "\\b" + java.util.regex.Pattern.quote(alias) + "\\.",
                ""
        );
    }

    public Map<String, String> getWeightsByAliasForDebug() {
        return new LinkedHashMap<String, String>(weightsByAlias);
    }

    public String getGlobalExpressionForDebug() {
        return globalExpression;
    }

    public List<String> getVariablesForDebug() {
        return new ArrayList<String>(variables);
    }
}