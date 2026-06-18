package infore.SDE.messages.Onepass;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WeightSpec {

    private String expression; // global fallback expression
    private List<String> variables = new ArrayList<String>();

    /*
     * Optional alias-specific tuple weights.
     *
     * Example:
     * {
     *   "customer": "1",
     *   "orders": "o_totalprice",
     *   "lineitem": "l_extendedprice"
     * }
     *
     * If this map contains the tuple alias, OnePassWeightEvaluator uses that
     * expression instead of the global expression.
     */
    private Map<String, String> weightsByAlias = new LinkedHashMap<String, String>();

    public WeightSpec() {
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public List<String> getVariables() {
        return variables;
    }

    public void setVariables(List<String> variables) {
        this.variables = variables;
    }

    public Map<String, String> getWeightsByAlias() {
        return weightsByAlias;
    }

    public void setWeightsByAlias(Map<String, String> weightsByAlias) {
        this.weightsByAlias = weightsByAlias;
    }
}