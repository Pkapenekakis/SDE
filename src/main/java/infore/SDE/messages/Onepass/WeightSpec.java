package infore.SDE.messages.Onepass;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WeightSpec {

    private String expression; // e.g. "((e1*(1-d1))*t1*(e2*(1-d2))*t2)"
    private List<String> variables = new ArrayList<>(); // Variables used in the above expression

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
}