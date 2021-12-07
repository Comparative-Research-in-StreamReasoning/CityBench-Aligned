package org.insight_centre.aceis.io.answers;

import java.util.List;

public class VariableBindings {
    private List<String> variableValuesAsString;

    public List<String> getVariableValuesAsString() {
        return variableValuesAsString;
    }

    public VariableBindings(List<String> variableValuesAsString) {
        this.variableValuesAsString = variableValuesAsString;
    }
}
