package org.insight_centre.aceis.io.answers;

import java.util.List;

public class TransferObject {
    private String queryId;
    private List<VariableBindings> variableBindings;
    private Double memoryConsumptionInMB;
    private Long timestamp;

    public TransferObject(String queryId, List<VariableBindings> timestampedVariableBindings, Double memoryConsumptionInMB, Long timestamp) {
        this.queryId = queryId;
        this.variableBindings = timestampedVariableBindings;
        this.memoryConsumptionInMB = memoryConsumptionInMB;
        this.timestamp = timestamp;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<VariableBindings> getTimestampedVariableBindings() {
        return variableBindings;
    }

    public Double getMemoryConsumptionInMB() {
        return memoryConsumptionInMB;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
