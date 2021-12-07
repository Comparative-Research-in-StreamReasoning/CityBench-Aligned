package org.insight_centre.aceis.utils.QueryInformation;

public class LogicalWindow extends Window{
    int size;
    int stepSize;

    public LogicalWindow(String streamURL, int size, int stepSize) {
        super(streamURL);
        this.size = size;
        this.stepSize = stepSize;
    }
}
