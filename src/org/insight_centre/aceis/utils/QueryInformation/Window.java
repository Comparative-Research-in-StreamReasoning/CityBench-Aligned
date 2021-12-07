package org.insight_centre.aceis.utils.QueryInformation;

public abstract class Window {
    String streamURL;

    public Window(String streamURL) {
        this.streamURL = streamURL;
    }

    public String getStreamURL() {
        return streamURL;
    }
}
