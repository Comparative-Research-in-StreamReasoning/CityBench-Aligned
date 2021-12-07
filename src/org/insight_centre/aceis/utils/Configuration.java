package org.insight_centre.aceis.utils;

import org.insight_centre.aceis.utils.QueryInformation.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Configuration {
    Map<String, String> queries;
    List<Query> queriesUnsupported;

    public Map<String, String> getQueries() {
        return queries;
    }

    public List<Query> getQueriesUnsupported() {
        return queriesUnsupported;
    }

    public Configuration(List<Query> unsupportedQueries, Map<String, String> queries) {
        this.queriesUnsupported = unsupportedQueries;
        this.queries = queries;
    }
}
