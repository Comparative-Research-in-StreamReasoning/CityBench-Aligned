package org.insight_centre.aceis.utils.QueryInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Query {
    public String queryId;
    public OutputOperator outputOperator;
    public String staticSparqlQuery;
    public List<String> staticKnowledge;
    public List<Window> windows;
    public List<String> windowSparqlQueries;

    public Query(String qStr, String qId) throws Exception {
        String[] lines = qStr.split("\n");
        queryId = qId;
        outputOperator = getOutputOperator(lines);
        staticSparqlQuery = getStaticSparqlQuery(lines);
        staticKnowledge = getStaticKnowledge(lines);
        windows = getWindows(lines);
        windowSparqlQueries = getWindowSparqlQueries(lines);
    }

    public Query(String queryId, OutputOperator outputOperator, String staticSparqlQuery, List<String> staticKnowledge, List<Window> windows, List<String> windowSparqlQueries) {
        this.queryId = queryId;
        this.outputOperator = outputOperator;
        this.staticSparqlQuery = staticSparqlQuery;
        this.staticKnowledge = staticKnowledge;
        this.windows = windows;
        this.windowSparqlQueries = windowSparqlQueries;
    }

    private List<String> getStaticKnowledge(String[] lines) {
        List<String> re = new ArrayList<>();
        for(String line : lines) {
            if(line.startsWith("FROM static")) {
                re.add(line.split("<")[1].split(">")[0]);
            }
        }
        return re;
    }
    private List<Window> getWindows(String[] lines) throws Exception {
        List<Window> re = new ArrayList<>();
        for(String line : lines) {
            if(line.startsWith("FROM dynamic stream")) {
                if(line.contains("triples")) {
                    Window w = new PhysicalWindow(line.split("<")[1].split(">")[0],
                            Integer.parseInt(line.split("triples ")[1].split("]")[0]));
                    re.add(w);
                }
                else if(line.contains("range")) {
                    Window w = new LogicalWindow(line.split("<")[1].split(">")[0],
                            Integer.parseInt(line.split("range ")[1].split("ms")[0]),
                            Integer.parseInt(line.split("step ")[1].split("ms")[0]));
                    re.add(w);
                }
                else {
                    throw new Exception("Unappropriate query format");
                }
            }
        }
        return re;
    }
    private OutputOperator getOutputOperator(String[] lines) throws Exception {
        for(String line : lines) {
            if(line.contains("RStream"))
                return OutputOperator.RStream;
            if(line.contains("IStream"))
                return OutputOperator.IStream;
            if(line.contains("DStream"))
                return OutputOperator.DStream;
        }
        throw new Exception("OutputOperator misssing");
    }
    private String getStaticSparqlQuery(String[] lines) {
        StringBuilder re = new StringBuilder();
        boolean readingQuery = false;
        for (String line : lines) {
            if (line.contains("QUERY:")) {
                readingQuery = true;
            }
            else if (line.contains("WINDOW:")) {
                readingQuery = false;
            }
            else if (readingQuery) {
                re.append(line+"\n");
            }
        }
        return re.toString();
    }
    private List<String> getWindowSparqlQueries(String[] lines) {
        List<String> re = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        boolean readingWindowQuery = false;
        for (String line : lines) {
            if (line.contains("WINDOW:")) {
                readingWindowQuery = true;
                if(!stringBuilder.toString().equals("")) {
                    re.add(stringBuilder.toString());
                    stringBuilder = new StringBuilder();
                }
            }
            else if (readingWindowQuery) {
                stringBuilder.append(line+"\n");
            }
        }
        re.add(stringBuilder.toString());
        return re;
    }
}
