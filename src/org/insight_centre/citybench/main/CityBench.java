package org.insight_centre.citybench.main;

import com.google.gson.Gson;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import org.deri.cqels.engine.ExecContext;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.*;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.utils.Configuration;
import org.insight_centre.aceis.io.streams.DataStream;
import org.insight_centre.aceis.utils.QueryInformation.Query;
import org.insight_centre.aceis.utils.QueryInformation.Window;
import org.insight_centre.aceis.utils.server.FileServer;
import org.insight_centre.aceis.utils.test.PerformanceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class CityBench {
	public enum QueryLanguage {
		cqels, csparql, rspql, unsupported
	}
	public enum RDFFormat {
		ntriples, turtle, rdfxml
	}
	public enum AnswerFormat {
		json
	}

	public static ExecContext tempContext;
	private static final Logger logger = LoggerFactory.getLogger(CityBench.class);
	public static Map<String, SensorObservation> obMap = new ConcurrentHashMap<String, SensorObservation>();
	public static List<DataStream> socketDataStreams = new ArrayList<>();
	public static List<FileServer> staticFileServers = new ArrayList<>();
	private Map<String, DataStream> nameToStreamMap = new HashMap<>();
	public static long t0;

	// HashMap<String, String> parameters;
	// Properties prop;

	public static void main(String[] args) {
		try {
			Properties prop = new Properties();
			// logger.info(Main.class.getClassLoader().);
			File in = new File("citybench.properties");
			FileInputStream fis = new FileInputStream(in);
			prop.load(fis);
			fis.close();
			// Thread.
			HashMap<String, String> parameters = new HashMap<String, String>();
			for (String s : args) {
				parameters.put(s.split("=")[0], s.split("=")[1]);
			}
			CityBench cb = new CityBench(prop, parameters);

			cb.prepareStaticData();
			cb.prepareStreams();
			cb.initializePerformanceMonitor();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

	}

	private String dataset, ontology, cqels_query, csparql_query, rspql_query, unsupported_query, streams;
	private String configPath = "http://localhost:11111/configuration.json";
	private String answerPath = "http://localhost:11112/answers.json";
	private long duration = 0; // experiment time in milliseconds
	private QueryLanguage queryLanguage;
	private RDFFormat rdfFormat;
	EventRepository er;
	private double frequency = 1.0;
	public static PerformanceMonitor pm;
	private List<String> queries;
	int queryDuplicates = 1;
	private Map<String, String> queryMap = new HashMap<>();
	private AnswerFormat answerFormat = AnswerFormat.json;

	// public Map<String, String> getQueryMap() {
	// return queryMap;
	// }

	private double rate = 1.0; // stream rate factor
	private int waitingTime = 10000;
	private String engineName;
	public static List startedStreamObjects = new ArrayList();
	private String resultName;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private Date start, end;
	private Set<String> startedStreams = new HashSet<String>();
	private List<String> registeredIDs = new ArrayList<>();
	private int duplicityCounter = 1;

	// private double rate,frequency
	// DatasetTDB
	/**
	 * @param prop
	 * @param parameters
	 *            Acceptable params: rates=(double)x, queryDuplicates=(int)y, duration=(long)z, startDate=(date in the
	 *            format of "yyyy-MM-dd'T'HH:mm:ss")a, endDate=b, frequency=(double)c. Start and end dates are
	 *            mandatory.
	 * @throws Exception
	 */
	public CityBench(Properties prop, HashMap<String, String> parameters) throws Exception {
		// parse configuration file
		// this.parameters = parameters;
		try {
			this.dataset = prop.getProperty("dataset");
			this.ontology = prop.getProperty("ontology");
			this.cqels_query = prop.getProperty("cqels_query");
			this.csparql_query = prop.getProperty("csparql_query");
			this.rspql_query = prop.getProperty("rspql_query");
			this.unsupported_query = prop.getProperty("unsupported_query");
			this.streams = prop.getProperty("streams");
			if (this.dataset == null || this.ontology == null || this.cqels_query == null || this.csparql_query == null
					|| this.rspql_query == null || this.streams == null)
				throw new Exception("Configuration properties incomplete.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		// parse parameters
		if (parameters.containsKey("query")) {
			this.queries = Arrays.asList(parameters.get("query").split(","));
		}
		if (parameters.get("queryDuplicates") != null) {
			queryDuplicates = Integer.parseInt(parameters.get("queryDuplicates"));
		}
		if (parameters.containsKey("queryLanguage")) {
			if (parameters.get("queryLanguage").equals("cqels"))
				this.queryLanguage = QueryLanguage.cqels;
			else if (parameters.get("queryLanguage").equals("csparql"))
				this.queryLanguage = QueryLanguage.csparql;
			else if (parameters.get("queryLanguage").equals("rspql"))
				this.queryLanguage = QueryLanguage.rspql;
			else {
				throw new Exception("Query language not supported");
			}
		} else
			this.queryLanguage = QueryLanguage.unsupported;

		if (parameters.containsKey("rate")) {
			this.rate = Double.parseDouble(parameters.get("rate"));
		}
		if (parameters.containsKey("duration")) {
			String durationStr = parameters.get("duration");
			String valueStr = durationStr.substring(0, durationStr.length() - 1);
			if (durationStr.contains("s"))
				duration = Integer.parseInt(valueStr) * 1000L;
			else if (durationStr.contains("m"))
				duration = Integer.parseInt(valueStr) * 60000L;
			else
				throw new Exception("Duration specification invalid.");
		}
		if (parameters.containsKey("queryDuplicates"))
			this.queryDuplicates = Integer.parseInt(parameters.get("queryDuplicates"));
		if (parameters.containsKey("startDate"))
			this.start = sdf.parse(parameters.get("startDate"));
		else
			throw new Exception("Start date not specified");
		if (parameters.containsKey("endDate"))
			this.end = sdf.parse(parameters.get("endDate"));
		else
			throw new Exception("End date not specified");
		if (parameters.containsKey("frequency"))
			this.frequency = Double.parseDouble(parameters.get("frequency"));

		if (parameters.containsKey("rdfFormat")) {
			if (parameters.get("rdfFormat").equals("ntriples"))
				this.rdfFormat = RDFFormat.ntriples;
			else if (parameters.get("rdfFormat").equals("turtle"))
				this.rdfFormat = RDFFormat.turtle;
			else if (parameters.get("rdfFormat").equals("rdfxml"))
				this.rdfFormat = RDFFormat.rdfxml;
			else {
				throw new Exception("RDF format not supported");
			}
		} else
			this.rdfFormat = RDFFormat.turtle;

		if (parameters.containsKey("waitingTime")) {
			this.waitingTime = Integer.parseInt(parameters.get("waitingTime"));
		}

		if (parameters.containsKey("configPath")) {
			this.configPath = parameters.get("configPath");
		}

		if (parameters.containsKey("answerPath")) {
			this.answerPath = parameters.get("answerPath");
		}

		if (parameters.containsKey("engineName")) {
			this.engineName = parameters.get("engineName");
		}
		else {
			throw new Exception("Engine name not specified");
		}

		logger.info("Parameters loaded: queryLanguage - " + this.queryLanguage + ", queries - " + this.queries
				+ ", rdfFormat - " + this.rdfFormat + ", answerFormat - " + this.answerFormat + ", configPath - " + this.configPath
				+ ", rate - " + this.rate + ", frequency - " + this.frequency + ", duration - " + this.duration + ", duplicates - "
				+ this.queryDuplicates + ", start - " + this.start + ", end - " + this.end);

		this.resultName = engineName + "_CityBench_" + "q=" + this.queries + " r=" + this.rate + ",f=" + this.frequency + ",dup="
				+ this.queryDuplicates + "_" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) ;// +
		// parameters.toString();
		// initialize datasets
		try {
			tempContext = RDFFileManager.initializeCQELSContext(this.dataset, ReasonerRegistry.getRDFSReasoner());
			er = RDFFileManager.buildRepoFromFile(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void loadQueries() throws Exception {
		String qd;
		if (this.queryLanguage == QueryLanguage.cqels)
			qd = this.cqels_query;
		else if (this.queryLanguage == QueryLanguage.csparql)
			qd = this.csparql_query;
		else if(this.queryLanguage == QueryLanguage.rspql)
			qd = this.rspql_query;
		else
			qd = this.unsupported_query;
		if (this.queries == null) {
			File queryDirectory = new File(qd);
			if (!queryDirectory.exists())
				throw new Exception("Query directory not exist. " + qd);
			else if (!queryDirectory.isDirectory())
				throw new Exception("Query path specified is not a directory.");
			else {
				File[] queryFiles = queryDirectory.listFiles();
				if (queryFiles != null) {
					for (File queryFile : queryFiles) {
						String qid = queryFile.getName().split("\\.")[0];
						String qStr = new String(Files.readAllBytes(java.nio.file.Paths.get(queryDirectory
								+ File.separator + queryFile.getName())));
						if (this.queryLanguage == QueryLanguage.csparql)
							qStr = "REGISTER QUERY " + qid + " AS " + qStr;
						this.queryMap.put(qid, qStr);
					}
				} else
					throw new Exception("Cannot find query files.");
			}
		} else {
			for (String qid : this.queries) {
				try {

					File queryFile = new File(qd + File.separator + qid);
					String qStr = new String(Files.readAllBytes(queryFile.toPath()));
					qid = qid.split("\\.")[0];
					if (this.queryLanguage == QueryLanguage.csparql)
						qStr = "REGISTER QUERY " + qid + " AS " + qStr;
					this.queryMap.put(qid, qStr);
				} catch (Exception e) {
					logger.error("Could not load query file.");
					e.printStackTrace();
				}
			}
		}
		// throw new Exception("Query directory not specified;");
	}

	private Map<String, String> getCQELSQueries() {
		Map<String, String> queries = new HashMap<>();
		for (Entry en : this.queryMap.entrySet()) {
			String qid = en.getKey() + "-" + UUID.randomUUID();
			queries.put(qid, en.getValue() + "");
		}
		return queries;
	}


	private Map<String, String> getCSPARQLQueries() {
		Map<String, String> queries = new HashMap<>();
		for (Entry en : this.queryMap.entrySet()) {
			String qid = en.getKey() + "-" + UUID.randomUUID();
			queries.put(qid, en.getValue() + "");
		}
		return queries;
	}


	private Map<String, String> getRSPQLQueries() throws ParseException {
		Map<String, String> queries = new HashMap<>();
		for (Entry en : this.queryMap.entrySet()) {
			String query = "";
			String queryID = en.getValue().toString().split("REGISTER RSTREAM <")[1].split("> AS")[0];
			if(registeredIDs.isEmpty()) {
				query = en.getValue() + "";
				registeredIDs.add(queryID);
				queries.put(queryID, query);
			}
			else {
				String nextQueryID = queryID + "-" + duplicityCounter++;
				query = en.getValue().toString().replaceAll(queryID, nextQueryID);
				registeredIDs.add(nextQueryID);
				queries.put(nextQueryID, query);
			}
		}
		return queries;
	}

	private List<Query> getUnsupportedQueries() throws Exception {
		List<Query> queries = new ArrayList<>();
		for (Entry en : this.queryMap.entrySet()) {
			String queryID = en.getKey() + "-" + UUID.randomUUID();
			String query = "";
			String nextQueryID = queryID;
			if(registeredIDs.isEmpty()) {
				query = en.getValue() + "";
				registeredIDs.add(queryID);
			}
			else {
				nextQueryID = queryID + "-" + duplicityCounter++;
				query = en.getValue().toString().replaceAll(queryID, nextQueryID);
				registeredIDs.add(nextQueryID);
			}
			queries.add(new Query(query, nextQueryID));
		}
		return queries;
	}

	private List<Query> getUnsupportedQueriesWithDuplicates() throws Exception {
		List<Query> queries = new ArrayList<>();
		for(int i = 0; i < queryDuplicates; i++) {
				queries.addAll(getUnsupportedQueries());
		}
		return queries;
	}
	private Map<String, String> getQueriesWithDuplicates() throws Exception {
		Map<String, String> queries = new HashMap<>();
		for(int i = 0; i < queryDuplicates; i++) {
			if(queryLanguage == QueryLanguage.csparql) {
				queries.putAll(getCSPARQLQueries());
			} else if(queryLanguage == QueryLanguage.cqels) {
				queries.putAll(getCQELSQueries());
			} else if(queryLanguage == QueryLanguage.rspql) {
				queries.putAll(getRSPQLQueries());
			}
		}
		return queries;
	}


	private void startStreamFromQuery(String streamURI, String streamName) throws Exception {
			logger.info("URI: " + streamURI);
			String path = this.streams + "/" + streamName + ".stream";
			if (!this.startedStreams.contains(streamURI)) {
				this.startedStreams.add(streamURI);
				SensorStream ss;
				EventDeclaration ed = er.getEds().get(filterPortOutOfURL(streamURI));
				if (ed.getEventType().contains("traffic")) {
					ss = new AarhusTrafficStream(streamURI, path, ed, start, end, nameToStreamMap.get(streamURI), rdfFormat);
				} else if (ed.getEventType().contains("pollution")) {
					ss = new AarhusPollutionStream(streamURI, path, ed, start, end, nameToStreamMap.get(streamURI), rdfFormat);
				} else if (ed.getEventType().contains("weather")) {
					ss = new AarhusWeatherStream(streamURI, path, ed, start, end, nameToStreamMap.get(streamURI), rdfFormat);
				} else if (ed.getEventType().contains("location"))
					ss = new LocationStream(streamURI, path, ed, nameToStreamMap.get(streamURI), rdfFormat);
				else if (ed.getEventType().contains("parking"))
					ss = new AarhusParkingStream(streamURI, path, ed, start, end, nameToStreamMap.get(streamURI), rdfFormat);
				else
					throw new Exception("Sensor type not supported.");
				ss.setRate(this.rate);
				ss.setFreq(this.frequency);

				new Thread(ss).start();
				startedStreamObjects.add(ss);
			}
	}

	private String filterPortOutOfURL(String s) {
		System.out.println("New URL: " + s.split(":(\\d)+")[0] + s.split(":(\\d)+")[1]);
		return s.split(":(\\d)+")[0] + s.split(":(\\d)+")[1];
	}

	protected void initializePerformanceMonitor() throws Exception {
		pm = new PerformanceMonitor(queryMap, duration, queryDuplicates, resultName, waitingTime, new URL(answerPath), answerFormat);
		new Thread(pm).start();
	}

	private void prepareStreams() throws Exception {
		List<String> streamsToBeStarted = getStreamNames();
		for(String s : streamsToBeStarted) {
			int portNumber = getPortNumberFromURL(s);
			DataStream dataStream = new DataStream(portNumber, s);
			socketDataStreams.add(dataStream);
			nameToStreamMap.put(s, dataStream);
			new Thread(dataStream).start();
		}
		prepareConfiguration();
		while (socketDataStreams.stream().anyMatch(c -> c.out.size() == 0)) {
			Thread.sleep(20);
		}

		t0 = System.currentTimeMillis();
		for (String s : streamsToBeStarted) {
			this.startStreamFromQuery(s, s.split("#")[1]);
		}
	}

	private void prepareStaticData() throws Exception {
		this.loadQueries();
		Map<String, File> toBeUploaded;
		if(queryLanguage == QueryLanguage.cqels) {
			toBeUploaded = findStaticDataSources(getCQELSQueries().values(), QueryLanguage.cqels);
		}
		else if(queryLanguage == QueryLanguage.csparql) {
			toBeUploaded = findStaticDataSources(getCSPARQLQueries().values(), QueryLanguage.csparql);
		}
		else if(queryLanguage == QueryLanguage.rspql) {
			toBeUploaded = findStaticDataSources(getRSPQLQueries().values(), QueryLanguage.rspql);
		}
		else {
			toBeUploaded = findStaticDataSources(getUnsupportedQueries());
		}
		uploadStaticData(toBeUploaded);
	}

	private void uploadStaticData(Map<String, File> staticDataFiles) throws IOException, InterruptedException {
		for(String s : staticDataFiles.keySet()) {
			System.out.println(getPortNumberFromURL(s));
			FileServer fs = new org.insight_centre.aceis.utils.server.FileServer(getPortNumberFromURL(s), s.split("http://www.insight-centre.org/")[0], staticDataFiles.get(s));
			staticFileServers.add(fs);
		}
	}

	private int getPortNumberFromURL(String url) {
		return Integer.parseInt(url.split(":")[2].split("/")[0]);
	}

	private void prepareConfiguration() throws Exception {
		File file = new File("configuration.json");
		try (OutputStream out = new FileOutputStream(file)) {
			if(queryLanguage == QueryLanguage.unsupported) {
				out.write(parseToJSON(new Configuration(getUnsupportedQueriesWithDuplicates(), null)).getBytes(StandardCharsets.UTF_8));
			}
			else {
				out.write(parseToJSON(new Configuration(null, getQueriesWithDuplicates())).getBytes(StandardCharsets.UTF_8));
			}
		}
		new org.insight_centre.aceis.utils.server.FileServer(getPortNumberFromURL(configPath), configPath, file);
		file.delete();
	}

	private String parseToJSON(Object o) {
		Gson gson = new Gson();
		return gson.toJson(o).replaceAll("\\\\r", "\r").replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t").replaceAll("\\\\u003c", "<").replaceAll("\\\\u003e", ">");
	}

	private Map<String, File> findStaticDataSources(Collection<String> queries, QueryLanguage ql) throws Exception {
		Map<String, File> re = new HashMap<>();
		if(ql == QueryLanguage.csparql || ql == QueryLanguage.rspql) {
			for(String s : queries) {
				re.putAll(parseStaticDataFromRSPQLAndCSPARQLQuery(s));
			}
		}
		else if (ql == QueryLanguage.cqels) {
			for(String s : queries) {
				re.putAll(parseStaticDataFromCQELSQuery(s));
			}
		}
		return re;
	}
	private Map<String, File> findStaticDataSources(List<Query> queries) throws Exception {
		Map<String, File> re = new HashMap<>();
		for(Query query : queries) {
			for(String staticDataSource : query.staticKnowledge) {
				String fileName = staticDataSource.split("/")[staticDataSource.split("/").length - 1];
				if (Files.exists(Path.of("dataset/" + fileName))) {
					File file = new File("dataset/" + fileName);
					re.put(staticDataSource, file);
				} else {
					throw new Exception("Static data file not found");
				}
			}
		}
		return re;
	}

	private Map<String, File> parseStaticDataFromRSPQLAndCSPARQLQuery(String query) throws Exception {
		Map<String, File> re = new HashMap<>();
		String[] lines = query.split("\n");
		for(String line : lines) {
			if(line.toUpperCase(Locale.ROOT).matches("(.)*FROM(\\s)+<(.)*")) {
				String path = line.split("<")[1].split(">")[0];
				String fileName = path.split("/")[path.split("/").length - 1];
				if(Files.exists(Path.of("dataset/" + fileName))) {
					File file = new File("dataset/" + fileName);
					re.put(path, file);
				}
				else {
					throw new Exception("Static data file not found");
				}
			}
		}
		return re;
	}

	private Map<String, File> parseStaticDataFromCQELSQuery(String query) throws Exception {
		Map<String, File> re = new HashMap<>();
		String[] lines = query.split("\n");
		for(String line : lines) {
			if(line.toUpperCase(Locale.ROOT).matches("(.)*FROM NAMED(\\s)+<(.)*")) {
				String path = line.split("<")[1].split(">")[0];
				String fileName = path.split("/")[path.split("/").length - 1];
				if(Files.exists(Path.of("dataset/" + fileName))) {
					File file = new File("dataset/" + fileName);
					re.put(path, file);
				}
				else {
					throw new Exception("Static data file not found");
				}
			}
		}
		return re;
	}


	private List<String> getStreamNames() throws Exception {
		if(queryLanguage == QueryLanguage.rspql) {
			return findStreamsA(getRSPQLQueries().values());
		} else if(queryLanguage == QueryLanguage.cqels) {
			return findStreamsA(getCQELSQueries().values());
		} else if(queryLanguage == QueryLanguage.csparql) {
			return findStreamsA(getCSPARQLQueries().values());
		} else {
			return findStreamsB(getUnsupportedQueries());
		}
	}

	private List<String> findStreamsA(Collection<String> queries) throws Exception {
		Set<String> re = new HashSet<>();
		for(String query : queries) {
			if(queryLanguage == QueryLanguage.rspql) {
				re.addAll(parseStreamNamesFromRSPQLQuery(query));
			} else if (queryLanguage == QueryLanguage.cqels) {
				re.addAll(parseStreamNamesFromCQELSQuery(query));
			}
			else if (queryLanguage == QueryLanguage.csparql) {
				re.addAll(parseStreamNamesFromCSPARQLQuery(query));
			}
		}
		return new ArrayList<>(re);
	}
	private List<String> findStreamsB(List<Query> queries) throws Exception {
		Set<String> re = new HashSet<>();
		for(Query query : queries) {
			re.addAll(parseStreamNamesFromUnsupportedQuery(query));
		}
		return new ArrayList<>(re);
	}


	private List<String> parseStreamNamesFromRSPQLQuery(String query) throws Exception {
		List<String> re = new ArrayList<>();
		String[] lines = query.split("\n");
		for(String line : lines) {
			if(line.toLowerCase(Locale.ROOT).matches("(.)*on(\\s)+<(.)*")) {
				String path = line.split("ON <")[1].split(">")[0];
//				String fileName = path.split("/")[path.split("/").length - 1];
				re.add(path);
			}
		}
		return re;
	}

	private List<String> parseStreamNamesFromCQELSQuery(String query) throws Exception {
		List<String> re = new ArrayList<>();
		String[] lines = query.split("\n");
		for(String line : lines) {
			if(line.toLowerCase(Locale.ROOT).matches("(.)*stream(\\s)+<(.)*")) {
				String path = line.split("<")[1].split(">")[0];
//				String fileName = path.split("/")[path.split("/").length - 1];
				re.add(path);
			}
		}
		return re;
	}

	private List<String> parseStreamNamesFromCSPARQLQuery(String query) throws Exception {
		List<String> re = new ArrayList<>();
		String[] lines = query.split("\n");
		for(String line : lines) {
			if(line.toLowerCase(Locale.ROOT).matches("(.)*from(\\s)+stream(\\s)+<(.)*")) {
				String path = line.split("<")[1].split(">")[0];
//				String fileName = path.split("/")[path.split("/").length - 1];
				re.add(path);
			}
		}
		return re;
	}

	private List<String> parseStreamNamesFromUnsupportedQuery(Query query) throws Exception {
		List<String> re = new ArrayList<>();
		for(Window window : query.windows) {
			re.add(window.getStreamURL());
		}
		return re;
	}
}