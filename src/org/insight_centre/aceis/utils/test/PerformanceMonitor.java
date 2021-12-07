package org.insight_centre.aceis.utils.test;

import com.csvreader.CsvWriter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
import org.insight_centre.aceis.io.answers.VariableBindings;
import org.insight_centre.aceis.io.answers.TransferObject;
import org.insight_centre.aceis.io.streams.SensorStream;
import org.insight_centre.aceis.io.streams.DataStream;
import org.insight_centre.aceis.utils.server.FileServer;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PerformanceMonitor implements Runnable {
	private Map<String, String> qMap;
	private long duration;
	private int duplicates;
	private String resultName;
	private long start = 0;
	private ConcurrentHashMap<String, List<Long>> latencyMap = new ConcurrentHashMap<String, List<Long>>();
	private List<Double> memoryList = new ArrayList<Double>();;
	private ConcurrentHashMap<String, Long> resultCntMap = new ConcurrentHashMap<String, Long>();
	private CsvWriter cw;
	private long resultInitTime = 0, lastCheckPoint = 0, globalInit = 0;
	private boolean stop = false;
	private List<String> qList;
	private long currentObIds = 0;
	public long streamedStatementInLastSecond = 0;
	public Queue<Long> streamedStatementsPerSecond = new ConcurrentLinkedQueue<>();
	private boolean throughputMeasurerRunning = false;
	private static final Logger logger = LoggerFactory.getLogger(PerformanceMonitor.class);
	private File outputFile;
	private int waitingTime;
	private URL answers;
	private CityBench.AnswerFormat answerFormat;
	private Gson gson = new Gson();
	private Set<TransferObject> transferObjects;
	private List<Integer> obIdIndexes = new ArrayList<>();
	private Set<String> seenObIds = new HashSet<>();

	public PerformanceMonitor(Map<String, String> queryMap, long duration, int duplicates, String resultName, int waitingTime, URL answers, CityBench.AnswerFormat answerFormat)
			throws Exception {
		qMap = queryMap;
		this.duration = duration;
		this.resultName = resultName;
		this.duplicates = duplicates;
		outputFile = new File("result_log" + File.separator + resultName + ".csv");
		if (outputFile.exists()) {
			throw new Exception("Result log file already exists.");
		}
		cw = new CsvWriter(new FileWriter(outputFile, true), ',');
		cw.write("CityBench");
		qList = new ArrayList(this.qMap.keySet());
		Collections.sort(qList);

		for (String qid : qList) {
			latencyMap.put(qid, new ArrayList<Long>());
			resultCntMap.put(qid, (long) 0);
			cw.write("Latency-" + qid);
		}
		initializeIndexes(qList.get(0));
		// for (String qid : qList) {
		// cw.write("cnt-" + qid);
		// }
		cw.write("Memory");
		cw.write("ObIds");
		cw.write("Completeness");
		cw.write("Estimated Throughput");
		cw.endRecord();
		// cw.flush();
		// cw.
		this.globalInit = System.currentTimeMillis();
		this.waitingTime = waitingTime;
		this.answers = answers;
		this.answerFormat = answerFormat;
	}

	@Override
	public void run() {
		while (System.currentTimeMillis() < CityBench.t0 + duration) {
			waitUntil(CityBench.t0 + duration);
		}
		this.cleanup();
		System.out.println("Waiting...");
		waitUntil(System.currentTimeMillis() + waitingTime);
		System.out.println("Finished Waiting");
		evaluateQueryAnswers();
	}

	private void evaluateQueryAnswers() {
		System.out.println("Evaluating");
		List<TransferObject> answers = loadQueryAnswers();

		Iterator<TransferObject> it = answers.iterator();
		while (it.hasNext()) {
			TransferObject transferObject = it.next();
			String queryId = transferObject.getQueryId();
			for(VariableBindings tsvb : transferObject.getTimestampedVariableBindings()) {
				for (int index : obIdIndexes) {
					if(tsvb.getVariableValuesAsString().get(index) == null)
						break;
					String obId = removeBracketFromURLIfNecessary(tsvb.getVariableValuesAsString().get(index));
					long latency = transferObject.getTimestamp() - CityBench.obMap.get(obId).getSysTimestamp().getTime();
					latencyMap.get(transferObject.getQueryId()).add(latency);
					seenObIds.add(obId);
				}
			}
			memoryList.add(transferObject.getMemoryConsumptionInMB());
			it.remove();
		}
		printResults();
	}

	private void printResults() {
		for(String qid : qList) {
			double avarageLatency = latencyMap.get(qid).stream().mapToDouble(a -> (double) a).average().getAsDouble();
			double avarageMemory = memoryList.stream().mapToDouble(a -> a).average().getAsDouble();
			int numberObIds = seenObIds.size();
			double completeness = (double)seenObIds.size()/CityBench.obMap.size();
			double estimatedThroughput = streamedStatementsPerSecond.stream().mapToDouble(a -> (double) a).average().getAsDouble();

			try {
				cw.write("");
				cw.write(avarageLatency + "");
				cw.write(avarageMemory + "");
				cw.write(numberObIds + "");
				cw.write(completeness + "");
				cw.write(estimatedThroughput + "");
				cw.flush();
				cw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			logger.info("Stopping after " + (System.currentTimeMillis() - this.globalInit) + " ms.");
			logger.info("Experimment stopped.");
			System.exit(0);
		}
	}
	
	private List<TransferObject> loadQueryAnswers() {
		System.out.println("Loading");
		List<TransferObject> re = null;
		try {
			Type collectionType = new TypeToken<List<TransferObject>>(){}.getType();
			re = new Gson().fromJson(IOUtils.toString(answers.openStream(), StandardCharsets.UTF_8), collectionType);
		} catch (IOException e) {
			System.out.println("Answers are not available at " + answers.toString());
		} catch (ClassCastException e) {
			System.out.println("Answers are not of type " + List.class);
		}
		return re;
	}

	public static void waitUntil(long targetTime) {
		long millis = targetTime - System.currentTimeMillis();
		if (millis <= 0)
			return;
		try {
			Thread.sleep(millis);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	private String removeBracketFromURLIfNecessary(String s) {
		if(s != null)
			return s.replaceAll("<", "").replaceAll(">", "");
		else
			return "";
	}

	private void cleanup() {

		for (Object css : CityBench.startedStreamObjects) {
			((SensorStream) css).stop();
		}

		for(DataStream ds : CityBench.socketDataStreams) {
			ds.stop();
		}

		for(FileServer fs : CityBench.staticFileServers) {
			fs.stop();
		}

		this.stop = true;
		System.gc();

	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	public synchronized void addNumberOfStreamedStatements(int streamStatements) {
		if(!throughputMeasurerRunning) {
			throughputMeasurerRunning = true;
			new Thread(new ThroughputMeasurer(this)).start();
		}
		streamedStatementInLastSecond += streamStatements;
	}

	private void initializeIndexes(String queryId) {
		if(queryId.equals("location_parking_1")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if(queryId.equals("pollution_weather_1")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if(queryId.equals("Q1") || queryId.equals("Q1_N3")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if(queryId.equals("Q1_20MB")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if(queryId.equals("Q1_30MB")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q2")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
			obIdIndexes.add(2);
			obIdIndexes.add(3);
		}
		else if (queryId.equals("Q3")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q4")) {
			obIdIndexes.add(3);
		}
		else if (queryId.equals("Q5")) {
			obIdIndexes.add(4);
		}else if (queryId.equals("Q6")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q7")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q8")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q9")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q10")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
		}
		else if (queryId.equals("Q10_5")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
			obIdIndexes.add(2);
			obIdIndexes.add(3);
			obIdIndexes.add(4);
		}
		else if (queryId.equals("Q10_8")) {
			obIdIndexes.add(0);
			obIdIndexes.add(1);
			obIdIndexes.add(2);
			obIdIndexes.add(3);
			obIdIndexes.add(4);
			obIdIndexes.add(5);
			obIdIndexes.add(6);
			obIdIndexes.add(7);
		}
		else if (queryId.equals("Q11")) {

		}
		else if (queryId.equals("Q12")) {

		}
		else if (queryId.equals("Q_RAND")) {
			obIdIndexes.add(0);
		}
	}
}
