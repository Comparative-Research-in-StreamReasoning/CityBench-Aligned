package org.insight_centre.aceis.io.streams;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.UUID;

public class LocationStream extends SensorStream implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(LocationStream.class);
	private String txtFile;
	private EventDeclaration ed;

	// private ContextualFilteringManager cfm;

	public LocationStream(String uri, String txtFile, EventDeclaration ed, DataStream stream, CityBench.RDFFormat format) {
		super(uri);
		this.txtFile = txtFile;
		this.ed = ed;
		this.stream = stream;
		this.format = format;
	}

	@Override
	public void run() {
		logger.info("Starting sensor stream: " + this.stream_uri);
		try {
			if (txtFile.contains("Location")) {
				BufferedReader reader = new BufferedReader(new FileReader(txtFile));
				String strLine;
				while ((strLine = reader.readLine()) != null && (!stop)) {

					SensorObservation so = this.createObservation(strLine);
					Model model = this.getModel(so);
					try {
						StringWriter s = new StringWriter();
						if(format == CityBench.RDFFormat.turtle) {
							RDFDataMgr.write(s, model, Lang.TURTLE);
						} else if(format == CityBench.RDFFormat.rdfxml) {
							RDFDataMgr.write(s, model, Lang.RDFXML);
						} else if(format == CityBench.RDFFormat.ntriples) {
							RDFDataMgr.write(s, model, Lang.NTRIPLES);
						}
						stream.send(s.toString());

						logger.debug(this.stream_uri + " Streaming: " + model.getGraph().toString());

					} catch (Exception e) {
						e.printStackTrace();
						logger.error(this.stream_uri + " YASPER streamming error.");
					}
					CityBench.pm.addNumberOfStreamedStatements(model.listStatements().toList().size());

					if (sleep > 0) {
						try {
							Thread.sleep(sleep);
						} catch (InterruptedException e) {

							e.printStackTrace();

						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected Model getModel(SensorObservation so) throws NumberFormatException, IOException {
		String userStr = so.getFoi();
		String coordinatesStr = so.getValue().toString();
		Model m = ModelFactory.createDefaultModel();
		double lat = Double.parseDouble(coordinatesStr.split(",")[0]);
		double lon = Double.parseDouble(coordinatesStr.split(",")[1]);
		Resource serviceID = m.createResource(ed.getServiceId());
		//
		// Resource user = m.createResource(userStr);

		Resource observation = m.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
		CityBench.obMap.put(observation.toString(), so);
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type, m.createResource(RDFFileManager.saoPrefix + "StreamData"));

		// location.addProperty(RDF.type, m.createResource(RDFFileManager.ctPrefix + "Location"));

		Resource coordinates = m.createResource();
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongitude"), lon);

		// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), user);
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(ed.getPayloads().get(0).split("\\|")[2]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
		// fake fixed foi
		observation
				.addProperty(
						m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"),
						m.createResource("http://iot.ee.surrey.ac.uk/citypulse/datasets/aarhusculturalevents/culturalEvents_aarhus#context_do63jk2t8c3bjkfb119ojgkhs7"));

		observation.addProperty(m.createProperty(RDFFileManager.saoPrefix + "hasValue"), coordinates);
		// System.out.println("transformed: " + m.listStatements().toList().size());s
		return m;
	}

	//@Override
	protected SensorObservation createObservation(Object data) {
		String str = data.toString();
		String userStr = str.split("\\|")[0];
		String coordinatesStr = str.split("\\|")[1];
		SensorObservation so = new SensorObservation();
		so.setFoi(userStr);
		// so.setServiceId(this.getURI());
		so.setValue(coordinatesStr);
		so.setObTimeStamp(new Date());
		so.setObId("UserLocationObservation-" + (int) Math.random() * 10000);
		// return so;
		//this.currentObservation = so;
		return so;
	}

}
