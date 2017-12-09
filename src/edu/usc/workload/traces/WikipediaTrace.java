package edu.usc.workload.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

public class WikipediaTrace extends WorkloadTrace {

	private final JSONObject obj;

	public WikipediaTrace(int capacityPerNode, String traceFile) throws Exception {
		super(capacityPerNode);
		String start = "2015-07-01";
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(traceFile)));
			String line = br.readLine();
			obj = new JSONObject(line);
			DateTime time = dtf.parseDateTime(start);
			while (obj.optInt(dtf.print(time), -1) != -1) {
				int qps = obj.getInt(dtf.print(time));
				this.qpsTimeline.add(qps);
				time = time.plusDays(1);
			}
			br.close();
		} catch (Exception e) {
			throw e;
		}
		qpsTimeline.forEach(qps -> {
			qpsFactorTimeline.add((double) qps / capacityPerNode);
		});

	}
}
