package edu.usc.workload.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.Maps;

public class WorldCup98DailyTrace extends WorkloadTrace {

	private static final DateTimeFormatter dtf = DateTimeFormat.forPattern("HH:mm:ss");
	private final Map<String, Integer> objectIdMap = Maps.newHashMap();
	private int objectIdCount = 0;

	public WorldCup98DailyTrace(int capacityPerNode, String statsFile, TimeUnit unit) {
		super(capacityPerNode);

		switch (unit) {
		case HOURS:
			initialize(24);
			break;
		case MINUTES:
			initialize(24 * 60);
			break;
		case SECONDS:
			initialize(24 * 60 * 60);
			break;
		default:
			break;
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(statsFile)));
			String line = null;
			while ((line = br.readLine()) != null) {
				Request request = process(line);
				switch (unit) {
				case HOURS:
					add(request, request.time.getHourOfDay());
					break;
				case MINUTES:
					add(request, request.time.getMinuteOfDay());
					break;
				case SECONDS:
					add(request, request.time.getSecondOfDay());
					break;
				default:
					break;
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < qpsTimeline.size(); i++) {
			qpsFactorTimeline.set(i, (double) qpsTimeline.get(i) / capacityPerNode);
		}
	}

	protected void add(Request request, int index) {
		qpsTimeline.set(index, qpsTimeline.get(index) + 1);
		Integer objectId = objectIdMap.get(request.key);
		if (objectId == null) {
			objectId = objectIdCount++;
			objectIdMap.put(request.key, objectId);
		}
		if (request.isRead) {
			readKeys.get(index).compute(objectId, (k, v) -> {
				if (v == null) {
					return 1;
				}
				return v + 1;
			});
		} else {
			writeKeys.get(index).compute(objectId, (k, v) -> {
				if (v == null) {
					return 1;
				}
				return v + 1;
			});
		}
	}

	private static Request process(String line) {
		// "34600 - - [30/Apr/1998:21:30:17 +0000] \"GET /images/hm_bg.jpg HTTP/1.0\"
		// 200 24736";
		DateTime time = dtf.parseDateTime(line.split("/1998:")[1].split(" ")[0]);
		String key = line.split("\"")[1].split(" ")[1];
		boolean isRead = line.contains("GET");
		return new Request(time, key, isRead);
	}

	public static void main(String[] args) {
		System.out.println(
				process("34600 - - [30/Apr/1998:21:30:17 +0000] \"GET /images/hm_bg.jpg HTTP/1.0\" 200 24736"));
	}

}
