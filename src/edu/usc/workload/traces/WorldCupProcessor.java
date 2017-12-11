package edu.usc.workload.traces;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class WorldCupProcessor {

	public static class Request {
		DateTime time;
		int key;
		boolean isRead;

		public Request(DateTime time, int key, boolean method) {
			super();
			this.time = time;
			this.key = key;
			this.isRead = method;
		}

		@Override
		public String toString() {
			return "Request [time=" + time + ", key=" + key + ", isRead=" + isRead + "]";
		}
	}

	private static final DateTimeFormatter dtf = DateTimeFormat.forPattern("HH:mm:ss");
	private static int missingKeys = 0;
	private static Map<String, Integer> missingKeyIdMap = Maps.newHashMap();
	private static Random r = new Random(0);
	private static Set<String> readActions = Sets.newHashSet();

	public static Request process(Map<String, Integer> objIdMap, String line) {
		// "34600 - - [30/Apr/1998:21:30:17 +0000] \"GET /images/hm_bg.jpg HTTP/1.0\"
		// 200 24736";
		DateTime time = dtf.parseDateTime(line.split("/1998:")[1].split(" ")[0]);
		String key = line.substring(line.indexOf('"'), line.lastIndexOf('"')).split(" ")[1];
		String action = line.substring(line.indexOf('"') + 1, line.lastIndexOf('"')).split(" ")[0];
		boolean isRead = false;
		if (readActions.contains(action.toUpperCase())) {
			isRead = true;
		}
		int keyInt = r.nextInt(objIdMap.size());
		if (missingKeyIdMap.containsKey(key)) {
			keyInt = missingKeyIdMap.get(key);
		}
		if (objIdMap.containsKey(key)) {
			keyInt = objIdMap.get(key);
		} else {
			missingKeys++;
			missingKeyIdMap.put(key, keyInt);
		}
		return new Request(time, keyInt, isRead);
	}

	public static void processOneDayTracePerHour(String oneDayTrace, String objIdMapping, String outputDir) {
		List<Integer> qps = Lists.newArrayList();
		List<List<Request>> requests = Lists.newArrayList();
		int hour = 24;
		int day = Integer.parseInt(oneDayTrace.split("\\.")[0].split("_")[1]);

		System.out.println(String.format("Process %s with ObjectMapping %s and output to %s", oneDayTrace, objIdMapping,
				outputDir));

		Map<String, Integer> objIdMap = Maps.newHashMap();
		Map<Integer, String> idObjMap = Maps.newHashMap();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(objIdMapping)));
			String line = null;
			int count = 0;
			int id = -1;
			while ((line = br.readLine()) != null) {
				line = new String(line.getBytes("UTF-8")).trim();
				try {
					id = Integer.parseInt(line.split(" ")[0]);
					String key = line.split(" ")[1];
					objIdMap.put(key, id);
					idObjMap.put(id, key);
				} catch (Exception e) {
					String prevL = idObjMap.get(id);
					idObjMap.put(id, prevL + line);
					objIdMap.remove(prevL);
					objIdMap.put(prevL + line, id);
					count += 1;
				}
			}
			br.close();
			System.out.println(count);
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int i = 0; i < hour; i++) {
			qps.add(0);
			requests.add(Lists.newArrayList());
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(oneDayTrace)));
			String line = null;
			while ((line = br.readLine()) != null) {
				line = new String(line.getBytes("ascii"), "UTF-8");
				Request request = process(objIdMap, line);
				int index = request.time.getHourOfDay();
				qps.set(index, qps.get(index) + 1);
				requests.get(index).add(request);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out
				.println("Total Missing keys " + missingKeys + " total unique missing keys " + missingKeyIdMap.size());

		for (int i = 0; i < hour; i++) {
			String fileName = String.format("%s/wc_day%d_%d", outputDir, day, i);
			try {
				new File(fileName).createNewFile();
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)));
				bw.write(String.valueOf(qps.get(i)));
				bw.write("\n");
				StringBuilder sb = new StringBuilder();
				for (int r = 0; r < requests.get(i).size(); r++) {
					sb.append(requests.get(i).get(r).key);
					sb.append(",");
					if (requests.get(i).get(r).isRead) {
						sb.append(0);
					} else {
						sb.append(1);
					}
					sb.append("\n");
				}
				bw.write(sb.toString());
				bw.flush();
				bw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		readActions.add("GET");
		readActions.add("HEAD");
		readActions.add("TRACE");
		readActions.add("OPTIONS");

		processOneDayTracePerHour(args[0], args[1], args[2]);
	}
}
