package edu.usc.workload.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class WorldCup98PreprocessedTrace extends WorkloadTrace {

	public WorldCup98PreprocessedTrace(int capacityPerNode, String statsFile) {
		super(capacityPerNode);

		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(statsFile)));
			JSONObject obj = new JSONObject(br.readLine());
			br.close();
			Set<Integer> allKeys = Sets.newHashSet();
			long numberOfReads = 0;
			long numberOfWrites = 0;
			for (int i = 0;; i++) {
				JSONObject time = obj.optJSONObject(String.valueOf(i));
				if (time == null) {
					break;
				}

				JSONObject reads = time.getJSONObject("read_keys");
				Map<Integer, Integer> readKeys = Maps.newHashMap();
				reads.keySet().forEach(key -> {
					readKeys.put(Integer.parseInt(key), reads.getInt(key));
				});

				JSONObject writes = time.getJSONObject("write_keys");
				Map<Integer, Integer> writeKeys = Maps.newHashMap();
				writes.keySet().forEach(key -> {
					writeKeys.put(Integer.parseInt(key), writes.getInt(key));
				});

				this.readKeys.add(readKeys);
				this.writeKeys.add(writeKeys);
				allKeys.addAll(readKeys.keySet());
				allKeys.addAll(writeKeys.keySet());

				numberOfReads += readKeys.values().stream().mapToInt(Integer::intValue).sum();
				numberOfWrites += writeKeys.values().stream().mapToInt(Integer::intValue).sum();
				qpsTimeline.add(time.getInt("qps"));
			}
			for (int i = 0; i < qpsTimeline.size(); i++) {
				qpsFactorTimeline.add((double) qpsTimeline.get(i) / capacityPerNode);
			}
			double ratio = (double) (numberOfReads) / (double) (numberOfReads + numberOfWrites);
			System.out.println(String.format("Found %d rounds", qpsTimeline.size()));
			System.out.println(String.format("Found %d keys, %d reads and %d writes, %f read ratio", allKeys.size(),
					numberOfReads, numberOfWrites, ratio));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
