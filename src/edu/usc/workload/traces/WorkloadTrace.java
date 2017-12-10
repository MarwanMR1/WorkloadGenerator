package edu.usc.workload.traces;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.joda.time.DateTime;
import org.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class WorkloadTrace {
	protected final int capacityPerNode;
	protected final List<Integer> qpsTimeline = Lists.newArrayList();
	protected final List<Double> qpsFactorTimeline = Lists.newArrayList();
	protected final List<Map<Integer, Integer>> readKeys = Lists.newArrayList();
	protected final List<Map<Integer, Integer>> writeKeys = Lists.newArrayList();

	public WorkloadTrace(int capacityPerNode) {
		super();
		this.capacityPerNode = capacityPerNode;
	}

	public JSONObject print() {
		JSONObject obj = new JSONObject();
		for (int i = 0; i < qpsTimeline.size(); i++) {
			JSONObject time = new JSONObject();
			time.put("qps", getQPS(i).orElse(-1));
			time.put("qps_factor", getQPSFactor(i).orElse(-1d));
			time.put("read_key_size", getReadKeys(i).orElse(Maps.newHashMap()).size());
			time.put("write_key_size", getWriteKeys(i).orElse(Maps.newHashMap()).size());
			time.put("read_keys", getReadKeys(i).orElse(Maps.newHashMap()));
			time.put("write_keys", getWriteKeys(i).orElse(Maps.newHashMap()));
			obj.put(String.valueOf(i), time);
		}
		System.out.println(obj.toString(4));
		return obj;
	}

	public Optional<Integer> getQPS(int round) {
		if (round >= qpsTimeline.size()) {
			return Optional.empty();
		}
		return Optional.of(qpsTimeline.get(round));
	}

	public Optional<Double> getQPSFactor(int round) {
		if (round >= qpsFactorTimeline.size()) {
			return Optional.empty();
		}
		return Optional.of(qpsFactorTimeline.get(round));
	}

	public Optional<Map<Integer, Integer>> getReadKeys(int round) {
		if (round >= readKeys.size()) {
			return Optional.empty();
		}
		return Optional.of(readKeys.get(round));
	}

	public Optional<Map<Integer, Integer>> getWriteKeys(int round) {
		if (round >= writeKeys.size()) {
			return Optional.empty();
		}
		return Optional.of(writeKeys.get(round));
	}

	protected void initialize(int size) {
		for (int i = 0; i < size; i++) {
			qpsTimeline.add(0);
			qpsFactorTimeline.add(0d);
			readKeys.add(Maps.newHashMap());
			writeKeys.add(Maps.newHashMap());
		}
	}

	public static class Request {
		DateTime time;
		String key;
		boolean isRead;

		public Request(DateTime time, String key, boolean method) {
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
}
