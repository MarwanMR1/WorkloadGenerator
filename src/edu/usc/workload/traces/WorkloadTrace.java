package edu.usc.workload.traces;

import java.util.List;
import java.util.Optional;

import org.json.JSONObject;

import com.google.common.collect.Lists;

public abstract class WorkloadTrace {
	protected final int capacityPerNode;
	private int currentRound = 0;
	protected final List<Integer> qpsTimeline = Lists.newArrayList();
	protected final List<Double> qpsFactorTimeline = Lists.newArrayList();
	protected final List<Stats> statsTimeline = Lists.newArrayList();

	public WorkloadTrace(int capacityPerNode) {
		super();
		this.capacityPerNode = capacityPerNode;
	}

	public JSONObject print() {
		JSONObject obj = new JSONObject();
		return obj;
	}

	public static abstract class Stats {
		int qps;
		double qpsFactor;

		public Stats(int qps, double qpsFactor) {
			super();
			this.qps = qps;
			this.qpsFactor = qpsFactor;
		}

		public abstract Optional<List<WCRequest>> getNextBatchRequests(int limit);
	}

	public Optional<Integer> getNextQPS() {
		if (currentRound >= qpsTimeline.size()) {
			return Optional.empty();
		}
		return Optional.of(qpsTimeline.get(currentRound));
	}

	public Optional<Double> getNextQPSFactor() {
		if (currentRound >= qpsFactorTimeline.size()) {
			return Optional.empty();
		}
		return Optional.of(qpsFactorTimeline.get(currentRound));
	}

	public Optional<Stats> getNextBatchRequests(int limit) {
		if (currentRound >= statsTimeline.size()) {
			return Optional.empty();
		}
		return Optional.of(statsTimeline.get(currentRound));
	}

	protected void initialize(int size) {
		for (int i = 0; i < size; i++) {
			qpsTimeline.add(0);
			qpsFactorTimeline.add(0d);
		}
	}

	public static class WCRequest {
		int key;
		boolean isRead;

		public WCRequest(int key, boolean method) {
			super();
			this.key = key;
			this.isRead = method;
		}

		@Override
		public String toString() {
			return "WCRequest [key=" + key + ", isRead=" + isRead + "]";
		}
	}
}
