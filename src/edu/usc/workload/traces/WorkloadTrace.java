package edu.usc.workload.traces;

import java.util.List;
import java.util.Optional;

import org.json.JSONObject;

import com.google.common.collect.Lists;

public abstract class WorkloadTrace {
	protected final int capacityPerNode;
	private int currentRound = 0;
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
		private int qps;
		private double qpsFactor;

		public int getQps() {
			return qps;
		}

		public double getQpsFactor() {
			return qpsFactor;
		}

		public Stats(int qps, double qpsFactor) {
			super();
			this.qps = qps;
			this.qpsFactor = qpsFactor;
		}

		public abstract Optional<List<WCRequest>> getNextBatchRequests(int limit);
	}

	public static class StatsImpl extends Stats {

		public StatsImpl(int qps, double qpsFactor) {
			super(qps, qpsFactor);
		}

		@Override
		public Optional<List<WCRequest>> getNextBatchRequests(int limit) {
			return Optional.empty();
		}

	}

	public Optional<Stats> getNextStats() {
		if (currentRound >= statsTimeline.size()) {
			return Optional.empty();
		}
		Stats stats = statsTimeline.get(currentRound);
		currentRound += 1;
		return Optional.of(stats);
	}

	public static class WCRequest {
		public int getKey() {
			return key;
		}

		public boolean isRead() {
			return isRead;
		}

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
