package edu.usc.workload.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;

public class WorldCup98Trace extends WorkloadTrace {

	int currentDay = 5;
	int currentHour = 0;
	final String baseDir;
	final int capacityPerNode;
	private final int MAX_DAY = 92;

	public WorldCup98Trace(int capacityPerNode, String baseDir) {
		super(capacityPerNode);
		this.capacityPerNode = capacityPerNode;
		this.baseDir = baseDir;
	}

	public int getCurrentDay() {
		return currentDay;
	}

	public int getCurrentHour() {
		return currentHour;
	}

	@Override
	public Optional<Stats> getNextStats() {
		if (currentDay > MAX_DAY) {
			return Optional.empty();
		}
		String file = String.format("%s/wcday%d/wc_day%d_%d", baseDir, currentDay, currentDay, currentHour);
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));
			int qps = Integer.parseInt(br.readLine());
			WCStats stats = new WCStats(qps, (double) qps / (double) capacityPerNode, br);
			currentHour += 1;
			if (currentHour == 24) {
				currentDay += 1;
				currentHour = 0;
			}
			return Optional.of(stats);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Optional.empty();
	}

	public static class WCStats extends Stats {

		BufferedReader br;

		public WCStats(int qps, double qpsFactor, BufferedReader br) {
			super(qps, qpsFactor);
			this.br = br;
		}

		@Override
		public Optional<List<WCRequest>> getNextBatchRequests(int limit) {
			List<WCRequest> requests = Lists.newArrayList();
			try {
				String line = null;
				int count = 0;
				while ((line = br.readLine()) != null) {
					processLine(requests, line);
					count++;
					if (count == limit - 1) {
						break;
					}
				}
				line = br.readLine();
				processLine(requests, line);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			if (requests.isEmpty()) {
				try {
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return Optional.empty();
			}
			return Optional.of(requests);
		}

		private void processLine(List<WCRequest> requests, String line) {
			if (line == null) {
				return;
			}
			int key = Integer.parseInt(line.split(",")[0]);
			int read = Integer.parseInt(line.split(",")[1]);
			if (read == 0) {
				requests.add(new WCRequest(key, true));
			} else {
				requests.add(new WCRequest(key, false));
			}
		}
	}

	public static void main(String[] args) {
		WorldCup98Trace trace = new WorldCup98Trace(100, "/mnt/output");
		Optional<Stats> stats = trace.getNextStats();
		while (stats.isPresent()) {
			Optional<List<WCRequest>> req = stats.get().getNextBatchRequests(10000);
			int reads = 0;
			int writes = 0;
			while (req.isPresent()) {
				for (int i = 0; i < req.get().size(); i++) {
					if (req.get().get(i).isRead) {
						reads++;
					} else {
						writes++;
					}
				}
				req = stats.get().getNextBatchRequests(10000);
			}
			System.out.println(String.format("day-%d-hour-%d, qps=%d, qpsf=%f, read=%d, write=%d, total=%d",
					trace.getCurrentDay(), trace.getCurrentHour(), stats.get().getQps(), stats.get().getQpsFactor(), reads,
					writes, reads + writes));
			stats = trace.getNextStats();
		}
	}

}
