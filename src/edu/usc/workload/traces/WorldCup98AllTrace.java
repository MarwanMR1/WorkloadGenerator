package edu.usc.workload.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class WorldCup98AllTrace extends WorkloadTrace {

	public WorldCup98AllTrace(int capacityPerNode, String file) {
		super(capacityPerNode);
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));
			String line = null;
			String requestLine = null;
			while ((line = br.readLine()) != null) {
				if (line.contains("Terminating")) {
					int qps = Integer.parseInt(requestLine);
					statsTimeline.add(new StatsImpl(qps, (double) qps / capacityPerNode));
				}
				requestLine = line;
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
