package edu.usc.workload.traces;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.junit.Test;

import com.google.common.collect.Lists;

import edu.usc.vmevent.traces.AzureVMEventTrace;
import edu.usc.vmevent.traces.GoogleVMEventTrace;

public class TestTraces {

	@Test
	public void testWikiTrace() throws Exception {
		List<String> traces = Lists.newArrayList(
				"/Users/haoyuh/Documents/PhdUSC/WorkloadGenerator/resources/donald_trump.json",
				"/Users/haoyuh/Documents/PhdUSC/WorkloadGenerator/resources/la.json");

		for (String trace : traces) {
			WikipediaTrace t = new WikipediaTrace(100, trace);
			t.print();
			System.out.println("##############");
		}
	}

	@Test
	public void testWorldCupTrace() {
		WorldCup98AllTrace all = new WorldCup98AllTrace(100,
				"/Users/haoyuh/Documents/PhdUSC/WorkloadGenerator/resources/wc.out");
		all.print();
	}

	@Test
	public void testWorldCupDailyTrace() {
		try {
			// WorldCup98DailyTrace t = new WorldCup98DailyTrace(100,
			// "/tmp/output/recreate_66.stats", TimeUnit.HOURS);
			// JSONObject obj = t.print();
			// new File("/tmp/wc_66_hour.stats").createNewFile();
			// BufferedWriter bw = new BufferedWriter(new FileWriter(new
			// File("/tmp/wc_66_hour.stats")));
			// bw.write(obj.toString());
			// bw.flush();
			// bw.close();

			WorldCup98DailyTrace t = new WorldCup98DailyTrace(100, "/tmp/output/recreate_66.stats", TimeUnit.MINUTES);
			JSONObject obj = t.print();
			new File("/tmp/wc_66_min.stats").createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/tmp/wc_66_min.stats")));
			bw.write(obj.toString());
			bw.flush();
			bw.close();
			//
			// t = new WorldCup98DailyTrace(100, "/tmp/output/recreate_66.stats",
			// TimeUnit.SECONDS);
			// obj = t.print();
			// new File("/tmp/wc_66_sec.stats").createNewFile();
			// bw = new BufferedWriter(new FileWriter(new File("/tmp/wc_66_sec.stats")));
			// bw.write(obj.toString());
			// bw.flush();
			// bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testWorldCupDailyPreTrace() {
		WorldCup98PreprocessedTrace t = new WorldCup98PreprocessedTrace(100, "/tmp/wc_66_hour.stats");
		t.print();

		// t = new WorldCup98PreprocessedTrace(100, "/tmp/wc_66_min.stats");
		// t.print();
	}

	@Test
	public void testAzureVMEvents() {
		AzureVMEventTrace azure = new AzureVMEventTrace("/Users/haoyuh/Downloads/vmtable.csv");

	}

	@Test
	public void testGoogleVMEvents() {
		GoogleVMEventTrace azure = new GoogleVMEventTrace("/Users/haoyuh/Downloads/google.csv");

	}
}
