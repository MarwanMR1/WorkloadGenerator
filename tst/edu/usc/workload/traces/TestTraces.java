package edu.usc.workload.traces;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.usc.vmevent.traces.AzureVMEventTrace;
import edu.usc.vmevent.traces.AzureVMEventTrace.VMEvent;
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
		WorldCup98AllTrace all = new WorldCup98AllTrace(10000000,
				"/Users/haoyuh/Documents/PhdUSC/WorkloadGenerator/resources/wc.out");
		all.print();
	}

	@Test
	public void testAzureVMEvents() {
		AzureVMEventTrace azure = new AzureVMEventTrace("/Users/haoyuh/Downloads/vmtable.csv");
		Set<Integer> servers = Sets.newHashSet();
		Optional<VMEvent> event = azure.getNextEvent();
		while (event.isPresent()) {
			event.get().getAddVMs().forEach(i -> {
				if (servers.contains(i)) {
					System.out.println("%%%%%%%%");
				}
			});
			servers.addAll(event.get().getAddVMs());
			final Optional<VMEvent> f = event;
			
			event.get().getRemoveVMs().forEach(i -> {
				if (f.get().getAddVMs().contains(i)) {
					System.out.println("#####");
				}
				if (!servers.contains(i)) {
					System.out.println("!!!!!!!!!!");
				}
			});
			servers.removeAll(event.get().getRemoveVMs());
			event = azure.getNextEvent();
			System.out.println(servers.size());
		}
	}

	@Test
	public void testGoogleVMEvents() {
		GoogleVMEventTrace azure = new GoogleVMEventTrace("/Users/haoyuh/Downloads/google.csv");
		Set<Integer> servers = Sets.newHashSet();
		Optional<VMEvent> event = azure.getNextEvent();
		while (event.isPresent()) {
			event.get().getAddVMs().forEach(i -> {
				if (servers.contains(i)) {
					System.out.println("%%%%%%%%");
				}
			});
			
			servers.addAll(event.get().getAddVMs());
			final Optional<VMEvent> f = event;
			
			event.get().getRemoveVMs().forEach(i -> {
				if (f.get().getAddVMs().contains(i)) {
					System.out.println("#####");
				}
				if (!servers.contains(i)) {
					System.out.println("!!!!!!!!!!");
				}
			});
			servers.removeAll(event.get().getRemoveVMs());
			event = azure.getNextEvent();
			System.out.println(servers.size());
		}

	}
}
