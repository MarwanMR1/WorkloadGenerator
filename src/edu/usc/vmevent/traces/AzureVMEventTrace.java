package edu.usc.vmevent.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class AzureVMEventTrace {

	public static class VMEvent {
		Set<Integer> addVMs = Sets.newHashSet();
		Set<Integer> removeVMs = Sets.newHashSet();
	}

	private final TreeMap<Long, VMEvent> timeVMStateMap = Maps.newTreeMap();
	private Long currentTime;

	public AzureVMEventTrace(String traceFile) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(traceFile)));
			String line = null;
			int objIdCount = 0;
			Map<String, Integer> vmIdMap = Maps.newHashMap();
			while ((line = br.readLine()) != null) {
				String vmIdStr = line.split(",")[0];
				Integer vmId = vmIdMap.get(vmIdStr);
				if (vmId == null) {
					vmId = objIdCount;
					vmIdMap.put(vmIdStr, vmId);
					objIdCount += 1;
				}
				final int id = vmId;
				long create = Long.parseLong(line.split(",")[3]);
				long delete = Long.parseLong(line.split(",")[4]);
				timeVMStateMap.compute(create, (k, v) -> {
					if (v == null) {
						v = new VMEvent();
					}
					v.addVMs.add(id);
					return v;
				});
				timeVMStateMap.compute(delete, (k, v) -> {
					if (v == null) {
						v = new VMEvent();
					}
					v.removeVMs.add(id);
					return v;
				});
			}
			br.close();
			currentTime = timeVMStateMap.firstKey();
			System.out.println(String.format("Total VMs %d", vmIdMap.size()));
			System.out.println(String.format("Total Number of Events %d", timeVMStateMap.size()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Optional<VMEvent> getNextEvent() {
		if (currentTime == null) {
			return Optional.empty();
		}
		VMEvent event = timeVMStateMap.get(currentTime);
		currentTime = timeVMStateMap.higherKey(currentTime);
		return Optional.of(event);
	}

}
