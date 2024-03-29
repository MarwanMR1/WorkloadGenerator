package edu.usc.vmevent.traces;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.google.common.collect.Maps;

import edu.usc.vmevent.traces.AzureVMEventTrace.VMEvent;

public class GoogleVMEventTrace {

	private final TreeMap<Long, VMEvent> timeVMStateMap = Maps.newTreeMap();
	private Long currentTime;

	public GoogleVMEventTrace(String traceFile) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(traceFile)));
			String line = null;
			int objIdCount = 0;
			Map<String, Integer> vmIdMap = Maps.newHashMap();
			while ((line = br.readLine()) != null) {
				String vmIdStr = line.split(",")[1];
				Integer vmId = vmIdMap.get(vmIdStr);
				if (vmId == null) {
					vmId = objIdCount;
					vmIdMap.put(vmIdStr, vmId);
					objIdCount += 1;
				}
				final int id = vmId;
				long time = Long.parseLong(line.split(",")[0]);
				long event = Long.parseLong(line.split(",")[2]);

				if (event == 0) {
					timeVMStateMap.compute(time, (k, v) -> {
						if (v == null) {
							v = new VMEvent();
						}
						if (v.removeVMs.contains(id)) {
							v.removeVMs.remove(id);
						} else {
							v.addVMs.add(id);
						}
						return v;
					});
				} else if (event == 1) {
					timeVMStateMap.compute(time, (k, v) -> {
						if (v == null) {
							v = new VMEvent();
						}
						if (v.addVMs.contains(id)) {
							v.addVMs.remove(id);
						} else {
							v.removeVMs.add(id);
						}
						return v;
					});
				}
			}
			br.close();
			currentTime = timeVMStateMap.firstKey();
			System.out.println(String.format("Total VMs %d", vmIdMap.size()));
			System.out.println(String.format("Total Number of Events %d", timeVMStateMap.size()));
			System.out.println(timeVMStateMap.get(0l));
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
