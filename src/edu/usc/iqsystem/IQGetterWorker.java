package edu.usc.iqsystem;

import java.util.HashMap;
import java.util.concurrent.Callable;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

import edu.usc.system.Partition;
import edu.usc.workload_generator.Outputs;

public class IQGetterWorker implements Callable<Outputs> {

	private int id;
	private IQConfiguration con;
	private int mod;
	private int K;
	private IQSystem sys;
	private int div;
	private int numOfPartitions;
	private boolean isNoAction;
	private HashMap<String, MemcachedClient> mc;
	private final int PORT = 11211;
	private int start, end;

	public IQGetterWorker(int id, IQSystem sys, int K, int mod, int div, int numOfPartitions) {
		this.id = id;
		this.K = K;
		this.mod = mod;
		this.sys = sys;
		this.div = div;
		this.numOfPartitions = numOfPartitions;
		mc = new HashMap<>();
		for (String ip : IQSystem.IPs) {
			setupNodePool(ip);
		}

		start = id * (K / div);
		end = (id + 1) * (K / div);
	}

	public void init(IQConfiguration con, boolean isNoAction) {
		this.con = con;
		this.isNoAction = isNoAction;
	}

	private void setupNodePool(String ip) {
		String host = ip + ":" + PORT;
		SockIOPool pool = SockIOPool.getInstance(host + "_" + id);
		String[] serverList = { host };
		pool.setServers(serverList);
		pool.setFailover(false);
		pool.setInitConn(1);
		pool.setMinConn(1);
		pool.setMaxConn(1);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(0);
		pool.setAliveCheck(false);
		pool.initialize();

		MemcachedClient memCache = new MemcachedClient(host + "_" + id);
		mc.put(host, memCache);
	}

	@Override
	public Outputs call() throws Exception {
		Outputs result = new Outputs();
		for(MemcachedClient m : mc.values()) {
			m.updateLocalConfigurationNumber(con.config);
		}

		boolean valid;

		for (int key = start; key < end; key++) {
			valid = true;
			GetOutputs getOutput = sys.get(key, mc);
			result.gets++;
			// if (key == 1) {
			// if(getOutput == null) {
			// GetOutputs out = sys.get(key);
			// System.out.println(id+") key " + key + " was a miss. set it with value: " +
			// out.value + " cid: " + con.config + ", p.config: " +
			// con.P[getHash(key)].config);
			//
			// }
			// System.out.println(id+") Got key " + key + ", value: " + getOutput.value + "
			// cid: " + con.config + ", p.config: " + con.P[getHash(key)].config);
			// }

			if (getOutput == null) {
				System.out.println(id + ") ERROR: output is null");
				System.exit(0);
			}
			if (getOutput.isMiss) {
				valid = false;
				result.miss_keys++;
			} else if (is_lost(key, getOutput.config, con.P)) {
				valid = false;
				result.lost_keys++;

				if (is_stale(key, isNoAction ? con.config : (con.config - 1), getOutput.value, 1)) {
					result.lost_key_stales++;
				}
			} else if (valid) {
				if (is_stale(key, isNoAction ? con.config : (con.config - 1), getOutput.value, 1)) {
					result.lost_key_stales++;
				}
				result.valid_keys++;
			}

			try {
				if (key % (end / 10) == 0) {
					System.out.println(id + "- GET Completed " + key + " out of " + end);
				}
			} catch (Exception e) {

			}
		}
		return result;
	}

	private boolean is_stale(int key, int cid, String value, int iiii) {
		for (int temp_cid = cid; temp_cid > 0; temp_cid--) {
			if ((key + temp_cid) % mod == 0)
				return !(value.equals(getValue(key, temp_cid)));
		}
		if (value.equals(getValue(key, IQWorkloadGenerator.initialConfig)))
			return false;
		System.out.println("ERROR in is_stale " + iiii + ": could not find source of set.");
		System.out.println("key: " + key + ", cid: " + cid + ", value: " + value);
		for (int i = cid; i >= 0; i--) {
			System.out.println(String.format("(%d + %d) mod %d != %d, value (%s) == (%s)", key, i, mod,
					((key + i) % mod), value, getValue(key, i)));
		}
		System.exit(0);
		return true;
	}

	String getValue(int key, int cid) {
		return String.valueOf(cid);
	}

	private int getHash(int key) {
		return key % numOfPartitions;
	}

	private boolean is_lost(int key, int key_config, IQPartition[] p) {
		IQPartition partition = p[getHash(key)];
		if (partition.config <= key_config)
			return false;
		return true;
	}

}
