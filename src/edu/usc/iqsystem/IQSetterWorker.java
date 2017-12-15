package edu.usc.iqsystem;

import java.util.HashMap;
import java.util.concurrent.Callable;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

import edu.usc.workload_generator.Outputs;

public class IQSetterWorker implements Callable<Outputs> {

	private int id;
	private IQConfiguration con;
	private int mod;
	private int K;
	private IQSystem sys;
	private int div;
	private HashMap<String, MemcachedClient> mc;
	private final int PORT = 11211;
	private int start, end;

	public IQSetterWorker(int id, IQSystem sys, int K, int div) {
		this.id = id;
		this.K = K;
		this.sys = sys;
		this.div = div;
		mc = new HashMap<>();
		for (String ip : IQSystem.IPs) {
			setupNodePool(ip);
		}

		start = id * (K / div);
		end = (id + 1) * (K / div);
	}

	public void init(IQConfiguration con, int mod) {
		this.con = con;
		this.mod = mod;
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

		for (int key = start; key < end; key++) {
			if ((key + con.config) % mod == 0) {
				// if (key == 1) {
				// System.out.println("Setting key " + key + ", with value: " + getValue(key,
				// con.config) + " cid: "
				// + con.config);
				// }
				sys.set(key, getValue(key, con.config), con.config, mc, true);
				result.sets++;

				// if (key == 1) {
				// GetOutputs out = sys.get(key);
				// System.out.println("Key just set, Get key " + key + ", value: " + out.value +
				// " cid: "
				// + con.config);
				// }
			}
			try {
				if (key % (end / 10) == 0) {
					System.out.println(id + "- SET Completed " + key + " out of " + end);
				}
			} catch (Exception e) {

			}

		}
		return result;
	}

	String getValue(int key, int cid) {
		return String.valueOf(cid);
	}

}
