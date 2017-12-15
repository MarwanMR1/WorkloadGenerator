package edu.usc.iqsystem;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.meetup.memcached.MemcachedClient;

public class IQNode {

	public ArrayList<IQCache> caches;
	public int id, port;
	public String ip, host;
	public boolean isAlive;
	private MemcachedClient mcHERE_ONLY;
	public static AtomicInteger nextID = new AtomicInteger(0);

	public IQNode(final int id, String ip, int port) {
		this.id = id;
		isAlive = true;
		caches = new ArrayList<>();
		this.ip = ip;
		this.port = port;
		host = this.ip + ":" + this.port;
		mcHERE_ONLY = new MemcachedClient(host);
		mcHERE_ONLY.updateLocalConfigurationNumber(1);
		try {
			mcHERE_ONLY.updateServerConfigurationNumber(1);
		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	public String toString() {
		return id + "[" + (isAlive ? "ON" : "OFF") + "]";
	}

	public void reset(int addKey, int config) {
		id = addKey;
		for(IQCache c : caches) {
			c.updateID(id);
			for(IQPartition p : c.partitions) {
				p.config = config;
			}
		}
	}

	public void updateITwemcacheConfigurationNumber(int config) {
		mcHERE_ONLY.updateLocalConfigurationNumber(config);
		try {
			mcHERE_ONLY.updateServerConfigurationNumber(config);
		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

//	public int getTotalNumPartitions() {
//		int sum = 0;
//		for (Cache c : caches) {
//			sum += c.partitions.size();
//		}
//		return sum;
//	}
//
//	public Partition getRandomPartitionForRemove() {
//		Cache c = null;
//		while (c == null) {
//			if (caches.isEmpty()) {
//				return null;
//			}
//			c = caches.get(rand.nextInt(caches.size()));
//			if (c.partitions.isEmpty()) {
//				caches.remove(c);
//				c = null;
//			}
//		}
//		Partition p = c.partitions.get(rand.nextInt(c.partitions.size()));
//		return p;
//	}

}
