package edu.usc.iqsystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import com.meetup.memcached.IQException;
import com.meetup.memcached.InvalidValueException;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.MemcachedClient.IQGetResult;
import com.meetup.memcached.SockIOPool;

import edu.usc.iqsystem.IQPartition.PartitionStatus;
import edu.usc.main.MainClass;
import edu.usc.system.ValueWrapper;
import edu.usc.workload_generator.WorkloadGenerator.Trace;

public class IQSystem {

	public static int MAX_GET_PER_MIGRATION;
	private int N;
	private int L;
	// public HashMap<Integer, IQNode> nodes = new HashMap<>();
	public ArrayList<IQNode> nodes = new ArrayList<>();
	public ArrayList<IQCache> caches = new ArrayList<>();
	public IQPartition[] P;
	private final int initialConfig = 1;
	private int config = initialConfig;
	private Random rand = new Random(1);
	private int numOfPartitions;
	private static final int PORT = 11211;
	private static final int CACHE_POOL_NUM_CONNECTIONS = 200;
	public static final boolean noMigration = true;
	private static final boolean useIQTwemcache = true;
	public static String[] emuIPs = { "h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "h11", "h12",
			"h13", "h14", "h15", "h16", "h17", "h18", "h19" };
	public static String[] labIPs = { "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5",
			"10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5",
			"10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5",
			"10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5", "10.0.1.5" };
	private LinkedList<String> notUsedIps = new LinkedList<>();
	static String[] IPs;
	private boolean deleteWithMigration = false;

	public IQSystem(int N, int L, int numOfPartitions, Trace trace, Set<Integer> vmAdd, Set<Integer> vmRemove) {
		this.N = N;
		this.numOfPartitions = numOfPartitions;
		this.L = L;

		if (MainClass.isLab) {
			IPs = labIPs;
		} else {
			IPs = emuIPs;
		}

		for (int i = 0; i < IPs.length; i++) {
			notUsedIps.addLast(IPs[i]);
			setupNodePool(IPs[i]);
		}

		if (trace == Trace.AzureVM || trace == Trace.GoogleVM) {
			initVM(vmAdd);
		} else {
			for (int i = 0; i < N; i++) {
				IQNode n = new IQNode(IQNode.nextID.getAndIncrement(), notUsedIps.pop(), PORT);
				// nodes.put(i, n);
				nodes.add(n);
				for (int j = 0; j < L; j++) {
					IQCache c = new IQCache(i + "-" + j, n);
					caches.add(c);
					n.caches.add(c);
				}
			}
		}

		P = new IQPartition[numOfPartitions];
		int tenP = P.length / 10;
		for (int i = 0; i < P.length; i++) {
			P[i] = new IQPartition(i);
			int serverIndex = i % caches.size();
			P[i].server = caches.get(serverIndex);
			P[i].config = initialConfig;
			P[i].status = PartitionStatus.Normal;
			P[i].srcServer = null;
			P[i].srcConfig = -1;
			P[i].server.partitions.add(P[i]);

			if (i % tenP == 0)
				System.out.println(i + "/" + P.length);
		}
	}

	private void setupNodePool(String ip) {
		String host = ip + ":" + PORT;
		SockIOPool pool = SockIOPool.getInstance(host);
		String[] serverList = { host };
		pool.setServers(serverList);
		pool.setFailover(false);
		pool.setInitConn(CACHE_POOL_NUM_CONNECTIONS);
		pool.setMinConn(CACHE_POOL_NUM_CONNECTIONS);
		pool.setMaxConn(CACHE_POOL_NUM_CONNECTIONS);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(0);
		pool.setAliveCheck(false);
		pool.initialize();
	}

	public IQConfiguration getConfiguration() {
		return new IQConfiguration(P, config);
	}

	public void addNode() {
		int migPartitions = calculateNumOfPartitionsToMigrate(1);
		sortNodesDesc();
		int numOfCaches = caches.size();

		N++;
		if (N <= nodes.size()) {
			System.out.println("N <= nodes.size() after increment.");
			System.exit(0);
		}
		int newid = IQNode.nextID.getAndIncrement();
		IQNode n = new IQNode(newid, notUsedIps.pop(), PORT);
		// nodes.put(newid, n);
		nodes.add(n);
		for (int j = 0; j < L; j++) {
			IQCache c = new IQCache(n.id + "-" + j, n);
			caches.add(c);
			n.caches.add(c);
		}

		if (N > nodes.size()) {
			System.out.println("N > nodes.size() twice.");
			System.exit(0);
		}
		config++;
		updateIQConfig();

		for (int i = 0, j = 0, k = 0; i < migPartitions; i++) {
			IQPartition p = null;
			try {
				p = caches.get(j).partitions.get(rand.nextInt(caches.get(j).partitions.size()));
			} catch (Exception e) {
				System.out.println("ERROR: caches.get(j).partitions.size = " + caches.get(j).partitions.size());
				System.out.println("i = " + i + ", migPartitions: " + migPartitions);
				e.printStackTrace(System.out);
				System.exit(0);
			}
			caches.get(j).partitions.remove(p);
			// nodes.get(newid).caches.get(k).partitions.add(p);
			nodes.get(N - 1).caches.get(k).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			// p.server = nodes.get(newid).caches.get(k);
			p.server = nodes.get(N - 1).caches.get(k);
			if (!noMigration) {
				p.status = PartitionStatus.Migration;
				p.resetGetCounter();
			}

			j++;
			if (j == numOfCaches) {
				j = 0;
			}

			k++;
			if (k == L) {
				k = 0;
			}
		}
	}

	private void updateIQConfig() {
		try {
			for (IQNode n : nodes) {
				n.updateITwemcacheConfigurationNumber(config);
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	public void addMultipleNodes(int numNodes, Iterator<Integer> itAdd, boolean incConfig) {
		int migPartitions = calculateNumOfPartitionsToMigrate(numNodes);
		// long start = System.nanoTime();
		sortNodesDesc();

		// long duration = System.nanoTime() - start;
		// System.out.println("sortNodesDesc " + (duration / 1000000000.0) + " sec");

		int numOfCaches = caches.size();
		int firstNew = N;

		N += numNodes;
		if (N <= nodes.size()) {
			System.out.println("N <= nodes.size() after increment.");
			System.exit(0);
		}
		for (int i = 0; i < numNodes; i++) {
			IQNode n = null;
			if (itAdd == null) {
				n = new IQNode(IQNode.nextID.getAndIncrement(), notUsedIps.pop(), PORT);
			} else {
				n = new IQNode(itAdd.next(), notUsedIps.pop(), PORT);
			}
			// nodes.put(n.id, n);
			nodes.add(n);
			for (int j = 0; j < L; j++) {
				IQCache c = new IQCache(n.id + "-" + j, n);
				caches.add(c);
				n.caches.add(c);
			}
		}

		if (N > nodes.size()) {
			System.out.println("N > nodes.size() twice.");
			System.exit(0);
		}
		if (incConfig) {
			config++;
			updateIQConfig();
		}
		// start = System.nanoTime();
		// ArrayList<Integer> keys = new ArrayList<Integer>(nodes.keySet());
		// for (int i = 0, j = 0, k = 0, l = firstNew; i < (migPartitions * numNodes *
		// L); i++) {
		for (int i = 0, j = 0, l = numOfCaches; i < (migPartitions * numNodes * L); i++) {
			IQPartition p = null;
			try {
				p = caches.get(j).partitions.get(rand.nextInt(caches.get(j).partitions.size()));
			} catch (IllegalArgumentException e) {
				break;
			}
			caches.get(j).partitions.remove(p);
			// nodes.get(keys.get(l)).caches.get(k).partitions.add(p);
			caches.get(l).partitions.add(p);
			// caches.get(N - 1).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			// p.server = nodes.get(keys.get(l)).caches.get(k);
			p.server = caches.get(l);
			if (!noMigration) {
				p.status = PartitionStatus.Migration;
				p.resetGetCounter();
			}

			j++;
			if (j == numOfCaches) {
				j = 0;
			}

			l++;
			if (l == caches.size()) {
				l = numOfCaches;
			}
			// k++;
			// if (k == L) {
			// k = 0;
			// l++;
			// if (l == N) {
			// l = firstNew;
			// }
			// }
		}
		// duration = System.nanoTime() - start;
		// System.out.println("sortNodesDesc " + (duration / 1000000000.0) + " sec");
	}

	public void sortNodesDesc() {
		Collections.sort(caches, new Comparator<IQCache>() {
			@Override
			public int compare(IQCache o1, IQCache o2) {
				return Integer.compare(o2.partitions.size(), o1.partitions.size());
			}
		});

		// for (int i = 0; i < N * L; i++) {
		// for (int j = i + 1; j < N * L; j++) {
		// if (caches.get(i).partitions.size() < caches.get(j).partitions.size()) {
		// IQCache temp = caches.get(i);
		// caches.set(i, caches.get(j));
		// caches.set(j, temp);
		// }
		// }
		// }
	}

	private int calculateNumOfPartitionsToMigrate(int i) {
		return (int) Math.round(((double) numOfPartitions) / ((N + i) * L));
	}

	public void removeNode() {
		if (N == 1) {
			System.out.println("ERROR: only one node,,,, can't delete");
			return;
		}
		config++;
		updateIQConfig();
		ArrayList<IQPartition> partitionsToMigrate = sortNodesInc(1, null);

		int size = partitionsToMigrate.size();
		for (int i = 0, j = caches.size() - 1; i < size; i++) {
			IQPartition p = partitionsToMigrate.get(rand.nextInt(partitionsToMigrate.size()));
			partitionsToMigrate.remove(p);
			caches.get(j).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			p.server = caches.get(j);

			if (!noMigration) {
				p.status = PartitionStatus.Migration;
				p.resetGetCounter();
			}

			j--;
			if (j == -1) {
				j = caches.size() - 1;
			}
		}
	}

	public void removeMultipleNode(int numNodes, Iterator<Integer> itRemove, boolean incConfig) {
		if (incConfig) {
			config++;
			updateIQConfig();
		}
		ArrayList<IQPartition> partitionsToMigrate = sortNodesInc(numNodes, itRemove);

		// IQNode toBeRemoved = nodes.get(N);

		// long start = System.nanoTime();

		int size = partitionsToMigrate.size();
		for (int i = 0, j = caches.size() - 1; i < size; i++) {

			// long start1 = System.nanoTime();
			IQPartition p = partitionsToMigrate.get(partitionsToMigrate.size() - 1);
			// long start2 = System.nanoTime();
			// String res = "1: " + (start2 - start1);
			partitionsToMigrate.remove(partitionsToMigrate.size() - 1);
			// start1 = System.nanoTime();
			// res += ", 2: " + (start1 - start2);
			caches.get(j).partitions.add(p);
			// start2 = System.nanoTime();
			// res += ", 3: " + (start2 - start1);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			p.server = caches.get(j);

			if (!noMigration) {
				p.status = PartitionStatus.Migration;
				p.resetGetCounter();
			}
			// start1 = System.nanoTime();
			// res += ", 4: " + (start1 - start2);
			// System.out.println(res);

			j--;
			if (j == -1) {
				j = caches.size() - 1;
			}
			// if(i % (size/10) == 0) {
			// long duration = System.nanoTime() - start;
			// System.out.println(i+"/"+size+"): " + (duration / 1000000000.0) + " sec");
			// }
		}

		// long duration = System.nanoTime() - start;
		// System.out.println("sort caches: " + (duration / 1000000000.0) + " sec");
	}

	private ArrayList<IQPartition> sortNodesInc(int numOfNodesToRemove, Iterator<Integer> itRemove) {
		// long start = System.nanoTime();
		ArrayList<IQPartition> partitionsToMigrate = new ArrayList<>();
		for (int i = 0; i < numOfNodesToRemove; i++) {
			int toRemove = rand.nextInt(N);
			if (itRemove != null) {
				toRemove = itRemove.next();
				toRemove = getNode(toRemove);
			}
			IQNode ntoRemove = nodes.get(toRemove);
			// nodes.set(toRemove, nodes.get(N - 1));
			// nodes.set(N - 1, ntoRemove);

			for (IQCache c : ntoRemove.caches) {
				caches.remove(c);
				partitionsToMigrate.addAll(c.partitions);
			}
			notUsedIps.addLast(ntoRemove.ip);
			nodes.remove(toRemove);
			N--;

		}

		Collections.shuffle(partitionsToMigrate, rand);

		// long duration = System.nanoTime() - start;
		// System.out.println("remove nodes: Remove caches: " + (duration /
		// 1000000000.0) + " sec");

		// start = System.nanoTime();
		Collections.sort(caches, new Comparator<IQCache>() {
			@Override
			public int compare(IQCache o1, IQCache o2) {
				return Integer.compare(o2.partitions.size(), o1.partitions.size());
			}
		});

		// duration = System.nanoTime() - start;
		// System.out.println("sort caches: " + (duration / 1000000000.0) + " sec");

		// for (int i = 0; i < caches.size(); i++) {
		// for (int j = i + 1; j < caches.size(); j++) {
		// if (caches.get(i).partitions.size() > caches.get(j).partitions.size()) {
		// IQCache ctemp = caches.get(i);
		// caches.set(i, caches.get(j));
		// caches.set(j, ctemp);
		// }
		// }
		// }
		return partitionsToMigrate;
	}

	public void sortNodesInc() {

		for (int i = 0; i < caches.size(); i++) {
			for (int j = i + 1; j < caches.size(); j++) {
				if (caches.get(i).partitions.size() > caches.get(j).partitions.size()) {
					IQCache ctemp = caches.get(i);
					caches.set(i, caches.get(j));
					caches.set(j, ctemp);
				}
			}
		}
	}

	public GetOutputs get(int key, HashMap<String, MemcachedClient> mc) {
		GetOutputs out = new GetOutputs();

		IQPartition p = P[getHash(key)];
		IQGetResult result = null;
		try {
			result = (IQGetResult) mc.get(p.server.node.host).iqget(String.valueOf(key), Integer.MAX_VALUE, p.config);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
		if (result == null || result.getO() == null) {
			out.isMiss = true;
			try {
				setWithLatestValue(key, config, mc.get(p.server.node.host));
			} catch (IOException | IQException e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
		} else {
			out.value = (String) result.getO();
			out.config = result.getConfigId();
		}
		return out;
	}

	private void setWithLatestValue(int key, int cid, MemcachedClient mc) throws IOException, IQException {
		for (int temp_cid = cid; temp_cid > 0; temp_cid--) {
			if ((key + temp_cid) % IQWorkloadGenerator.keys_to_update_mod == 0) {
				mc.iqset(String.valueOf(key), IQWorkloadGenerator.getValue(key, temp_cid));
				return;
			}
		}
		mc.iqset(String.valueOf(key), IQWorkloadGenerator.getValue(key, initialConfig));
	}

	public void iqset(int key, String value, int config, HashMap<String, MemcachedClient> mc, boolean invalidate) {
		IQPartition p = P[getHash(key)];
		try {
			mc.get(p.server.node.host).set(String.valueOf(key), /* new ValueWrapper(value, config) */ value);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	public void set(int key, String value, int config, HashMap<String, MemcachedClient> mc, boolean invalidate) {
		IQPartition p = P[getHash(key)];
		try {
			mc.get(p.server.node.host).set(String.valueOf(key), /* new ValueWrapper(value, config) */ value);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	// public ValueWrapper get(int key) {
	// WorkloadGenerator.isMig = false;
	// WorkloadGenerator.isMig_invalid = false;
	// IQPartition p = P[getHash(key)];
	// ValueWrapper result = p.server.node.hashTable.get(key);
	//
	// if (noMigration) {
	// return result;
	// }
	// if (result == null) {
	// if (p.status == PartitionStatus.Migration) {
	// result = p.srcServer.node.hashTable.get(key);
	// if (result == null) {
	// return result;
	// } else {
	// if (isValidForMigration(key, result.config_num)) {
	// set(key, result.value, config, false);
	// result.config_num = config;
	// WorkloadGenerator.isMig = true;
	// } else {
	// result = null;
	// WorkloadGenerator.isMig_invalid = true;
	// }
	// if (deleteWithMigration)
	// p.srcServer.node.hashTable.remove(key);
	// }
	// }
	// }
	// p.incGetCounter();
	// return result;
	// }

	private boolean isValidForMigration(int key, int config_num) {
		IQPartition partition = P[getHash(key)];
		if (partition.srcConfig <= config_num)
			return true;
		return false;
	}

	// public void set(int key, String value, int config, boolean invalidate) {
	// IQPartition p = P[getHash(key)];
	// p.server.node.hashTable.put(key, new ValueWrapper(value, config));
	//
	// if (!noMigration) {
	// if (invalidate && p.status == PartitionStatus.Migration) {
	// p.srcServer.node.hashTable.remove(key);
	// }
	// }
	// }

	private int getHash(int key) {
		return key % P.length;
	}

	public int getN() {
		return N;
	}

	public void setN(int n) {
		N = n;
	}

	public String cachesPartitionsInfo() {
		int max = -1, min = Integer.MAX_VALUE, total = 0;
		for (IQCache c : caches) {
			total += c.partitions.size();
			if (c.partitions.size() > max) {
				max = c.partitions.size();
			}
			if (c.partitions.size() < min) {
				min = c.partitions.size();
			}
		}
		return "total: " + total + ", max: " + max + ", min: " + min;
	}

	public void debugPartitions() {
		for (int i = 0; i < P.length; i++) {
			IQPartition p = P[i];
			if (!p.server.partitions.contains(p)) {
				System.out.println();
			}
			if (!caches.contains(p.server)) {
				System.out.println();
			}
		}
		for (IQCache c : caches) {
			for (IQPartition p : c.partitions) {
				if (!p.server.equals(c)) {
					System.out.println();
				}
			}
		}
	}

	public void initVM(Set<Integer> vmAdd) {
		Iterator<Integer> itAdd = vmAdd.iterator();
		int tenP = (int) (vmAdd.size() * 0.1);
		int count = 0, total = vmAdd.size();
		while (itAdd.hasNext()) {
			IQNode n = new IQNode(itAdd.next(), notUsedIps.pop(), PORT);
			// nodes.put(i, n);
			nodes.add(n);
			for (int j = 0; j < L; j++) {
				IQCache c = new IQCache(n.id + "-" + j, n);
				caches.add(c);
				n.caches.add(c);
			}
			count++;
			if (count % tenP == 0)
				System.out.println(count + "/" + total);
		}
		sortNodes();
	}

	public void addRemoveVM(Set<Integer> vmAdd, Set<Integer> vmRemove) {
		Iterator<Integer> itAdd = vmAdd.iterator();
		Iterator<Integer> itRemove = vmRemove.iterator();
		ArrayList<IQNode> newnode = new ArrayList<>();
		config++;
		updateIQConfig();
		while (itAdd.hasNext() && itRemove.hasNext()) {
			int addKey = itAdd.next();
			int removeKey = itRemove.next();
			int nIndex = getNode(removeKey);
			if (nIndex == -1) {
				System.out.println("ERROR: could not find node with id = " + removeKey);
				System.exit(0);
			}
			IQNode n = nodes.get(nIndex);
			nodes.remove(nIndex);
			n.reset(addKey, config);
			newnode.add(n);
			// SortNode(n);
		}
		if (newnode.size() > 0) {
			for (IQNode n : newnode) {
				nodes.add(n);
			}
			sortNodes();
		}

		int nodesToAdd = vmAdd.size() - vmRemove.size();
		int nodesToRemove = vmRemove.size() - vmAdd.size();
		// N += nodesToAdd;

		if (itAdd.hasNext()) {
			addMultipleNodes(nodesToAdd, itAdd, false);
		}

		if (itRemove.hasNext()) {
			removeMultipleNode(nodesToRemove, itRemove, false);
		}
		sortNodes();
	}

	private void sortNodes() {
		Collections.sort(nodes, new Comparator<IQNode>() {

			@Override
			public int compare(IQNode o1, IQNode o2) {
				return Integer.compare(o1.id, o2.id);
			}
		});
	}

	private void SortNode(IQNode n) {
		// int sid = 0;
		// for (int eid = nodes.size() - 1; eid >= sid || eid == 0;) {
		// int mid = eid - sid;
		// if (n.id == nodes.get(mid).id) {
		// System.out.println("ERROR:");
		// System.exit(0);
		// } else if (n.id > nodes.get(mid).id) {
		// sid = mid + 1;
		// } else {
		// eid = mid - 1;
		// }
		// }
		// nodes.add(sid, n);
		sortNodes();
	}

	private int getNode(int id) {
		for (int sid = 0, eid = nodes.size() - 1; eid >= sid;) {
			int mid = eid - sid;
			if (id == nodes.get(mid).id) {
				return mid;
			} else if (id > nodes.get(mid).id) {
				sid = mid + 1;
			} else {
				eid = mid - 1;
			}
		}
		return -1;
	}

	private int getNode2(int id) {
		for (int sid = 0; sid < nodes.size(); sid++) {
			if (id == nodes.get(sid).id) {
				return sid;
			}
		}
		return -1;
	}

	// private void update(IQPartition[] oldP, IQPartition[] newP,
	// ArrayList<Integer>
	// arrayList, int newConfig) {
	// for (int i = 0; i < oldP.length; i++) {
	// if (oldP[i].server == newP[i].server) {
	// newP[i].config = oldP[i].config;
	// newP[i].srcServer = null;
	// newP[i].srcConfig = -1;
	// newP[i].status = PartitionStatus.Normal;
	// newP[i].resetGetCounter();
	// } else {
	// newP[i].config = newConfig;
	// newP[i].srcConfig = oldP[i].config;
	// newP[i].srcServer = oldP[i].server;
	// newP[i].status = PartitionStatus.Migration;
	// newP[i].resetGetCounter();
	// }
	// }
	// }

}
