package edu.usc.system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import edu.usc.system.Partition.PartitionStatus;
import edu.usc.workload_generator.WorkloadGenerator;
import edu.usc.workload_generator.WorkloadGenerator.Trace;

public class TheSystem {

	public static int MAX_GET_PER_MIGRATION;
	private int N;
	private int L;
	// public HashMap<Integer, Node> nodes = new HashMap<>();
	public ArrayList<Node> nodes = new ArrayList<>();
	public ArrayList<Cache> caches = new ArrayList<>();
	public Partition[] P;
	private final int initialConfig = 0;
	private int config = initialConfig;
	private Random rand = new Random(1);
	private int numOfPartitions;
	private int nodeID = 0;
	public static final boolean noMigration = true;

	private boolean deleteWithMigration = false;

	public TheSystem(int N, int L, int numOfPartitions, Trace trace, Set<Integer> vmAdd, Set<Integer> vmRemove) {
		this.N = N;
		this.numOfPartitions = numOfPartitions;
		this.L = L;

		if (trace == Trace.AzureVM || trace == Trace.GoogleVM) {
			initVM(vmAdd);
		} else {
			for (int i = 0; i < N; i++) {
				Node n = new Node(nodeID++);
				// nodes.put(i, n);
				nodes.add(n);
				for (int j = 0; j < L; j++) {
					Cache c = new Cache(i + "-" + j, n);
					caches.add(c);
					n.caches.add(c);
				}
			}
		}

		P = new Partition[numOfPartitions];
		int tenP = P.length / 10;
		for (int i = 0; i < P.length; i++) {
			P[i] = new Partition(i);
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

	public Configuration getConfiguration() {
		return new Configuration(P, config);
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
		int newid = nodeID++;
		Node n = new Node(newid);
		// nodes.put(newid, n);
		nodes.add(n);
		for (int j = 0; j < L; j++) {
			Cache c = new Cache(n.id + "-" + j, n);
			caches.add(c);
			n.caches.add(c);
		}

		if (N > nodes.size()) {
			System.out.println("N > nodes.size() twice.");
			System.exit(0);
		}
		config++;

		for (int i = 0, j = 0, k = 0; i < migPartitions; i++) {
			Partition p = null;
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
			Node n = null;
			if (itAdd == null) {
				n = new Node(nodeID++);
			} else {
				n = new Node(itAdd.next());
			}
			// nodes.put(n.id, n);
			nodes.add(n);
			for (int j = 0; j < L; j++) {
				Cache c = new Cache(n.id + "-" + j, n);
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
		}
		// start = System.nanoTime();
		// ArrayList<Integer> keys = new ArrayList<Integer>(nodes.keySet());
		for (int i = 0, j = 0, k = 0, l = firstNew; i < (migPartitions * numNodes); i++) {
			Partition p = null;
			try {
				p = caches.get(j).partitions.get(rand.nextInt(caches.get(j).partitions.size()));
			} catch (IllegalArgumentException e) {
				break;
			}
			caches.get(j).partitions.remove(p);
			// nodes.get(keys.get(l)).caches.get(k).partitions.add(p);
			nodes.get(l).caches.get(k).partitions.add(p);
			// caches.get(N - 1).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			// p.server = nodes.get(keys.get(l)).caches.get(k);
			p.server = nodes.get(l).caches.get(k);
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
				l++;
				if (l == N) {
					l = firstNew;
				}
			}
		}
		// duration = System.nanoTime() - start;
		// System.out.println("sortNodesDesc " + (duration / 1000000000.0) + " sec");
	}

	public void sortNodesDesc() {
		Collections.sort(caches, new Comparator<Cache>() {
			@Override
			public int compare(Cache o1, Cache o2) {
				return Integer.compare(o1.partitions.size(), o1.partitions.size());
			}
		});

		// for (int i = 0; i < N * L; i++) {
		// for (int j = i + 1; j < N * L; j++) {
		// if (caches.get(i).partitions.size() < caches.get(j).partitions.size()) {
		// Cache temp = caches.get(i);
		// caches.set(i, caches.get(j));
		// caches.set(j, temp);
		// }
		// }
		// }
	}

	private int calculateNumOfPartitionsToMigrate(int i) {
		return (int) Math.round(((double) numOfPartitions) / (N + i));
	}

	public void removeNode() {
		if (N == 1) {
			System.out.println("ERROR: only one node,,,, can't delete");
			return;
		}
		config++;
		ArrayList<Partition> partitionsToMigrate = sortNodesInc(1, null);

		int size = partitionsToMigrate.size();
		for (int i = 0, j = caches.size() - 1; i < size; i++) {
			Partition p = partitionsToMigrate.get(rand.nextInt(partitionsToMigrate.size()));
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
		}
		ArrayList<Partition> partitionsToMigrate = sortNodesInc(numNodes, itRemove);

		// Node toBeRemoved = nodes.get(N);

		// long start = System.nanoTime();

		int size = partitionsToMigrate.size();
		for (int i = 0, j = 0; i < size; i++) {

			// long start1 = System.nanoTime();
			Partition p = partitionsToMigrate.get(partitionsToMigrate.size() - 1);
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

			j++;
			if (j == caches.size()) {
				j = 0;
			}
			// if(i % (size/10) == 0) {
			// long duration = System.nanoTime() - start;
			// System.out.println(i+"/"+size+"): " + (duration / 1000000000.0) + " sec");
			// }
		}

		// long duration = System.nanoTime() - start;
		// System.out.println("sort caches: " + (duration / 1000000000.0) + " sec");
	}

	private ArrayList<Partition> sortNodesInc(int numOfNodesToRemove, Iterator<Integer> itRemove) {
		// long start = System.nanoTime();
		ArrayList<Partition> partitionsToMigrate = new ArrayList<>();
		for (int i = 0; i < numOfNodesToRemove; i++) {
			int toRemove = rand.nextInt(N);
			if (itRemove != null) {
				toRemove = itRemove.next();
				toRemove = getNode(toRemove);
			}
			Node ntoRemove = nodes.get(toRemove);
			// nodes.set(toRemove, nodes.get(N - 1));
			// nodes.set(N - 1, ntoRemove);

			for (Cache c : ntoRemove.caches) {
				caches.remove(c);
				partitionsToMigrate.addAll(c.partitions);
			}
			nodes.remove(toRemove);
			N--;

		}

		Collections.shuffle(partitionsToMigrate, rand);

		// long duration = System.nanoTime() - start;
		// System.out.println("remove nodes: Remove caches: " + (duration /
		// 1000000000.0) + " sec");

		// start = System.nanoTime();
		Collections.sort(caches, new Comparator<Cache>() {
			@Override
			public int compare(Cache o1, Cache o2) {
				return Integer.compare(o1.partitions.size(), o1.partitions.size());
			}
		});

		// duration = System.nanoTime() - start;
		// System.out.println("sort caches: " + (duration / 1000000000.0) + " sec");

		// for (int i = 0; i < caches.size(); i++) {
		// for (int j = i + 1; j < caches.size(); j++) {
		// if (caches.get(i).partitions.size() > caches.get(j).partitions.size()) {
		// Cache ctemp = caches.get(i);
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
					Cache ctemp = caches.get(i);
					caches.set(i, caches.get(j));
					caches.set(j, ctemp);
				}
			}
		}
	}

	public ValueWrapper get(int key) {
		WorkloadGenerator.isMig = false;
		WorkloadGenerator.isMig_invalid = false;
		Partition p = P[getHash(key)];
		ValueWrapper result = p.server.node.hashTable.get(key);
		
		if (noMigration) {
			return result;
		}
		if (result == null) {
			if (p.status == PartitionStatus.Migration) {
				result = p.srcServer.node.hashTable.get(key);
				if (result == null) {
					return result;
				} else {
					if (isValidForMigration(key, result.config_num)) {
						set(key, result.value, config, false);
						result.config_num = config;
						WorkloadGenerator.isMig = true;
					} else {
						result = null;
						WorkloadGenerator.isMig_invalid = true;
					}
					if (deleteWithMigration)
						p.srcServer.node.hashTable.remove(key);
				}
			}
		}
		p.incGetCounter();
		return result;
	}

	private boolean isValidForMigration(int key, int config_num) {
		Partition partition = P[getHash(key)];
		if (partition.srcConfig <= config_num)
			return true;
		return false;
	}

	public void set(int key, String value, int config, boolean invalidate) {
		Partition p = P[getHash(key)];
		p.server.node.hashTable.put(key, new ValueWrapper(value, config));

		if (!noMigration) {
			if (invalidate && p.status == PartitionStatus.Migration) {
				p.srcServer.node.hashTable.remove(key);
			}
		}
	}

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
		for (Cache c : caches) {
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
			Partition p = P[i];
			if (!p.server.partitions.contains(p)) {
				System.out.println();
			}
			if (!caches.contains(p.server)) {
				System.out.println();
			}
		}
		for (Cache c : caches) {
			for (Partition p : c.partitions) {
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
			Node n = new Node(itAdd.next());
			// nodes.put(i, n);
			nodes.add(n);
			for (int j = 0; j < L; j++) {
				Cache c = new Cache(n.id + "-" + j, n);
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
		ArrayList<Node> newnode = new ArrayList<>();
		config++;
		while (itAdd.hasNext() && itRemove.hasNext()) {
			int addKey = itAdd.next();
			int removeKey = itRemove.next();
			int nIndex = getNode(removeKey);
			if (nIndex == -1) {
				System.out.println("ERROR: could not find node with id = " + removeKey);
				System.exit(0);
			}
			Node n = nodes.get(nIndex);
			nodes.remove(nIndex);
			n.reset(addKey, config);
			newnode.add(n);
			// SortNode(n);
		}
		if (newnode.size() > 0) {
			for (Node n : newnode) {
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
		Collections.sort(nodes, new Comparator<Node>() {

			@Override
			public int compare(Node o1, Node o2) {
				return Integer.compare(o1.id, o2.id);
			}
		});
	}

	private void SortNode(Node n) {
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

	// private void update(Partition[] oldP, Partition[] newP, ArrayList<Integer>
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
