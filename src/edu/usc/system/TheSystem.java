package edu.usc.system;

import java.util.ArrayList;
import java.util.Random;

import edu.usc.system.Partition.PartitionStatus;
import edu.usc.workload_generator.WorkloadGenerator;

public class TheSystem {

	public static int MAX_GET_PER_MIGRATION;
	private int N;
	private int L;
	private ArrayList<Node> nodes = new ArrayList<>();
	private ArrayList<Cache> caches = new ArrayList<>();
	private Partition[] P;
	private final int initialConfig = 0;
	private int config = initialConfig;
	private Random rand = new Random(1);
	private int numOfPartitions;

	private boolean deleteWithMigration = false;

	public TheSystem(int N, int L, int numOfPartitions) {
		this.N = N;
		this.numOfPartitions = numOfPartitions;
		this.L = L;

		for (int i = 0; i < N; i++) {
			Node n = new Node(i);
			nodes.add(n);
			for (int j = 0; j < L; j++) {
				Cache c = new Cache(i + "-" + j);
				caches.add(c);
				n.caches.add(c);
			}
		}

		P = new Partition[numOfPartitions];

		for (int i = 0; i < P.length; i++) {
			P[i] = new Partition(i);
			int serverIndex = i % caches.size();
			P[i].server = caches.get(serverIndex);
			P[i].config = initialConfig;
			P[i].status = PartitionStatus.Normal;
			P[i].srcServer = null;
			P[i].srcConfig = -1;
			P[i].server.partitions.add(P[i]);
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
		Node n = new Node(nodes.size());
		nodes.add(n);
		for (int j = 0; j < L; j++) {
			Cache c = new Cache(n.id + "-" + j);
			caches.add(c);
			n.caches.add(c);
		}

		if (N > nodes.size()) {
			System.out.println("N > nodes.size() twice.");
			System.exit(0);
		}
		config++;

		for (int i = 0, j = 0, k = 0; i < migPartitions; i++) {
			Partition p = caches.get(j).partitions.get(rand.nextInt(caches.get(j).partitions.size()));
			caches.get(j).partitions.remove(p);
			nodes.get(N - 1).caches.get(k).partitions.add(p);
			//			caches.get(N - 1).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			p.server = nodes.get(N - 1).caches.get(k);
			p.status = PartitionStatus.Migration;
			p.resetGetCounter();

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

	public void addMultipleNodes(int numNodes) {
		int migPartitions = calculateNumOfPartitionsToMigrate(numNodes);
		sortNodesDesc();

		int numOfCaches = caches.size();
		int firstNew = N;

		N += numNodes;
		if (N <= nodes.size()) {
			System.out.println("N <= nodes.size() after increment.");
			System.exit(0);
		}
		for (int i = 0; i < numNodes; i++) {
			Node n = new Node(nodes.size());
			nodes.add(n);
			for (int j = 0; j < L; j++) {
				Cache c = new Cache(n.id + "-" + j);
				caches.add(c);
				n.caches.add(c);
			}
		}

		if (N > nodes.size()) {
			System.out.println("N > nodes.size() twice.");
			System.exit(0);
		}
		config++;

		for (int i = 0, j = 0, k = 0, l = firstNew; i < (migPartitions*numNodes); i++) {
			Partition p = caches.get(j).partitions.get(rand.nextInt(caches.get(j).partitions.size()));
			caches.get(j).partitions.remove(p);
			nodes.get(l).caches.get(k).partitions.add(p);
			//			caches.get(N - 1).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			p.server = nodes.get(N - 1).caches.get(k);
			p.status = PartitionStatus.Migration;
			p.resetGetCounter();

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
	}

	private void sortNodesDesc() {
		for (int i = 0; i < N * L; i++) {
			for (int j = i + 1; j < N * L; j++) {
				if (caches.get(i).partitions.size() < caches.get(j).partitions.size()) {
					Cache temp = caches.get(i);
					caches.set(i, caches.get(j));
					caches.set(j, temp);
				}
			}
		}
	}

	private int calculateNumOfPartitionsToMigrate(int i) {
		return numOfPartitions / (N + i);
	}

	public void removeNode() {
		if(N == 1) {
			System.out.println("ERROR: only one node,,,, can't delete");
			return;
		}
		config++;
		ArrayList<Partition> partitionsToMigrate = sortNodesInc(1);

		int size = partitionsToMigrate.size();
		for (int i = 0, j = 0; i < size; i++) {
			Partition p = partitionsToMigrate.get(rand.nextInt(partitionsToMigrate.size()));
			partitionsToMigrate.remove(p);
			caches.get(j).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			p.server = caches.get(j);
			p.status = PartitionStatus.Migration;
			p.resetGetCounter();

			j++;
			if (j == caches.size()) {
				j = 0;
			}
		}
	}

	public void removeMultipleNode(int numNodes) {
		config++;
		ArrayList<Partition> partitionsToMigrate = sortNodesInc(numNodes);

//		Node toBeRemoved = nodes.get(N);
		
		int size = partitionsToMigrate.size();
		for (int i = 0, j = 0; i < size; i++) {
			Partition p = partitionsToMigrate.get(rand.nextInt(partitionsToMigrate.size()));
			partitionsToMigrate.remove(p);
			caches.get(j).partitions.add(p);
			p.srcConfig = p.config;
			p.config = config;
			p.srcServer = p.server;
			p.server = caches.get(j);
			p.status = PartitionStatus.Migration;
			p.resetGetCounter();

			j++;
			if (j == caches.size()) {
				j = 0;
			}
		}
	}

	private ArrayList<Partition> sortNodesInc(int numOfNodesToRemove) {

		ArrayList<Partition> partitionsToMigrate = new ArrayList<>();
		for (int i = 0; i < numOfNodesToRemove; i++) {
			int toRemove = rand.nextInt(N);
			Node ntoRemove = nodes.get(toRemove);
//			nodes.set(toRemove, nodes.get(N - 1));
//			nodes.set(N - 1, ntoRemove);

			for (Cache c : ntoRemove.caches) {
				caches.remove(c);
				partitionsToMigrate.addAll(c.partitions);
			}
			nodes.remove(ntoRemove);
			N--;

		}

		for (int i = 0; i < caches.size(); i++) {
			for (int j = i + 1; j < caches.size(); j++) {
				if (caches.get(i).partitions.size() > caches.get(j).partitions.size()) {
					Cache ctemp = caches.get(i);
					caches.set(i, caches.get(j));
					caches.set(j, ctemp);
				}
			}
		}
		return partitionsToMigrate;
	}

	public ValueWrapper get(int key) {
		WorkloadGenerator.isMig = false;
		WorkloadGenerator.isMig_invalid = false;
		Partition p = P[getHash(key)];
		ValueWrapper result = p.server.hashTable.get(key);
		if (result == null) {
			if (p.status == PartitionStatus.Migration) {
				result = p.srcServer.hashTable.get(key);
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
						p.srcServer.hashTable.remove(key);
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
		p.server.hashTable.put(key, new ValueWrapper(value, config));
		if (invalidate && p.status == PartitionStatus.Migration) {
			p.srcServer.hashTable.remove(key);
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

//	private void update(Partition[] oldP, Partition[] newP, ArrayList<Integer> arrayList, int newConfig) {
//		for (int i = 0; i < oldP.length; i++) {
//			if (oldP[i].server == newP[i].server) {
//				newP[i].config = oldP[i].config;
//				newP[i].srcServer = null;
//				newP[i].srcConfig = -1;
//				newP[i].status = PartitionStatus.Normal;
//				newP[i].resetGetCounter();
//			} else {
//				newP[i].config = newConfig;
//				newP[i].srcConfig = oldP[i].config;
//				newP[i].srcServer = oldP[i].server;
//				newP[i].status = PartitionStatus.Migration;
//				newP[i].resetGetCounter();
//			}
//		}
//	}

}
