package edu.usc.system;

import java.util.ArrayList;
import java.util.HashMap;
//import java.util.Random;

public class Node {
	public ArrayList<Cache> caches;
	public int id;
	public boolean isAlive;
//	private Random rand;
	public HashMap<Integer, ValueWrapper> hashTable;

	public Node(final int id) {
		this.id = id;
		isAlive = true;
		caches = new ArrayList<>();
//		rand = new Random(1);
		hashTable = new HashMap<>();
	}

	public String toString() {
		return id + "[" + (isAlive ? "ON" : "OFF") + "]";
	}

	public void reset(int addKey, int config) {
		id = addKey;
		hashTable.clear();
		for(Cache c : caches) {
			c.updateID(id);
			for(Partition p : c.partitions) {
				p.config = config;
			}
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
