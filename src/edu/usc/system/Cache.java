package edu.usc.system;

import java.util.ArrayList;
import java.util.HashMap;

public class Cache {
	public final String id;
	public HashMap<Integer, ValueWrapper> hashTable;
	public ArrayList<Partition> partitions;
	
	public Cache(final String id) {
		this.id = id;
		hashTable = new HashMap<>();
		partitions = new ArrayList<>();
	}
	
	public String toString() {
		return id+"[numP: " + partitions.size()+"]";
	}
}
