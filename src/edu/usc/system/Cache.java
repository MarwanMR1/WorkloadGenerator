package edu.usc.system;

import java.util.ArrayList;

public class Cache {
	public String id;
	public ArrayList<Partition> partitions;
	public Node node;
	
	public Cache(final String id, Node node) {
		this.id = id;
		this.node = node;
		partitions = new ArrayList<>();
	}
	
	public String toString() {
		return id+"[numP: " + partitions.size()+"]";
	}
	
	public void updateID(int nodeID) {
		id = nodeID + id.substring(id.indexOf("-"));
	}
}
