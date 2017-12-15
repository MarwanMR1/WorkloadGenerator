package edu.usc.iqsystem;

import java.util.ArrayList;

public class IQCache {

	public String id;
	public ArrayList<IQPartition> partitions;
	public IQNode node;
	
	public IQCache(final String id, IQNode node) {
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
