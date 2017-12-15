package edu.usc.iqsystem;

public class IQPartition {

	private int id;
	public int config;
	public int srcConfig;
	// public int serverId;
	// public int srcServerId;
	public IQCache server;
	public IQCache srcServer;
	public PartitionStatus status;
	public boolean dirty_key_pending = false;
	public boolean skip_config_check = false;

	private int get_counter;

	public enum PartitionStatus {
		Normal, Migration
	}

	public IQPartition(int i) {
		id = i;
	}

	public void resetGetCounter() {
		get_counter = 0;
	}

	public void incGetCounter() {
		if (status == PartitionStatus.Migration) {
			get_counter++;
			if (get_counter == IQSystem.MAX_GET_PER_MIGRATION) {
				status = PartitionStatus.Normal;
				get_counter = 0;
				srcServer = null;
				srcConfig = -1;
			}
		}
	}

	public String toString() {
		return id + "[config: " + config + ", " + status.toString() + ", " + (status == PartitionStatus.Migration
				? (srcServer.id + " -> " + server.id + ", " + get_counter + "/" + IQSystem.MAX_GET_PER_MIGRATION)
				: server.id) + "]";
	}

	public void terminateMigration() {
		status = PartitionStatus.Normal;
		get_counter = 0;
		srcServer = null;
		srcConfig = -1;
	}
}
