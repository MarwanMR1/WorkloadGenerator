package edu.usc.workload_generator;

public class Outputs {
	public long lost_keys;
	public long lost_key_stales;
	public long valid_keys;
	public long miss_keys;
	public long mig;
	public long mig_invalid;
	
	public long gets;
	public long sets;

	public Outputs() {
		lost_keys = 0;
		lost_key_stales = 0;
		valid_keys = 0;
		miss_keys = 0;
		mig = 0;
		mig_invalid = 0;
		
		gets = 0;
		sets = 0;
	}

	public void add(Outputs input) {
		this.lost_keys += input.lost_keys;
		this.lost_key_stales += input.lost_key_stales;
		this.valid_keys += input.valid_keys;
		this.miss_keys += input.miss_keys;
		this.mig += input.mig;
		this.mig_invalid += input.mig_invalid;
		
		this.gets += input.gets;
		this.sets += input.sets;
	}

	public String toString() {
		return "lost_keys: " + lost_keys + ", stales: " + lost_key_stales;
	}

	public void print() {
		double total = valid_keys + lost_keys + miss_keys;
		double vP = valid_keys * 100.0 / total;
		double lP = lost_keys * 100.0 / total;
		double sP = lost_key_stales * 100.0 / lost_keys;
		double mP = miss_keys * 100.0 / total;
		double migTotal = mig + mig_invalid;
		double migP = mig * 100.0 / migTotal;
		double migInvP = mig_invalid * 100.0 / migTotal;
		System.out.println();
		System.out.println("Total number of valid keys," + valid_keys + "," + vP + "%");
		System.out.println("Total number of lost keys," + lost_keys + "," + lP + "%");
		System.out.println("Total stale lost keys," + lost_key_stales + "," + sP + "%");
		System.out.println("Total number of miss keys," + miss_keys + "," + mP + "%");
//		System.out.println("Total number of migrated keys," + mig + "," + migP + "%");
//		System.out.println("Total number of keys not migrated due invalid configId," + mig_invalid + "," + migInvP + "%");
	}
}
