package edu.usc.workload_generator;

import java.util.ArrayList;

import edu.usc.system.Configuration;
import edu.usc.system.Partition;
import edu.usc.system.TheSystem;
import edu.usc.system.ValueWrapper;

public class WorkloadGenerator {

	public enum ReconfigurationType {
		ADD_NODE, REMOVE_NODE, ADD_M_NODES, REMOVE_M_NODES, NOTHING
	}

	private int N, L, K, number_Of_Iterations, keys_to_update_mod, numOfPartitions;
	private final int initialConfig = 0;
	private float kuP, q, qBase;
	//	private Random rand;
	private ArrayList<ReconfigurationType> op;
	public static int addM, removeM, numOfRounds;
	public static boolean isMig;
	public static boolean isMig_invalid;
	public static boolean Q;

	//	private HashMap<Integer, GetOutput> cache = new HashMap<>();

	public WorkloadGenerator(int N, int L, int K, float kuP, int numOfPartitions, int number_Of_Iterations,
			ArrayList<ReconfigurationType> op) {
		this.N = N;
		this.L = L;
		this.K = K;
		this.kuP = kuP;
		this.number_Of_Iterations = number_Of_Iterations;
		this.numOfPartitions = numOfPartitions;
		//		rand = new Random(1);
		this.op = op;
		if(Q) {
			q = 0.1f;
			qBase = 0.1f;
		}
	}

	public Outputs run() {
		check_input();

		//		long start = System.nanoTime();
		TheSystem sys = new TheSystem(N, L, numOfPartitions);
		//		long duration = System.nanoTime() - start;
		//		System.out.println("init TheSystem " + (duration / 1000000000.0) + " sec");

		Configuration con = sys.getConfiguration();

		//		start = System.nanoTime();
		for (int key = 0; key < K; key++) {
			sys.set(key, getValue(key, con.config), con.config, true);
			if (key % (K / 10) == 0) {
				System.out.println("Completed " + key + " out of " + K);
			}
		}
		//		duration = System.nanoTime() - start;
		//		System.out.println("init set " + (duration / 1000000000.0) + " sec");

		keys_to_update_mod = Math.round(1 / kuP);

		Outputs total = new Outputs();

		System.out.println("Round,Action,Valid,Lost,Lost(stale),miss,migrated,not migrated");
		int loop_size = Q ? numOfRounds : op.size();

		for (int loop = 0; loop < loop_size; loop++) {
			String actionName = "";
			if(Q) {
				q = q + qBase;
				if(q >= 1 || q <= 0.1) {
					qBase *= -1;
				}
			}
			ReconfigurationType action = getAction(loop, sys.getN());
			switch (action) {//rand.nextInt(2)
			case ADD_NODE:
				//				start = System.nanoTime();
				sys.addNode();
				//				duration = System.nanoTime() - start;
				//				System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "addNode";
				break;
			case REMOVE_NODE:
				//				start = System.nanoTime();
				sys.removeNode();
				//				duration = System.nanoTime() - start;
				//				System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "removeNode";
				break;
			case ADD_M_NODES:
				//				start = System.nanoTime();
				sys.addMultipleNodes(addM);
				//				duration = System.nanoTime() - start;
				//				System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "addM-" + addM;
				break;
			case REMOVE_M_NODES:
				//				start = System.nanoTime();
				sys.removeMultipleNode(removeM);
				//				duration = System.nanoTime() - start;
				//				System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "removeM-" + removeM;
				break;
			case NOTHING:
				actionName = "noAction";
				break;
			}
			System.out.print(loop + "," + actionName + ",");
			if(Q) {
				System.out.print(q + ",");
			}
			con = sys.getConfiguration();
			Outputs result = perform_Operations(sys, con.P, K, con.config);
			total.add(result);
		}

		return total;
	}

	private ReconfigurationType getAction(int round, int numOfNodes) {
		if (Q) {
			if (q > 0.8f) {
				return ReconfigurationType.ADD_NODE;
			} else if (q <= 0.3) {
				if (numOfNodes > 1) {
					return ReconfigurationType.REMOVE_NODE;
				} else {
					return ReconfigurationType.NOTHING;
				}
			} else {
				return ReconfigurationType.NOTHING;
			}
		} else {
			return op.get(round);
		}
	}

	private Outputs perform_Operations(TheSystem sys, Partition[] P, int K, int cid) {
		Outputs result = new Outputs();
		int debugKey = -1; //TODO for debugging
		boolean valid = true;
		for (int key = 0; key < K; key++) {
			valid = true;
			ValueWrapper getOutput = sys.get(key);
			if (key == debugKey) {
				System.out.print("key " + key + ", P" + P[getHash(key)] + ", value" + getOutput);
			}
			if (isMig) {
				result.mig++;
			}
			if (isMig_invalid) {
				result.mig_invalid++;
			}
			if (getOutput == null) {
				if (key == debugKey) {
					System.out.print(" ... is null");
				}
				result.miss_keys++;
				setWithLatestValue(sys, key, cid);
				valid = false;
			} else if (is_lost(key, getOutput.config_num, P)) {
				valid = false;
				result.lost_keys++;
				if (key == debugKey) {
					System.out.print(" ... is lost");
				}
				if (is_stale(key, cid - 1, getOutput.value)) {
					result.lost_key_stales++;
					if (key == debugKey) {
						System.out.print(" ... is stale");
					}
				}
			} else if (is_stale(key, cid - 1, getOutput.value)) {
				valid = false;
				//Exit debug
				if (key == debugKey) {
					System.out.println(" ... is stale");
				}
				System.out.println("ERROR: stale value that is not lost in key " + key);
				System.exit(0);
			}
			if (key == debugKey) {
				System.out.println();
			}
			if (valid) {
				result.valid_keys++;
			}
		}
		for (int key = 0; key < K; key++) {
			if ((key + cid) % keys_to_update_mod == 0)
				sys.set(key, getValue(key, cid), cid, true);
		}

		for (int key = 0; key < K; key++) {
			if ((key + cid) % keys_to_update_mod != 0)
				continue;
			ValueWrapper getOutput = sys.get(key);
			if (is_stale(key, cid, getOutput.value)) {
				//Exit debug
				System.out.println("ERROR: stale value for a key we just set");
				System.exit(0);
			} else if (is_lost(key, getOutput.config_num, P)) {
				//Exit debug
				System.out.println("ERROR: lost key for a key we just set");
				System.exit(0);
			}
		}

		//		System.out.println(String.format("Valid: %d, Lost: %d (Stale: %d), null: %d", result.valid_keys, result.lost_keys, result.lost_key_stales,
		//				result.miss_keys));
		System.out.println(String.format("%d,%d,%d,%d,%d,%d", result.valid_keys, result.lost_keys,
				result.lost_key_stales, result.miss_keys, result.mig, result.mig_invalid));

		return result;
	}

	private void setWithLatestValue(TheSystem sys, int key, int cid) {
		for (int temp_cid = cid; temp_cid > 0; temp_cid--) {
			if ((key + temp_cid) % keys_to_update_mod == 0) {
				sys.set(key, getValue(key, temp_cid), cid, true);
				return;
			}
		}
		sys.set(key, getValue(key, initialConfig), cid, true);
	}

	private boolean is_stale(int key, int cid, String value) {
		for (int temp_cid = cid; temp_cid > 0; temp_cid--) {
			if ((key + temp_cid) % keys_to_update_mod == 0)
				return !(value.equals(getValue(key, temp_cid)));
		}
		if (value.equals(getValue(key, initialConfig)))
			return false;
		System.out.println("ERROR in is_stale: could not find source of set.");
		System.out.println("key: " + key + ", cid: " + cid + ", value: " + value);
		System.exit(0);
		return true;
	}

	private String getValue(int key, int cid) {
		StringBuilder br = new StringBuilder();
		br.append(key);
		br.append("_");
		br.append(cid);
		return br.toString();
	}

	private boolean is_lost(int key, int key_config, Partition[] P) {
		Partition partition = P[getHash(key)];
		if (partition.config <= key_config)
			return false;
		return true;
	}

	private int getHash(int key) {
		return key % numOfPartitions;
	}

	private void check_input() {
		if (N < 1) {
			System.out.println("ERROR in input: N is less than 1. N = " + N);
			System.exit(0);
		}
		if (L < 1) {
			System.out.println("ERROR in input: L is less than 1. L = " + L);
			System.exit(0);
		}
		if (K < 1) {
			System.out.println("ERROR in input: K is less than 1. K = " + K);
			System.exit(0);
		}
		if (kuP > 1.0 || kuP < 0.0) {
			System.out.println("ERROR in input: kuP must be between 0 and 1. kuP = " + kuP);
			System.exit(0);
		}
		if (number_Of_Iterations < 1) {
			System.out.println("ERROR in input: number_Of_Iterations is less than 1. number_Of_Iterations = "
					+ number_Of_Iterations);
			System.exit(0);
		}
	}

	//	private GetOutput get(int key) { 
	//		return cache.get(key);
	//	}
	//
	//	private void set(int key, String value, int cid) { 
	//		GetOutput v = new GetOutput(value, cid);
	//		cache.put(key, v);
	//	}

	//	private ConfigOutput get_Configurations() {
	//		ConfigOutput conf = new ConfigOutput();
	//		conf.cid = initialConfig;
	//		conf.P = new Partition[N * L];
	//		for (int i = 0; i < conf.P.length; i++) {
	//			conf.P[i] = new Partition();
	//			conf.P[i].config_num = conf.cid;
	//		}
	//		return conf;
	//	}

}
