package edu.usc.workload_generator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import edu.usc.main.MainClass;
import edu.usc.system.Configuration;
import edu.usc.system.Partition;
import edu.usc.system.TheSystem;
import edu.usc.system.ValueWrapper;
import edu.usc.vmevent.traces.AzureVMEventTrace;
import edu.usc.vmevent.traces.AzureVMEventTrace.VMEvent;
import edu.usc.vmevent.traces.GoogleVMEventTrace;
import edu.usc.workload.traces.WorkloadTrace;
import edu.usc.workload.traces.WorkloadTrace.Stats;
import edu.usc.workload.traces.WorkloadTrace.WCRequest;

public class WorkloadGenerator {

	public enum ReconfigurationType {
		ADD_NODE, REMOVE_NODE, ADD_M_NODES, REMOVE_M_NODES, NOTHING
	}

	public enum Trace {
		AzureVM, GoogleVM, Wikipedia, WC98All, WC98Daily
	}

	private int Nn, L, K, qps, number_Of_Iterations, keys_to_update_mod, numOfPartitions, q, qBase;
	private final int initialConfig = 0;
	private float kuP;
	private double w;
	// private Random rand;
	private ArrayList<ReconfigurationType> op;
	public static int addM, removeM, numOfRounds;
	public static boolean isMig;
	public static boolean isMig_invalid;
	public static boolean Q;
	public static boolean W;
	public static WorkloadTrace wt;
	public static int WORKLOAD;
	public static Trace trace;
	public static AzureVMEventTrace azure;
	public static GoogleVMEventTrace google;
	private String outputFileLocation;
	private Optional<Stats> wtObject;
	private final int BATCH_SIZE = 1000;
	private boolean firstTime = true;

	// private HashMap<Integer, GetOutput> cache = new HashMap<>();

	public WorkloadGenerator(int N, int L, int K, float kuP, int numOfPartitions, int number_Of_Iterations,
			ArrayList<ReconfigurationType> op, String outputFileLocation) {
		this.Nn = N;
		this.L = L;
		this.K = K;
		this.kuP = kuP;
		this.number_Of_Iterations = number_Of_Iterations;
		this.numOfPartitions = numOfPartitions;
		this.outputFileLocation = outputFileLocation;
		// rand = new Random(1);
		this.op = op;
		if (Q) {
			q = 1;
			qBase = 1;
		}
	}

	public Outputs run() {
		check_input();

		PrintWriter out = null;
		try {
			out = new PrintWriter(outputFileLocation);
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		}

		if (W) {
			wtObject = wt.getNextStats();
			if (wtObject.isPresent()) {
				Stats stats = wtObject.get();
				w = stats.getQpsFactor();
				qps = stats.getQps();
				Nn = (int) Math.round(((double) qps) / MainClass.C);
				if(Nn == 0) {
					Nn = 1;
				}

				System.out.println("UPDATE init values");
				System.out.println("N: " + Nn);
			} else {
				System.out.println("ERROR: wtObject isEmpty in the first check");
				System.exit(0);
			}

		}
		// long start = System.nanoTime();
		TheSystem sys = new TheSystem(Nn, L, numOfPartitions);
		// long duration = System.nanoTime() - start;
		// System.out.println("init TheSystem " + (duration / 1000000000.0) + " sec");

		Configuration con = sys.getConfiguration();

		// start = System.nanoTime();
		for (int key = 0; key < K; key++) {
			sys.set(key, getValue(key, con.config), con.config, true);
			if (key % (K / 10) == 0) {
				System.out.println("Completed " + key + " out of " + K);
			}
		}
		// duration = System.nanoTime() - start;
		// System.out.println("init set " + (duration / 1000000000.0) + " sec");

		keys_to_update_mod = Math.round(1 / kuP);

		Outputs total = new Outputs();

		String extraValue = "";
		if (Q) {
			extraValue = "q,";
		} else if (W) {
			extraValue = "W,QPS,wantedNodes,";
		}

		System.out.println("Round,Action,cid," + extraValue
				+ "N,addedNodes,removedNodes,Gets,Sets,Valid,Lost,Lost(stale),miss,migrated,not migrated");
		int loop_size = Q ? numOfRounds : op.size();

		int addM_round = 0, logAdded = 0;
		int removeM_round = 0, logRemoved = 0;
		int valid_keys = 0, lost_keys = 0, missed_keys = 0;
		int gets = 0, sets = 0;
		double oldW = 0;

		int numHours = 0;
		out.println("config,N,Added,Removed,W,valid_keys,lost_keys,missed_keys,gets,sets");

		for (int loop = 0; isDone(loop, loop_size); loop++) {
			String actionName = "";
			if (Q) {
				q = q + qBase;
				if (q >= 10 || q <= 1) {
					qBase *= -1;
				}
			} else if (W) {
				Stats stats = wtObject.get();
				w = stats.getQpsFactor();
				qps = stats.getQps();
			}
			ReconfigurationType action = getAction(loop, sys.getN());

			if ((trace == Trace.WC98Daily && numHours == 24)
					|| (trace != Trace.WC98Daily && action != ReconfigurationType.NOTHING)) {
				out.println(String.format("%d,%d,%d,%d,%f,%d,%d,%d", con.config, sys.getN(), logAdded, logRemoved, oldW,
						valid_keys, lost_keys, missed_keys, gets, sets));
				out.flush();
				lost_keys = 0;
				missed_keys = 0;
				gets = 0;
				sets = 0;
				oldW = w;
				logAdded = 0;
				logRemoved = 0;
				numHours = 0;
			} else {
				numHours++;
			}
			switch (action) {// rand.nextInt(2)
			case ADD_NODE:
				addM_round = 1;
				removeM_round = 0;
				logAdded = 1;
				logRemoved = 0;
				// start = System.nanoTime();
				sys.addNode();
				// duration = System.nanoTime() - start;
				// System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "addNode";
				break;
			case REMOVE_NODE:
				addM_round = 0;
				removeM_round = 1;
				logAdded = 0;
				logRemoved = 1;
				// start = System.nanoTime();
				sys.removeNode();
				// duration = System.nanoTime() - start;
				// System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "removeNode";
				break;
			case ADD_M_NODES:
				addM_round = addM;
				removeM_round = 0;
				logAdded = addM;
				logRemoved = 0;
				// start = System.nanoTime();
				// long start = System.nanoTime();
				sys.addMultipleNodes(addM);
				// long duration = System.nanoTime() - start;
				// System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "addM-" + addM;
				// System.out.println(sys.cachesPartitionsInfo());
				// sys.debugPartitions();
				break;
			case REMOVE_M_NODES:
				addM_round = 0;
				removeM_round = removeM;
				logAdded = 0;
				logRemoved = removeM;
				// long start = System.nanoTime();
				sys.removeMultipleNode(removeM);
				// long duration = System.nanoTime() - start;
				// System.out.println("RemoveNode " + (duration / 1000000000.0) + " sec");
				actionName = "removeM-" + removeM;
				// System.out.println(sys.cachesPartitionsInfo());
				// sys.debugPartitions();
				break;
			case NOTHING:
				addM_round = 0;
				removeM_round = 0;
				actionName = "noAction";
				break;
			}
			con = sys.getConfiguration();
			System.out.print(loop + "," + actionName + "," + con.config + ",");
			if (Q) {
				if (q == 10) {
					System.out.print("1,");
				} else {
					System.out.print("0." + q + ",");
				}
			} else if (W) {
				System.out.print(w + "," + qps + "," + ((int) Math.round(((double) qps) / MainClass.C)) + ",");
			}
			System.out.print(sys.getN() + "," + addM_round + "," + removeM_round + ",");

			Outputs result = null;
			if (trace == Trace.WC98Daily) {
				result = perform_Operations_Provided(sys, con.P, con.config, action == ReconfigurationType.NOTHING);
			} else {
				result = perform_Operations(sys, con.P, con.config, action == ReconfigurationType.NOTHING);
			}
			valid_keys += result.valid_keys;
			lost_keys += result.lost_keys;
			missed_keys += result.miss_keys;
			gets += result.gets;
			sets += result.sets;

			total.add(result);
		}

		out.close();
		return total;
	}

	private Outputs perform_Operations_Provided(TheSystem sys, Partition[] P, int config, boolean isNoAction) {
		Outputs result = new Outputs();
		int debugKey = -1;

		// get list
		Stats stats = wtObject.get();
		Optional<List<WCRequest>> listObject = stats.getNextBatchRequests(BATCH_SIZE);
		// while is present
		while (listObject.isPresent()) {
			List<WCRequest> list = listObject.get();
			// loop list
			for (WCRequest keyObject : list) {
				int key = keyObject.getKey();
				boolean op = keyObject.isRead(); // true -> get, false -> set
				if (op) {
					// get
					boolean valid = true;
					ValueWrapper getOutput = sys.get(key);
					result.gets++;
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
						sys.set(key, getValue(key, config), config, true);
						valid = false;
					} else if (is_lost(key, getOutput.config_num, P)) {
						valid = false;
						result.lost_keys++;
						if (key == debugKey) {
							System.out.print(" ... is lost");
						}
					}
					if (key == debugKey) {
						System.out.println();
					}
					if (valid) {
						result.valid_keys++;
					}
				} else {
					// set
					sys.set(key, getValue(key, config), config, true);
					result.sets++;
				}
			}
			// get list
			listObject = stats.getNextBatchRequests(BATCH_SIZE);
		}

		// loop P and terminate migration
		if (!TheSystem.noMigration) {
			for (int i = 0; i < P.length; i++) {
				P[i].terminateMigration();
			}
		}
		// print line
		System.out.println(String.format("%d,%d,%d,%d,%d,%d,%d,%d", result.gets, result.sets, result.valid_keys,
				result.lost_keys, result.lost_key_stales, result.miss_keys, result.mig, result.mig_invalid));

		return result;
	}

	private boolean isDone(int loop, int loop_size) {
		if (W) {
			if (trace == Trace.AzureVM) {
				Optional<VMEvent> a = azure.getNextEvent();
			} else if (trace == Trace.GoogleVM) {
				google.getNextEvent();
			} else {
				if (!firstTime) {
					wtObject = wt.getNextStats();
				} else {
					firstTime = false;
				}
				return wtObject.isPresent();
			}
		}
		if (loop < loop_size)
			return true;
		return false;
	}

	private ReconfigurationType getAction(int round, int numOfNodes) {
		if (Q) {
			if (q >= 8) {
				return ReconfigurationType.ADD_NODE;
			} else if (q <= 3) {
				if (numOfNodes > 1) {
					return ReconfigurationType.REMOVE_NODE;
				} else {
					return ReconfigurationType.NOTHING;
				}
			} else {
				return ReconfigurationType.NOTHING;
			}
		} else if (W) {
			addM = 0;
			removeM = 0;
			boolean a = true;
			if (a) {
				int delta = (int) Math.round((((double) qps) / MainClass.C) - numOfNodes);
				if (delta > 1) {
					addM = delta;
					return ReconfigurationType.ADD_M_NODES;
				} else if (delta == 1) {
					return ReconfigurationType.ADD_NODE;
				} else if (delta == -1) {
					if (numOfNodes == 1) {
						return ReconfigurationType.NOTHING;
					}
					return ReconfigurationType.REMOVE_NODE;
				} else if (delta < -1) {
					if (numOfNodes == 1) {
						return ReconfigurationType.NOTHING;
					}
					removeM = Math.min(delta * -1, numOfNodes - 1);

					return ReconfigurationType.REMOVE_M_NODES;
				} else {
					return ReconfigurationType.NOTHING;
				}
			} else {
				if (w > 1) {
					// addM = (int) Math.round(Math.max(((w / 1) * numOfNodes) - numOfNodes, 1));
					addM = (int) Math.round(Math.max((((double) qps) / MainClass.C) - numOfNodes, 1));
					return ReconfigurationType.ADD_M_NODES;
				} else if (w < 0.1) {
					removeM = (int) Math.round(Math.max((0.1 - w) * numOfNodes, 1));
					removeM = Math.min(removeM, numOfNodes - 1);
					if (removeM == 0) {
						return ReconfigurationType.NOTHING;
					}
					return ReconfigurationType.REMOVE_M_NODES;
				} else if (w >= 0.8) {
					return ReconfigurationType.ADD_NODE;
				} else if (w <= 0.300000000) {
					if (numOfNodes > 1) {
						return ReconfigurationType.REMOVE_NODE;
					} else {
						return ReconfigurationType.NOTHING;
					}
				} else {
					return ReconfigurationType.NOTHING;
				}
			}
		} else {
			return op.get(round);
		}
	}

	private Outputs perform_Operations(TheSystem sys, Partition[] P, int cid, boolean isNoAction) {
		Outputs result = new Outputs();
		int debugKey = -1; // TODO for debugging
		boolean valid = true;

		for (int key = 0; key < K; key++) {
			valid = true;
			ValueWrapper getOutput = sys.get(key);
			result.gets++;
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
				if (is_stale(key, isNoAction ? cid : (cid - 1), getOutput.value, 1)) {
					result.lost_key_stales++;
					if (key == debugKey) {
						System.out.print(" ... is stale");
					}
				}
			} else if (is_stale(key, isNoAction ? cid : (cid - 1), getOutput.value, 2)) {

				valid = false;
				// Exit debug
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
			if ((key + cid) % keys_to_update_mod == 0) {
				sys.set(key, getValue(key, cid), cid, true);
				result.sets++;
			}
		}

		for (int key = 0; key < K; key++) {
			if ((key + cid) % keys_to_update_mod != 0) {
				continue;
			}
			ValueWrapper getOutput = sys.get(key);
			if (is_stale(key, cid, getOutput.value, 3)) {
				// Exit debug
				System.out.println("ERROR: stale value for a key we just set");
				System.exit(0);
			}
			if (is_lost(key, getOutput.config_num, P)) {
				// Exit debug
				System.out.println("ERROR: lost key for a key we just set");
				System.exit(0);
			}
		}

		// System.out.println(String.format("Valid: %d, Lost: %d (Stale: %d), null: %d",
		// result.valid_keys, result.lost_keys, result.lost_key_stales,
		// result.miss_keys));
		System.out.println(String.format("%d,%d,%d,%d,%d,%d,%d,%d", result.gets, result.sets, result.valid_keys,
				result.lost_keys, result.lost_key_stales, result.miss_keys, result.mig, result.mig_invalid));

		return result;
	}

	// private boolean hasKeys(int key, Iterator<Integer> it, boolean hashMapDone) {
	// if (trace != Trace.WC98Daily) {
	// return key < K;
	// } else {
	// if (it == null) {
	// return false;
	// }
	// if (hashMapDone)
	// return it.hasNext();
	// return true;
	// }
	// }

	private void setWithLatestValue(TheSystem sys, int key, int cid) {
		for (int temp_cid = cid; temp_cid > 0; temp_cid--) {
			if ((key + temp_cid) % keys_to_update_mod == 0) {
				sys.set(key, getValue(key, temp_cid), cid, true);
				return;
			}
		}
		sys.set(key, getValue(key, initialConfig), cid, true);
	}

	private boolean is_stale(int key, int cid, String value, int iiii) {
		for (int temp_cid = cid; temp_cid > 0; temp_cid--) {
			if ((key + temp_cid) % keys_to_update_mod == 0)
				return !(value.equals(getValue(key, temp_cid)));
		}
		if (value.equals(getValue(key, initialConfig)))
			return false;
		System.out.println("ERROR in is_stale " + iiii + ": could not find source of set.");
		System.out.println("key: " + key + ", cid: " + cid + ", value: " + value);
		for (int i = cid; i >= 0; i--) {
			System.out.println(String.format("(%d + %d) mod %d != %d, value (%s) == (%s)", key, i, keys_to_update_mod,
					((key + i) % keys_to_update_mod), value, getValue(key, i)));
		}
		System.exit(0);
		return true;
	}

	private String getValue(int key, int cid) {
		// StringBuilder br = new StringBuilder();
		// br.append(key);
		// br.append("_");
		// br.append(cid);
		// return br.toString();
		return String.valueOf(cid);
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
		if (Nn < 1) {
			System.out.println("ERROR in input: N is less than 1. N = " + Nn);
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

	// private GetOutput get(int key) {
	// return cache.get(key);
	// }
	//
	// private void set(int key, String value, int cid) {
	// GetOutput v = new GetOutput(value, cid);
	// cache.put(key, v);
	// }

	// private ConfigOutput get_Configurations() {
	// ConfigOutput conf = new ConfigOutput();
	// conf.cid = initialConfig;
	// conf.P = new Partition[N * L];
	// for (int i = 0; i < conf.P.length; i++) {
	// conf.P[i] = new Partition();
	// conf.P[i].config_num = conf.cid;
	// }
	// return conf;
	// }

}
