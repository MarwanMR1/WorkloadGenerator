package edu.usc.iqsystem;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.usc.main.MainClass;
import edu.usc.iqsystem.IQConfiguration;
import edu.usc.iqsystem.IQPartition;
import edu.usc.iqsystem.IQSystem;
import edu.usc.vmevent.traces.AzureVMEventTrace;
import edu.usc.vmevent.traces.AzureVMEventTrace.VMEvent;
import edu.usc.vmevent.traces.GoogleVMEventTrace;
import edu.usc.workload.traces.WorkloadTrace;
import edu.usc.workload.traces.WorkloadTrace.Stats;
import edu.usc.workload.traces.WorkloadTrace.WCRequest;
import edu.usc.workload_generator.WorkloadGenerator.ReconfigurationType;
import edu.usc.workload_generator.WorkloadGenerator.Trace;
import edu.usc.workload_generator.Outputs;

public class IQWorkloadGenerator {

	private int Nn, L, K, qps, number_Of_Iterations, numOfPartitions, q, qBase;
	static int keys_to_update_mod;
	static final int initialConfig = 1;
	private float kuP;
	private double w;
	// private Random rand;
	private ArrayList<ReconfigurationType> op;
	public static int addM, removeM, numOfRounds;
	public static boolean isMiss;
	public static boolean isLost;
	public static boolean Q;
	public static boolean W;
	public static WorkloadTrace wt;
	public static int WORKLOAD;
	public static Trace trace;
	public static AzureVMEventTrace azure;
	public static GoogleVMEventTrace google;
	private String outputFileLocation;
	private Optional<Stats> wtObject;
	private Optional<VMEvent> vmObject;
	private Set<Integer> vmAdd;
	private Set<Integer> vmRemove;
	private boolean firstTime = true;
	private String p2str = "";
	public static int numOfThreads = 10;
	PrintWriter outRound = null;
	private ExecutorService exe;// = Executors.newFixedThreadPool(numOfThreads);
	private ArrayList<IQGetterWorker> getters = new ArrayList<>();
	private ArrayList<IQSetterWorker> setters = new ArrayList<>();


	// private HashMap<Integer, GetOutput> cache = new HashMap<>();

	public IQWorkloadGenerator(int N, int L, int K, float kuP, int numOfPartitions, int number_Of_Iterations,
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
		if(MainClass.isLab) {
			numOfThreads = 5;
		}
		exe = Executors.newFixedThreadPool(numOfThreads);
		System.out.println("numOfThreads: " + numOfThreads);

		PrintWriter out = null;
		try {
			out = new PrintWriter(outputFileLocation);
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		}

		
		try {
			outRound = new PrintWriter(outputFileLocation.replace(".txt",  "")+"_round.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		}

		if (W) {
			if (trace != Trace.GoogleVM && trace != Trace.AzureVM) {
				wtObject = wt.getNextStats();
				if (wtObject.isPresent()) {
					Stats stats = wtObject.get();
					w = stats.getQpsFactor();
					qps = stats.getQps();
					Nn = (int) Math.round(((double) qps) / MainClass.C);
					if (Nn == 0) {
						Nn = 1;
					}

					System.out.println("UPDATE init values");
					System.out.println("N: " + Nn);
				} else {
					System.out.println("ERROR: wtObject isEmpty in the first check");
					System.exit(0);
				}
			} else {
				if (trace == Trace.GoogleVM) {
					vmObject = google.getNextEvent();
				} else {
					vmObject = azure.getNextEvent();
				}
				if (vmObject.isPresent()) {
					VMEvent stats = vmObject.get();
					vmAdd = stats.getAddVMs();
					vmRemove = stats.getRemoveVMs();
					Nn = vmAdd.size() - vmRemove.size();
					if (Nn == 0) {
						Nn = 1;
					}

					System.out.println("UPDATE init values");
					System.out.println("N: " + Nn);
				} else {
					System.out.println("ERROR: vmObject isEmpty in the first check");
					System.exit(0);
				}
			}
		}
		// long start = System.nanoTime();
		IQSystem sys = new IQSystem(Nn, L, numOfPartitions, trace, vmAdd, vmRemove);
		// long duration = System.nanoTime() - start;

		IQConfiguration con = sys.getConfiguration();
		keys_to_update_mod = Math.round(1 / kuP);

		
		for (int i = 0; i < numOfThreads; i++) {
			IQGetterWorker g = new IQGetterWorker(i, sys, K, keys_to_update_mod, numOfThreads, numOfPartitions);
			getters.add(g);
			IQSetterWorker s = new IQSetterWorker(i, sys, K, numOfThreads);
			setters.add(s);
		}

		// start = System.nanoTime();
		run_init_sets(sys, con);
		// for (int key = 0; key < K; key++) {
		// sys.set(key, getValue(key, con.config), con.config, true);
		// if (key % (K / 10) == 0) {
		// System.out.println("Completed " + key + " out of " + K);
		// }
		// }
		// duration = System.nanoTime() - start;


		Outputs total = new Outputs();

		String extraValue = "";
		if (Q) {
			extraValue = "q,";
		} else if (W) {
			extraValue = "W,QPS,wantedNodes,";
		}

		outRound.println("Round,Action,cid," + extraValue
				+ "N,addedNodes,removedNodes,Gets,Sets,Valid,Lost,Lost(stale),miss,migrated,not migrated");
		outRound.flush();
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
				if (trace != Trace.GoogleVM && trace != Trace.AzureVM) {
					Stats stats = wtObject.get();
					w = stats.getQpsFactor();
					qps = stats.getQps();
				}
			}
			p2str = sys.P[2].toString();
			ReconfigurationType action = getAction(loop, sys.getN());

			if ((trace == Trace.WC98Daily && numHours == 24)
					|| (trace != Trace.WC98Daily && action != ReconfigurationType.NOTHING)) {
				out.println(String.format("%d,%d,%d,%d,%f,%d,%d,%d,%d,%d", con.config, sys.getN(), logAdded, logRemoved,
						oldW, valid_keys, lost_keys, missed_keys, gets, sets));
				out.flush();
				lost_keys = 0;
				missed_keys = 0;
				valid_keys = 0;
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
				logAdded += 1;
				logRemoved += 0;
				// start = System.nanoTime();
				sys.addNode();
				System.out.println(sys.cachesPartitionsInfo());
				// duration = System.nanoTime() - start;
				// System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "addNode";
				break;
			case REMOVE_NODE:
				addM_round = 0;
				removeM_round = 1;
				logAdded += 0;
				logRemoved += 1;
				// start = System.nanoTime();
				sys.removeNode();
				System.out.println(sys.cachesPartitionsInfo());
				// duration = System.nanoTime() - start;
				// System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "removeNode";
				break;
			case ADD_M_NODES:
				addM_round = addM;
				removeM_round = 0;
				logAdded += addM;
				logRemoved += 0;
				// start = System.nanoTime();
				// long start = System.nanoTime();
				sys.addMultipleNodes(addM, null, true);
				if (MainClass.isLab) {
					System.out.println(sys.cachesPartitionsInfo());
				}

				// long duration = System.nanoTime() - start;
				// System.out.println("addNode " + (duration / 1000000000.0) + " sec");
				actionName = "addM-" + addM;
				// System.out.println(sys.cachesPartitionsInfo());
				// sys.debugPartitions();
				break;
			case REMOVE_M_NODES:
				addM_round = 0;
				removeM_round = removeM;
				logAdded += 0;
				logRemoved += removeM;
				// long start = System.nanoTime();
				sys.removeMultipleNode(removeM, null, true);
				if (MainClass.isLab) {
					System.out.println(sys.cachesPartitionsInfo());
				}
				// long duration = System.nanoTime() - start;
				// System.out.println("RemoveNode " + (duration / 1000000000.0) + " sec");
				actionName = "removeM-" + removeM;
				// System.out.println(sys.cachesPartitionsInfo());
				// sys.debugPartitions();
				break;
			case ADD_REMOVE_VM:
				addM_round = vmAdd.size();
				removeM_round = vmRemove.size();
				logAdded += vmAdd.size();
				logRemoved += vmRemove.size();

				sys.addRemoveVM(vmAdd, vmRemove);

				actionName = "add-" + addM_round + "-remove-" + removeM_round;

				break;
			case NOTHING:
				addM_round = 0;
				removeM_round = 0;
				actionName = "noAction";
				break;
			}

			p2str += ", " + sys.P[2].toString();

			con = sys.getConfiguration();
			outRound.print(loop + "," + actionName + "," + con.config + ",");
			if (Q) {
				if (q == 10) {
					outRound.print("1,");
				} else {
					outRound.print("0." + q + ",");
				}
			} else if (W) {
				outRound.print(w + "," + qps + "," + ((int) Math.round(((double) qps) / MainClass.C)) + ",");
			}
			outRound.print(sys.getN() + "," + addM_round + "," + removeM_round + ",");

			outRound.flush();
			Outputs result = null;
			// if (trace == Trace.WC98Daily) {
			// result = perform_Operations_Provided(sys, con.P, con.config, action ==
			// ReconfigurationType.NOTHING);
			// } else {
			// result = perform_Operations(sys, con.P, con.config, action ==
			// ReconfigurationType.NOTHING);
			// }
			result = multithreaded_operations(sys, con, action == ReconfigurationType.NOTHING);

			valid_keys += result.valid_keys;
			lost_keys += result.lost_keys;
			missed_keys += result.miss_keys;
			gets += result.gets;
			sets += result.sets;

			total.add(result);
			// System.out.println(p2str);
		}

		exe.shutdown();
		outRound.close();
		out.close();
		return total;
	}

	private Outputs multithreaded_operations(IQSystem sys, IQConfiguration con, boolean isNoAction) {
		Outputs result = new Outputs();

		ArrayList<Future<Outputs>> fSET = new ArrayList<>();
		for (int i = 0; i < numOfThreads; i++) {
			getters.get(i).init(con, isNoAction);
			fSET.add(exe.submit(getters.get(i)));
		}
		for (Future<Outputs> f : fSET) {
			try {
				result.add(f.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
		}

		fSET.clear();
		for (int i = 0; i < numOfThreads; i++) {
			setters.get(i).init(con, keys_to_update_mod);
			fSET.add(exe.submit(setters.get(i))); 
		}
		for (Future<Outputs> f : fSET) {
			try {
				result.add(f.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
		}
		outRound.println(String.format("%d,%d,%d,%d,%d,%d,%d,%d", result.gets, result.sets, result.valid_keys,
				result.lost_keys, result.lost_key_stales, result.miss_keys, result.mig, result.mig_invalid));

		outRound.flush();
		return result;
	}

	private void run_init_sets(IQSystem sys, IQConfiguration con) {
		ArrayList<Future<Outputs>> fSET = new ArrayList<>();
		for (int i = 0; i < numOfThreads; i++) {
			setters.get(i).init(con, 1);
			fSET.add(exe.submit(setters.get(i)));
		}
		int count = 0;
		for (Future<Outputs> f : fSET) {
			try {
				f.get();
				System.out.println(count++ + " Has completed setting the keys");
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
		}
		System.out.println("Completed init set");
	}

	// private Outputs perform_Operations_Provided(IQSystem sys, IQPartition[] P,
	// int config, boolean isNoAction) {
	// Outputs result = new Outputs();
	// int debugKey = -1;
	//
	// // get list
	// Stats stats = wtObject.get();
	// Optional<List<WCRequest>> listObject =
	// stats.getNextBatchRequests(BATCH_SIZE);
	// // while is present
	// while (listObject.isPresent()) {
	// List<WCRequest> list = listObject.get();
	// // loop list
	// for (WCRequest keyObject : list) {
	// int key = keyObject.getKey();
	// boolean op = keyObject.isRead(); // true -> get, false -> set
	// if (op) {
	// // get
	// boolean valid = true;
	// String getOutput = sys.get(key);
	// result.gets++;
	// if (key == debugKey) {
	// System.out.print("key " + key + ", P" + P[getHash(key)] + ", value" +
	// getOutput);
	// }
	//
	// if (getOutput == null) {
	// valid = false;
	// if (key == debugKey) {
	// System.out.print(" ... is null");
	// }
	// if (isMiss) {
	// result.miss_keys++;
	// } else if (isLost) {
	// result.lost_keys++;
	// } else {
	// System.out.println("ERROR: null but not miss nor lost");
	// System.exit(0);
	// }
	// }
	// if (key == debugKey) {
	// System.out.println();
	// }
	// if (valid) {
	// result.valid_keys++;
	// }
	// } else {
	// // set
	// sys.set(key, getValue(key, config), config, true);
	// result.sets++;
	// }
	// }
	// // get list
	// listObject = stats.getNextBatchRequests(BATCH_SIZE);
	// }
	//
	// // loop P and terminate migration
	// if (!IQSystem.noMigration) {
	// for (int i = 0; i < P.length; i++) {
	// P[i].terminateMigration();
	// }
	// }
	// // print line
	// System.out.println(String.format("%d,%d,%d,%d,%d,%d,%d,%d", result.gets,
	// result.sets, result.valid_keys,
	// result.lost_keys, result.lost_key_stales, result.miss_keys, result.mig,
	// result.mig_invalid));
	//
	// return result;
	// }

	private boolean isDone(int loop, int loop_size) {
		if (W) {
			if (trace == Trace.AzureVM) {
				firstTime = false;
				if (!firstTime) {
					vmObject = azure.getNextEvent();
					if (vmObject.isPresent()) {
						VMEvent stats = vmObject.get();
						vmAdd = stats.getAddVMs();
						vmRemove = stats.getRemoveVMs();
					}
				} else {
					firstTime = false;
				}
				return vmObject.isPresent();
			} else if (trace == Trace.GoogleVM) {
				firstTime = false;
				if (!firstTime) {
					vmObject = google.getNextEvent();
					if (vmObject.isPresent()) {
						VMEvent stats = vmObject.get();
						vmAdd = stats.getAddVMs();
						vmRemove = stats.getRemoveVMs();
					}
				} else {
					firstTime = false;
				}
				return vmObject.isPresent();
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
			if (trace != Trace.GoogleVM && trace != Trace.AzureVM) {
				boolean a = true;
				if (a) {
					if (qps == 0) {
						return ReconfigurationType.NOTHING;
					}
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
				if (vmAdd.size() == 0 && vmRemove.size() == 0) {
					return ReconfigurationType.NOTHING;
				} else {
					return ReconfigurationType.ADD_REMOVE_VM;
				}
			}
		} else {
			return op.get(round);
		}
	}

	// private Outputs perform_Operations(IQSystem sys, IQPartition[] P, int cid,
	// boolean isNoAction) {
	// Outputs result = new Outputs();
	// int debugKey = -1; // TODO for debugging
	// boolean valid = true;
	//
	// for (int key = 0; key < K; key++) {
	// valid = true;
	// String getOutput = sys.get(key);
	// if (key == 2) {
	// p2str += ", GET " + getOutput;
	// }
	// result.gets++;
	// if (key == debugKey) {
	// System.out.print("key " + key + ", P" + P[getHash(key)] + ", value" +
	// getOutput);
	// }
	// if (getOutput == null) {
	// if (key == debugKey) {
	// System.out.print(" ... is null");
	// }
	// if (isMiss) {
	// valid = false;
	// result.miss_keys++;
	// }
	// if (isLost) {
	// valid = false;
	// result.lost_keys++;
	// }
	// }
	// if (key == debugKey) {
	// System.out.println();
	// }
	// if (valid) {
	// result.valid_keys++;
	// }
	// }
	//
	// for (int key = 0; key < K; key++) {
	// if ((key + cid) % keys_to_update_mod == 0) {
	// if (key == 2) {
	// p2str += ", SET " + cid;
	// }
	// sys.set(key, getValue(key, cid), cid, true);
	// result.sets++;
	// }
	// }
	//
	// // int setloop = K/keys_to_update_mod;
	// // for (int i = 0; i < setloop; i++) {
	// // int key = i* keys_to_update_mod - cid;
	// // if (key <= -1) {
	// // setloop++;
	// // continue;
	// // }
	// // if(key >= K) {
	// // break;
	// // }
	// // if ((key + cid) % keys_to_update_mod == 0) {
	// // sys.set(key, getValue(key, cid), cid, true);
	// // result.sets++;
	// // } else {
	// // System.out.println("ERROR in set formla");
	// // System.exit(0);
	// // }
	// // }
	//
	// for (int key = 0; key < K; key++) {
	// if ((key + cid) % keys_to_update_mod != 0) {
	// continue;
	// }
	// String getOutput = sys.get(key);
	//
	// if (getOutput == null) {
	// if (isMiss) {
	// System.out.println("ERROR: missing key that we just set");
	// System.exit(0);
	// }
	// if (isLost) {
	// System.out.println("ERROR: lost key that we just set");
	// System.exit(0);
	// }
	//
	// System.out.println("ERROR: null key that we just set");
	// System.exit(0);
	// }
	// }
	//
	// // System.out.println(String.format("Valid: %d, Lost: %d (Stale: %d), null:
	// %d",
	// // result.valid_keys, result.lost_keys, result.lost_key_stales,
	// // result.miss_keys));
	// System.out.println(String.format("%d,%d,%d,%d,%d,%d,%d,%d", result.gets,
	// result.sets, result.valid_keys,
	// result.lost_keys, result.lost_key_stales, result.miss_keys, result.mig,
	// result.mig_invalid));
	//
	// return result;
	// }

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

	static String getValue(int key, int cid) {
		// StringBuilder br = new StringBuilder();
		// br.append(key);
		// br.append("_");
		// br.append(cid);
		// return br.toString();
		return String.valueOf(cid);
	}

	private boolean is_lost(int key, int key_config, IQPartition[] P) {
		IQPartition partition = P[getHash(key)];
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
	// conf.P = new IQPartition[N * L];
	// for (int i = 0; i < conf.P.length; i++) {
	// conf.P[i] = new IQPartition();
	// conf.P[i].config_num = conf.cid;
	// }
	// return conf;
	// }

}
