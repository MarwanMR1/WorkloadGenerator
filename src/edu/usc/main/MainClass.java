package edu.usc.main;

import java.util.ArrayList;

import edu.usc.system.TheSystem;
import edu.usc.workload_generator.Outputs;
import edu.usc.workload_generator.WorkloadGenerator;

public class MainClass {

	static int N = 100;
	static int L = 100;
	static float kuP = 0.05f;
	static int numOfPartitionsInL = 100;
	static int numOfKeysInPartition = 100;
	static int workload = -1;
	private static final int MAX_WORKLOAD = 5;

	public static void main(String[] args) {
		TheSystem.MAX_GET_PER_MIGRATION = 50;
		handleArguments(args);
		System.out.println(String.format("N = %d, L = %d, kuP = %f, P_in_L = %d, Key_in_P = %d, max get per mig = %d",
				N, L, kuP, numOfPartitionsInL, numOfKeysInPartition, TheSystem.MAX_GET_PER_MIGRATION));
		System.out.println("Workload " + workload);
		int numOfPartitions = N * L * numOfPartitionsInL;
		int K = numOfPartitions * numOfKeysInPartition;
		int number_Of_Iterations = 30;

		ArrayList<WorkloadGenerator.ReconfigurationType> op = new ArrayList<>();

		int iterations = 0, add = 0, remove = 0, addM = 0, removeM = 0;
		WorkloadGenerator.addM = -1;
		WorkloadGenerator.removeM = -1;
		WorkloadGenerator.Q = false;
		switch (workload) {
		case 1:
			iterations = 1;
			add = 30;
			break;
		case 2:
			iterations = 1;
			remove = 30;
			break;
		case 3:
			iterations = 6;
			add = 10;
			remove = 5;
			break;
		case 4:
			iterations = 6;
			addM = 1;
			removeM = 1;
			WorkloadGenerator.addM = 10;
			WorkloadGenerator.removeM = 5;
			break;
		case 5:
			WorkloadGenerator.Q = true;
			WorkloadGenerator.numOfRounds = 100;
			break;
		default:
			System.out.println("ERROR: incorrect workload. Exit");
			System.exit(0);
		}

		for (int i = 0; i < iterations; i++) {
			for (int j = 0; j < add; j++) {
				op.add(WorkloadGenerator.ReconfigurationType.ADD_NODE);
			}
			for (int j = 0; j < remove; j++) {
				op.add(WorkloadGenerator.ReconfigurationType.REMOVE_NODE);
			}
			for (int j = 0; j < addM; j++) {
				op.add(WorkloadGenerator.ReconfigurationType.ADD_M_NODES);
			}
			for (int j = 0; j < removeM; j++) {
				op.add(WorkloadGenerator.ReconfigurationType.REMOVE_M_NODES);
			}
		}

		WorkloadGenerator w = new WorkloadGenerator(N, L, K, kuP, numOfPartitions, number_Of_Iterations, op);
		Outputs o = w.run();
		o.print();

	}

	private static void handleArguments(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "-N":
				handleArguments_valueExist(i, args.length, "Missing number of nodes. The value for -N.");
				i++;
				N = handleArguments_parseInt(args[i]);
				break;
			case "-workload":
				handleArguments_valueExist(i, args.length, "Missing workload id [1,"+MAX_WORKLOAD+"] after -workload.");
				i++;
				workload = handleArguments_parseInt(args[i]);
				break;
			case "-L":
				handleArguments_valueExist(i, args.length,
						"Missing number of logical nodes per node. The value for -L.");
				i++;
				L = handleArguments_parseInt(args[i]);
				break;
			case "-P":
				handleArguments_valueExist(i, args.length,
						"Missing number of partitions per logical node. The value for -P.");
				i++;
				numOfPartitionsInL = handleArguments_parseInt(args[i]);
				break;
			case "-K":
				handleArguments_valueExist(i, args.length, "Missing number of keys per partition. The value for -K.");
				i++;
				numOfKeysInPartition = handleArguments_parseInt(args[i]);
				TheSystem.MAX_GET_PER_MIGRATION = numOfKeysInPartition / 2;
				break;
			case "-Update":
				handleArguments_valueExist(i, args.length, "Missing keys to update percentage. The value for -Update.");
				i++;
				kuP = handleArguments_parseFloat(args[i]);
				break;
			case "-h":
			case "-help":
				//				System.out.println("Tuner v." + version);
				System.out.println("How to use?");
				System.out.println("MainClass [-N num][-L num][-P num][-K num][-Update float]");
				System.out.println();
				System.out.println("-N: number of nodes.");
				System.out.println("-L: number of logical nodes per node.");
				System.out.println("-P: number of partitions per logical node.");
				System.out.println("-K: number of keys per partition.");
				System.out.println("-Update: Keys to update percentage pre reconfiguration.");
				System.out.println("-h,-help: Prints help.");
				System.exit(0);
			}
		}
		if (N < 1) {
			System.err.println("ERROR: N < 1.");
			System.exit(0);
		} else if (L < 1) {
			System.err.println("ERROR: L < 1.");
			System.exit(0);
		} else if (numOfPartitionsInL < 1) {
			System.err.println("ERROR: P < 1.");
			System.exit(0);
		} else if (numOfKeysInPartition < 1) {
			System.err.println("ERROR: K < 1.");
			System.exit(0);
		} else if (kuP < 0 || kuP > 1) {
			System.err.println("ERROR: Update is incorrect. Must be 0 <= Update <= 1.");
			System.exit(0);
		} else if (workload < 1 || workload > MAX_WORKLOAD) {
			System.err.println("ERROR: workload can be 1, 2, ... to " + MAX_WORKLOAD);
			System.exit(0);
		}
	}

	private static void handleArguments_valueExist(int cur, int length, String errorMSG) {
		if (cur + 1 == length) {
			System.err.println("ERROR: " + errorMSG);
			System.exit(0);
		}
	}

	private static int handleArguments_parseInt(String value) {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			System.err.println("ERROR: Incorrect number \"" + value + "\"");
			System.exit(0);
		}
		return -1;
	}

	private static float handleArguments_parseFloat(String value) {
		try {
			return Float.parseFloat(value);
		} catch (NumberFormatException e) {
			System.err.println("ERROR: Incorrect number \"" + value + "\"");
			System.exit(0);
		}
		return -1;
	}

}