package edu.usc.main;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import edu.usc.system.TheSystem;
import edu.usc.vmevent.traces.AzureVMEventTrace;
import edu.usc.vmevent.traces.GoogleVMEventTrace;
import edu.usc.workload.traces.WikipediaTrace;
import edu.usc.workload.traces.WorldCup98AllTrace;
import edu.usc.workload.traces.WorldCup98Trace;
import edu.usc.workload_generator.Outputs;
import edu.usc.workload_generator.WorkloadGenerator;
import edu.usc.workload_generator.WorkloadGenerator.Trace;

public class MainClass {

	static int N = 100;
	static int L = 100;
	static float kuP = 0.05f;
	public static int numOfPartitionsInL = 100;
	public static int numOfKeysInPartition = 100;
	static int workload = -1;
	public static int C = 1000000;
	static TimeUnit timeUnit = TimeUnit.HOURS;
	private static String outputFileLocation;

	static String location = "/home/mr1/eclipse-workspace/WorkloadGenerator/resources/wc.out";

	private static final int MAX_WORKLOAD = 6;

	public static void main(String[] args) {
		TheSystem.MAX_GET_PER_MIGRATION = 50;
		handleArguments(args);
		System.out.println(
				String.format("N = %d, L = %d, kuP = %f, P_in_L = %d, Key_in_P = %d, max get per mig = %d, C = %d", N,
						L, kuP, numOfPartitionsInL, numOfKeysInPartition, TheSystem.MAX_GET_PER_MIGRATION, C));
		if (workload == 6) {
			System.out.println("Trace: " + WorkloadGenerator.trace.toString() + ", Location: " + location);
		}
		System.out.println("Workload " + workload);
		int numOfPartitions = N * L * numOfPartitionsInL;
		int K = numOfPartitions * numOfKeysInPartition;
		int number_Of_Iterations = 30;

		ArrayList<WorkloadGenerator.ReconfigurationType> op = new ArrayList<>();

		int iterations = 0, add = 0, remove = 0, addM = 0, removeM = 0;
		WorkloadGenerator.addM = -1;
		WorkloadGenerator.removeM = -1;
		WorkloadGenerator.Q = false;
		WorkloadGenerator.W = false;
		WorkloadGenerator.WORKLOAD = workload;
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
		case 6:
			WorkloadGenerator.W = true;
			switch (WorkloadGenerator.trace) {
			case AzureVM:
				WorkloadGenerator.azure = new AzureVMEventTrace(location);
				break;
			case GoogleVM:
				WorkloadGenerator.google = new GoogleVMEventTrace(location);
				break;
			case WC98All:
				WorkloadGenerator.wt = new WorldCup98AllTrace(C, location);
				break;
			case WC98Daily:
//				WorkloadGenerator.wt = new WorldCup98DailyTrace(C, location, timeUnit);
				WorkloadGenerator.wt = new WorldCup98Trace(C, location);
				break;
			case Wikipedia:
				try {
					WorkloadGenerator.wt = new WikipediaTrace(C, location);
				} catch (Exception e) {
					e.printStackTrace(System.out);
					System.exit(0);
				}
				break;
			default:
				break;

			}
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

		WorkloadGenerator w = new WorkloadGenerator(N, L, K, kuP, numOfPartitions, number_Of_Iterations, op, outputFileLocation);
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date)); 
		
		Outputs o = w.run();
		
		o.print();
		
		date = new Date();
		System.out.println(dateFormat.format(date)); 

	}

	private static void handleArguments(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "-N":
				handleArguments_valueExist(i, args.length, "Missing number of nodes. The value for -N.");
				i++;
				N = handleArguments_parseInt(args[i]);
				break;
			case "-C":
				handleArguments_valueExist(i, args.length, "Missing node capacity. The value for -C.");
				i++;
				C = handleArguments_parseInt(args[i]);
				break;
			case "-workload":
				handleArguments_valueExist(i, args.length,
						"Missing workload id [1," + MAX_WORKLOAD + "] after -workload.");
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
			case "-file":
				handleArguments_valueExist(i, args.length, "Missing file location after -file.");
				i++;
				location = args[i];
				break;
			case "-outputfile":
				handleArguments_valueExist(i, args.length, "Missing output file location after -outputfile.");
				i++;
				outputFileLocation = args[i];
				break;
			case "-trace":
				handleArguments_valueExist(i, args.length, "Missing trace [azure, google, wcAll, wcDaily, wiki] after -trace.");
				i++;
				WorkloadGenerator.trace = handleArguments_parseTrace(args[i]);
				break;
			case "-time":
				handleArguments_valueExist(i, args.length,
						"Missing time unit [day, hour, min, sec, milli, micro, nano] after -time.");
				i++;
				timeUnit = handleArguments_parseTimeUnit(args[i]);
				break;
			case "-h":
			case "-help":
				// System.out.println("Tuner v." + version);
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
		} else if (workload == 6 && WorkloadGenerator.trace == null) {
			System.err.println("ERROR: trace is missing. Use -trace [azure, google, wcAll, wcDaily, wiki]");
			System.exit(0);
		} else if (outputFileLocation == null) {
			System.err.println("ERROR: output file location is null. Please use -outputfile to set the output file location.");
			System.exit(0);
		}
	}

	private static TimeUnit handleArguments_parseTimeUnit(String str) {

		switch (str) {
		case "day":
			return TimeUnit.DAYS;
		case "hour":
			return TimeUnit.HOURS;
		case "min":
			return TimeUnit.MINUTES;
		case "sec":
			return TimeUnit.SECONDS;
		case "milli":
			return TimeUnit.MILLISECONDS;
		case "micro":
			return TimeUnit.MICROSECONDS;
		case "nano":
			return TimeUnit.NANOSECONDS;
		default:
			System.out.println("ERROR: incorrect time unit (" + str + ").");
			System.out.println("You can select from [day, hour, min, sec, milli, micro, nano]");
			System.exit(0);
		}
		return null;
	}

	private static Trace handleArguments_parseTrace(String str) {
		switch (str) {
		case "wiki":
			return Trace.Wikipedia;
		case "wcAll":
			return Trace.WC98All;
		case "wcDaily":
			return Trace.WC98Daily;
		case "google":
			return Trace.GoogleVM;
		case "azure":
			return Trace.AzureVM;
		default:
			System.out.println("ERROR: incorrect trace (" + str + ").");
			System.out.println("You can select from [azure, google, wcAll, wcDaily, wiki]");
			System.exit(0);
		}
		return null;
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
