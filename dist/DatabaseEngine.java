import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;


public class DatabaseEngine {
	
	static final int equijoinBufferSize = 500000;
	static final int mergejoinBufferSize = 500000;
	static int tempNumber = 0;

	static HashMap<String, String> sortedColumnsMap = new HashMap<>();

	// static int finalNumber = 0;

	public static void main(String[] args) throws IOException {
		// long totalStart = System.currentTimeMillis();
		
		// LOADER
		Loader loader = new Loader();
		
		Scanner scanner = new Scanner(System.in);
		
		// Get the CSV files
		String CSVFiles = loader.getCSVFiles(scanner);
		// String CSVFiles = "../../data/xxxs/B.csv,../../data/xxxs/C.csv,../../data/xxxs/A.csv,../../data/xxxs/D.csv,../../data/xxxs/E.csv";
		// String CSVFiles = "../../data/xxs/B.csv,../../data/xxs/C.csv,../../data/xxs/A.csv,../../data/xxs/D.csv,../../data/xxs/E.csv,../../data/xxs/F.csv";
		// String CSVFiles = "../../data/xs/B.csv,../../data/xs/C.csv,../../data/xs/A.csv,../../data/xs/D.csv,../../data/xs/E.csv,../../data/xs/F.csv";
		// String CSVFiles = "../../data/s/B.csv,../../data/s/C.csv,../../data/s/A.csv,../../data/s/D.csv,../../data/s/E.csv,../../data/s/F.csv";
		// m size dataset
		// String CSVFiles = "../../data/m/A.csv,../../data/m/B.csv,../../data/m/C.csv,../../data/m/D.csv,../../data/m/E.csv,../../data/m/F.csv,";
		// CSVFiles += "../../data/m/G.csv,../../data/m/H.csv,../../data/m/I.csv,../../data/m/J.csv,../../data/m/K.csv,../../data/m/L.csv,";
		// CSVFiles += "../../data/m/M.csv,../../data/m/N.csv,../../data/m/O.csv,../../data/m/P.csv";
		// l size dataset
		// String CSVFiles = "../../data/l/A.csv,../../data/l/B.csv,../../data/l/C.csv,../../data/l/D.csv,../../data/l/E.csv,../../data/l/F.csv,";
		// CSVFiles += "../../data/l/G.csv,../../data/l/H.csv,../../data/l/I.csv,../../data/l/J.csv,../../data/l/K.csv,../../data/l/L.csv,";
		// CSVFiles += "../../data/l/M.csv,../../data/l/N.csv,../../data/l/O.csv,../../data/l/P.csv,../../data/l/Q.csv";
		// l2 dataset
		// String CSVFiles = "../../data/l2/B.csv,../../data/l2/C.csv,../../data/l2/A.csv,../../data/l2/D.csv,../../data/l2/E.csv,../../data/l2/F.csv";
		
		// long start = System.currentTimeMillis();
		// Loader loads all the data into storage
		loader.readAllCSVFiles(CSVFiles);
		// long stop = System.currentTimeMillis();
		// System.err.println("Loading Time : " + (stop - start));
		// System.out.println();
		// System.err.println("Size of map is " + DatabaseEngine.sortedColumnsMap.size());
		

		// PARSER
		Parser parser = new Parser();

		// Optimizer
		Optimizer optimizer = new Optimizer();
		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();

		///////////////////////////////////////////////////////////////////

		// TableMetaData tmd = new TableMetaData("");
		// tmd.setRows(10000000);
		// tmd.setColumns(50);
		// // tmd.setMin(minArray);
		// // tmd.setMax(maxArray);
		// // tmd.setUnique(uniqueArray);
		// Catalog.addData("A.dat", tmd);

		// TableMetaData tmd2 = new TableMetaData("");
		// tmd2.setRows(10000000);
		// tmd2.setColumns(50);
		// // tmd2.setMin(minArray);
		// // tmd2.setMax(maxArray);
		// // tmd2.setUnique(uniqueArray);
		// Catalog.addData("Q.dat", tmd2);

		// TableMetaData tmd3 = new TableMetaData("");
		// tmd3.setRows(1670);
		// tmd3.setColumns(15);
		// // tmd3.setMin(minArray);
		// // tmd3.setMax(maxArray);
		// // tmd3.setUnique(uniqueArray);
		// Catalog.addData("C.dat", tmd3);

		// TableMetaData tmd4 = new TableMetaData("");
		// tmd4.setRows(1083);
		// tmd4.setColumns(6);
		// // tmd4.setMin(minArray);
		// // tmd4.setMax(maxArray);
		// // tmd4.setUnique(uniqueArray);
		// Catalog.addData("B.dat", tmd4);

		// Representation of the first query from the xxs dataset
		// Queue<Queue<RAOperation>> tablesQueue = new LinkedList<>();
		// Queue<Queue<Predicate>> predicatesQueue = new LinkedList<>();
		// Queue<Queue<Boolean>> switchesQueue = new LinkedList<>();


		// Queue<RAOperation> tableQueue = new LinkedList<>();
		// RAOperation scanA = new Scan("A.dat");
		// RAOperation scanB = new Scan("B.dat");
		// RAOperation scanC = new Scan("C.dat");
		// RAOperation scanD = new Scan("D.dat");
		// RAOperation scanE = new Scan("E.dat");
		// RAOperation scanQ = new Scan("Q.dat");

		
		
		
		// RAOperation projA = new Project(scanA, new int[]{0, 1, 5, 12, 32});
		// RAOperation projC = new Project(scanC, new int[]{0, 4});
		// RAOperation projB = new Project(scanB, new int[]{0, 4});
		// RAOperation projQ = new Project(scanQ, new int[]{0, 5, 14, 15, 19});

		
		// tableQueue.add(projA);
		// tableQueue.add(projC);
		// tableQueue.add(projB);
		// tableQueue.add(projQ);
		// tableQueue.add(new Scan("fake table"));
		
		// // tableQueue.add(projD);
		// // tableQueue.add(projE);
		// // tableQueue.add(projF);
		// // tableQueue.add(projE);
		// // tableQueue.add(projB);

		// Queue<Predicate> predicateQueue = new LinkedList<>();
		
		// // // Predicate ce = new MergeJoinPredicate(1, 0);

		// ArrayList<int[]> AList = new ArrayList<>();
		// int[] aPred = {3, 1, -3000};
		// // int[] aPred2 = {2, 1, 3487};
		// AList.add(aPred);
		// // AList.add(aPred2);
		// Predicate sigmaA = new FilterPredicate(AList);

		// Predicate acmg = new MergeJoinPredicate(1, 0);
		// // Predicate ac = new EquijoinPredicate(1, 0, true);
		// Predicate ac = new EquijoinPredicate(1, 0, true, "equijoinWritePredicate");
		// Predicate ca = new EquijoinPredicate(0, 1, true, "equijoinWritePredicate");
		// //Predicate ca = new EquijoinPredicate(0, 1, true);
		// Predicate b1 = new EquijoinPredicate(0, 0, true);
		// Predicate b2 = new EquijoinPredicate(0, 2, true, "equijoinWritePredicate");
		// Predicate b3 = new EquijoinPredicate(0, 0, true);
		// Predicate q1 = new EquijoinPredicate(1, 6, true);
		// Predicate q2 = new EquijoinPredicate(6, 1, true);
		// Predicate q3 = new MergeJoinPredicate(6, 1);
		// Predicate disjoint = new EquijoinPredicate(0, 9, false);

		
		// Predicate d = new EquijoinPredicate(0, 1, true);
		// Predicate e = new EquijoinPredicate(0, 7, true);
		// Predicate f = new EquijoinPredicate(0, 1, true);
		


		// // Predicate cd = new MergeJoinPredicate(1, 2);
		// // Predicate a = new MergeJoinPredicate(2, 0);
		// // Predicate f = new MergeJoinPredicate(3, 0);
		// // Predicate b = new MergeJoinPredicate(3, 0);

		// ArrayList<int[]> CList = new ArrayList<>();
		// int[] cPred = {1, 0, 0};
		// CList.add(cPred);
		// Predicate sigmaC = new FilterPredicate(CList);

		// ArrayList<int[]> BList = new ArrayList<>();
		// int[] bPred = {1, 1, 0};
		// BList.add(bPred);
		// Predicate sigmaB = new FilterPredicate(BList);

		// ArrayList<int[]> QList = new ArrayList<>();
		// int[] qPred = {2, 2, 5000};
		// QList.add(qPred);
		// int[] qPred2 = {3, 1, 10};
		// QList.add(qPred2);
		// Predicate sigmaQ = new FilterPredicate(QList);


		// // // Predicate e = new EquijoinPredicate(36, 1, true);
		// // // Predicate d = new EquijoinPredicate(56, 1, true);
		// // // Predicate a = new EquijoinPredicate(10, 2, true);
		
		// predicateQueue.add(sigmaA);
		// predicateQueue.add(sigmaC);
		// predicateQueue.add(ca);
		// predicateQueue.add(sigmaB);
		// predicateQueue.add(b2);
		// predicateQueue.add(sigmaQ);
		// predicateQueue.add(q2);
		// predicateQueue.add(disjoint);


		// // predicateQueue.add(e);
		// // predicateQueue.add(b);
		
		
		// // predicateQueue.add(d);
		// // predicateQueue.add(b);

		// Queue<Boolean> switchQueue = new LinkedList<>();
		// switchQueue.add(true);
		// switchQueue.add(true);
		// switchQueue.add(false);
		// switchQueue.add(false);

		//  switchesQueue.add(switchQueue);
		//  tablesQueue.add(tableQueue);
		//  predicatesQueue.add(predicateQueue);

		//  Queue<Predicate> finalPredicateQueue = new LinkedList<>();

		// // int[] colsToSum = {7, 2, 11};

		// int[] colsToSum = {8, 13};
		// long start = System.currentTimeMillis();
		
		// executionEngine.executeQuery(tablesQueue, predicatesQueue, finalPredicateQueue, colsToSum, switchesQueue);
		// long stop = System.currentTimeMillis();
		// System.out.println("time taken: " + (stop - start));
		// System.exit(0);
		// ///////////////////////////////////////////////////////////////////

		//Scanner queryScanner = new Scanner(System.in);
		
		// scanner = new Scanner(new File("../../data/xxxs/queries.sql"));
		// scanner = new Scanner(new File("../../data/l2/queryTest.sql"));
		// Gets the number of queries
		int numOfQueries = parser.getNumOfQueries(scanner);

		// long totalParseTime = 0;
		// long totalOptimizerTime = 0;
		// long totalExecutionTime = 0;
		// Read each query
		for (int i = 0; i < numOfQueries - 1; i++) {
			// start = System.currentTimeMillis();
			parser.readQuery(scanner);
			// stop = System.currentTimeMillis();
			// System.out.println("Parse Time: " + (stop - start));
			// totalParseTime += (stop - start);

			scanner.nextLine(); // Blank Line
			
			// start = System.currentTimeMillis();
			optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
			// stop = System.currentTimeMillis();
			// System.out.println("Optimizer Time: " + (stop - start));
			// totalOptimizerTime += (stop - start);

			// Gets rid of all the data in the parser
			parser.empty();

			// start = System.currentTimeMillis();
			// Execute Query
			executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum(), optimizer.getswitchesQueue());
			// stop = System.currentTimeMillis();
			// System.err.println("Execution Time: " + (stop - start));
			// totalExecutionTime += (stop - start);

			
		}
		// Last Query
		parser.readQuery(scanner);

		optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
		
		// start = System.currentTimeMillis();
		// Execute Query
		executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum(), optimizer.getswitchesQueue());
		// stop = System.currentTimeMillis();
		// System.err.println("Execution Time: " + (stop - start));
		// totalExecutionTime += (stop - start);
		
		// long totalStop = System.currentTimeMillis();

		// System.out.println("Total Parse Time " + totalParseTime);
		// System.out.println("Total Optimizer Time " + totalOptimizerTime);
		// System.out.println("Total Execution Time " + totalExecutionTime);
		// System.out.println("Total Time: " + (totalStop - totalStart));
		
	}
}

	