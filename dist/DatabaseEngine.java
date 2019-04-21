import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;


public class DatabaseEngine {
	
	static final int bufferSize = 500000;
	static int tempNumber = 0;

	//static int finalNumber = 0;

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
		
		// long start = System.currentTimeMillis();
		// Loader loads all the data into storage
		loader.readAllCSVFiles(CSVFiles);
		// long stop = System.currentTimeMillis();
		// System.out.println("Loading Time : " + (stop - start));
		// System.out.println();
		// System.exit(0);
		
		// PARSER
		Parser parser = new Parser();

		// Optimizer
		Optimizer optimizer = new Optimizer();
		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();

		///////////////////////////////////////////////////////////////////

		// Representation of the first query from the xxs dataset
		// long start = System.currentTimeMillis();
		// Queue<Queue<RAOperation>> tablesQueue = new LinkedList<>();
		// Queue<Queue<Predicate>> predicatesQueue = new LinkedList<>();


		// Queue<RAOperation> tableQueue = new LinkedList<>();
		// RAOperation scanA = new Scan("A.dat");
		// RAOperation scanC = new Scan("C.dat");
		// RAOperation scanB = new Scan("B.dat");
		
		// RAOperation scanD = new Scan("D.dat");
		// RAOperation scanE = new Scan("E.dat");
		// RAOperation scanF = new Scan("F.dat");

		// RAOperation projD = new Project(scanD, new int[]{0, 1, 2});
		// RAOperation projA = new Project(scanA, new int[]{3, 43});
		// RAOperation projF = new Project(scanF, new int[]{0, 2, 4});
		// RAOperation projE = new Project(scanE, new int[]{1, 4});
		// RAOperation projC = new Project(scanC, new int[]{1, 2});

		// tableQueue.add(projC);
		// tableQueue.add(projD);
		// tableQueue.add(projA);
		// tableQueue.add(projF);
		// // tableQueue.add(projE);
		// // tableQueue.add(projB);

		// Queue<Predicate> predicateQueue = new LinkedList<>();
		
		// // // Predicate ce = new MergeJoinPredicate(1, 0);

		// ArrayList<int[]> CList = new ArrayList<>();
		// int[] cPred = {0, 2, -1};
		// CList.add(cPred);
		// Predicate sigmaC = new FilterPredicate(CList);

		

		// Predicate cd = new MergeJoinPredicate(1, 2);
		// Predicate a = new MergeJoinPredicate(2, 0);
		// Predicate f = new MergeJoinPredicate(3, 0);
		// // Predicate b = new MergeJoinPredicate(3, 0);

		// // ArrayList<int[]> EList = new ArrayList<>();
		// // int[] ePred = {1, 0, -8144};
		// // EList.add(ePred);
		// // Predicate sigmaE = new FilterPredicate(EList);


		// // // Predicate e = new EquijoinPredicate(36, 1, true);
		// // // Predicate d = new EquijoinPredicate(56, 1, true);
		// // // Predicate a = new EquijoinPredicate(10, 2, true);
		
		// predicateQueue.add(sigmaC);
		// predicateQueue.add(cd);		
		// predicateQueue.add(a);
		// predicateQueue.add(f);


		// // predicateQueue.add(e);
		// // predicateQueue.add(b);
		
		
		// // predicateQueue.add(d);
		// // predicateQueue.add(b);

		//  tablesQueue.add(tableQueue);
		//  predicatesQueue.add(predicateQueue);

		//  Queue<Predicate> finalPredicateQueue = new LinkedList<>();

		// // int[] colsToSum = {7, 2, 11};

		// int[] colsToSum = {0, 1};
		// executionEngine.executeQuery(tablesQueue, predicatesQueue, finalPredicateQueue, colsToSum);
		// // long stop = System.currentTimeMillis();
		// // System.out.println(stop - start);
		// System.exit(0);
		// ///////////////////////////////////////////////////////////////////

		//Scanner queryScanner = new Scanner(System.in);
		
		// scanner = new Scanner(new File("../../data/xs/queries.sql"));
		// scanner = new Scanner(new File("../../data/xs/queryTest.sql"));
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

			// long start = System.currentTimeMillis();
			// Execute Query
			executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum());
			// long stop = System.currentTimeMillis();
			// System.out.println("Execution Time: " + (stop - start));
			// totalExecutionTime += (stop - start);

			// Gets rid of all the data in the parser
			parser.empty();
		}
		// Last Query
		parser.readQuery(scanner);

		optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
		
		// long start = System.currentTimeMillis();
		// Execute Query
		executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum());
		// long stop = System.currentTimeMillis();
		// System.out.println("Execution Time: " + (stop - start));
		
		// long totalStop = System.currentTimeMillis();

		// System.out.println("Total Parse Time " + totalParseTime);
		// System.out.println("Total Optimizer Time " + totalOptimizerTime);
		// System.out.println("Total Execution Time " + totalExecutionTime);
		// System.out.println("Total Time: " + (totalStop - totalStart));
		
	}
}

	